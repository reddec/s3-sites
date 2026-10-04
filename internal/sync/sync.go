// Package sync mirrors the site roots stored in an S3-compatible bucket into
// a local directory and points Caddy at the result.
package sync

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/reddec/s3-sites/internal/caddy"
	"github.com/reddec/s3-sites/internal/events"
	"github.com/reddec/s3-sites/internal/storage"
)

const (
	Cooldown time.Duration = 5 * time.Second // minimal time since last event (per root path)
	MaxDelay time.Duration = time.Minute     // maximum time since first event (per root path)
)

// Permissions of the created output tree: Caddy only needs to read it, and a
// second user (Caddy runs as another account) has to reach every level.
const (
	dirMode  fs.FileMode = 0o755
	fileMode fs.FileMode = 0o644
)

// tempPrefix marks the file a download lands in before it is renamed into
// place; anything left under it is safe to remove.
const tempPrefix = ".sync-"

// retryInterval is the pause between two attempts to publish a Caddy
// configuration that Caddy refused.
const retryInterval = time.Second

// errStreamClosed reports that the event stream ended for good while the
// context was still live: without events nothing triggers incremental syncs.
var errStreamClosed = errors.New("event stream closed")

type Config struct {
	Storage *storage.Storage
	Caddy   *caddy.Caddy
	Output  string // output directory
	Events  <-chan events.Event
	Resync  time.Duration // manual re-sync interval
}

// Sync starts syncing procedure in blocking way.
// it maintains internal, in-memory index of fetched items.
// It assumes that root "folders" in storage are domain names (foo.example.com/index.html).
// It stores files in `<output>/<domain>/<file>`.
// Storage is source of truth - if file is missing in source, then it removed in output.
// If sync failed (eg: storage error), last known state is used.
// Once sync done, it should check served domains and update Caddy if needed (lazy).
// Resync interval is used to forcefully sync domains in order to recap missing or broken syncs.
// Events debounced as [Cooldown] after last event, but no more than [MaxDelay] from the first event.
//
// The first pass runs immediately, so output and Caddy are in place before the
// first event is handled. Temporary download files left by an interrupted run
// are removed on every full pass. A rejected Caddy update is published again
// every [retryInterval], forever, until Caddy accepts it. A nil
// Config.Events disables event-driven syncing, a nil Config.Caddy skips config
// updates. Sync returns nil once ctx is canceled and an error if the event
// stream closes while ctx is still live.
func Sync(ctx context.Context, config Config) error {
	config.Resync = cmp.Or(config.Resync, time.Minute)

	if err := os.MkdirAll(config.Output, dirMode); err != nil {
		return fmt.Errorf("create output directory: %w", err)
	}

	s := &syncer{
		storage: config.Storage,
		caddy:   config.Caddy,
		output:  config.Output,
		events:  config.Events,
		resync:  config.Resync,
		index:   make(map[string]map[string]string),
		pending: make(map[string]pending),
	}
	return s.loop(ctx)
}

// syncer owns the whole state; the single loop goroutine is its only reader
// and writer, so no field needs synchronization.
type syncer struct {
	storage *storage.Storage
	caddy   *caddy.Caddy
	output  string
	events  <-chan events.Event
	resync  time.Duration

	index   map[string]map[string]string // domain -> object key -> ETag of the synced copy
	pending map[string]pending           // dirty domains waiting out their debounce window
}

// pending is the debounce state of one dirty domain.
type pending struct {
	first time.Time
	last  time.Time
}

// deadline returns when the domain may be synced: [Cooldown] after its last
// event, but never later than [MaxDelay] after its first one.
func (p pending) deadline() time.Time {
	cooldown := p.last.Add(Cooldown)
	maxDelay := p.first.Add(MaxDelay)
	if maxDelay.Before(cooldown) {
		return maxDelay
	}
	return cooldown
}

// loop syncs everything once and then reacts to events, debounce checks, and
// resync ticks until ctx is canceled or the event stream closes.
func (s *syncer) loop(ctx context.Context) error {
	s.reconcileAll(ctx)

	// Dirty domains are checked on a fixed tick, so one syncs within a tick of
	// its deadline instead of exactly on it.
	debounce := time.NewTicker(checkInterval())
	defer debounce.Stop()
	resync := time.NewTicker(s.resync)
	defer resync.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case event, ok := <-s.events:
			if !ok {
				if ctx.Err() != nil {
					return nil
				}
				return errStreamClosed
			}
			s.track(event)
		case <-debounce.C:
			// Only a domain entering or leaving the served set changes the
			// Caddyfile; content churn does not.
			if s.reconcileDue(ctx) {
				s.updateCaddy(ctx)
			}
		case <-resync.C:
			s.reconcileAll(ctx)
		}
	}
}

// reconcileAll syncs every root folder storage lists and drops domains that
// disappeared from it, then removes temporary files an interrupted run left
// behind and publishes the resulting sites to Caddy. A failed domain keeps its
// previous state.
func (s *syncer) reconcileAll(ctx context.Context) {
	defer s.cleanTemp(ctx)

	roots, err := s.storage.List(ctx)
	if err != nil {
		if ctx.Err() == nil {
			slog.Error("sync sites failed", "error", err)
		}
		return
	}

	seen := make(map[string]struct{}, len(roots))
	for _, domain := range roots {
		if !validDomain(domain) {
			continue
		}
		seen[domain] = struct{}{}
		delete(s.pending, domain)
		if _, err := s.reconcileDomain(ctx, domain); err != nil {
			if ctx.Err() != nil {
				return
			}
			slog.Error("sync site failed", "domain", domain, "error", err)
		}
	}
	for domain := range s.index {
		if _, ok := seen[domain]; ok {
			continue
		}
		delete(s.pending, domain)
		if err := s.dropDomain(domain); err != nil {
			if ctx.Err() != nil {
				return
			}
			slog.Error("drop site failed", "domain", domain, "error", err)
		}
	}

	// Publish on every full pass: startup clears whatever config ran before
	// and a resync restores one Caddy lost. Caddy ignores an identical config.
	s.updateCaddy(ctx)
}

// reconcileDue syncs every dirty domain whose debounce deadline passed and
// reports whether the set of served domains changed.
func (s *syncer) reconcileDue(ctx context.Context) bool {
	dirty := false
	now := time.Now()
	for domain, p := range s.pending {
		if p.deadline().After(now) {
			continue
		}
		delete(s.pending, domain)
		changed, err := s.reconcileDomain(ctx, domain)
		if err != nil {
			if ctx.Err() != nil {
				return dirty
			}
			slog.Error("sync site failed", "domain", domain, "error", err)
			continue
		}
		dirty = dirty || changed
	}
	return dirty
}

// track marks the domain an event belongs to as dirty and extends its debounce
// window; keys outside a root folder are ignored.
func (s *syncer) track(event events.Event) {
	domain := rootPath(event.Key)
	if domain == "" {
		return
	}
	now := time.Now()
	p, ok := s.pending[domain]
	if !ok {
		p = pending{first: now}
	}
	p.last = now
	s.pending[domain] = p
}

// reconcileDomain brings one domain directory in line with the objects stored
// under it and reports whether the domain entered or left the set of served
// domains. Downloads finish before stale files are removed, so a failure
// leaves the previous state in place and the next attempt can retry.
func (s *syncer) reconcileDomain(ctx context.Context, domain string) (bool, error) {
	entries, err := s.storage.Index(ctx, domain)
	if err != nil {
		return false, err
	}
	objects := make(map[string]string, len(entries))
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Key, domain+"/") || !fs.ValidPath(entry.Key) {
			continue
		}
		objects[entry.Key] = entry.ETag
	}

	for key, etag := range objects {
		if etag != "" && s.index[domain][key] == etag {
			continue
		}
		if err := s.fetch(ctx, key); err != nil {
			return false, err
		}
	}

	dir := filepath.Join(s.output, domain)
	if err := s.removeStale(dir, domain, objects); err != nil {
		return false, err
	}
	_, served := s.index[domain]
	if len(objects) == 0 {
		if err := os.RemoveAll(dir); err != nil {
			return false, fmt.Errorf("remove %s: %w", domain, err)
		}
		delete(s.index, domain)
		return served, nil
	}
	s.index[domain] = objects
	return !served, nil
}

// fetch downloads one object and installs it under output. The body replaces
// the previous version only after it was copied in full, so a broken download
// never serves a truncated file.
func (s *syncer) fetch(ctx context.Context, key string) error {
	path := filepath.Join(s.output, filepath.FromSlash(key))
	body, err := s.storage.Get(ctx, key)
	if err != nil {
		return err
	}
	defer body.Close()

	if err := os.MkdirAll(filepath.Dir(path), dirMode); err != nil {
		return fmt.Errorf("create %s: %w", filepath.Dir(path), err)
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), tempPrefix+"*")
	if err != nil {
		return fmt.Errorf("create temporary file: %w", err)
	}
	defer os.Remove(tmp.Name())
	defer tmp.Close()

	if _, err := io.Copy(tmp, body); err != nil {
		return fmt.Errorf("write %s: %w", path, err)
	}
	if err := tmp.Chmod(fileMode); err != nil {
		return fmt.Errorf("chmod %s: %w", path, err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close %s: %w", path, err)
	}
	if err := os.Rename(tmp.Name(), path); err != nil {
		return fmt.Errorf("install %s: %w", path, err)
	}
	return nil
}

// removeStale deletes every file under dir that storage no longer lists.
func (s *syncer) removeStale(dir, domain string, objects map[string]string) error {
	var stale []string
	err := filepath.WalkDir(dir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(dir, path)
		if err != nil {
			return err
		}
		if _, ok := objects[domain+"/"+filepath.ToSlash(rel)]; !ok {
			stale = append(stale, path)
		}
		return nil
	})
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("scan %s: %w", dir, err)
	}
	for _, path := range stale {
		if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("remove %s: %w", path, err)
		}
	}
	return nil
}

// cleanTemp removes temporary download files that an interrupted run left
// behind.
func (s *syncer) cleanTemp(ctx context.Context) {
	var stale []string
	err := filepath.WalkDir(s.output, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || !strings.HasPrefix(entry.Name(), tempPrefix) {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		stale = append(stale, path)
		return nil
	})
	if err != nil && ctx.Err() == nil {
		slog.Warn("scan temporary files failed", "error", err)
	}
	for _, path := range stale {
		if ctx.Err() != nil {
			return
		}
		if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
			slog.Warn("remove temporary file failed", "path", path, "error", err)
		}
	}
}

// dropDomain removes the local directory of a domain that storage no longer
// holds.
func (s *syncer) dropDomain(domain string) error {
	if err := os.RemoveAll(filepath.Join(s.output, domain)); err != nil {
		return fmt.Errorf("remove %s: %w", domain, err)
	}
	delete(s.index, domain)
	return nil
}

// updateCaddy uploads a Caddyfile for the domains with local content and keeps
// publishing it every [retryInterval] until Caddy accepts it, so a Caddy that
// starts or recovers later gets its configuration without a restart of the
// syncer. It returns once the config is accepted or ctx is canceled.
func (s *syncer) updateCaddy(ctx context.Context) {
	if s.caddy == nil {
		return
	}
	file := caddy.Caddyfile{Sites: s.sites()}
	for {
		err := s.caddy.Upload(ctx, file)
		if err == nil {
			return
		}
		if ctx.Err() != nil {
			return
		}
		slog.Error("update caddy failed, retrying", "error", err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(retryInterval):
		}
	}
}

// sites describes the domains with synced content, in a stable order. Every
// site falls back to its root index.html for paths the mirror does not hold,
// so an unknown route answers the site instead of 404.
func (s *syncer) sites() []caddy.Site {
	domains := make([]string, 0, len(s.index))
	for domain, objects := range s.index {
		if len(objects) > 0 {
			domains = append(domains, domain)
		}
	}
	slices.Sort(domains)

	sites := make([]caddy.Site, 0, len(domains))
	for _, domain := range domains {
		sites = append(sites, caddy.Site{
			Domain:   domain,
			Root:     filepath.Join(s.output, domain),
			TryFiles: []string{"{path}", "/index.html"},
		})
	}
	return sites
}

// checkInterval returns how often dirty domains are checked: the greatest
// common divisor of the two debounce windows, so both boundaries land on a
// check.
func checkInterval() time.Duration {
	a, b := Cooldown, MaxDelay
	for b != 0 {
		a, b = b, a%b
	}
	return a
}

// rootPath returns the domain an object key belongs to: its first path
// segment. Objects outside a root folder have no domain.
func rootPath(key string) string {
	domain, _, found := strings.Cut(key, "/")
	if !found || !validDomain(domain) {
		return ""
	}
	return domain
}

// validDomain reports whether a name can serve as a root folder and site
// address. Only hostname characters are accepted, so no name can escape the
// output directory or break out of its site block in the rendered Caddyfile.
func validDomain(domain string) bool {
	if domain == "" || domain == "." || domain == ".." || len(domain) > 253 {
		return false
	}
	for _, r := range domain {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '.', r == '_', r == '*':
		default:
			return false
		}
	}
	return true
}
