// Command s3-sites mirrors the site roots of an S3-compatible bucket into a
// local directory and publishes the result to a running Caddy.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/alecthomas/kong"

	"github.com/reddec/s3-sites/internal/caddy"
	"github.com/reddec/s3-sites/internal/events"
	"github.com/reddec/s3-sites/internal/storage"
	"github.com/reddec/s3-sites/internal/sync"
)

//nolint:gochecknoglobals // package-level logger is convention
var logger = slog.Default().With("app", "s3-sites")

// Config is the command line interface of the syncer.
type Config struct {
	Output  string        `help:"Directory served by Caddy, one sub-directory per domain" default:"./sites"`
	Resync  time.Duration `help:"Interval of a full re-sync" default:"1m"`
	Storage struct {
		Endpoint        string `help:"S3 endpoint URL; empty uses the default endpoint of the region"`
		Region          string `help:"S3 region" default:"us-east-1"`
		Bucket          string `help:"Bucket holding one root folder per domain" required:""`
		PathStyle       bool   `help:"Address the bucket by path instead of by sub-domain" default:"true"`
		AccessKeyID     string `help:"Access key; empty sends unsigned requests"`
		SecretAccessKey string `help:"Secret key of the access key"`
	} `embed:"" prefix:"storage."`
	Caddy struct {
		Admin    string `help:"Caddy admin API URL; empty stops updating Caddy" default:"http://localhost:2019"`
		Snippet  string `help:"Path to a Caddyfile snippet rendered before the generated site blocks; read once at startup"`
		Compress bool   `help:"Compress responses with zstd, then gzip; disable with --caddy.compress=false" default:"true"`
	} `embed:"" prefix:"caddy."`
	Events struct {
		NATS      string `help:"NATS URL receiving the store notifications; empty syncs on the re-sync interval only"`
		Subject   string `help:"Subject the store publishes object notifications to" default:"sites.events"`
		Buffer    int    `help:"Events queued while the syncer is busy" default:"4096"`
		Reconnect bool   `help:"Keep listening across broker outages instead of ending the stream" default:"true"`
	} `embed:"" prefix:"events."`
}

func main() {
	var cfg Config
	kong.Parse(&cfg,
		kong.Name("s3-sites"),
		kong.Description("Mirror the site roots of an S3-compatible bucket into a directory served by Caddy."),
		kong.DefaultEnvars("S3SITES"),
	)
	if err := run(cfg); err != nil {
		panic(err)
	}
}

// run wires the configured dependencies and syncs until SIGINT or SIGTERM
// arrives, which cancels the sync loop and ends the process cleanly.
func run(cfg Config) error {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	var snippet string
	if cfg.Caddy.Snippet != "" {
		body, err := os.ReadFile(cfg.Caddy.Snippet)
		if err != nil {
			return fmt.Errorf("read caddy snippet: %w", err)
		}
		snippet = string(body)
	}

	store, err := storage.New(ctx, storage.Config{
		Endpoint:        cfg.Storage.Endpoint,
		Region:          cfg.Storage.Region,
		Bucket:          cfg.Storage.Bucket,
		PathStyle:       cfg.Storage.PathStyle,
		AccessKeyID:     cfg.Storage.AccessKeyID,
		SecretAccessKey: cfg.Storage.SecretAccessKey,
	})
	if err != nil {
		return fmt.Errorf("create storage: %w", err)
	}

	var stream <-chan events.Event
	if cfg.Events.NATS != "" {
		stream, err = events.Listen(ctx, events.Config{
			URL:       cfg.Events.NATS,
			Subject:   cfg.Events.Subject,
			Buffer:    cfg.Events.Buffer,
			Reconnect: cfg.Events.Reconnect,
		})
		if err != nil {
			return fmt.Errorf("listen for events: %w", err)
		}
	}

	var admin *caddy.Caddy
	if cfg.Caddy.Admin != "" {
		admin = new(caddy.New(cfg.Caddy.Admin))
	}

	logger.Info("syncing sites",
		"bucket", cfg.Storage.Bucket,
		"output", cfg.Output,
		"snippet", cfg.Caddy.Snippet,
		"compress", cfg.Caddy.Compress,
		"events", redacted(cfg.Events.NATS),
		"caddy", redacted(cfg.Caddy.Admin),
	)
	if err := sync.Sync(ctx, sync.Config{
		Storage:  store,
		Caddy:    admin,
		Output:   cfg.Output,
		Snippet:  snippet,
		Compress: cfg.Caddy.Compress,
		Events:   stream,
		Resync:   cfg.Resync,
	}); err != nil {
		return fmt.Errorf("sync sites: %w", err)
	}
	logger.Info("stopped", "cause", context.Cause(ctx))
	return nil
}

// redacted returns a URL with the password of its userinfo masked, so an
// address can be logged as it was configured.
func redacted(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return "" // an unparseable URL may carry credentials, echo nothing
	}
	return u.Redacted()
}
