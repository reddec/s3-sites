// Package caddy renders a Caddyfile from a list of sites and uploads it to a
// running Caddy instance through the admin API.
package caddy

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"text/template"
	"time"
)

const (
	loadPath       = "/load"
	caddyfileType  = "text/caddyfile"
	errorCap       = 512
	requestTimeout = 30 * time.Second
)

// errRejected reports that the admin API refused the uploaded configuration.
var errRejected = errors.New("caddy rejected config")

//go:embed caddyfile.tmpl
var templateSource string

// fileTemplate renders the whole Caddyfile. The embedded source is fixed at
// build time, so parsing it cannot fail.
//
//nolint:gochecknoglobals // the embedded template is parsed once at startup
var fileTemplate = template.Must(
	template.New("caddyfile").Funcs(template.FuncMap{"join": strings.Join}).Parse(templateSource),
)

// client posts configurations with a project-wide timeout; per-call deadlines
// come from the context passed to Upload.
//
//nolint:gochecknoglobals // a shared client with a timeout is convention
var client = &http.Client{Timeout: requestTimeout}

// Site is one virtual host: the domain it answers for, the directory it
// serves, and optional try_files targets in order.
type Site struct {
	Domain   string
	Root     string
	TryFiles []string
}

// Caddyfile is the complete configuration uploaded to Caddy.
type Caddyfile struct {
	Sites   []Site
	Snippet string // base content rendered before the site blocks
}

// Caddy talks to one Caddy admin API, for example http://localhost:2019.
type Caddy struct {
	url string
}

// New returns a client for the Caddy admin API at url.
func New(url string) Caddy {
	return Caddy{url: strings.TrimSuffix(url, "/")}
}

// Upload renders the file and posts it to /load as text/caddyfile, which
// replaces Caddy's running configuration atomically. The replacement includes
// the admin listener: the rendered Caddyfile carries no global options, so a
// Caddy reached over the network falls back to its default localhost:2019
// admin address and later uploads fail.
func (c Caddy) Upload(ctx context.Context, file Caddyfile) error {
	body, err := file.render()
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url+loadPath, strings.NewReader(body))
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", caddyfileType)
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("upload config: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%w: %s: %s", errRejected, resp.Status, excerpt(resp.Body))
	}
	return nil
}

// render executes the embedded template: a header comment, the optional
// snippet, then one block per site with its root, optional try_files, and
// file_server. The comment keeps a file without sites valid, which Caddy needs
// to accept an empty site set as a configuration that clears whatever ran
// before. Trimming the snippet keeps the rendered spacing independent of how
// the snippet file was written.
func (f Caddyfile) render() (string, error) {
	f.Snippet = strings.TrimSpace(f.Snippet)
	var b strings.Builder
	if err := fileTemplate.Execute(&b, f); err != nil {
		return "", fmt.Errorf("render caddyfile: %w", err)
	}
	return b.String(), nil
}

// excerpt reads a bounded prefix of an error response body.
func excerpt(body io.Reader) string {
	text, err := io.ReadAll(io.LimitReader(body, errorCap))
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(text))
}
