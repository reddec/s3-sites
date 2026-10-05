package caddy_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/tls"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/log"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/reddec/s3-sites/internal/caddy"
)

const (
	fooIndex = "<h1>foo index</h1>"
	barIndex = "<h1>bar index</h1>"

	// bootstrapCaddyfile exposes the admin API on all interfaces so /load is
	// reachable from the test host; the uploaded config replaces it entirely.
	bootstrapCaddyfile = "{\n\tadmin 0.0.0.0:2019\n}\n"
)

// largePage is bigger than the 512-byte minimum length of Caddy's encoder, so
// a compressed response to it is observable.
var largePage = strings.Repeat("<p>compressible content</p>", 32)

func TestUploadServesSitesOnRunningCaddy(t *testing.T) {
	adminURL, tlsPort := startCaddy(t)

	file := caddy.Caddyfile{Sites: []caddy.Site{
		{
			Domain:   "foo.localhost",
			Root:     "/srv/sites/foo.localhost",
			TryFiles: []string{"{path}", "/index.html"},
		},
		{
			Domain: "bar.localhost",
			Root:   "/srv/sites/bar.localhost",
		},
	}}
	require.NoError(t, caddy.New(adminURL).Upload(t.Context(), file))

	fooClient := siteClient(t, tlsPort, "foo.localhost")
	waitForSite(t, fooClient, "https://foo.localhost/")

	status, body := get(t, fooClient, "https://foo.localhost/")
	assert.Equal(t, http.StatusOK, status)
	assert.Equal(t, fooIndex, body)

	// try_files was rendered and applied: an unknown path falls back to index.html.
	status, body = get(t, fooClient, "https://foo.localhost/missing")
	assert.Equal(t, http.StatusOK, status)
	assert.Equal(t, fooIndex, body)

	barClient := siteClient(t, tlsPort, "bar.localhost")
	waitForSite(t, barClient, "https://bar.localhost/")
	status, body = get(t, barClient, "https://bar.localhost/")
	assert.Equal(t, http.StatusOK, status)
	assert.Equal(t, barIndex, body)

	// The site without try_files answers 404 for the same path.
	status, _ = get(t, barClient, "https://bar.localhost/missing")
	assert.Equal(t, http.StatusNotFound, status)
}

// TestUploadAppliesSnippetBase checks that a snippet is rendered ahead of the
// generated site blocks and reaches the running config: it defines a whole
// extra site, imported at the top level, plus a global options block that keeps
// the admin listener reachable for the next upload.
func TestUploadAppliesSnippetBase(t *testing.T) {
	adminURL, tlsPort := startCaddy(t)

	file := caddy.Caddyfile{
		Snippet: "{\n\tadmin 0.0.0.0:2019\n}\n\n" +
			"(extra) {\n\textra.localhost {\n\t\trespond \"from snippet\"\n\t}\n}\n\nimport extra\n",
		Sites: []caddy.Site{{
			Domain: "foo.localhost",
			Root:   "/srv/sites/foo.localhost",
		}},
	}
	require.NoError(t, caddy.New(adminURL).Upload(t.Context(), file))

	foo := siteClient(t, tlsPort, "foo.localhost")
	waitForSite(t, foo, "https://foo.localhost/")
	status, body := get(t, foo, "https://foo.localhost/")
	assert.Equal(t, http.StatusOK, status)
	assert.Equal(t, fooIndex, body)

	extra := siteClient(t, tlsPort, "extra.localhost")
	waitForSite(t, extra, "https://extra.localhost/")
	status, body = get(t, extra, "https://extra.localhost/")
	assert.Equal(t, http.StatusOK, status)
	assert.Equal(t, "from snippet", body)

	// The snippet's global options kept the admin listener on all interfaces,
	// so uploading the same configuration again still reaches it. A file with
	// only site blocks resets it to localhost:2019 and fails here.
	require.NoError(t, caddy.New(adminURL).Upload(t.Context(), file))
}

// TestUploadCompressesResponses checks that compression is per site: a block
// with Compress serves gzip-encoded responses to a client that asks for gzip,
// while a block without it answers the same request as it is.
func TestUploadCompressesResponses(t *testing.T) {
	adminURL, tlsPort := startCaddy(t)

	file := caddy.Caddyfile{Sites: []caddy.Site{
		{
			Domain:   "foo.localhost",
			Root:     "/srv/sites/foo.localhost",
			TryFiles: []string{"{path}", "/index.html"},
			Compress: true,
		},
		{
			Domain: "bar.localhost",
			Root:   "/srv/sites/bar.localhost",
		},
	}}
	require.NoError(t, caddy.New(adminURL).Upload(t.Context(), file))

	foo := compressionClient(t, tlsPort, "foo.localhost")
	waitForSite(t, foo, "https://foo.localhost/")

	status, encoding, body := getEncoded(t, foo, "https://foo.localhost/large.html")
	assert.Equal(t, http.StatusOK, status)
	assert.Equal(t, "gzip", encoding)

	zr, err := gzip.NewReader(bytes.NewReader(body))
	require.NoError(t, err)
	plain, err := io.ReadAll(zr)
	require.NoError(t, err)
	assert.Equal(t, largePage, string(plain))

	bar := compressionClient(t, tlsPort, "bar.localhost")
	waitForSite(t, bar, "https://bar.localhost/")

	status, encoding, body = getEncoded(t, bar, "https://bar.localhost/")
	assert.Equal(t, http.StatusOK, status)
	assert.Empty(t, encoding, "a site without Compress must not be encoded")
	assert.Equal(t, barIndex, string(body))
}

func TestUploadSurfacesAdapterRejection(t *testing.T) {
	adminURL, _ := startCaddy(t)

	file := caddy.Caddyfile{Sites: []caddy.Site{{
		Domain: "foo.localhost",
		Root:   "/srv/sites\nbogus_directive",
	}}}
	err := caddy.New(adminURL).Upload(t.Context(), file)

	require.Error(t, err)
	assert.ErrorContains(t, err, "400 Bad Request")
	assert.ErrorContains(t, err, "unrecognized directive: bogus_directive")
}

func TestUploadHonorsCanceledContext(t *testing.T) {
	adminURL, _ := startCaddy(t)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	err := caddy.New(adminURL).Upload(ctx, caddy.Caddyfile{})

	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
}

// startCaddy runs the official Caddy image with the admin API reachable from
// the host and fixture files under /srv/sites. It returns the admin base URL
// and the host port mapped to the container's TLS port.
func startCaddy(t *testing.T) (adminURL, tlsPort string) {
	t.Helper()
	if testing.Short() {
		t.Skip("needs Docker")
	}
	files := []testcontainers.ContainerFile{
		{
			ContainerFilePath: "/etc/caddy/Caddyfile",
			FileMode:          0o644,
			Reader:            strings.NewReader(bootstrapCaddyfile),
		},
		{
			ContainerFilePath: "/srv/sites/foo.localhost/index.html",
			FileMode:          0o644,
			Reader:            strings.NewReader(fooIndex),
		},
		{
			ContainerFilePath: "/srv/sites/foo.localhost/large.html",
			FileMode:          0o644,
			Reader:            strings.NewReader(largePage),
		},
		{
			ContainerFilePath: "/srv/sites/bar.localhost/index.html",
			FileMode:          0o644,
			Reader:            strings.NewReader(barIndex),
		},
	}

	ctr, err := testcontainers.Run(t.Context(), "caddy:2-alpine",
		testcontainers.WithLogger(log.TestLogger(t)),
		testcontainers.WithFiles(files...),
		testcontainers.WithExposedPorts("2019/tcp", "443/tcp"),
		testcontainers.WithWaitStrategy(wait.ForHTTP("/config/").WithPort("2019/tcp")),
	)
	testcontainers.CleanupContainer(t, ctr)
	require.NoError(t, err)

	adminURL, err = ctr.PortEndpoint(t.Context(), "2019/tcp", "http")
	require.NoError(t, err)
	tlsMapped, err := ctr.MappedPort(t.Context(), "443/tcp")
	require.NoError(t, err)
	return adminURL, tlsMapped.Port()
}

// waitForSite blocks until url answers 200; Caddy issues the internal
// certificate while applying the config.
func waitForSite(t *testing.T, client *http.Client, url string) {
	t.Helper()
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, nil)
		if !assert.NoError(c, err) {
			return
		}
		resp, err := client.Do(req)
		if !assert.NoError(c, err) {
			return
		}
		defer resp.Body.Close()
		assert.Equal(c, http.StatusOK, resp.StatusCode)
	}, 15*time.Second, 250*time.Millisecond, "caddy never served %s", url)
}

// siteClient returns a client that dials the container's TLS port while
// presenting domain as SNI and the Host header, so Caddy matches the vhost.
func siteClient(t *testing.T, tlsPort, domain string) *http.Client {
	t.Helper()
	return &http.Client{Transport: &http.Transport{
		DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
			var dialer net.Dialer
			return dialer.DialContext(ctx, network, net.JoinHostPort("127.0.0.1", tlsPort))
		},
		// The container serves Caddy's internal CA, which the test host does
		// not trust; domain still goes out as SNI to select the certificate.
		TLSClientConfig: &tls.Config{ServerName: domain, InsecureSkipVerify: true},
	}}
}

// get issues a GET and returns the status and body.
func get(t *testing.T, client *http.Client, url string) (int, string) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, nil)
	require.NoError(t, err)
	resp, err := client.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp.StatusCode, string(body)
}

// compressionClient returns a client that asks for gzip but leaves the response
// encoded, so tests can observe the Content-Encoding header.
func compressionClient(t *testing.T, tlsPort, domain string) *http.Client {
	t.Helper()
	client := siteClient(t, tlsPort, domain)
	client.Transport.(*http.Transport).DisableCompression = true
	return client
}

// getEncoded issues a GET that asks for gzip and returns the status, the
// Content-Encoding header, and the body as it arrived.
func getEncoded(t *testing.T, client *http.Client, url string) (int, string, []byte) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, nil)
	require.NoError(t, err)
	req.Header.Set("Accept-Encoding", "gzip")
	resp, err := client.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp.StatusCode, resp.Header.Get("Content-Encoding"), body
}
