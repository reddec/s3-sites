package sync_test

import (
	"context"
	"crypto/tls"
	"io"
	"io/fs"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/network"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/log"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/reddec/s3-sites/internal/caddy"
	"github.com/reddec/s3-sites/internal/events"
	"github.com/reddec/s3-sites/internal/storage"
	"github.com/reddec/s3-sites/internal/sync"
)

const (
	versityImage = "ghcr.io/versity/versitygw:latest"
	caddyImage   = "caddy:2-alpine"
	bucket       = "sites"
	region       = "us-east-1"
	user         = "test"
	secret       = "testtest"

	// bootstrapCaddyfile exposes the admin API on all interfaces so the test
	// host can reach it. CADDY_ADMIN keeps it there after every upload, since
	// the syncer renders site blocks without global options.
	bootstrapCaddyfile = "{\n\tadmin 0.0.0.0:2019\n}\n"
)

func TestSyncDownloadsSitesOnStart(t *testing.T) {
	env := newEnvironment(t)
	env.put(t, "alpha.localhost/index.html", "alpha home")
	env.put(t, "alpha.localhost/assets/app.css", "body {}")
	env.put(t, "beta.localhost/index.html", "beta home")

	env.start(t, env.config())

	env.waitForFile(t, "alpha.localhost/index.html", "alpha home")
	env.waitForFile(t, "alpha.localhost/assets/app.css", "body {}")
	env.waitForFile(t, "beta.localhost/index.html", "beta home")

	alpha := siteClient(t, env.sitePort, "alpha.localhost")
	waitForSite(t, alpha, "https://alpha.localhost/")
	status, body := get(t, alpha, "https://alpha.localhost/assets/app.css")
	assert.Equal(t, http.StatusOK, status)
	assert.Equal(t, "body {}", body)

	beta := siteClient(t, env.sitePort, "beta.localhost")
	waitForSite(t, beta, "https://beta.localhost/")
	_, body = get(t, beta, "https://beta.localhost/")
	assert.Equal(t, "beta home", body)
}

// TestSyncServesRootIndexForUnknownPath checks the fallback every synced site
// gets: a path the mirror does not hold answers the site's index.html.
func TestSyncServesRootIndexForUnknownPath(t *testing.T) {
	env := newEnvironment(t)
	env.put(t, "alpha.localhost/index.html", "alpha home")
	env.put(t, "alpha.localhost/page.html", "alpha page")

	env.start(t, env.config())

	alpha := siteClient(t, env.sitePort, "alpha.localhost")
	waitForSite(t, alpha, "https://alpha.localhost/")

	status, body := get(t, alpha, "https://alpha.localhost/missing")
	assert.Equal(t, http.StatusOK, status)
	assert.Equal(t, "alpha home", body)

	// A file the mirror holds is still served as it is.
	status, body = get(t, alpha, "https://alpha.localhost/page.html")
	assert.Equal(t, http.StatusOK, status)
	assert.Equal(t, "alpha page", body)
}

// TestSyncRetriesRejectedCaddyUpdate checks that a configuration the admin
// API refuses is published again until it is accepted.
func TestSyncRetriesRejectedCaddyUpdate(t *testing.T) {
	env := newEnvironment(t)
	env.put(t, "alpha.localhost/index.html", "alpha home")

	var attempts atomic.Int64
	var lastBody atomic.Pointer[string]
	admin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		text := string(body)
		lastBody.Store(&text)
		if err != nil {
			http.Error(w, "read request", http.StatusInternalServerError)
			return
		}
		if attempts.Add(1) == 1 {
			// Only the first upload is refused; every retry is accepted.
			http.Error(w, "admin unavailable", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(admin.Close)

	cfg := env.config()
	cfg.Caddy = new(caddy.New(admin.URL))
	cfg.Resync = time.Hour // keep full passes out of the retry window
	cfg.Events = nil       // only the upload that keeps retrying may publish
	env.start(t, cfg)

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.GreaterOrEqual(c, attempts.Load(), int64(2))
	}, 30*time.Second, 50*time.Millisecond, "rejected caddy config was never published again")

	body := lastBody.Load()
	require.NotNil(t, body)
	assert.Contains(t, *body, "alpha.localhost {")
	assert.Contains(t, *body, "try_files {path} /index.html")

	// The accepted configuration ends the retrying.
	time.Sleep(250 * time.Millisecond)
	assert.Equal(t, int64(2), attempts.Load(), "config was published again after caddy accepted it")
}

func TestSyncAppliesEventsAfterCooldown(t *testing.T) {
	env := newEnvironment(t)
	env.put(t, "alpha.localhost/index.html", "alpha v1")
	env.put(t, "alpha.localhost/assets/old.css", "old")

	cfg := env.config()
	cfg.Resync = time.Hour // isolate events from the resync ticker
	env.start(t, cfg)
	env.waitForFile(t, "alpha.localhost/index.html", "alpha v1")

	// Two events two seconds apart must not sync the domain before the
	// cooldown since the last event elapses.
	env.put(t, "alpha.localhost/index.html", "alpha v2")
	env.event("alpha.localhost/index.html")
	time.Sleep(2 * time.Second)
	env.put(t, "alpha.localhost/index.html", "alpha v3")
	env.event("alpha.localhost/index.html")

	time.Sleep(2 * time.Second)
	content, err := env.file("alpha.localhost/index.html")
	require.NoError(t, err)
	assert.Equal(t, "alpha v1", content, "domain synced before the debounce window elapsed")

	// The sync that eventually runs sees the latest storage state.
	env.waitForFile(t, "alpha.localhost/index.html", "alpha v3")

	// Deletions and a new domain arrive with one more batch of events.
	env.remove(t, "alpha.localhost/assets/old.css")
	env.put(t, "gamma.localhost/index.html", "gamma home")
	env.event("alpha.localhost/assets/old.css")
	env.event("gamma.localhost/index.html")

	env.waitForGone(t, "alpha.localhost/assets/old.css")
	env.waitForFile(t, "gamma.localhost/index.html", "gamma home")
	env.waitForCaddy(t, "gamma.localhost", true)
}

func TestSyncResyncsWithoutEvents(t *testing.T) {
	env := newEnvironment(t)
	env.put(t, "alpha.localhost/index.html", "alpha v1")
	env.put(t, "alpha.localhost/assets/old.css", "old")

	cfg := env.config()
	cfg.Events = nil
	cfg.Resync = 500 * time.Millisecond
	env.start(t, cfg)
	env.waitForFile(t, "alpha.localhost/index.html", "alpha v1")

	// Changes that no event announced are still picked up by the resync.
	env.put(t, "alpha.localhost/index.html", "alpha v2")
	env.put(t, "alpha.localhost/new.html", "new")
	env.remove(t, "alpha.localhost/assets/old.css")
	env.put(t, "gamma.localhost/index.html", "gamma home")

	env.waitForFile(t, "alpha.localhost/index.html", "alpha v2")
	env.waitForFile(t, "alpha.localhost/new.html", "new")
	env.waitForGone(t, "alpha.localhost/assets/old.css")
	env.waitForFile(t, "gamma.localhost/index.html", "gamma home")
	env.waitForCaddy(t, "gamma.localhost", true)

	// A domain whose last object disappeared leaves output and Caddy.
	env.remove(t, "gamma.localhost/index.html")
	env.waitForDomainGone(t, "gamma.localhost")
	env.waitForCaddy(t, "gamma.localhost", false)
}

func TestSyncCleansUpLeftoverTempFiles(t *testing.T) {
	env := newEnvironment(t)
	env.put(t, "alpha.localhost/index.html", "alpha")

	// An interrupted run can leave partial downloads both under a domain that
	// still exists and under one storage no longer lists.
	for _, path := range []string{
		filepath.Join(env.output, "alpha.localhost", ".sync-1234"),
		filepath.Join(env.output, "gone.localhost", ".sync-5678"),
	} {
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
		require.NoError(t, os.WriteFile(path, []byte("partial"), 0o644))
	}

	env.start(t, env.config())

	env.waitForFile(t, "alpha.localhost/index.html", "alpha")
	env.waitForGone(t, "alpha.localhost/.sync-1234")
	env.waitForGone(t, "gone.localhost/.sync-5678")
}

func TestSyncKeepsLastStateWhileStorageIsDown(t *testing.T) {
	env := newEnvironment(t)
	env.put(t, "alpha.localhost/index.html", "alpha v1")

	cfg := env.config()
	cfg.Resync = 500 * time.Millisecond
	env.start(t, cfg)
	env.waitForFile(t, "alpha.localhost/index.html", "alpha v1")

	require.NoError(t, env.bucket.Stop(t.Context(), nil))
	env.event("alpha.localhost/index.html")
	time.Sleep(7 * time.Second) // past the cooldown and several resync ticks

	content, err := env.file("alpha.localhost/index.html")
	require.NoError(t, err)
	assert.Equal(t, "alpha v1", content, "local content was dropped while storage was down")
	env.waitForCaddy(t, "alpha.localhost", true)

	// The bucket comes back empty on the same endpoint; the next resync
	// recovers and serves the new content.
	require.NoError(t, env.bucket.Start(t.Context()))
	_, err = env.seed.CreateBucket(t.Context(), &s3.CreateBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err)
	env.put(t, "alpha.localhost/index.html", "alpha v2")

	env.waitForFile(t, "alpha.localhost/index.html", "alpha v2")
}

func TestSyncStopsWhenContextIsCanceled(t *testing.T) {
	env := newEnvironment(t)
	env.put(t, "alpha.localhost/index.html", "alpha")

	cfg := env.config()
	cfg.Resync = time.Hour
	p := env.start(t, cfg)
	env.waitForFile(t, "alpha.localhost/index.html", "alpha")

	p.cancel()

	require.NoError(t, p.await(t))
}

func TestSyncReportsClosedEventStream(t *testing.T) {
	env := newEnvironment(t)
	env.put(t, "alpha.localhost/index.html", "alpha")

	cfg := env.config()
	cfg.Resync = time.Hour
	p := env.start(t, cfg)
	env.waitForFile(t, "alpha.localhost/index.html", "alpha")

	close(env.events)

	err := p.await(t)
	require.Error(t, err)
	assert.ErrorContains(t, err, "event stream closed")
}

// environment is a bucket, a Caddy instance, and the output directory the
// syncer under test fills.
type environment struct {
	store    *storage.Storage
	caddy    *caddy.Caddy
	seed     *s3.Client
	events   chan events.Event
	output   string
	adminURL string
	sitePort string
	bucket   testcontainers.Container
}

// newEnvironment runs an empty bucket and a Caddy instance that serves the
// output directory through a bind mount at its own path.
func newEnvironment(t *testing.T) *environment {
	t.Helper()
	if testing.Short() {
		t.Skip("needs Docker")
	}
	output := t.TempDir()
	// Caddy runs in its own container; every level of the output tree has to
	// be reachable by another user.
	require.NoError(t, os.Chmod(filepath.Dir(output), 0o755))
	require.NoError(t, os.Chmod(output, 0o755))

	bucketCtr, endpoint := startBucket(t)
	adminURL, sitePort := startCaddy(t, output)

	store, err := storage.New(t.Context(), storage.Config{
		Endpoint:        endpoint,
		Region:          region,
		Bucket:          bucket,
		PathStyle:       true,
		AccessKeyID:     user,
		SecretAccessKey: secret,
	})
	require.NoError(t, err)

	return &environment{
		store:    store,
		caddy:    new(caddy.New(adminURL)),
		seed:     seedClient(t, endpoint),
		events:   make(chan events.Event, 16),
		output:   output,
		adminURL: adminURL,
		sitePort: sitePort,
		bucket:   bucketCtr,
	}
}

// config returns the wiring shared by the tests; each test adjusts Resync and
// Events as needed.
func (e *environment) config() sync.Config {
	return sync.Config{
		Storage: e.store,
		Caddy:   e.caddy,
		Output:  e.output,
		Events:  e.events,
	}
}

// put seeds an object in the bucket.
func (e *environment) put(t *testing.T, key, content string) {
	t.Helper()
	_, err := e.seed.PutObject(t.Context(), &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   strings.NewReader(content),
	})
	require.NoError(t, err)
}

// remove deletes an object from the bucket.
func (e *environment) remove(t *testing.T, key string) {
	t.Helper()
	_, err := e.seed.DeleteObject(t.Context(), &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	require.NoError(t, err)
}

// event publishes one change notification for key.
func (e *environment) event(key string) {
	e.events <- events.Event{Name: "s3:ObjectCreated:Put", Key: key}
}

// file reads a synced file, addressed by its object key.
func (e *environment) file(key string) (string, error) {
	content, err := os.ReadFile(filepath.Join(e.output, filepath.FromSlash(key)))
	return string(content), err
}

// waitForFile blocks until the synced file holds content.
func (e *environment) waitForFile(t *testing.T, key, content string) {
	t.Helper()
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		got, err := e.file(key)
		if !assert.NoError(c, err) {
			return
		}
		assert.Equal(c, content, got)
	}, 30*time.Second, 200*time.Millisecond, "file %s never held %q", key, content)
}

// waitForGone blocks until the synced file is removed.
func (e *environment) waitForGone(t *testing.T, key string) {
	t.Helper()
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		_, err := os.Stat(filepath.Join(e.output, filepath.FromSlash(key)))
		assert.ErrorIs(c, err, fs.ErrNotExist)
	}, 30*time.Second, 200*time.Millisecond, "file %s was not removed", key)
}

// waitForDomainGone blocks until the whole domain directory is removed.
func (e *environment) waitForDomainGone(t *testing.T, domain string) {
	t.Helper()
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		_, err := os.Stat(filepath.Join(e.output, domain))
		assert.ErrorIs(c, err, fs.ErrNotExist)
	}, 30*time.Second, 200*time.Millisecond, "domain %s was not removed", domain)
}

// waitForCaddy blocks until Caddy's running config lists domain or not.
func (e *environment) waitForCaddy(t *testing.T, domain string, served bool) {
	t.Helper()
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		config, err := e.caddyConfig(t.Context())
		if !assert.NoError(c, err) {
			return
		}
		assert.Equal(c, served, strings.Contains(config, `"`+domain+`"`))
	}, 30*time.Second, 200*time.Millisecond, "domain %s served=%t never became true", domain, served)
}

// caddyConfig reads Caddy's running configuration from its admin API.
func (e *environment) caddyConfig(ctx context.Context) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, e.adminURL+"/config/", nil)
	if err != nil {
		return "", err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	return string(body), nil
}

// process is one running Sync call.
type process struct {
	cancel context.CancelFunc
	done   chan error

	stopped bool
	err     error
}

// start runs Sync in the background; the test cleanup stops it.
func (e *environment) start(t *testing.T, cfg sync.Config) *process {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	p := &process{cancel: cancel, done: make(chan error, 1)}
	go func() {
		p.done <- sync.Sync(ctx, cfg)
	}()
	t.Cleanup(func() { p.stop(t) })
	return p
}

// stop cancels the run and returns the error Sync finished with. It may be
// called more than once.
func (p *process) stop(t *testing.T) error {
	t.Helper()
	if !p.stopped {
		p.cancel()
		p.await(t)
	}
	return p.err
}

// await blocks until Sync returns, without canceling it.
func (p *process) await(t *testing.T) error {
	t.Helper()
	if p.stopped {
		return p.err
	}
	select {
	case p.err = <-p.done:
		p.stopped = true
	case <-time.After(time.Minute):
		t.Error("sync did not stop")
	}
	return p.err
}

// startBucket runs VersityGW on a pinned host port, so the endpoint survives a
// container restart, and returns the container and its endpoint.
func startBucket(t *testing.T) (testcontainers.Container, string) {
	t.Helper()
	ctx := t.Context()
	ctr, err := testcontainers.Run(ctx, versityImage,
		testcontainers.WithLogger(log.TestLogger(t)),
		testcontainers.WithExposedPorts("9000/tcp"),
		testcontainers.WithCmd("-p", ":9000", "-a", user, "-s", secret, "posix", "/data"),
		testcontainers.WithTmpfs(map[string]string{"/data": ""}),
		testcontainers.WithHostConfigModifier(func(hostConfig *container.HostConfig) {
			hostConfig.PortBindings = network.PortMap{
				network.MustParsePort("9000/tcp"): []network.PortBinding{{HostPort: freePort(t)}},
			}
		}),
		testcontainers.WithWaitStrategy(wait.ForListeningPort("9000/tcp")),
	)
	testcontainers.CleanupContainer(t, ctr)
	require.NoError(t, err)

	endpoint, err := ctr.PortEndpoint(ctx, "9000/tcp", "http")
	require.NoError(t, err)
	_, err = seedClient(t, endpoint).CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err)
	return ctr, endpoint
}

// startCaddy runs the official Caddy image with the admin API reachable from
// the host and output bind-mounted at the same path. It returns the admin base
// URL and the host port mapped to the container's TLS port.
func startCaddy(t *testing.T, output string) (adminURL, tlsPort string) {
	t.Helper()
	ctr, err := testcontainers.Run(t.Context(), caddyImage,
		testcontainers.WithLogger(log.TestLogger(t)),
		testcontainers.WithFiles(testcontainers.ContainerFile{
			ContainerFilePath: "/etc/caddy/Caddyfile",
			FileMode:          0o644,
			Reader:            strings.NewReader(bootstrapCaddyfile),
		}),
		testcontainers.WithEnv(map[string]string{"CADDY_ADMIN": "0.0.0.0:2019"}),
		testcontainers.WithExposedPorts("2019/tcp", "443/tcp"),
		testcontainers.WithHostConfigModifier(func(hostConfig *container.HostConfig) {
			hostConfig.Binds = append(hostConfig.Binds, output+":"+output)
		}),
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

// seedClient writes to the bucket with static test credentials, independent of
// how the store under test resolves its own.
func seedClient(t *testing.T, endpoint string) *s3.Client {
	t.Helper()
	awsCfg, err := awsconfig.LoadDefaultConfig(t.Context(),
		awsconfig.WithRegion(region),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(user, secret, "")),
	)
	require.NoError(t, err)
	return s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.BaseEndpoint = new(endpoint)
		o.UsePathStyle = true
	})
}

// freePort returns a host port that is free at the moment of the call.
func freePort(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := listener.Addr().(*net.TCPAddr).Port
	require.NoError(t, listener.Close())
	return strconv.Itoa(port)
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
	}, 30*time.Second, 250*time.Millisecond, "caddy never served %s", url)
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
