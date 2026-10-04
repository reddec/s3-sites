package events_test

import (
	"context"
	_ "embed"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/network"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/log"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/reddec/s3-sites/internal/events"
)

const subject = "sites.events"

// notification is one object change exactly as the store publishes it.
//
//go:embed testdata/notification.json
var notification string

func TestListenEmitsObjectChange(t *testing.T) {
	_, url := startNATS(t)
	stream, err := events.Listen(t.Context(), events.Config{
		URL:       url,
		Subject:   subject,
		Buffer:    4,
		Reconnect: true,
	})
	require.NoError(t, err)
	publisher := connect(t, url)
	require.NoError(t, publisher.Publish(subject, []byte(notification)))
	require.NoError(t, publisher.Flush())

	select {
	case event, ok := <-stream:
		require.True(t, ok)
		assert.Equal(t, "s3:ObjectCreated:Put", event.Name)
		assert.Equal(t, "sites", event.Bucket)
		assert.Equal(t, "foo.bar.example.com/index.html", event.Key)
		assert.Equal(t, int64(1024), event.Size)
		assert.Equal(t, "d41d8cd98f00b204e9800998ecf8427e", event.ETag)
		assert.Equal(t, "2026-10-02T15:17:58+08:00", event.Time.Format(time.RFC3339))
	case <-time.After(10 * time.Second):
		t.Fatal("no event within 10s")
	}
}

func TestListenSkipsMalformedMessage(t *testing.T) {
	_, url := startNATS(t)
	stream, err := events.Listen(t.Context(), events.Config{URL: url, Subject: subject, Reconnect: true})
	require.NoError(t, err)
	publisher := connect(t, url)

	require.NoError(t, publisher.Publish(subject, []byte("not json")))
	require.NoError(t, publisher.Publish(subject, []byte(notification)))
	require.NoError(t, publisher.Flush())

	// The unparsable message is dropped and the stream keeps delivering.
	select {
	case event, ok := <-stream:
		require.True(t, ok)
		assert.Equal(t, "foo.bar.example.com/index.html", event.Key)
	case <-time.After(10 * time.Second):
		t.Fatal("no event within 10s")
	}
}

func TestListenClosesChannelOnCanceledContext(t *testing.T) {
	_, url := startNATS(t)
	ctx, cancel := context.WithCancel(t.Context())
	stream, err := events.Listen(ctx, events.Config{URL: url, Subject: subject, Reconnect: true})
	require.NoError(t, err)
	cancel()

	select {
	case _, ok := <-stream:
		assert.False(t, ok)
	case <-time.After(10 * time.Second):
		t.Fatal("stream stayed open after the context was canceled")
	}
}

func TestListenClosesChannelWhenBrokerStops(t *testing.T) {
	ctr, url := startNATS(t)
	stream, err := events.Listen(t.Context(), events.Config{URL: url, Subject: subject, Reconnect: false})
	require.NoError(t, err)

	require.NoError(t, ctr.Stop(t.Context(), nil))

	select {
	case _, ok := <-stream:
		assert.False(t, ok)
	case <-time.After(30 * time.Second):
		t.Fatal("stream stayed open after the broker stopped")
	}
}

func TestListenReportsConnectionFailure(t *testing.T) {
	// Nothing listens on the port, so the subscription never comes up.
	stream, err := events.Listen(t.Context(), events.Config{
		URL:     "nats://127.0.0.1:" + freePort(t),
		Subject: subject,
	})

	require.Error(t, err)
	assert.Nil(t, stream)
}

func TestListenSurvivesBrokerRestart(t *testing.T) {
	ctr, url := startNATS(t)
	stream, err := events.Listen(t.Context(), events.Config{
		URL:       url,
		Subject:   subject,
		Buffer:    4,
		Reconnect: true,
	})
	require.NoError(t, err)
	publisher := connect(t, url)

	require.NoError(t, ctr.Stop(t.Context(), nil))
	require.NoError(t, ctr.Start(t.Context()))

	// The subscription is re-registered on reconnect, so publish until the
	// listener confirms it is back.
	var event events.Event
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		if !assert.NoError(c, publisher.Publish(subject, []byte(notification))) {
			return
		}
		if !assert.NoError(c, publisher.Flush()) {
			return
		}
		select {
		case received, ok := <-stream:
			if assert.True(c, ok) {
				event = received
			}
		case <-time.After(2 * time.Second):
			assert.Fail(c, "no event after the broker restarted")
		}
	}, 60*time.Second, 2*time.Second)

	assert.Equal(t, "foo.bar.example.com/index.html", event.Key)
}

// startNATS runs a NATS server for one test on a host port chosen up front,
// so the URL survives a container restart, and returns the container and the
// broker URL.
func startNATS(t *testing.T) (testcontainers.Container, string) {
	t.Helper()
	if testing.Short() {
		t.Skip("needs Docker")
	}
	ctr, err := testcontainers.Run(t.Context(), "nats:2-alpine",
		testcontainers.WithLogger(log.TestLogger(t)),
		testcontainers.WithExposedPorts("4222/tcp"),
		// An ephemeral binding is re-picked when the container restarts, which
		// would leave every client dialing the port the broker no longer has.
		testcontainers.WithHostConfigModifier(func(hostConfig *container.HostConfig) {
			hostConfig.PortBindings = network.PortMap{
				network.MustParsePort("4222/tcp"): []network.PortBinding{{HostPort: freePort(t)}},
			}
		}),
		testcontainers.WithWaitStrategy(wait.ForListeningPort("4222/tcp")),
	)
	testcontainers.CleanupContainer(t, ctr)
	require.NoError(t, err)
	url, err := ctr.PortEndpoint(t.Context(), "4222/tcp", "nats")
	require.NoError(t, err)
	return ctr, url
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

// connect opens a publishing connection to url that lives as long as the test.
func connect(t *testing.T, url string) *nats.Conn {
	t.Helper()
	nc, err := nats.Connect(url)
	require.NoError(t, err)
	t.Cleanup(nc.Close)
	return nc
}
