// Package events streams object change notifications from the S3-compatible
// store: the store publishes S3 event records to a NATS subject and Listen
// decodes them into Events.
package events

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go"
)

// flushTimeout bounds the round trip that proves the subscription reached the
// broker before Listen returns.
const flushTimeout = 5 * time.Second

// Config describes the broker to listen on and how to behave when it goes away.
type Config struct {
	URL     string
	Subject string
	// Buffer is the capacity of the returned channel: zero delivers one event
	// at a time, a positive value lets that many wait for a slow consumer.
	Buffer int
	// Reconnect keeps the stream alive across broker outages: a broker that is
	// down at startup is waited for, and a lost connection is retried without
	// a limit. Without it the stream ends at the first disconnect.
	Reconnect bool
}

// Event is one object change. Fields are reported by the notification
// verbatim: Key is not URL-decoded and Time is zero when the record omits
// eventTime. A message whose payload does not decode is dropped with a log
// line instead of reaching the caller.
type Event struct {
	Name   string // notification name, e.g. "s3:ObjectCreated:Put"
	Bucket string
	Key    string
	Size   int64
	ETag   string
	Time   time.Time
}

// Listen subscribes to config.Subject and returns the events published there,
// one per record, or an error when the subscription cannot be established. The
// subscription is registered before Listen returns, so a publish right after
// the call is not lost. The channel closes when ctx is canceled, or when the
// connection ends for good — Config.Reconnect decides whether the listener
// itself ever gives up. Failures that only surface while the stream runs (a
// dead connection, an unparsable message) have no return path: they end or
// pass through the channel and are logged.
func Listen(ctx context.Context, config Config) (<-chan Event, error) {
	opts := []nats.Option{
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			slog.Warn("nats disconnected", "error", err)
		}),
		nats.ReconnectHandler(func(_ *nats.Conn) {
			slog.Info("nats reconnected")
		}),
		nats.ErrorHandler(func(_ *nats.Conn, sub *nats.Subscription, err error) {
			slog.Warn("nats subscription error", "subject", sub.Subject, "error", err)
		}),
	}
	if config.Reconnect {
		opts = append(opts, nats.RetryOnFailedConnect(true), nats.MaxReconnects(-1))
	} else {
		opts = append(opts, nats.MaxReconnects(0))
	}

	nc, err := nats.Connect(config.URL, opts...)
	if err != nil {
		return nil, fmt.Errorf("connect to nats: %w", err)
	}

	sub, err := nc.SubscribeSync(config.Subject)
	if err == nil && nc.Status() == nats.CONNECTED {
		// Round-trip the broker so the interest is registered. A connection
		// still waiting for its broker (Config.Reconnect) registers on
		// reconnect instead and cannot confirm anything yet.
		err = nc.FlushTimeout(flushTimeout)
	}
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("subscribe to %s: %w", config.Subject, err)
	}

	out := make(chan Event, config.Buffer)

	go func() {
		defer close(out)
		defer nc.Close()
		for {
			msg, err := sub.NextMsgWithContext(ctx)
			if err != nil {
				if ctx.Err() == nil {
					slog.Warn("nats stream ended", "error", err)
				}
				return
			}
			var note notification
			if err := json.Unmarshal(msg.Data, &note); err != nil {
				slog.Warn("skip malformed notification", "error", err)
				continue
			}
			for _, rec := range note.Records {
				event := Event{
					Name:   rec.EventName,
					Bucket: rec.S3.Bucket.Name,
					Key:    rec.S3.Object.Key,
					Size:   rec.S3.Object.Size,
					ETag:   rec.S3.Object.ETag,
					Time:   rec.EventTime,
				}
				select {
				case out <- event:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return out, nil
}

// notification is one message as the store publishes it: a batch of records.
type notification struct {
	Records []record `json:"Records"`
}

// record is a single object change inside a notification.
type record struct {
	EventName string    `json:"eventName"`
	EventTime time.Time `json:"eventTime"`
	S3        struct {
		Bucket struct {
			Name string `json:"name"`
		} `json:"bucket"`
		Object struct {
			Key  string `json:"key"`
			Size int64  `json:"size"`
			ETag string `json:"eTag"`
		} `json:"object"`
	} `json:"s3"`
}
