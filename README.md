# s3-sites

[![CI](https://github.com/reddec/s3-sites/actions/workflows/ci.yaml/badge.svg)](https://github.com/reddec/s3-sites/actions/workflows/ci.yaml)

Mirrors the site roots stored in an S3-compatible bucket into a local directory and keeps a
running Caddy pointed at them. Every root folder of the bucket is a domain
(`foo.example.com/index.html`), so uploading a file is enough to publish a site.

## Configuration

Every flag has an environment variable: `--storage.bucket` becomes `S3SYNC_STORAGE_BUCKET`.

| Flag | Default | Meaning |
|------|---------|---------|
| `--storage.bucket` | — (required) | Bucket holding one root folder per domain |
| `--storage.endpoint` | AWS default | S3-compatible endpoint URL |
| `--storage.region` | `us-east-1` | Region sent to the store |
| `--storage.path-style` | on | Path addressing; needed by MinIO and friends |
| `--storage.access-key-id`, `--storage.secret-access-key` | unsigned | Credentials used exactly as given |
| `--output` | `./sites` | Directory Caddy serves, one sub-directory per domain |
| `--resync` | `1m` | Interval of a full re-sync |
| `--caddy.admin` | `http://localhost:2019` | Caddy admin API; empty stops updating Caddy |
| `--events.nats` | disabled | NATS URL carrying the store's notifications |
| `--events.subject` | `sites.events` | Subject the notifications are published to |
| `--events.buffer` | `0` | Events queued while the syncer is busy |
| `--events.reconnect` | on | Keep listening across broker outages |

## Run

Build from source, or install the released binary:

```bash
go build -o s3-sites .   # or: go install github.com/reddec/s3-sites@latest
./s3-sites \
  --storage.endpoint http://127.0.0.1:9000 \
  --storage.bucket sites \
  --storage.access-key-id minioadmin \
  --storage.secret-access-key minioadmin \
  --events.nats nats://127.0.0.1:4222
```

Caddy needs its admin API reachable at `--caddy.admin`. Run Caddy with `CADDY_ADMIN` set when it
is not on this machine: the uploaded Caddyfile holds only site blocks, so every upload resets the
admin listener to `localhost:2019`.

Releases also publish a container image; mount the output directory where Caddy reads it and keep
the credentials in environment variables:

```bash
docker run --rm -v /srv/sites:/srv/sites \
  -e S3SYNC_STORAGE_ACCESS_KEY_ID=minioadmin \
  -e S3SYNC_STORAGE_SECRET_ACCESS_KEY=minioadmin \
  ghcr.io/reddec/s3-sites:latest \
  --output /srv/sites \
  --storage.endpoint http://minio:9000 \
  --storage.bucket sites \
  --events.nats nats://nats:4222
```

## Layout

Bucket `sites`, synced with `--output /srv/sites`:

```text
sites/
├── alpha.example.com/
│   ├── index.html
│   └── assets/app.css
└── beta.example.com/
    └── index.html
```

Every object is written under the same relative path in the output directory:

```text
/srv/sites/
├── alpha.example.com/
│   ├── index.html
│   └── assets/app.css
└── beta.example.com/
    └── index.html
```

and produces this Caddyfile, uploaded to the admin API with the domains in sorted order:

```caddyfile
alpha.example.com {
	root * /srv/sites/alpha.example.com
	try_files {path} /index.html
	file_server
}

beta.example.com {
	root * /srv/sites/beta.example.com
	try_files {path} /index.html
	file_server
}
```

Root folders are domains: objects at the bucket root and folders whose name is not a valid
hostname are skipped.

## How it works

The bucket is the source of truth. The first pass runs at startup and copies every object into
`<output>/<domain>/`, then repeats on `--resync`; objects and whole domains missing from the
bucket disappear from the output, and a failed download leaves the previous state in place. Files
are installed under a temporary name and renamed into place, so a truncated download never gets
served.

With `--events.nats` the store publishes S3 event records to the subject, and the matching domain
is synced after 5s of quiet — but never later than 1min after the first event. Without it only
the re-sync interval drives updates.

The listener is built around VersityGW: its `--event-nats-url` and `--event-nats-topic` options
point it at this subject, and the records it publishes are what the decoder reads. Nothing here
is gateway-specific — every store that publishes standard S3 event JSON to a NATS subject works
the same way.

Every full pass uploads a Caddyfile listing all served domains to the admin API; between passes an
upload happens only when a domain enters or leaves that set, so content changes inside a served
domain never touch Caddy. An identical configuration is ignored by Caddy.

Each site block carries `try_files {path} /index.html`, so a path the mirror does not hold
answers that site's root `index.html` instead of a 404.

Notification records must arrive as S3 event JSON, the shape the store itself emits:

```json
{"Records": [{"eventName": "s3:ObjectCreated:Put", "s3": {"bucket": {"name": "sites"},
  "object": {"key": "foo.example.com/index.html", "size": 1024, "eTag": "..."}}}]}
```
