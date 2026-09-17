# Cairn

Distributed object storage in Go: one coordinator, N storage nodes, replicated checksummed blobs.

[![ci](https://github.com/manavmann/distributed-object-storage/actions/workflows/ci.yml/badge.svg)](https://github.com/manavmann/distributed-object-storage/actions/workflows/ci.yml)

**Status:** single-node vertical slice. One coordinator and one storage node run in
Docker; objects are stored with replication factor 1.

## Quickstart

Requires Docker with Compose v2, `make`, `bash`, `curl` and `sha256sum`.

```sh
make up      # build the image, start coordinator + node1, wait until both are healthy
make smoke   # bucket, 8 MiB PUT, GET + sha256 check, HEAD, list, DELETE, GET → 404; prints PASS
make logs    # follow both containers
make down    # stop and delete the named volumes
```

The coordinator's API is on http://localhost:8080:

```sh
curl -X PUT localhost:8080/v1/photos                       # create bucket   → 201
curl -X PUT localhost:8080/v1/photos/cat.jpg -T cat.jpg    # put object      → 200, ETag = sha256
curl      localhost:8080/v1/photos/cat.jpg -o out.jpg      # get object      → 200 (verified payload)
curl -I   localhost:8080/v1/photos/cat.jpg                 # head object     → 200
curl      "localhost:8080/v1/photos/?prefix=c&limit=10"    # list objects    → 200
curl -X DELETE localhost:8080/v1/photos/cat.jpg            # delete object   → 204
curl      localhost:8080/cluster/status                   # node health     → 200 (UP/DOWN per node)
```

## What works

- Buckets: create, info (`GET /v1/{bucket}` with object count).
- Objects: PUT (spooled to disk, sha256-hashed, ≤ 64 MiB), GET, HEAD, DELETE,
  listing with `prefix`, `limit` (≤ 1000) and `start_after`.
- Storage node: atomic blob writes (tmp → fsync → rename), full-hash verification
  before serving a byte, corrupt blobs quarantined and never served.
- Metadata in SQLite is the only source of truth; replica rows exist only for
  acknowledged, checksummed writes.
- Cluster membership from heartbeats: nodes register by posting to
  `/internal/heartbeat`; the monitor marks a node DOWN after
  `CAIRN_HEARTBEAT_TIMEOUT` (default 15s) of silence and UP again on the next
  heartbeat. `GET /cluster/status` shows every node.
- Packaging: one distroless image with both binaries (`deploy/Dockerfile`), no
  shell; `-healthcheck` flag on each binary backs the compose healthchecks.

Not yet: replication factor > 1, repair, garbage
collection of deleted blobs, metrics. See `docs/architecture.md` for the design.

## Development

```sh
make build   # binaries into bin/
make test    # go test -race ./...
make lint    # gofmt + go vet
```

Configuration is by `CAIRN_*` environment variables; see `internal/config`.
