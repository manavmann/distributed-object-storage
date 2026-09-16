# Cairn — working notes for Claude Code

## What this is
Distributed object storage in Go. One coordinator (HTTP API, SQLite metadata, placement,
health monitor, repair/GC worker) + N storage nodes (checksummed blobs on disk).
Read docs/architecture.md for flows. Read docs/progress.md for current state.

## Non-negotiable invariants
- Metadata (SQLite) is the only source of truth. `replicas` rows exist ONLY for nodes
  that acknowledged a checksummed write. Readers never recompute placement.
- Every multi-row metadata change is one transaction. One DB connection (SetMaxOpenConns(1)).
- Every PUT gets a fresh blob_id. Old blobs are queued in pending_deletes, never deleted inline.
- Storage nodes write tmp → fsync → rename → fsync dir. Nothing under blobs/ is ever partial.
- Nodes verify the full payload hash before serving a single byte. Corrupt files go to
  quarantine/, never deleted, never served.
- Client bodies are spooled to disk and hashed; never buffered in memory; never streamed
  straight to nodes.
- Repair commits are conditional: insert the replica row only if objects still references
  the blob; otherwise queue the copy for deletion.
- Grace period: never re-replicate for a node that went DOWN less than REPAIR_GRACE ago.
- Coordinator is an explicit single point of failure. Do not add leader election, Raft,
  or a second coordinator.

## Code rules
- Go standard library first. Allowed deps: modernc.org/sqlite, prometheus/client_golang.
  Anything else requires an entry in docs/decisions.md.
- No frameworks: net/http ServeMux patterns, log/slog, database/sql, flag.
- No interfaces with one implementation. No pkg/, utils/, service/, domain/ packages.
- Errors: sentinel errors per package; wrap with %w; one error→HTTP mapping in
  internal/api/errors.go. Never swallow errors.
- Goroutines must have an owner and a stop condition (ctx).
- No time.Sleep in tests. Use internal/testcluster.WaitFor.
- Handlers stream; use io.Copy / io.CopyN / SectionReader.
- Log with slog and the event names in internal/events. Metrics labels are bounded.

## Layout
cmd/{coordinator,storagenode,bench}  internal/{api,blob,cluster,config,coordinator,httpx,
meta,metrics,nodeclient,placement,repair,replication,storage,testcluster}
test/integration  deploy/  scripts/  docs/

## Commands
make build | make test (go test -race ./...) | make lint | make up | make down |
make smoke | make demo | make bench

## Process
- Implement only the roadmap item you were given. Stop and ask when something contradicts it.
- Never run git commit/push. Never edit docs/roadmap.md or this file.
- Finish every task with tests passing under -race and a ≤10-line summary.