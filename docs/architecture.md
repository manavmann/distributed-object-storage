# Architecture

Shardwell is one coordinator and N storage nodes. The coordinator owns all
metadata and every decision; nodes are checksummed blob stores that know
nothing about buckets, keys or each other. The coordinator is an explicit
single point of failure: there is no leader election and no second
coordinator.

## Components

Coordinator (`cmd/coordinator`), one process, one SQLite file:

- `internal/api` — the public HTTP API: `PUT`/`GET /v1/{bucket}` (create,
  info), `GET /v1/{bucket}/` (listing), `PUT`/`GET`/`HEAD`/`DELETE
  /v1/{bucket}/{key}`, `GET /cluster/status`, `GET /cluster/locate`,
  `GET /healthz`, `GET /metrics`, plus the node-facing
  `POST /internal/heartbeat`. Handlers stream; a client body is never held
  in memory. `errors.go` is the only error→HTTP mapping.
- `internal/meta` — SQLite via `database/sql` with one connection. Tables:
  `buckets`, `objects` (bucket, key → blob_id, size, sha256), `replicas`
  (blob_id, node_id), `nodes`, `pending_deletes`, `pending_uploads`. Every
  multi-row change is one transaction. **Metadata is the only source of
  truth**: a `replicas` row exists only for a node that acknowledged a
  checksummed write, and readers never recompute placement.
- `internal/placement` — rendezvous hashing: ranks node ids for a key. The
  write path ranks the UP nodes and takes the top N; repair ranks the UP
  nodes to pick a re-replication target and a trim victim; `/cluster/locate`
  ranks every known node for display.
- `internal/cluster` — the node registry (loaded from and persisted to
  `nodes`) and the health monitor that flips a node to DOWN after
  `CAIRN_HEARTBEAT_TIMEOUT` without a heartbeat and back to UP on the next.
- `internal/replication` — `Writer` (quorum fan-out) and `Reader` (failover
  reads), both over `internal/nodeclient`, the HTTP client for nodes.
- `internal/repair` — the repair/GC worker: one goroutine that ticks every
  `CAIRN_REPAIR_INTERVAL`; see below.

Storage node (`cmd/storagenode`, `internal/storage`): `PUT/GET/HEAD/DELETE
/blobs/{id}` over a data directory holding `blobs/` and `quarantine/`. A blob
file is a header (size, sha256) plus payload. Writes go `tmp → fsync →
rename → fsync dir`, so nothing under `blobs/` is ever partial. A GET
verifies the full payload hash before the first byte is sent; a file that
fails verification is moved to `quarantine/`, never deleted and never
served. The node heartbeats the coordinator every `CAIRN_HEARTBEAT_INTERVAL`
with its advertise address, free bytes and blob count.

Deployment (`deploy/`): one distroless image with both binaries; compose
runs a coordinator on :8080 and `node1..node4`, each with its own volume,
with `CAIRN_REPLICATION_FACTOR=3` and `CAIRN_WRITE_QUORUM=2`.

## Write path

`PUT /v1/{bucket}/{key}`, N = replication factor, W = write quorum:

1. The API generates a fresh `blob_id` — for every PUT, including an
   overwrite — and opens a spool file under it in `spool/`.
2. The body is spooled to disk and SHA-256 hashed as it streams. Bodies
   above `CAIRN_MAX_OBJECT_SIZE` are rejected; at most `CAIRN_MAX_UPLOADS`
   PUTs spool or replicate at once. A client that disconnects here leaves
   nothing behind: the spool is removed and no node was contacted.
3. An upload intent (`blob_id`, bucket, key, start time) is inserted into
   `pending_uploads` before any node is contacted, so a coordinator crash
   from here on leaves a row to reclaim from (see "The coordinator dies").
4. `replication.Writer` asks placement for the top N UP nodes for the key
   and PUTs the blob to all of them in parallel, each from its own
   `SectionReader` over the spool, sending the digest so a node rejects a
   mismatching body. If fewer than W acknowledge, it walks the remaining UP
   nodes one at a time until W copies exist. Each node write has its own
   timeout (`CAIRN_NODE_TIMEOUT`). If fewer than W nodes are UP, no node is
   contacted.
5. Commit, in one transaction: upsert the `objects` row, insert one
   `replicas` row per acknowledging node — and no other node — move the
   previous version's replicas into `pending_deletes`, and delete the
   `pending_uploads` row. The response carries `ETag: "<sha256>"`.
6. If fewer than W nodes acknowledged, the PUT fails with
   `503 InsufficientReplicas`. If the commit fails, the PUT fails with that
   error's own mapping: `500 InternalError`, or `404 NoSuchBucket` if the
   bucket was deleted meanwhile. In both cases every copy that did land is
   queued in `pending_deletes` and the intent row is removed, in one
   transaction. Nothing is deleted from a node inline; the spool is removed
   when the handler returns.

A PUT that reaches W but not N leaves the object under-replicated. That is
repair's job, not the client's.

## Read path

`GET /v1/{bucket}/{key}` (HEAD is the same minus the body):

1. Look up the `objects` row, then its `replicas` rows joined with node
   status. No placement is computed.
2. `replication.Reader` tries the UP holders in random order, then the DOWN
   ones, one attempt per node. The first node that serves a verified copy
   wins; the response is streamed from it, with `X-Cairn-Replica` naming the
   node and `ETag` the sha256.
3. A node that reports its copy corrupt (it has already quarantined the
   file) loses its `replicas` row immediately and the event
   `integrity_failure` is logged. A node that is unreachable, lacks the
   blob or returns bytes whose digest does not match the object is skipped.
4. If every holder said "not found", the object is re-read once from
   metadata in case it was overwritten meanwhile and the new blob is tried
   the same way. Otherwise the read fails with 503.

`GET /cluster/locate?bucket=&key=` exposes step 1: the blob id, size,
sha256, the holders with their status, and every known node — UP and DOWN —
ranked by placement for the key. A fresh write ranks only the UP nodes, so
the two orders agree only while every node is UP.

## Cluster status

`GET /cluster/status` is the coordinator's view of membership, straight from
the in-memory registry (which is loaded from and persisted to the `nodes`
table). Nodes are sorted by id. Times are RFC 3339 in UTC.

```json
{
  "nodes": [
    {
      "node_id": "node1",
      "addr": "http://node1:9000",
      "status": "UP",
      "free_bytes": 53687091200,
      "blob_count": 12,
      "last_seen": "2026-09-16T10:15:04.123Z",
      "status_changed_at": "2026-09-16T10:00:00.000Z"
    }
  ],
  "under_replicated": 0
}
```

- `status` is `UP` or `DOWN`. A node is UP from its first heartbeat until the
  health monitor sees no heartbeat for `CAIRN_HEARTBEAT_TIMEOUT`; the next
  heartbeat flips it back to UP.
- `last_seen` is the last accepted heartbeat; `status_changed_at` is when the
  current status was set. Both come from the coordinator's clock.
- `under_replicated` is the number of objects with fewer UP holders than
  the replication factor, counted from metadata on every request.

## Repair and GC

`repair.Worker` is one goroutine started with the coordinator. It ticks
every `CAIRN_REPAIR_INTERVAL`, does nothing between ticks, and never
touches a blob that metadata has not told it to. Each tick fetches at most
64 rows per step and runs three steps in order:

1. **GC.** Fetch queued `pending_deletes` whose node is UP and ask each
   node to `DELETE /blobs/{id}`; the node moves the file to `quarantine/`.
   A copy the node no longer has counts as deleted. Any other failure bumps
   the row's `attempts`, and a row that has failed k times is left alone
   until k × interval after it was enqueued.
2. **Re-replication.** Fetch blobs with fewer than N UP holders. A blob is
   skipped while any of its DOWN holders went DOWN less than
   `CAIRN_REPAIR_GRACE` ago (`repair_skipped reason=grace`), and when it has
   no UP holder to copy from (`reason=no_source`). Otherwise the worker
   streams it from a random UP holder to the first UP non-holder in
   placement order for the key, then inserts the `replicas` row only if
   `objects` still references the blob; if not, the fresh copy is queued
   for deletion (`reason=stale`). A source that reports its copy corrupt
   loses its `replicas` row, as on a failover read, and the blob waits for
   the next tick.
3. **Trim.** Fetch blobs with more than N UP holders and, for each, drop the
   replica on the UP holder placement ranks last, queueing that copy for the
   next tick's GC. The drop is conditional inside the transaction: the blob
   must still have more than N UP holders, so a node going DOWN in between
   never leaves it short.

After the three steps the worker refreshes the `under_replicated_blobs`,
`pending_deletes` and `objects_total` gauges from metadata.

Stale `pending_uploads` rows are not the worker's job: the coordinator
sweeps them once at startup.

## Failure model

- A storage node dies: PUTs route around it (placement only ranks UP nodes;
  a write in flight falls back to the next healthy node), GETs fail over to
  another holder, and `/cluster/status` shows it DOWN after
  `CAIRN_HEARTBEAT_TIMEOUT`. Its blobs are still valid when it returns, so
  it resumes serving them on the next heartbeat.
- A node's disk corrupts a blob: the node notices on read, quarantines the
  file and reports the failure; the coordinator drops that replica row and
  serves from another holder.
- The coordinator dies: the cluster is unavailable until it restarts. Its
  state is one SQLite file. On restart it empties `spool/`, queues every
  `pending_uploads` intent older than twice `CAIRN_NODE_TIMEOUT` for
  deletion on every known node (nobody recorded which nodes the blob
  reached), reloads the node registry and continues. There is no failover
  by design.
- A PUT is interrupted: the spool file is removed and any copy that reached
  a node is queued in `pending_deletes`. Readers never see a partial object
  because `objects` and `replicas` change only in the commit transaction.
