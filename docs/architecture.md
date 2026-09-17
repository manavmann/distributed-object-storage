# Architecture

Cairn is one coordinator and N storage nodes. The coordinator owns all
metadata and every decision; nodes are checksummed blob stores that know
nothing about buckets, keys or each other. The coordinator is an explicit
single point of failure: there is no leader election and no second
coordinator.

## Components

Coordinator (`cmd/coordinator`), one process, one SQLite file:

- `internal/api` — the public HTTP API (`/v1/{bucket}`, `/v1/{bucket}/{key}`,
  `/cluster/status`, `/cluster/locate`) and the node-facing
  `/internal/heartbeat`. Handlers stream; a client body is never held in
  memory. `errors.go` is the only error→HTTP mapping.
- `internal/meta` — SQLite via `database/sql` with one connection. Tables:
  `buckets`, `objects` (bucket, key → blob_id, size, sha256), `replicas`
  (blob_id, node_id), `nodes`, `pending_deletes`, `pending_uploads`. Every
  multi-row change is one transaction. **Metadata is the only source of
  truth**: a `replicas` row exists only for a node that acknowledged a
  checksummed write, and readers never recompute placement.
- `internal/placement` — rendezvous hashing: ranks node ids for a key so a
  fresh write picks the top N healthy nodes. Only the write path uses it.
- `internal/cluster` — the node registry (loaded from and persisted to
  `nodes`) and the health monitor that flips a node to DOWN after
  `CAIRN_HEARTBEAT_TIMEOUT` without a heartbeat and back to UP on the next.
- `internal/replication` — `Writer` (quorum fan-out) and `Reader` (failover
  reads), both over `internal/nodeclient`, the HTTP client for nodes.
- `internal/repair` — the repair/GC worker. Not implemented yet; see below.

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
3. `replication.Writer` asks placement for the top N UP nodes for the key
   and PUTs the blob to all of them in parallel, each from its own
   `SectionReader` over the spool, sending the digest so a node rejects a
   mismatching body. If fewer than W acknowledge, it walks the remaining UP
   nodes one at a time until W copies exist. Each node write has its own
   timeout (`CAIRN_NODE_TIMEOUT`). If fewer than W nodes are UP, no node is
   contacted.
4. Commit, in one transaction: upsert the `objects` row, insert one
   `replicas` row per acknowledging node — and nothing else — and move the
   previous version's replicas into `pending_deletes`. The response carries
   `ETag: <sha256>`.
5. If fewer than W nodes acknowledged, or the commit fails, the PUT fails
   with 503 and every copy that did land is queued in `pending_deletes`.
   Nothing is deleted from a node inline; the spool is removed when the
   handler returns.

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
sha256, the holders with their status, and the placement ranking a fresh
write would use.

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
- `under_replicated` is the number of objects with fewer live replicas than
  the replication factor. It is always 0 until repair lands.

## Repair and GC

Not implemented yet. The intended worker, `internal/repair`, runs every
`CAIRN_REPAIR_INTERVAL` and does two things: re-replicates objects with
fewer than N live replicas, and drains `pending_deletes` by asking nodes to
quarantine the listed blobs. Two rules are fixed now: a repair commit
inserts the new `replicas` row only if `objects` still references the blob
(otherwise the copy is queued for deletion), and no re-replication happens
for a node that went DOWN less than `CAIRN_REPAIR_GRACE` ago.

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
  state is one SQLite file plus the spool; on restart it reloads the node
  registry and continues. There is no failover by design.
- A PUT is interrupted: the spool file is removed and any copy that reached
  a node is queued in `pending_deletes`. Readers never see a partial object
  because `objects` and `replicas` change only in the commit transaction.
