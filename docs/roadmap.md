# Roadmap

One row per item. Titles for done items are the commit subjects that landed
them (`git log`).

| Item | Status | Title |
|---|---|---|
| C1  | done | build: initialize module, CI, Makefile, and docs skeleton |
| C2  | done | feat(blob): self-describing blob format with verified header and payload |
| C3  | done | feat(storage): atomic blob write, read with verification, quarantine on corruption |
| C4  | done | feat(storage): node HTTP server, heartbeat loop, storagenode binary |
| C5  | done | feat(meta): SQLite metadata store — buckets, objects, replicas, nodes, pending deletes |
| C6  | done | feat(api): coordinator vertical slice — PUT/GET/HEAD/DELETE through one storage node |
| C7  | done | feat(api): object listing with prefix and cursor, bucket info, error table |
| C8  | done | build: Dockerfile, single-node compose, smoke script, README quickstart |
| C9  | done | feat(cluster): heartbeat registry, health monitor, and /cluster/status |
| C10 | done | feat(placement): rendezvous hashing over healthy nodes |
| C11 | done | test(testcluster): in-process multi-node harness |
| C12 | done | feat(replication): quorum write fan-out with fallbacks and cleanup |
| C13 | done | feat(replication): failover reads, integrity handling, and /cluster/locate |
| C14 | done | build: four-node cluster, failover smoke step, README architecture diagram |
| C15 | done | feat(repair): pending-delete processing and GC worker |
| C16 | done | feat(repair): under-replication detection and re-replication with grace period |
| C17 | done | feat: corruption detection, quarantine, and repair end to end |
| C18 | done | feat(api): crash-safe upload intent log |
| C19 | done | feat(repair): trim over-replicated blobs |
| C20 | done | feat(observability): Prometheus metrics and named events |
| C21 | done | test: concurrency and restart suites |
| C22 | done | feat: graceful shutdown, server timeouts, limits, and spool hygiene |
| C23 | done | verification pass across all suites (no commit of its own) |
| C24 | done | feat(bench): load generator and benchmark report |
| C25 | done | docs: architecture, design decisions, failure model |
| C26 | done | docs: README polish, demo script, MIT license, v1.0 |
| C27 | done | feat(cairnctl): minimal CLI — mb ls put get rm status locate |
| C28 | done | feat(storage): background scrubber with heartbeat reporting |
| C29 | done | feat: shared secret on internal endpoints |
| C30 | done | docs: rename project to Shardwell |
