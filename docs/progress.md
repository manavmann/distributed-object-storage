# Progress

## State
Done: C1–C12. C12: internal/replication.Writer (quorum PUT: parallel targets, sequential fallbacks, per-node timeout, partial Result on failure); API commits only acknowledged nodes, strays → pending_deletes, quorum "k/N"; nodeclient retries dial errors once; CAIRN_REPLICATION_FACTOR/CAIRN_WRITE_QUORUM (3/2).
Next: C13 — (next roadmap item)

## Hours ledger
| Item | Est | Actual | Notes |
|---|---|---|---|
| C1 | 3h | | |
| C2 | 3h | | fuzz 10s clean; -race needs gcc on Windows, runs in CI |
| C3 | 3h | | 12 tests pass; -race needs gcc on Windows, runs in CI; dir fsync is a no-op on Windows |
| C4 | 3h | | 64 MiB PUT allocates ~160 KB; -race needs gcc on Windows, runs in CI |
| C5 | 3h | | 10 tests pass under -race (pure-Go driver, no gcc needed); pending_uploads has no writer yet |
| C6 | 8h | | RF=1 vertical slice clean; sha256 ETag verified manually; client abort leaves no spool/blob |
| C7 | 3h | | listing at /v1/{bucket}/ (trailing slash); docs/architecture.md still empty — error table lives in statusFor doc comment |
| C8 | 3h | | nonroot writes named volume fine via COPY --chown /data (no root fallback); node1 logs heartbeat 404s until coordinator serves /internal/heartbeat |
| C9 | 3h | | compose: node1 DOWN ~12s after stop, UP <1s after start; node_up/node_down logged; under_replicated hardwired 0; status strings are UP/DOWN, old "up" rows self-heal on first heartbeat |
| C10 | 3h | | raw FNV-1a fails ±20% on sequential keys (800/3600 with hostname ids); splitmix64 finalizer added ; Rank4 227ns, Rank16 1.07µs |
| C11 | 3h | | integration -race -count=5 in 6.7s; Locate reads metadata (no endpoint yet); RF/W/RepairInterval/RepairGrace left out of Opts until consumed; api pkg can't import testcluster (cycle) so env tests became api_test |
| C12 | 3h | | replication+integration -race -count=3 in ~11s; W met → fallbacks skipped (N=3,W=2 with one bad target ends at 2 replicas, repair's job); commit-failure path now enqueues instead of inline Delete; hand-built configs must set RF/W or Validate rejects them |
