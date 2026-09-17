# Progress

## State
Done: C1–C14. C14: compose scaled to node1..node4 (RF=3, W=2, heartbeat timeout 3s, repair env set); smoke.sh asserts 3 replicas via /cluster/locate, stops a holder, GET still matches sha256, DOWN→start→UP; README diagram + "How it works"; docs/architecture.md first draft.
Next: C15 — (next roadmap item)

## Hours ledger
| Item | Est | Actual | Notes |
|---|---|---|---|
| C1  | 
| C2  | fuzz 10s clean; -race needs gcc on Windows, runs in CI |
| C3  | 12 tests pass; -race needs gcc on Windows, runs in CI; dir fsync is a no-op on Windows |
| C4  | 64 MiB PUT allocates ~160 KB; -race needs gcc on Windows, runs in CI |
| C5  | 10 tests pass under -race (pure-Go driver, no gcc needed); pending_uploads has no writer yet |
| C6  | RF=1 vertical slice clean; sha256 ETag verified manually; client abort leaves no spool/blob |
| C7  | listing at /v1/{bucket}/ (trailing slash); docs/architecture.md still empty — error table lives in statusFor doc comment |
| C8  | nonroot writes named volume fine via COPY --chown /data (no root fallback); node1 logs heartbeat 404s until coordinator serves /internal/heartbeat |
| C9  | compose: node1 DOWN ~12s after stop, UP <1s after start; node_up/node_down logged; under_replicated hardwired 0; status strings are UP/DOWN, old "up" rows self-heal on first heartbeat |
| C10 | raw FNV-1a fails ±20% on sequential keys (800/3600 with hostname ids); splitmix64 finalizer added ; Rank4 227ns, Rank16 1.07µs |
| C11 | integration -race -count=5 in 6.7s; Locate reads metadata (no endpoint yet); RF/W/RepairInterval/RepairGrace left out of Opts until consumed; api pkg can't import testcluster (cycle) so env tests became api_test |
| C12 | replication+integration -race -count=3 in ~11s; W met → fallbacks skipped (N=3,W=2 with one bad target ends at 2 replicas, repair's job); commit-failure path now enqueues instead of inline Delete; hand-built configs must set RF/W or Validate rejects them |
| C13 | integration -race -count=3 in 7.6s; HEAD now hits a node (full verify) and 503s like GET when no replica is reachable; a HEAD 503 has no body so the harness only sees the status; corrupt-copy rows are dropped, not queued (node already quarantined the file) |
| C14 | smoke PASS in ~6.5s: DOWN within 3s heartbeat timeout, UP <1s after start; CAIRN_ADVERTISE_ADDR must be a full URL (config rejects nodeN:9000); CAIRN_REPAIR_INTERVAL/GRACE set in compose but not read by config until repair lands; smoke trap restarts a stopped node on failure |
