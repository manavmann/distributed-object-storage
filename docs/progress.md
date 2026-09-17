# Progress

## State
Done: C1–C21. C21: concurrency + restart integration suites (test/integration/{concurrency,restart}_test.go); harness gained RawDB/Blobs/QuarantineCount and a goroutine-safe Client; pre-existing tests moved to 50/300ms heartbeats and 1s node timeouts.
Next: C22 — (next roadmap item)

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
| C15 | repair+integration -race -count=3 in ~9s; Placement field omitted (placement is a stateless package), back-off filtered in the worker not SQL; harness RepairInterval defaults to 1m so pre-GC pending-count assertions stay exact; node DELETE quarantines rather than unlinks, so "reclaimed" = gone from blobs/, present in quarantine/ |
| C16 | | | internal+integration -race -count=3 in ~13s; repair ranks by key (same as the writer), not bucket/key; HangWrites cuts the PUT before the node stores anything so the stale-copy path is covered by a repair unit test whose target deletes the object mid-PUT; no_source test needs grace > monitor sweep (1s) so three kills land before any repair; holders is fetched per candidate inside the same tx rather than group_concat |
| C17 | | | Go chain (quarantine→ErrIntegrity→drop→repair) was already wired; the only hole was repair reading a corrupt source. Corruption is read-triggered, so "two corrupt → repaired" must keep GETting until both are visited (reads shuffle). Git Bash rewrites /data paths in docker args — keep them inside sh -c. Compose nodes heartbeat every 5s (default) vs 3s coordinator timeout, so nodes flap DOWN/UP every 5s in `make up`; smoke passes anyway. |
| C18 | | | meta+api+coordinator+integration -race clean; sweep runs only at startup, so an intent younger than 2× timeout at restart waits for the next restart; sweep fans out to every `nodes` row since nobody recorded which nodes the blob reached, GC treats "already gone" as done; StartedAt is caller-supplied so tests can seed a stale intent without a clock hook; `git stash` on this box rewrites touched files to CRLF (autocrlf=true) and trips gofmt — `gofmt -w` on them fixes it |
| C19 | | | meta+repair+integration -race clean; the trimmed copy is always the spare in practice (PUT and repair both rank by key, so the restarted original holder outranks the repair target); TrimReplica re-checks the UP count inside the tx so a node going DOWN between fetch and commit cannot push a blob below RF; a DOWN holder is never the victim, only UP copies are trimmed |
| C20 | | | all pkgs -race clean; one Metrics struct serves both binaries (coordinator gauges sit at 0 on a node); nodes_up/total refresh on transitions, DB gauges only at the end of a repair tick so objects_total lags a PUT by one tick; the monitor can mark a node DOWN between a tick's repair step and its gauge refresh, so a grace skip may land one tick after the gauge moves; -race works locally now (gcc via WinLibs) |
| C21 | | | integration -race -count=10 in 55.8s, 0 flakes; 20ms heartbeats / 100ms node timeouts flapped healthy nodes DOWN once the package ran under load (heartbeat client timeout = its interval); RestartCoordinator used to open a second Store on a live node dir (Open deletes in-flight .tmp files); cluster teardown leaves ~30 TIME_WAIT sockets each, ~11k per -count=10 run vs Windows' 16k ephemeral range, so don't add clusters casually; wall time is fsync-bound (heartbeat/tick rate made no difference in A/B) |

