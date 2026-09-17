# Progress

## State
Done: C1–C11. C11: internal/testcluster in-process harness (K nodes + coordinator on httptest, Kill/Restart/RestartCoordinator, fault middleware, typed client); storage.NewNode + coordinator.New(cfg, Deps)/Start/Stop; api tests now run on the harness as package api_test.
Next: C12 — (next roadmap item)

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