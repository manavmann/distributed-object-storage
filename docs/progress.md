# Progress

## State
Done: C1–C6. internal/api + internal/coordinator + cmd/coordinator: RF=1 vertical slice over a StaticRegistry (CAIRN_NODES); routes PUT /v1/{bucket}, PUT/GET/HEAD/DELETE /v1/{bucket}/{key...}; nodeclient with typed errors; spool-to-disk PUT with sha256 ETag.
Next: C7 — object listing with prefix and cursor, bucket info, error table.

## Hours ledger
| Item | Est | Actual | Notes |
|---|---|---|---|
| C1 | 3h | | |
| C2 | 3h | | fuzz 10s clean; -race needs gcc on Windows, runs in CI |
| C3 | 3h | | 12 tests pass; -race needs gcc on Windows, runs in CI; dir fsync is a no-op on Windows |
| C4 | 3h | | 64 MiB PUT allocates ~160 KB; -race needs gcc on Windows, runs in CI |
| C5 | 3h | | 10 tests pass under -race (pure-Go driver, no gcc needed); pending_uploads has no writer yet |
| C6 | 8h | | RF=1 vertical slice clean; sha256 ETag verified manually; client abort leaves no spool/blob |