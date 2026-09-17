# Progress

## State
Done: C1–C7. internal/api: object listing GET /v1/{bucket}/ (prefix/limit/start_after, limit ≤ 1000, InvalidArgument on bad query), GET /v1/{bucket} bucket info with object_count; validate.go owns key/bucket/query validation; errors.go is the single 13-code public error table.
Next: C8 — Dockerfile, single-node compose, smoke script, README quickstart.

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