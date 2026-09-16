# Progress

## State
Done: C1, C2, C3, C4. Storage node speaks HTTP: internal/httpx (request ids, access log, JSON errors), internal/config (CAIRN_* NodeConfig), internal/storage/server.go (PUT/GET/HEAD/DELETE /blobs/{id}, /healthz) + heartbeat loop, cmd/storagenode wires them with graceful shutdown.
Next: C5 — SQLite metadata store.
Known gaps: heartbeat target /internal/heartbeat does not exist on the coordinator yet.

## Hours ledger
| Item | Est | Actual | Notes |
|---|---|---|---|
| C1 | 3h | | |
| C2 | 3h | | fuzz 10s clean; -race needs gcc on Windows, runs in CI |
| C3 | 3h | | 12 tests pass; -race needs gcc on Windows, runs in CI; dir fsync is a no-op on Windows |
| C4 | 3h | | 64 MiB PUT allocates ~160 KB; -race needs gcc on Windows, runs in CI |