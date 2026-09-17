# Progress

## State
Done: C1, C2, C3, C4, C5. internal/meta: SQLite metadata store (modernc.org/sqlite, WAL+FULL, one connection, embedded schema.sql), bucket/object/replica/node/pending_deletes methods, all multi-row changes in one tx.
Next: C6 — coordinator HTTP API + placement on top of internal/meta.
Known gaps: heartbeat target /internal/heartbeat does not exist on the coordinator yet.

## Hours ledger
| Item | Est | Actual | Notes |
|---|---|---|---|
| C1 | 3h | | |
| C2 | 3h | | fuzz 10s clean; -race needs gcc on Windows, runs in CI |
| C3 | 3h | | 12 tests pass; -race needs gcc on Windows, runs in CI; dir fsync is a no-op on Windows |
| C4 | 3h | | 64 MiB PUT allocates ~160 KB; -race needs gcc on Windows, runs in CI |
| C5 | 3h | | 10 tests pass under -race (pure-Go driver, no gcc needed); pending_uploads has no writer yet — CommitObject only clears it |