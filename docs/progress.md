# Progress

## State
Done: C1, C2. internal/blob has the 64-byte header (magic, version, length, SHA-256), Encode/Decode/ReadHeader, Verify, and four sentinel errors.
Next: C3 — storage node on-disk layout and atomic blob write.
Known gaps: no HTTP, no SQLite, no on-disk node layout yet.

## Hours ledger
| Item | Est | Actual | Notes |
|---|---|---|---|
| C1 | 3h | | |
| C2 | 3h | | fuzz 10s clean; -race needs gcc on Windows, runs in CI |

## In my own words
- C1: Set up the Go module, two binary stubs, Makefile, GitHub Actions CI, gitignore and README skeleton. The decision was to keep main.go under 30 lines with no dependencies. What could break: CI failing on first push if the workflow YAML has an issue.
- C2: Blob header codec and streaming payload verifier. Big-endian fields, 16 reserved bytes ignored on read, rejects lengths above MaxInt64. What could break: nothing reads or writes blob files yet, so format is only exercised by its own tests.