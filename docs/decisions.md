# Decisions

Each entry: context, decision, consequences. Add one for every non-stdlib dependency.

## modernc.org/sqlite for the metadata store

Context: the coordinator needs a durable, transactional metadata store with
real foreign keys and range scans over (bucket, key). CLAUDE.md names it as
the one allowed database dependency.

Decision: use modernc.org/sqlite, a pure-Go transpilation of SQLite, through
database/sql. No ORM, no migration framework; the schema is one embedded
idempotent schema.sql.

Consequences: no cgo, so `go build` and `-race` work on every platform
without a C toolchain. The driver pulls in a handful of indirect modernc.org
modules. One connection (SetMaxOpenConns(1)) serializes all access, which is
what the single-writer coordinator wants anyway.
