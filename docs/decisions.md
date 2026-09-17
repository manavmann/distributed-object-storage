# Decisions

Each entry: context, decision, consequences. Add one for every non-stdlib dependency.

## modernc.org/sqlite for the metadata store

Context: the coordinator needs a durable, transactional metadata store with
real foreign keys and range scans over (bucket, key). CLAUDE.md names it as
the one allowed database dependency.

Decision: use modernc.org/sqlite, a pure-Go transpilation of SQLite, through
database/sql. No ORM, no migration framework; the schema is one embedded
idempotent schema.sql.

Consequences: no cgo, so `go build` works on every platform without a C
toolchain (the Docker image is built with `CGO_ENABLED=0`). The race
detector is a separate matter: `go test -race` needs cgo and therefore a C
compiler on Windows regardless of the SQLite driver, so `-race` runs on
Linux in CI and locally only where gcc is installed. The driver pulls in a
handful of indirect modernc.org modules. One connection (SetMaxOpenConns(1)) serializes all access, which is
what the single-writer coordinator wants anyway.

## github.com/prometheus/client_golang for metrics

Context: both binaries need a /metrics endpoint an operator can scrape,
with counters, gauges and a latency histogram. CLAUDE.md names
prometheus/client_golang as the one allowed metrics dependency.

Decision: use prometheus/client_golang directly. internal/metrics builds
every collector on its own prometheus.NewRegistry() (no promauto, no
default registry) so tests can build a registry per cluster, and serves it
with promhttp.HandlerFor. HTTP series are labelled by mux pattern
(r.Pattern), never the raw path, so cardinality is bounded by the route
table.

Consequences: the module pulls in prometheus/client_model, common,
procfs and google.golang.org/protobuf as indirect dependencies. The
prometheus/testutil package from the same module is used in tests to read
collector values without scraping.
