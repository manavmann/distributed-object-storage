-- Cairn metadata. This file is applied on every Open and must stay
-- idempotent. All *_at columns are unix milliseconds.

CREATE TABLE IF NOT EXISTS buckets (
    name       TEXT    NOT NULL PRIMARY KEY,
    created_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS nodes (
    node_id           TEXT    NOT NULL PRIMARY KEY,
    addr              TEXT    NOT NULL,
    status            TEXT    NOT NULL,
    free_bytes        INTEGER NOT NULL DEFAULT 0,
    blob_count        INTEGER NOT NULL DEFAULT 0,
    last_seen         INTEGER NOT NULL,
    status_changed_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS objects (
    bucket       TEXT    NOT NULL REFERENCES buckets(name),
    key          TEXT    NOT NULL,
    blob_id      TEXT    NOT NULL UNIQUE,
    size         INTEGER NOT NULL,
    sha256       TEXT    NOT NULL,
    content_type TEXT    NOT NULL DEFAULT '',
    created_at   INTEGER NOT NULL,
    PRIMARY KEY (bucket, key)
);

-- A replica row exists only for a node that acknowledged a checksummed
-- write of a blob that objects still references.
CREATE TABLE IF NOT EXISTS replicas (
    blob_id    TEXT    NOT NULL REFERENCES objects(blob_id),
    node_id    TEXT    NOT NULL REFERENCES nodes(node_id),
    created_at INTEGER NOT NULL,
    PRIMARY KEY (blob_id, node_id)
);

CREATE INDEX IF NOT EXISTS replicas_node_id ON replicas(node_id);

-- Blobs no longer referenced by objects, waiting for the GC worker to
-- delete them on the node. No FK: the object row is already gone.
CREATE TABLE IF NOT EXISTS pending_deletes (
    blob_id     TEXT    NOT NULL,
    node_id     TEXT    NOT NULL,
    attempts    INTEGER NOT NULL DEFAULT 0,
    enqueued_at INTEGER NOT NULL,
    PRIMARY KEY (blob_id, node_id)
);

-- Uploads in flight. A row is removed when its blob is committed; the GC
-- worker sweeps stale rows.
CREATE TABLE IF NOT EXISTS pending_uploads (
    blob_id    TEXT    NOT NULL PRIMARY KEY,
    bucket     TEXT    NOT NULL,
    key        TEXT    NOT NULL,
    started_at INTEGER NOT NULL
);
