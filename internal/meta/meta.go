// Package meta is the coordinator's metadata store: a single SQLite file
// that is the only source of truth for buckets, objects, replica placement,
// node state and pending work.
//
// Every multi-row change is one transaction on the store's single
// connection, so readers never observe a half-applied write. Replica rows
// exist only for nodes that acknowledged a checksummed write of a blob that
// objects still references; when an object is overwritten or deleted its
// replicas move into pending_deletes in the same transaction.
package meta

import (
	"context"
	"database/sql"
	_ "embed"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

//go:embed schema.sql
var schema string

var (
	// ErrNoSuchBucket means the bucket does not exist.
	ErrNoSuchBucket = errors.New("meta: no such bucket")
	// ErrBucketExists means CreateBucket was asked for a name already in use.
	ErrBucketExists = errors.New("meta: bucket exists")
	// ErrBucketNotEmpty means DeleteBucket refused because objects remain.
	ErrBucketNotEmpty = errors.New("meta: bucket not empty")
	// ErrNoSuchKey means the bucket exists but holds no object with that key.
	ErrNoSuchKey = errors.New("meta: no such key")
	// ErrNoSuchNode means no node with that id has ever been upserted.
	ErrNoSuchNode = errors.New("meta: no such node")
)

// Bucket is a row of buckets.
type Bucket struct {
	Name      string
	CreatedAt int64
}

// Object is a row of objects. CreatedAt is set by the store on commit.
type Object struct {
	Bucket      string
	Key         string
	BlobID      string
	Size        int64
	SHA256      string
	ContentType string
	CreatedAt   int64
}

// Replica is a replicas row joined with the node that holds the copy.
// CreatedAt is when the node acknowledged the write.
type Replica struct {
	BlobID    string
	NodeID    string
	Addr      string
	Status    string
	CreatedAt int64
}

// Node is a row of nodes. Status is owned by the health monitor and only
// changes through SetNodeStatus; UpsertNode uses n.Status for new rows only.
type Node struct {
	ID              string
	Addr            string
	Status          string
	FreeBytes       int64
	BlobCount       int64
	LastSeen        int64
	StatusChangedAt int64
}

// DB is an open metadata store. It is safe for concurrent use; all access
// is serialized on one connection.
type DB struct {
	db *sql.DB
}

// Open opens or creates the SQLite database at path and applies the
// schema. The connection runs in WAL mode with synchronous=FULL so a
// committed transaction survives power loss.
func Open(path string) (*DB, error) {
	q := url.Values{}
	for _, p := range []string{
		"journal_mode(WAL)",
		"synchronous(FULL)",
		"busy_timeout(5000)",
		"foreign_keys(ON)",
	} {
		q.Add("_pragma", p)
	}
	db, err := sql.Open("sqlite", path+"?"+q.Encode())
	if err != nil {
		return nil, fmt.Errorf("meta: open: %w", err)
	}
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("meta: apply schema: %w", err)
	}
	return &DB{db: db}, nil
}

// Close releases the underlying connection.
func (d *DB) Close() error {
	return d.db.Close()
}

func now() int64 {
	return time.Now().UnixMilli()
}

// tx runs fn in one transaction and commits it if fn returns nil.
func (d *DB) tx(ctx context.Context, fn func(tx *sql.Tx) error) error {
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("meta: begin: %w", err)
	}
	if err := fn(tx); err != nil {
		if rerr := tx.Rollback(); rerr != nil {
			return errors.Join(err, fmt.Errorf("meta: rollback: %w", rerr))
		}
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("meta: commit: %w", err)
	}
	return nil
}

func bucketExists(ctx context.Context, tx *sql.Tx, name string) error {
	var n int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM buckets WHERE name = ?`, name).Scan(&n); err != nil {
		return fmt.Errorf("meta: lookup bucket: %w", err)
	}
	if n == 0 {
		return ErrNoSuchBucket
	}
	return nil
}

// CreateBucket creates an empty bucket.
func (d *DB) CreateBucket(ctx context.Context, name string) error {
	return d.tx(ctx, func(tx *sql.Tx) error {
		switch err := bucketExists(ctx, tx, name); {
		case err == nil:
			return ErrBucketExists
		case !errors.Is(err, ErrNoSuchBucket):
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO buckets (name, created_at) VALUES (?, ?)`, name, now()); err != nil {
			return fmt.Errorf("meta: create bucket: %w", err)
		}
		return nil
	})
}

// GetBucket returns the bucket named name.
func (d *DB) GetBucket(ctx context.Context, name string) (Bucket, error) {
	var b Bucket
	err := d.db.QueryRowContext(ctx, `SELECT name, created_at FROM buckets WHERE name = ?`, name).Scan(&b.Name, &b.CreatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return Bucket{}, ErrNoSuchBucket
	}
	if err != nil {
		return Bucket{}, fmt.Errorf("meta: get bucket: %w", err)
	}
	return b, nil
}

// ListBuckets returns every bucket ordered by name.
func (d *DB) ListBuckets(ctx context.Context) ([]Bucket, error) {
	rows, err := d.db.QueryContext(ctx, `SELECT name, created_at FROM buckets ORDER BY name`)
	if err != nil {
		return nil, fmt.Errorf("meta: list buckets: %w", err)
	}
	defer rows.Close()
	var out []Bucket
	for rows.Next() {
		var b Bucket
		if err := rows.Scan(&b.Name, &b.CreatedAt); err != nil {
			return nil, fmt.Errorf("meta: list buckets: %w", err)
		}
		out = append(out, b)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("meta: list buckets: %w", err)
	}
	return out, nil
}

// DeleteBucket removes an empty bucket. It returns ErrBucketNotEmpty if
// any object still lives in it.
func (d *DB) DeleteBucket(ctx context.Context, name string) error {
	return d.tx(ctx, func(tx *sql.Tx) error {
		if err := bucketExists(ctx, tx, name); err != nil {
			return err
		}
		var n int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM objects WHERE bucket = ?`, name).Scan(&n); err != nil {
			return fmt.Errorf("meta: count objects: %w", err)
		}
		if n > 0 {
			return ErrBucketNotEmpty
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM buckets WHERE name = ?`, name); err != nil {
			return fmt.Errorf("meta: delete bucket: %w", err)
		}
		return nil
	})
}

const objectCols = `bucket, key, blob_id, size, sha256, content_type, created_at`

func scanObject(row interface{ Scan(...any) error }) (Object, error) {
	var o Object
	err := row.Scan(&o.Bucket, &o.Key, &o.BlobID, &o.Size, &o.SHA256, &o.ContentType, &o.CreatedAt)
	return o, err
}

// GetObject returns the current version of bucket/key.
func (d *DB) GetObject(ctx context.Context, bucket, key string) (Object, error) {
	var o Object
	err := d.tx(ctx, func(tx *sql.Tx) error {
		if err := bucketExists(ctx, tx, bucket); err != nil {
			return err
		}
		var err error
		o, err = scanObject(tx.QueryRowContext(ctx, `SELECT `+objectCols+` FROM objects WHERE bucket = ? AND key = ?`, bucket, key))
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNoSuchKey
		}
		if err != nil {
			return fmt.Errorf("meta: get object: %w", err)
		}
		return nil
	})
	return o, err
}

// CommitObject makes obj the current version of obj.Bucket/obj.Key with a
// replica on each of nodeIDs, all in one transaction: the previous
// version's replicas move to pending_deletes and the pending_uploads row
// for obj.BlobID, if any, is removed. Every node in nodeIDs must already
// be known via UpsertNode.
func (d *DB) CommitObject(ctx context.Context, obj Object, nodeIDs []string) error {
	return d.tx(ctx, func(tx *sql.Tx) error {
		if err := bucketExists(ctx, tx, obj.Bucket); err != nil {
			return err
		}
		ts := now()
		var old string
		err := tx.QueryRowContext(ctx, `SELECT blob_id FROM objects WHERE bucket = ? AND key = ?`, obj.Bucket, obj.Key).Scan(&old)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("meta: read current object: %w", err)
		}
		if old != "" && old != obj.BlobID {
			if err := retireReplicas(ctx, tx, old, ts); err != nil {
				return err
			}
		}
		_, err = tx.ExecContext(ctx, `
			INSERT INTO objects (`+objectCols+`) VALUES (?, ?, ?, ?, ?, ?, ?)
			ON CONFLICT (bucket, key) DO UPDATE SET
				blob_id = excluded.blob_id, size = excluded.size, sha256 = excluded.sha256,
				content_type = excluded.content_type, created_at = excluded.created_at`,
			obj.Bucket, obj.Key, obj.BlobID, obj.Size, obj.SHA256, obj.ContentType, ts)
		if err != nil {
			return fmt.Errorf("meta: upsert object: %w", err)
		}
		for _, id := range nodeIDs {
			if _, err := tx.ExecContext(ctx, `INSERT INTO replicas (blob_id, node_id, created_at) VALUES (?, ?, ?)`, obj.BlobID, id, ts); err != nil {
				return fmt.Errorf("meta: insert replica %s on %s: %w", obj.BlobID, id, err)
			}
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM pending_uploads WHERE blob_id = ?`, obj.BlobID); err != nil {
			return fmt.Errorf("meta: clear pending upload: %w", err)
		}
		return nil
	})
}

// retireReplicas moves every replica of blobID into pending_deletes.
func retireReplicas(ctx context.Context, tx *sql.Tx, blobID string, ts int64) error {
	_, err := tx.ExecContext(ctx, `
		INSERT OR IGNORE INTO pending_deletes (blob_id, node_id, enqueued_at)
		SELECT blob_id, node_id, ? FROM replicas WHERE blob_id = ?`, ts, blobID)
	if err != nil {
		return fmt.Errorf("meta: queue old replicas: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM replicas WHERE blob_id = ?`, blobID); err != nil {
		return fmt.Errorf("meta: drop old replicas: %w", err)
	}
	return nil
}

// DeleteObject removes bucket/key and queues its replicas for deletion in
// one transaction. Deleting a key that does not exist is not an error.
func (d *DB) DeleteObject(ctx context.Context, bucket, key string) error {
	return d.tx(ctx, func(tx *sql.Tx) error {
		if err := bucketExists(ctx, tx, bucket); err != nil {
			return err
		}
		var blobID string
		err := tx.QueryRowContext(ctx, `SELECT blob_id FROM objects WHERE bucket = ? AND key = ?`, bucket, key).Scan(&blobID)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("meta: read current object: %w", err)
		}
		if err := retireReplicas(ctx, tx, blobID, now()); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM objects WHERE bucket = ? AND key = ?`, bucket, key); err != nil {
			return fmt.Errorf("meta: delete object: %w", err)
		}
		return nil
	})
}

// prefixUpperBound returns the smallest string greater than every string
// that starts with prefix, and false if no such bound exists (the prefix is
// empty or entirely 0xFF bytes).
func prefixUpperBound(prefix string) (string, bool) {
	b := []byte(prefix)
	for i := len(b) - 1; i >= 0; i-- {
		if b[i] != 0xFF {
			b[i]++
			return string(b[:i+1]), true
		}
	}
	return "", false
}

// ListObjects returns up to limit objects in bucket whose key starts with
// prefix and sorts after startAfter, in key order. truncated reports
// whether more matches remain beyond the last returned key.
func (d *DB) ListObjects(ctx context.Context, bucket, prefix, startAfter string, limit int) (objs []Object, truncated bool, err error) {
	if limit <= 0 {
		return nil, false, fmt.Errorf("meta: list objects: limit must be positive, got %d", limit)
	}
	err = d.tx(ctx, func(tx *sql.Tx) error {
		if err := bucketExists(ctx, tx, bucket); err != nil {
			return err
		}
		var sb strings.Builder
		sb.WriteString(`SELECT ` + objectCols + ` FROM objects WHERE bucket = ? AND key >= ? AND key > ?`)
		args := []any{bucket, prefix, startAfter}
		if upper, ok := prefixUpperBound(prefix); ok {
			sb.WriteString(` AND key < ?`)
			args = append(args, upper)
		}
		sb.WriteString(` ORDER BY key LIMIT ?`)
		args = append(args, limit+1)
		rows, err := tx.QueryContext(ctx, sb.String(), args...)
		if err != nil {
			return fmt.Errorf("meta: list objects: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			o, err := scanObject(rows)
			if err != nil {
				return fmt.Errorf("meta: list objects: %w", err)
			}
			objs = append(objs, o)
		}
		if err := rows.Err(); err != nil {
			return fmt.Errorf("meta: list objects: %w", err)
		}
		if len(objs) > limit {
			objs = objs[:limit]
			truncated = true
		}
		return nil
	})
	if err != nil {
		return nil, false, err
	}
	return objs, truncated, nil
}

// Replicas returns the nodes that hold blobID, ordered by node id.
func (d *DB) Replicas(ctx context.Context, blobID string) ([]Replica, error) {
	var out []Replica
	err := d.tx(ctx, func(tx *sql.Tx) error {
		var err error
		out, err = replicasIn(ctx, tx, blobID)
		return err
	})
	return out, err
}

func replicasIn(ctx context.Context, tx *sql.Tx, blobID string) ([]Replica, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT r.blob_id, r.node_id, n.addr, n.status, r.created_at
		FROM replicas r JOIN nodes n ON n.node_id = r.node_id
		WHERE r.blob_id = ? ORDER BY r.node_id`, blobID)
	if err != nil {
		return nil, fmt.Errorf("meta: replicas: %w", err)
	}
	defer rows.Close()
	var out []Replica
	for rows.Next() {
		var r Replica
		if err := rows.Scan(&r.BlobID, &r.NodeID, &r.Addr, &r.Status, &r.CreatedAt); err != nil {
			return nil, fmt.Errorf("meta: replicas: %w", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("meta: replicas: %w", err)
	}
	return out, nil
}

// DropReplica forgets that nodeID holds blobID, because the node reported
// its copy corrupt and quarantined it. Dropping a row that does not exist
// is not an error.
func (d *DB) DropReplica(ctx context.Context, blobID, nodeID string) error {
	if _, err := d.db.ExecContext(ctx, `DELETE FROM replicas WHERE blob_id = ? AND node_id = ?`, blobID, nodeID); err != nil {
		return fmt.Errorf("meta: drop replica %s on %s: %w", blobID, nodeID, err)
	}
	return nil
}

// UpsertNode records a heartbeat: it inserts n, or updates the address,
// capacity and last_seen of the existing row. Status is taken from n only
// when the row is new.
func (d *DB) UpsertNode(ctx context.Context, n Node) error {
	ts := now()
	_, err := d.db.ExecContext(ctx, `
		INSERT INTO nodes (node_id, addr, status, free_bytes, blob_count, last_seen, status_changed_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (node_id) DO UPDATE SET
			addr = excluded.addr, free_bytes = excluded.free_bytes,
			blob_count = excluded.blob_count, last_seen = excluded.last_seen`,
		n.ID, n.Addr, n.Status, n.FreeBytes, n.BlobCount, ts, ts)
	if err != nil {
		return fmt.Errorf("meta: upsert node: %w", err)
	}
	return nil
}

// SetNodeStatus changes a node's status, stamping status_changed_at only
// when the status actually changes.
func (d *DB) SetNodeStatus(ctx context.Context, id, status string) error {
	return d.tx(ctx, func(tx *sql.Tx) error {
		var cur string
		err := tx.QueryRowContext(ctx, `SELECT status FROM nodes WHERE node_id = ?`, id).Scan(&cur)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNoSuchNode
		}
		if err != nil {
			return fmt.Errorf("meta: read node status: %w", err)
		}
		if cur == status {
			return nil
		}
		if _, err := tx.ExecContext(ctx, `UPDATE nodes SET status = ?, status_changed_at = ? WHERE node_id = ?`, status, now(), id); err != nil {
			return fmt.Errorf("meta: set node status: %w", err)
		}
		return nil
	})
}

// ListNodes returns every known node ordered by id.
func (d *DB) ListNodes(ctx context.Context) ([]Node, error) {
	rows, err := d.db.QueryContext(ctx, `
		SELECT node_id, addr, status, free_bytes, blob_count, last_seen, status_changed_at
		FROM nodes ORDER BY node_id`)
	if err != nil {
		return nil, fmt.Errorf("meta: list nodes: %w", err)
	}
	defer rows.Close()
	var out []Node
	for rows.Next() {
		var n Node
		if err := rows.Scan(&n.ID, &n.Addr, &n.Status, &n.FreeBytes, &n.BlobCount, &n.LastSeen, &n.StatusChangedAt); err != nil {
			return nil, fmt.Errorf("meta: list nodes: %w", err)
		}
		out = append(out, n)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("meta: list nodes: %w", err)
	}
	return out, nil
}

// EnqueueDeletes queues blobID for deletion on each of nodeIDs. Entries
// already queued are left untouched.
func (d *DB) EnqueueDeletes(ctx context.Context, blobID string, nodeIDs []string) error {
	return d.tx(ctx, func(tx *sql.Tx) error {
		ts := now()
		for _, id := range nodeIDs {
			if _, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO pending_deletes (blob_id, node_id, enqueued_at) VALUES (?, ?, ?)`, blobID, id, ts); err != nil {
				return fmt.Errorf("meta: enqueue delete %s on %s: %w", blobID, id, err)
			}
		}
		return nil
	})
}

// PendingUpload is a row of pending_uploads: a PUT that has picked a blob
// id and is about to write it to nodes. StartedAt is when the PUT began.
type PendingUpload struct {
	BlobID    string
	Bucket    string
	Key       string
	StartedAt int64
}

// BeginUpload records that u.BlobID is about to be written to nodes, so a
// coordinator crash before CommitObject leaves a trace to reclaim it from.
func (d *DB) BeginUpload(ctx context.Context, u PendingUpload) error {
	_, err := d.db.ExecContext(ctx, `INSERT INTO pending_uploads (blob_id, bucket, key, started_at) VALUES (?, ?, ?, ?)`,
		u.BlobID, u.Bucket, u.Key, u.StartedAt)
	if err != nil {
		return fmt.Errorf("meta: begin upload %s: %w", u.BlobID, err)
	}
	return nil
}

// AbortUpload ends a PUT that will not commit: the copies that landed on
// nodeIDs are queued for deletion and the upload intent is dropped, in one
// transaction.
func (d *DB) AbortUpload(ctx context.Context, blobID string, nodeIDs []string) error {
	return d.tx(ctx, func(tx *sql.Tx) error {
		ts := now()
		for _, id := range nodeIDs {
			if _, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO pending_deletes (blob_id, node_id, enqueued_at) VALUES (?, ?, ?)`, blobID, id, ts); err != nil {
				return fmt.Errorf("meta: enqueue delete %s on %s: %w", blobID, id, err)
			}
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM pending_uploads WHERE blob_id = ?`, blobID); err != nil {
			return fmt.Errorf("meta: clear pending upload %s: %w", blobID, err)
		}
		return nil
	})
}

// SweepStaleUploads reclaims every upload intent older than maxAge: its
// blob is queued for deletion on every known node, since nobody recorded
// which nodes it reached, and the intent is dropped. Both happen in one
// transaction. It returns the swept intents.
func (d *DB) SweepStaleUploads(ctx context.Context, maxAge time.Duration) ([]PendingUpload, error) {
	var swept []PendingUpload
	err := d.tx(ctx, func(tx *sql.Tx) error {
		ts := now()
		cutoff := ts - maxAge.Milliseconds()
		_, err := tx.ExecContext(ctx, `
			INSERT OR IGNORE INTO pending_deletes (blob_id, node_id, enqueued_at)
			SELECT u.blob_id, n.node_id, ? FROM pending_uploads u CROSS JOIN nodes n
			WHERE u.started_at < ?`, ts, cutoff)
		if err != nil {
			return fmt.Errorf("meta: queue stale uploads: %w", err)
		}
		rows, err := tx.QueryContext(ctx, `
			DELETE FROM pending_uploads WHERE started_at < ?
			RETURNING blob_id, bucket, key, started_at`, cutoff)
		if err != nil {
			return fmt.Errorf("meta: sweep stale uploads: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			var u PendingUpload
			if err := rows.Scan(&u.BlobID, &u.Bucket, &u.Key, &u.StartedAt); err != nil {
				return fmt.Errorf("meta: sweep stale uploads: %w", err)
			}
			swept = append(swept, u)
		}
		if err := rows.Err(); err != nil {
			return fmt.Errorf("meta: sweep stale uploads: %w", err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return swept, nil
}

// PendingUploads returns every upload intent, oldest first.
func (d *DB) PendingUploads(ctx context.Context) ([]PendingUpload, error) {
	rows, err := d.db.QueryContext(ctx, `SELECT blob_id, bucket, key, started_at FROM pending_uploads ORDER BY started_at, blob_id`)
	if err != nil {
		return nil, fmt.Errorf("meta: pending uploads: %w", err)
	}
	defer rows.Close()
	var out []PendingUpload
	for rows.Next() {
		var u PendingUpload
		if err := rows.Scan(&u.BlobID, &u.Bucket, &u.Key, &u.StartedAt); err != nil {
			return nil, fmt.Errorf("meta: pending uploads: %w", err)
		}
		out = append(out, u)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("meta: pending uploads: %w", err)
	}
	return out, nil
}

// CountPending returns the number of (blob, node) pairs in pending_deletes.
func (d *DB) CountPending(ctx context.Context) (int, error) {
	var n int
	if err := d.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM pending_deletes`).Scan(&n); err != nil {
		return 0, fmt.Errorf("meta: count pending: %w", err)
	}
	return n, nil
}

// PendingDelete is a row of pending_deletes: one copy of a blob that no
// object references any more, waiting to be removed from a node.
type PendingDelete struct {
	BlobID     string
	NodeID     string
	Attempts   int
	EnqueuedAt int64
}

// PendingDeletesForNodes returns up to limit queued deletes whose node is
// one of nodeIDs, oldest first. It returns nothing for an empty nodeIDs.
func (d *DB) PendingDeletesForNodes(ctx context.Context, nodeIDs []string, limit int) ([]PendingDelete, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("meta: pending deletes: limit must be positive, got %d", limit)
	}
	if len(nodeIDs) == 0 {
		return nil, nil
	}
	args := make([]any, 0, len(nodeIDs)+1)
	for _, id := range nodeIDs {
		args = append(args, id)
	}
	args = append(args, limit)
	rows, err := d.db.QueryContext(ctx, `
		SELECT blob_id, node_id, attempts, enqueued_at FROM pending_deletes
		WHERE node_id IN (?`+strings.Repeat(", ?", len(nodeIDs)-1)+`)
		ORDER BY enqueued_at, blob_id, node_id LIMIT ?`, args...)
	if err != nil {
		return nil, fmt.Errorf("meta: pending deletes: %w", err)
	}
	defer rows.Close()
	var out []PendingDelete
	for rows.Next() {
		var p PendingDelete
		if err := rows.Scan(&p.BlobID, &p.NodeID, &p.Attempts, &p.EnqueuedAt); err != nil {
			return nil, fmt.Errorf("meta: pending deletes: %w", err)
		}
		out = append(out, p)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("meta: pending deletes: %w", err)
	}
	return out, nil
}

// RemovePending drops the queued delete of blobID on nodeID because the
// node no longer serves the copy. Removing a row that is not queued is
// not an error.
func (d *DB) RemovePending(ctx context.Context, blobID, nodeID string) error {
	if _, err := d.db.ExecContext(ctx, `DELETE FROM pending_deletes WHERE blob_id = ? AND node_id = ?`, blobID, nodeID); err != nil {
		return fmt.Errorf("meta: remove pending %s on %s: %w", blobID, nodeID, err)
	}
	return nil
}

// BumpAttempts records one more failed attempt to delete blobID on nodeID
// and returns the new count. It returns 0 if the row is no longer queued.
func (d *DB) BumpAttempts(ctx context.Context, blobID, nodeID string) (int, error) {
	var n int
	err := d.db.QueryRowContext(ctx, `
		UPDATE pending_deletes SET attempts = attempts + 1
		WHERE blob_id = ? AND node_id = ? RETURNING attempts`, blobID, nodeID).Scan(&n)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("meta: bump attempts %s on %s: %w", blobID, nodeID, err)
	}
	return n, nil
}

// UnderReplicated is one blob with fewer than n UP holders. Holders lists
// every replica row of the blob, UP and DOWN; LastDownChange is the latest
// status_changed_at among DOWN holders, or 0 if none is DOWN.
type UnderReplicated struct {
	BlobID         string
	Bucket         string
	Key            string
	Size           int64
	SHA256         string
	Holders        []Replica
	LastDownChange int64
}

// underReplicatedSQL selects the blob_id of every object with fewer than
// n UP replicas. It takes n as its single parameter.
const underReplicatedSQL = `
	SELECT o.blob_id FROM objects o
	LEFT JOIN replicas r ON r.blob_id = o.blob_id
	LEFT JOIN nodes n ON n.node_id = r.node_id
	GROUP BY o.blob_id
	HAVING COALESCE(SUM(n.status = 'UP'), 0) < ?`

// UnderReplicated returns up to limit blobs that fewer than n UP nodes
// hold, ordered by blob id, each with all of its holders.
func (d *DB) UnderReplicated(ctx context.Context, n, limit int) ([]UnderReplicated, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("meta: under-replicated: limit must be positive, got %d", limit)
	}
	var out []UnderReplicated
	err := d.tx(ctx, func(tx *sql.Tx) error {
		rows, err := tx.QueryContext(ctx, `
			SELECT o.blob_id, o.bucket, o.key, o.size, o.sha256,
				COALESCE(MAX(CASE WHEN n.status = 'DOWN' THEN n.status_changed_at END), 0)
			FROM objects o
			LEFT JOIN replicas r ON r.blob_id = o.blob_id
			LEFT JOIN nodes n ON n.node_id = r.node_id
			WHERE o.blob_id IN (`+underReplicatedSQL+`)
			GROUP BY o.blob_id ORDER BY o.blob_id LIMIT ?`, n, limit)
		if err != nil {
			return fmt.Errorf("meta: under-replicated: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			var u UnderReplicated
			if err := rows.Scan(&u.BlobID, &u.Bucket, &u.Key, &u.Size, &u.SHA256, &u.LastDownChange); err != nil {
				return fmt.Errorf("meta: under-replicated: %w", err)
			}
			out = append(out, u)
		}
		if err := rows.Err(); err != nil {
			return fmt.Errorf("meta: under-replicated: %w", err)
		}
		for i := range out {
			out[i].Holders, err = replicasIn(ctx, tx, out[i].BlobID)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// CountUnderReplicated returns how many blobs fewer than n UP nodes hold.
func (d *DB) CountUnderReplicated(ctx context.Context, n int) (int, error) {
	var c int
	if err := d.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM (`+underReplicatedSQL+`)`, n).Scan(&c); err != nil {
		return 0, fmt.Errorf("meta: count under-replicated: %w", err)
	}
	return c, nil
}

// AddReplicaIfLive records that nodeID holds blobID, but only if objects
// still references blobID. It reports whether the row was inserted; a
// row that already exists is not inserted again.
func (d *DB) AddReplicaIfLive(ctx context.Context, blobID, nodeID string) (bool, error) {
	var added bool
	err := d.tx(ctx, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx, `
			INSERT INTO replicas (blob_id, node_id, created_at)
			SELECT ?, ?, ? WHERE EXISTS (SELECT 1 FROM objects WHERE blob_id = ?)
			ON CONFLICT (blob_id, node_id) DO NOTHING`, blobID, nodeID, now(), blobID)
		if err != nil {
			return fmt.Errorf("meta: add replica %s on %s: %w", blobID, nodeID, err)
		}
		n, err := res.RowsAffected()
		if err != nil {
			return fmt.Errorf("meta: add replica %s on %s: %w", blobID, nodeID, err)
		}
		added = n == 1
		return nil
	})
	return added, err
}
