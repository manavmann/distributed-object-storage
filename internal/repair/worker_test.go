package repair

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/manavmann/distributed-object-storage/internal/blob"
	"github.com/manavmann/distributed-object-storage/internal/cluster"
	"github.com/manavmann/distributed-object-storage/internal/meta"
	"github.com/manavmann/distributed-object-storage/internal/nodeclient"
	"github.com/manavmann/distributed-object-storage/internal/storage"
)

// TestBatchSizeRespected queues five deletes on one node and checks that a
// tick sends exactly BatchSize DELETEs and clears exactly that many rows.
func TestBatchSizeRespected(t *testing.T) {
	ctx := context.Background()
	var deletes atomic.Int32
	node := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			t.Errorf("unexpected %s %s", r.Method, r.URL.Path)
		}
		deletes.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(node.Close)

	db, err := meta.Open(filepath.Join(t.TempDir(), "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	reg, err := cluster.Load(ctx, db, time.Minute, time.Now, log)
	if err != nil {
		t.Fatal(err)
	}
	if err := reg.Heartbeat(ctx, cluster.Heartbeat{NodeID: "n1", Addr: node.URL}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		if err := db.EnqueueDeletes(ctx, "blob-"+strconv.Itoa(i), []string{"n1"}); err != nil {
			t.Fatal(err)
		}
	}

	w := &Worker{
		DB: db, Client: nodeclient.New(), Registry: reg,
		Interval: time.Minute, Grace: time.Minute, BatchSize: 2, Timeout: time.Second, Log: log,
	}
	for tick, wantSent := 1, 2; tick <= 3; tick++ {
		w.tick(ctx)
		if got := int(deletes.Load()); got != wantSent {
			t.Fatalf("after tick %d: %d DELETEs sent, want %d", tick, got, wantSent)
		}
		if n, err := db.CountPending(ctx); err != nil || n != 5-wantSent {
			t.Fatalf("after tick %d: pending = %d, %v; want %d", tick, n, err, 5-wantSent)
		}
		wantSent = min(wantSent+2, 5)
	}
}

// startNode serves a real storage node from a temp dir and returns its
// URL wrapped by wrap, plus the dir for on-disk assertions.
func startNode(t *testing.T, id string, wrap func(http.Handler) http.Handler) (url, dir string) {
	t.Helper()
	dir = t.TempDir()
	sn, err := storage.NewNode(dir, storage.NodeOptions{ID: id, HeartbeatInterval: time.Minute, Log: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(wrap(sn.Handler()))
	t.Cleanup(srv.Close)
	return srv.URL, dir
}

// TestRepairStaleCopyQueued deletes the object while its repair copy is
// in flight (the target node deletes it before acknowledging the PUT), so
// the commit must find the blob unreferenced, leave no replica row and
// queue the fresh copy, which the next tick reclaims.
func TestRepairStaleCopyQueued(t *testing.T) {
	ctx := context.Background()
	db, err := meta.Open(filepath.Join(t.TempDir(), "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	identity := func(h http.Handler) http.Handler { return h }
	sourceURL, _ := startNode(t, "src", identity)
	targetURL, targetDir := startNode(t, "dst", func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodPut {
				if err := db.DeleteObject(ctx, "b", "k"); err != nil {
					t.Errorf("delete during repair: %v", err)
				}
			}
			next.ServeHTTP(w, r)
		})
	})
	reg, err := cluster.Load(ctx, db, time.Minute, time.Now, log)
	if err != nil {
		t.Fatal(err)
	}
	for id, url := range map[string]string{"src": sourceURL, "dst": targetURL} {
		if err := reg.Heartbeat(ctx, cluster.Heartbeat{NodeID: id, Addr: url}); err != nil {
			t.Fatal(err)
		}
	}
	client := nodeclient.New()
	payload := []byte("copy me")
	sum := sha256.Sum256(payload)
	const blobID = "blob-stale"
	if err := client.Put(ctx, sourceURL, blobID, bytes.NewReader(payload), int64(len(payload)), hex.EncodeToString(sum[:])); err != nil {
		t.Fatal(err)
	}
	if err := db.CreateBucket(ctx, "b"); err != nil {
		t.Fatal(err)
	}
	obj := meta.Object{Bucket: "b", Key: "k", BlobID: blobID, Size: int64(len(payload)), SHA256: hex.EncodeToString(sum[:])}
	if err := db.CommitObject(ctx, obj, []string{"src"}); err != nil {
		t.Fatal(err)
	}

	w := &Worker{
		DB: db, Client: client, Registry: reg,
		Interval: time.Minute, Grace: time.Millisecond, RF: 2, BatchSize: 8, Timeout: 5 * time.Second, Log: log,
	}
	w.tick(ctx)
	if reps, err := db.Replicas(ctx, blobID); err != nil || len(reps) != 0 {
		t.Fatalf("replica rows after stale repair = %+v, %v; want none", reps, err)
	}
	if _, err := os.Stat(filepath.Join(targetDir, "blobs", blobID)); err != nil {
		t.Fatalf("target should hold the stale copy until GC: %v", err)
	}
	rows, err := db.PendingDeletesForNodes(ctx, []string{"dst"}, 10)
	if err != nil || len(rows) != 1 || rows[0].BlobID != blobID {
		t.Fatalf("pending deletes on dst = %+v, %v; want the stale copy", rows, err)
	}

	w.tick(ctx)
	if n, err := db.CountPending(ctx); err != nil || n != 0 {
		t.Fatalf("pending after gc = %d, %v; want 0", n, err)
	}
	if _, err := os.Stat(filepath.Join(targetDir, "blobs", blobID)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stale copy still served on target: %v", err)
	}
	if w.SkippedNoSource() != 0 {
		t.Fatalf("no_source skips = %d, want 0", w.SkippedNoSource())
	}
}

// TestRepairCorruptSourceDropped corrupts the only source before the
// worker reads it: the node quarantines the copy and answers corrupt, so
// the worker must drop the source's replica row, write nothing to the
// target and, on the next tick, skip the blob for want of a source.
func TestRepairCorruptSourceDropped(t *testing.T) {
	ctx := context.Background()
	db, err := meta.Open(filepath.Join(t.TempDir(), "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	identity := func(h http.Handler) http.Handler { return h }
	sourceURL, sourceDir := startNode(t, "src", identity)
	targetURL, targetDir := startNode(t, "dst", identity)
	reg, err := cluster.Load(ctx, db, time.Minute, time.Now, log)
	if err != nil {
		t.Fatal(err)
	}
	for id, url := range map[string]string{"src": sourceURL, "dst": targetURL} {
		if err := reg.Heartbeat(ctx, cluster.Heartbeat{NodeID: id, Addr: url}); err != nil {
			t.Fatal(err)
		}
	}
	client := nodeclient.New()
	payload := []byte("about to rot")
	sum := sha256.Sum256(payload)
	const blobID = "blob-corrupt"
	if err := client.Put(ctx, sourceURL, blobID, bytes.NewReader(payload), int64(len(payload)), hex.EncodeToString(sum[:])); err != nil {
		t.Fatal(err)
	}
	if err := db.CreateBucket(ctx, "b"); err != nil {
		t.Fatal(err)
	}
	obj := meta.Object{Bucket: "b", Key: "k", BlobID: blobID, Size: int64(len(payload)), SHA256: hex.EncodeToString(sum[:])}
	if err := db.CommitObject(ctx, obj, []string{"src"}); err != nil {
		t.Fatal(err)
	}
	// Flip the first payload byte on disk; the node discovers it on the read.
	path := filepath.Join(sourceDir, "blobs", blobID)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	raw[blob.HeaderSize] ^= 0xff
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatal(err)
	}

	w := &Worker{
		DB: db, Client: client, Registry: reg,
		Interval: time.Minute, Grace: time.Millisecond, RF: 2, BatchSize: 8, Timeout: 5 * time.Second, Log: log,
	}
	w.tick(ctx)
	if reps, err := db.Replicas(ctx, blobID); err != nil || len(reps) != 0 {
		t.Fatalf("replica rows after corrupt source = %+v, %v; want none", reps, err)
	}
	if _, err := os.Stat(filepath.Join(targetDir, "blobs", blobID)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("target got a copy from a corrupt source: %v", err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("corrupt copy still under blobs/ on the source: %v", err)
	}
	if _, err := os.Stat(filepath.Join(sourceDir, "quarantine", blobID)); err != nil {
		t.Fatalf("corrupt copy not quarantined on the source: %v", err)
	}
	if w.SkippedNoSource() != 0 {
		t.Fatalf("no_source skips = %d before the row was dropped, want 0", w.SkippedNoSource())
	}
	w.tick(ctx)
	if w.SkippedNoSource() != 1 {
		t.Fatalf("no_source skips = %d after the row was dropped, want 1", w.SkippedNoSource())
	}
	if _, err := db.GetObject(ctx, "b", "k"); err != nil {
		t.Fatalf("object gone from metadata: %v", err)
	}
}
