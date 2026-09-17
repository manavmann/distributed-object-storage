package meta

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
)

func openTemp(t *testing.T) (*DB, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "meta.db")
	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db, path
}

func mustCreateBucket(t *testing.T, db *DB, name string) {
	t.Helper()
	if err := db.CreateBucket(context.Background(), name); err != nil {
		t.Fatalf("CreateBucket(%q): %v", name, err)
	}
}

func mustUpsertNodes(t *testing.T, db *DB, ids ...string) {
	t.Helper()
	for _, id := range ids {
		if err := db.UpsertNode(context.Background(), Node{ID: id, Addr: "http://" + id, Status: "UP"}); err != nil {
			t.Fatalf("UpsertNode(%q): %v", id, err)
		}
	}
}

func mustCommit(t *testing.T, db *DB, bucket, key, blobID string, nodes ...string) {
	t.Helper()
	obj := Object{Bucket: bucket, Key: key, BlobID: blobID, Size: 1, SHA256: "x"}
	if err := db.CommitObject(context.Background(), obj, nodes); err != nil {
		t.Fatalf("CommitObject(%q/%q): %v", bucket, key, err)
	}
}

func TestPragmas(t *testing.T) {
	db, _ := openTemp(t)
	var mode string
	if err := db.db.QueryRow(`PRAGMA journal_mode`).Scan(&mode); err != nil || mode != "wal" {
		t.Fatalf("journal_mode = %q, %v; want wal", mode, err)
	}
	var fk int
	if err := db.db.QueryRow(`PRAGMA foreign_keys`).Scan(&fk); err != nil || fk != 1 {
		t.Fatalf("foreign_keys = %d, %v; want 1", fk, err)
	}
	var sync int
	if err := db.db.QueryRow(`PRAGMA synchronous`).Scan(&sync); err != nil || sync != 2 {
		t.Fatalf("synchronous = %d, %v; want 2 (FULL)", sync, err)
	}
}

func TestBucketCRUD(t *testing.T) {
	db, _ := openTemp(t)
	ctx := context.Background()

	if _, err := db.GetBucket(ctx, "b"); !errors.Is(err, ErrNoSuchBucket) {
		t.Fatalf("GetBucket before create: %v, want ErrNoSuchBucket", err)
	}
	mustCreateBucket(t, db, "b")
	if err := db.CreateBucket(ctx, "b"); !errors.Is(err, ErrBucketExists) {
		t.Fatalf("duplicate CreateBucket: %v, want ErrBucketExists", err)
	}
	b, err := db.GetBucket(ctx, "b")
	if err != nil || b.Name != "b" || b.CreatedAt == 0 {
		t.Fatalf("GetBucket = %+v, %v", b, err)
	}
	mustCreateBucket(t, db, "a")
	bs, err := db.ListBuckets(ctx)
	if err != nil || len(bs) != 2 || bs[0].Name != "a" || bs[1].Name != "b" {
		t.Fatalf("ListBuckets = %+v, %v", bs, err)
	}

	mustUpsertNodes(t, db, "n1")
	mustCommit(t, db, "b", "k", "blob-1", "n1")
	if err := db.DeleteBucket(ctx, "b"); !errors.Is(err, ErrBucketNotEmpty) {
		t.Fatalf("DeleteBucket non-empty: %v, want ErrBucketNotEmpty", err)
	}
	if err := db.DeleteObject(ctx, "b", "k"); err != nil {
		t.Fatalf("DeleteObject: %v", err)
	}
	if err := db.DeleteBucket(ctx, "b"); err != nil {
		t.Fatalf("DeleteBucket: %v", err)
	}
	if _, err := db.GetBucket(ctx, "b"); !errors.Is(err, ErrNoSuchBucket) {
		t.Fatalf("GetBucket after delete: %v, want ErrNoSuchBucket", err)
	}
	if err := db.DeleteBucket(ctx, "b"); !errors.Is(err, ErrNoSuchBucket) {
		t.Fatalf("DeleteBucket twice: %v, want ErrNoSuchBucket", err)
	}
	if _, err := db.GetObject(ctx, "b", "k"); !errors.Is(err, ErrNoSuchBucket) {
		t.Fatalf("GetObject in missing bucket: %v, want ErrNoSuchBucket", err)
	}
	if _, err := db.GetObject(ctx, "a", "k"); !errors.Is(err, ErrNoSuchKey) {
		t.Fatalf("GetObject missing key: %v, want ErrNoSuchKey", err)
	}
}

func TestCommitOverwriteQueuesOldReplicas(t *testing.T) {
	db, _ := openTemp(t)
	ctx := context.Background()
	mustCreateBucket(t, db, "b")
	mustUpsertNodes(t, db, "n1", "n2", "n3")

	mustCommit(t, db, "b", "k", "blob-1", "n1", "n2")
	got, err := db.GetObject(ctx, "b", "k")
	if err != nil || got.BlobID != "blob-1" || got.CreatedAt == 0 {
		t.Fatalf("GetObject = %+v, %v", got, err)
	}
	reps, err := db.Replicas(ctx, "blob-1")
	if err != nil || len(reps) != 2 || reps[0].NodeID != "n1" || reps[0].Addr != "http://n1" || reps[0].Status != "UP" || reps[0].CreatedAt == 0 {
		t.Fatalf("Replicas(blob-1) = %+v, %v", reps, err)
	}

	mustCommit(t, db, "b", "k", "blob-2", "n3")
	got, err = db.GetObject(ctx, "b", "k")
	if err != nil || got.BlobID != "blob-2" {
		t.Fatalf("GetObject after overwrite = %+v, %v", got, err)
	}
	if reps, err = db.Replicas(ctx, "blob-1"); err != nil || len(reps) != 0 {
		t.Fatalf("Replicas(blob-1) after overwrite = %+v, %v; want none", reps, err)
	}
	if reps, err = db.Replicas(ctx, "blob-2"); err != nil || len(reps) != 1 || reps[0].NodeID != "n3" {
		t.Fatalf("Replicas(blob-2) = %+v, %v", reps, err)
	}
	if n, err := db.CountPending(ctx); err != nil || n != 2 {
		t.Fatalf("CountPending = %d, %v; want 2", n, err)
	}
	var queued []string
	rows, err := db.db.Query(`SELECT blob_id || '@' || node_id FROM pending_deletes ORDER BY node_id`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			t.Fatal(err)
		}
		queued = append(queued, s)
	}
	rows.Close()
	if fmt.Sprint(queued) != "[blob-1@n1 blob-1@n2]" {
		t.Fatalf("pending_deletes = %v", queued)
	}

	// Re-committing the same blob id must not queue its own replicas.
	if err := db.CommitObject(ctx, Object{Bucket: "b", Key: "k", BlobID: "blob-2", Size: 1, SHA256: "x"}, nil); err != nil {
		t.Fatalf("recommit same blob: %v", err)
	}
	if reps, err = db.Replicas(ctx, "blob-2"); err != nil || len(reps) != 1 {
		t.Fatalf("Replicas(blob-2) after recommit = %+v, %v", reps, err)
	}
	if n, err := db.CountPending(ctx); err != nil || n != 2 {
		t.Fatalf("CountPending after recommit = %d, %v; want 2", n, err)
	}
}

func TestDeleteObjectIdempotent(t *testing.T) {
	db, _ := openTemp(t)
	ctx := context.Background()
	mustCreateBucket(t, db, "b")
	mustUpsertNodes(t, db, "n1", "n2")
	mustCommit(t, db, "b", "k", "blob-1", "n1", "n2")

	for i := 0; i < 3; i++ {
		if err := db.DeleteObject(ctx, "b", "k"); err != nil {
			t.Fatalf("DeleteObject #%d: %v", i, err)
		}
	}
	if _, err := db.GetObject(ctx, "b", "k"); !errors.Is(err, ErrNoSuchKey) {
		t.Fatalf("GetObject after delete: %v, want ErrNoSuchKey", err)
	}
	if reps, err := db.Replicas(ctx, "blob-1"); err != nil || len(reps) != 0 {
		t.Fatalf("Replicas after delete = %+v, %v", reps, err)
	}
	if n, err := db.CountPending(ctx); err != nil || n != 2 {
		t.Fatalf("CountPending = %d, %v; want 2", n, err)
	}
	if err := db.DeleteObject(ctx, "nope", "k"); !errors.Is(err, ErrNoSuchBucket) {
		t.Fatalf("DeleteObject in missing bucket: %v, want ErrNoSuchBucket", err)
	}
}

func TestListObjectsBoundaries(t *testing.T) {
	db, _ := openTemp(t)
	ctx := context.Background()
	mustCreateBucket(t, db, "b")
	mustCreateBucket(t, db, "other")
	keys := []string{"a", "a/", "a/1", "a/2", "a/3", "a0", "b", "\xff", "\xffx", "\xff\xff"}
	for i, k := range keys {
		mustCommit(t, db, "b", k, fmt.Sprintf("blob-%d", i))
	}
	mustCommit(t, db, "other", "a/9", "blob-other")

	cases := []struct {
		name       string
		prefix     string
		startAfter string
		limit      int
		want       []string
		truncated  bool
	}{
		{"all", "", "", 100, keys, false},
		{"prefix excludes a0", "a/", "", 100, []string{"a/", "a/1", "a/2", "a/3"}, false},
		{"start_after exclusive", "a/", "a/1", 100, []string{"a/2", "a/3"}, false},
		{"start_after below prefix", "a/", "0", 100, []string{"a/", "a/1", "a/2", "a/3"}, false},
		{"truncated at limit", "a/", "", 2, []string{"a/", "a/1"}, true},
		{"limit equals matches", "a/", "", 4, []string{"a/", "a/1", "a/2", "a/3"}, false},
		{"0xff prefix", "\xff", "", 100, []string{"\xff", "\xffx", "\xff\xff"}, false},
		{"0xff 0xff prefix", "\xff\xff", "", 100, []string{"\xff\xff"}, false},
		{"prefix a", "a", "", 100, []string{"a", "a/", "a/1", "a/2", "a/3", "a0"}, false},
		{"no match", "zzz", "", 100, nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			objs, truncated, err := db.ListObjects(ctx, "b", tc.prefix, tc.startAfter, tc.limit)
			if err != nil {
				t.Fatalf("ListObjects: %v", err)
			}
			var got []string
			for _, o := range objs {
				got = append(got, o.Key)
			}
			if fmt.Sprintf("%q", got) != fmt.Sprintf("%q", tc.want) || truncated != tc.truncated {
				t.Fatalf("got %q truncated=%v; want %q truncated=%v", got, truncated, tc.want, tc.truncated)
			}
		})
	}
	if _, _, err := db.ListObjects(ctx, "nope", "", "", 10); !errors.Is(err, ErrNoSuchBucket) {
		t.Fatalf("ListObjects missing bucket: %v, want ErrNoSuchBucket", err)
	}
	if _, _, err := db.ListObjects(ctx, "b", "", "", 0); err == nil {
		t.Fatal("ListObjects with limit 0 succeeded, want error")
	}
}

func TestPrefixUpperBound(t *testing.T) {
	cases := []struct {
		in   string
		want string
		ok   bool
	}{
		{"", "", false},
		{"a/", "a0", true},
		{"a\xff", "b", true},
		{"\xff", "", false},
		{"\xff\xff", "", false},
		{"ab\xff\xff", "ac", true},
	}
	for _, tc := range cases {
		got, ok := prefixUpperBound(tc.in)
		if got != tc.want || ok != tc.ok {
			t.Errorf("prefixUpperBound(%q) = %q, %v; want %q, %v", tc.in, got, ok, tc.want, tc.ok)
		}
	}
}

func TestReopenPersists(t *testing.T) {
	db, path := openTemp(t)
	ctx := context.Background()
	mustCreateBucket(t, db, "b")
	mustUpsertNodes(t, db, "n1")
	mustCommit(t, db, "b", "k", "blob-1", "n1")
	if err := db.SetNodeStatus(ctx, "n1", "DOWN"); err != nil {
		t.Fatalf("SetNodeStatus: %v", err)
	}
	if err := db.EnqueueDeletes(ctx, "stale", []string{"n1", "n1"}); err != nil {
		t.Fatalf("EnqueueDeletes: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	db2, err := Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db2.Close()
	obj, err := db2.GetObject(ctx, "b", "k")
	if err != nil || obj.BlobID != "blob-1" {
		t.Fatalf("GetObject after reopen = %+v, %v", obj, err)
	}
	reps, err := db2.Replicas(ctx, "blob-1")
	if err != nil || len(reps) != 1 || reps[0].Status != "DOWN" {
		t.Fatalf("Replicas after reopen = %+v, %v", reps, err)
	}
	nodes, err := db2.ListNodes(ctx)
	if err != nil || len(nodes) != 1 || nodes[0].Status != "DOWN" || nodes[0].StatusChangedAt == 0 {
		t.Fatalf("ListNodes after reopen = %+v, %v", nodes, err)
	}
	if n, err := db2.CountPending(ctx); err != nil || n != 1 {
		t.Fatalf("CountPending after reopen = %d, %v; want 1", n, err)
	}
}

func TestSetNodeStatus(t *testing.T) {
	db, _ := openTemp(t)
	ctx := context.Background()
	if err := db.SetNodeStatus(ctx, "ghost", "DOWN"); !errors.Is(err, ErrNoSuchNode) {
		t.Fatalf("SetNodeStatus unknown: %v, want ErrNoSuchNode", err)
	}
	mustUpsertNodes(t, db, "n1")
	nodes, err := db.ListNodes(ctx)
	if err != nil || len(nodes) != 1 {
		t.Fatalf("ListNodes = %+v, %v", nodes, err)
	}
	first := nodes[0].StatusChangedAt

	// A heartbeat must not reset status; it only refreshes capacity fields.
	if err := db.SetNodeStatus(ctx, "n1", "DOWN"); err != nil {
		t.Fatal(err)
	}
	if err := db.UpsertNode(ctx, Node{ID: "n1", Addr: "http://n1b", Status: "UP", FreeBytes: 42, BlobCount: 7}); err != nil {
		t.Fatal(err)
	}
	nodes, err = db.ListNodes(ctx)
	if err != nil || nodes[0].Status != "DOWN" || nodes[0].Addr != "http://n1b" || nodes[0].FreeBytes != 42 || nodes[0].BlobCount != 7 {
		t.Fatalf("ListNodes after heartbeat = %+v, %v", nodes, err)
	}
	if nodes[0].StatusChangedAt < first {
		t.Fatalf("status_changed_at went backwards: %d < %d", nodes[0].StatusChangedAt, first)
	}
}

func TestConcurrentCommits(t *testing.T) {
	db, _ := openTemp(t)
	ctx := context.Background()
	mustCreateBucket(t, db, "b")
	mustUpsertNodes(t, db, "n1", "n2")

	const workers = 8
	const rounds = 5
	var wg sync.WaitGroup
	errs := make(chan error, workers*rounds)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for r := 0; r < rounds; r++ {
				obj := Object{Bucket: "b", Key: fmt.Sprintf("k%d", w), BlobID: fmt.Sprintf("blob-%d-%d", w, r), Size: 1, SHA256: "x"}
				if err := db.CommitObject(ctx, obj, []string{"n1", "n2"}); err != nil {
					errs <- err
				}
			}
		}(w)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("CommitObject: %v", err)
	}

	objs, truncated, err := db.ListObjects(ctx, "b", "", "", 100)
	if err != nil || truncated || len(objs) != workers {
		t.Fatalf("ListObjects = %d objects, truncated=%v, %v; want %d", len(objs), truncated, err, workers)
	}
	for _, o := range objs {
		reps, err := db.Replicas(ctx, o.BlobID)
		if err != nil || len(reps) != 2 {
			t.Fatalf("Replicas(%s) = %+v, %v", o.BlobID, reps, err)
		}
	}
	// Each key was overwritten rounds-1 times, retiring 2 replicas each time.
	if n, err := db.CountPending(ctx); err != nil || n != workers*(rounds-1)*2 {
		t.Fatalf("CountPending = %d, %v; want %d", n, err, workers*(rounds-1)*2)
	}
}

func TestForeignKeysEnforced(t *testing.T) {
	db, _ := openTemp(t)
	ctx := context.Background()
	mustCreateBucket(t, db, "b")

	obj := Object{Bucket: "b", Key: "k", BlobID: "blob-1", Size: 1, SHA256: "x"}
	if err := db.CommitObject(ctx, obj, []string{"unknown-node"}); err == nil {
		t.Fatal("CommitObject with unknown node succeeded, want FK error")
	}
	// The failed transaction must have left nothing behind.
	if _, err := db.GetObject(ctx, "b", "k"); !errors.Is(err, ErrNoSuchKey) {
		t.Fatalf("GetObject after failed commit: %v, want ErrNoSuchKey", err)
	}
	if err := db.CommitObject(ctx, Object{Bucket: "nope", Key: "k", BlobID: "blob-2", Size: 1, SHA256: "x"}, nil); !errors.Is(err, ErrNoSuchBucket) {
		t.Fatalf("CommitObject into missing bucket: %v, want ErrNoSuchBucket", err)
	}
	if _, err := db.db.Exec(`INSERT INTO replicas (blob_id, node_id, created_at) VALUES ('orphan', 'n', 0)`); err == nil {
		t.Fatal("raw replica insert for unknown blob and node succeeded, want FK error")
	}
	if _, err := db.db.Exec(`INSERT INTO objects (bucket, key, blob_id, size, sha256, created_at) VALUES ('nope', 'k', 'blob-3', 1, 'x', 0)`); err == nil {
		t.Fatal("raw object insert into unknown bucket succeeded, want FK error")
	}
}

func TestDropReplica(t *testing.T) {
	db, _ := openTemp(t)
	ctx := context.Background()
	mustCreateBucket(t, db, "b")
	mustUpsertNodes(t, db, "n1", "n2")
	mustCommit(t, db, "b", "k", "blob-1", "n1", "n2")

	if err := db.DropReplica(ctx, "blob-1", "n1"); err != nil {
		t.Fatalf("DropReplica: %v", err)
	}
	reps, err := db.Replicas(ctx, "blob-1")
	if err != nil || len(reps) != 1 || reps[0].NodeID != "n2" {
		t.Fatalf("Replicas after drop = %+v, %v; want only n2", reps, err)
	}
	if err := db.DropReplica(ctx, "blob-1", "n1"); err != nil {
		t.Fatalf("second DropReplica: %v", err)
	}
	if _, err := db.GetObject(ctx, "b", "k"); err != nil {
		t.Fatalf("object gone after dropping a replica: %v", err)
	}
	if n, err := db.CountPending(ctx); err != nil || n != 0 {
		t.Fatalf("pending deletes = %d, %v; want 0 (quarantined copy needs no GC)", n, err)
	}
}
