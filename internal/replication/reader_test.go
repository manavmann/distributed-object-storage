package replication

import (
	"context"
	"errors"
	"github.com/manavmann/distributed-object-storage/internal/metrics"
	"io"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/manavmann/distributed-object-storage/internal/cluster"
	"github.com/manavmann/distributed-object-storage/internal/meta"
	"github.com/manavmann/distributed-object-storage/internal/nodeclient"
)

const (
	readerPayload = "verified bytes"
	readerObject  = "blob-1"
)

// fakeGetter answers each node's Get with the error in errs, or the
// payload when the node has no entry. onMiss, if set, runs once on the
// first ErrNotFound so a test can overwrite the object underneath the
// reader. Every call is recorded by node address.
type fakeGetter struct {
	errs   map[string]error
	onMiss func()

	mu    sync.Mutex
	calls []string
	once  sync.Once
}

func (f *fakeGetter) Get(ctx context.Context, addr, id string) (*nodeclient.Blob, error) {
	f.mu.Lock()
	f.calls = append(f.calls, addr+"/"+id)
	f.mu.Unlock()
	if err := f.errs[addr]; err != nil {
		if errors.Is(err, nodeclient.ErrNotFound) && f.onMiss != nil {
			f.once.Do(f.onMiss)
		}
		return nil, err
	}
	return &nodeclient.Blob{
		ReadCloser: io.NopCloser(strings.NewReader(readerPayload)),
		Length:     int64(len(readerPayload)),
		SHA256:     digest([]byte(readerPayload)),
	}, nil
}

// readerDB is a metadata store with bucket b, nodes n0..n{n-1} at
// http://nX, and b/k committed as blob-1 on every node.
func readerDB(t *testing.T, n int) *meta.DB {
	t.Helper()
	db, err := meta.Open(filepath.Join(t.TempDir(), "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	ctx := context.Background()
	if err := db.CreateBucket(ctx, "b"); err != nil {
		t.Fatal(err)
	}
	for _, id := range ids(n) {
		if err := db.UpsertNode(ctx, meta.Node{ID: id, Addr: addr(id), Status: cluster.StatusUp}); err != nil {
			t.Fatal(err)
		}
	}
	commit(t, db, readerObject, ids(n)...)
	return db
}

func commit(t *testing.T, db *meta.DB, blobID string, nodes ...string) {
	t.Helper()
	obj := meta.Object{Bucket: "b", Key: "k", BlobID: blobID, Size: int64(len(readerPayload)), SHA256: digest([]byte(readerPayload))}
	if err := db.CommitObject(context.Background(), obj, nodes); err != nil {
		t.Fatal(err)
	}
}

func openObject(t *testing.T, db *meta.DB, fake *fakeGetter) (*nodeclient.Blob, string, error) {
	t.Helper()
	obj, err := db.GetObject(context.Background(), "b", "k")
	if err != nil {
		t.Fatal(err)
	}
	r := &Reader{Meta: db, Client: fake, Metrics: metrics.New(), Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	return r.Open(context.Background(), obj)
}

func readAll(t *testing.T, blob *nodeclient.Blob) string {
	t.Helper()
	defer blob.Close()
	b, err := io.ReadAll(blob)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestOpenServesFromAnyReplica(t *testing.T) {
	db := readerDB(t, 3)
	fake := &fakeGetter{errs: map[string]error{addr("n0"): nodeclient.ErrUnavailable}}
	blob, from, err := openObject(t, db, fake)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if from == "n0" || (from != "n1" && from != "n2") {
		t.Fatalf("served from %s", from)
	}
	if got := readAll(t, blob); got != readerPayload {
		t.Fatalf("body = %q", got)
	}
}

func TestAllNotFoundRetriesWithNewBlob(t *testing.T) {
	db := readerDB(t, 2)
	fake := &fakeGetter{errs: map[string]error{addr("n0"): nodeclient.ErrNotFound, addr("n1"): nodeclient.ErrNotFound}}
	// The object is overwritten while the reader is looking for blob-1:
	// blob-2 lands on n2, which serves it.
	fake.onMiss = func() {
		if err := db.UpsertNode(context.Background(), meta.Node{ID: "n2", Addr: addr("n2"), Status: cluster.StatusUp}); err != nil {
			t.Error(err)
		}
		commit(t, db, "blob-2", "n2")
	}
	blob, from, err := openObject(t, db, fake)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if from != "n2" {
		t.Fatalf("served from %s, want n2 (the new blob's holder)", from)
	}
	if got := readAll(t, blob); got != readerPayload {
		t.Fatalf("body = %q", got)
	}
	want := []string{addr("n0") + "/blob-1", addr("n1") + "/blob-1", addr("n2") + "/blob-2"}
	if len(fake.calls) != 3 || fake.calls[2] != want[2] || !sameCalls(fake.calls[:2], want[:2]) {
		t.Fatalf("calls = %v, want %v (in any order for the first two)", fake.calls, want)
	}
}

func TestAllNotFoundUnchangedIsNoHealthyReplica(t *testing.T) {
	db := readerDB(t, 3)
	fake := &fakeGetter{errs: map[string]error{
		addr("n0"): nodeclient.ErrNotFound, addr("n1"): nodeclient.ErrNotFound, addr("n2"): nodeclient.ErrNotFound,
	}}
	if _, _, err := openObject(t, db, fake); !errors.Is(err, ErrNoHealthyReplica) {
		t.Fatalf("Open = %v, want ErrNoHealthyReplica", err)
	}
	if len(fake.calls) != 3 || !sameCalls(fake.calls, []string{addr("n0") + "/blob-1", addr("n1") + "/blob-1", addr("n2") + "/blob-1"}) {
		t.Fatalf("calls = %v, want each node exactly once", fake.calls)
	}
}

func TestIntegrityFailureDropsReplica(t *testing.T) {
	db := readerDB(t, 3)
	// n2 is DOWN so it is tried last: the corrupt n0 and unreachable n1
	// are both visited before n2 serves the object.
	if err := db.SetNodeStatus(context.Background(), "n2", cluster.StatusDown); err != nil {
		t.Fatal(err)
	}
	fake := &fakeGetter{errs: map[string]error{addr("n0"): nodeclient.ErrIntegrity, addr("n1"): nodeclient.ErrUnavailable}}
	blob, from, err := openObject(t, db, fake)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if from != "n2" {
		t.Fatalf("served from %s, want n2", from)
	}
	readAll(t, blob)
	reps, err := db.Replicas(context.Background(), readerObject)
	if err != nil || len(reps) != 2 || reps[0].NodeID != "n1" || reps[1].NodeID != "n2" {
		t.Fatalf("replicas after integrity failure = %+v, %v; want n1 and n2 (n0 dropped, n1 merely unreachable)", reps, err)
	}
}

func TestUpReplicasBeforeDown(t *testing.T) {
	db := readerDB(t, 4)
	ctx := context.Background()
	for _, id := range []string{"n0", "n2"} {
		if err := db.SetNodeStatus(ctx, id, cluster.StatusDown); err != nil {
			t.Fatal(err)
		}
	}
	fake := &fakeGetter{errs: map[string]error{
		addr("n0"): nodeclient.ErrUnavailable, addr("n1"): nodeclient.ErrUnavailable,
		addr("n2"): nodeclient.ErrUnavailable, addr("n3"): nodeclient.ErrUnavailable,
	}}
	if _, _, err := openObject(t, db, fake); !errors.Is(err, ErrNoHealthyReplica) {
		t.Fatalf("Open = %v, want ErrNoHealthyReplica", err)
	}
	if len(fake.calls) != 4 {
		t.Fatalf("calls = %v, want all 4 nodes once", fake.calls)
	}
	up, down := []string{addr("n1") + "/blob-1", addr("n3") + "/blob-1"}, []string{addr("n0") + "/blob-1", addr("n2") + "/blob-1"}
	if !sameCalls(fake.calls[:2], up) || !sameCalls(fake.calls[2:], down) {
		t.Fatalf("calls = %v, want UP nodes %v before DOWN nodes %v", fake.calls, up, down)
	}
}

func TestMismatchedCopyIsSkipped(t *testing.T) {
	db := readerDB(t, 1)
	// Metadata says the object is one byte; the node would serve more.
	if err := db.CommitObject(context.Background(), meta.Object{Bucket: "b", Key: "k", BlobID: "blob-2", Size: 1, SHA256: digest([]byte("x"))}, []string{"n0"}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := openObject(t, db, &fakeGetter{}); !errors.Is(err, ErrNoHealthyReplica) {
		t.Fatalf("Open = %v, want ErrNoHealthyReplica for a copy that does not match metadata", err)
	}
}

func sameCalls(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	seen := map[string]int{}
	for _, s := range a {
		seen[s]++
	}
	for _, s := range b {
		seen[s]--
	}
	for _, n := range seen {
		if n != 0 {
			return false
		}
	}
	return true
}
