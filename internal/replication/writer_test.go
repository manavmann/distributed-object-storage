package replication

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"github.com/manavmann/distributed-object-storage/internal/metrics"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/manavmann/distributed-object-storage/internal/cluster"
	"github.com/manavmann/distributed-object-storage/internal/meta"
	"github.com/manavmann/distributed-object-storage/internal/placement"
)

var errInjected = errors.New("injected")

// call is what the fake saw on one Put, after reading the whole body.
type call struct {
	addr string
	id   string
	body []byte
	sha  string
}

// fakeClient records every Put. Nodes listed in fail answer errInjected
// after reading the body; nodes listed in hang block until ctx is done.
type fakeClient struct {
	fail map[string]bool
	hang map[string]bool

	mu    sync.Mutex
	calls []call
}

func (f *fakeClient) Put(ctx context.Context, addr, id string, r io.Reader, size int64, sha string) error {
	body, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	f.mu.Lock()
	f.calls = append(f.calls, call{addr: addr, id: id, body: body, sha: sha})
	f.mu.Unlock()
	switch {
	case f.hang[addr]:
		<-ctx.Done()
		return ctx.Err()
	case f.fail[addr]:
		return errInjected
	}
	return nil
}

func (f *fakeClient) snapshot() []call {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]call(nil), f.calls...)
}

// addr is the fake address node id heartbeats from.
func addr(id string) string { return "http://" + id }

// newRegistry returns a registry with n UP nodes named n0..n{n-1}.
func newRegistry(t *testing.T, n int) *cluster.Registry {
	t.Helper()
	db, err := meta.Open(filepath.Join(t.TempDir(), "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	reg, err := cluster.Load(context.Background(), db, time.Minute, time.Now, metrics.New(), slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		id := "n" + strconv.Itoa(i)
		if err := reg.Heartbeat(context.Background(), cluster.Heartbeat{NodeID: id, Addr: addr(id)}); err != nil {
			t.Fatal(err)
		}
	}
	return reg
}

func ids(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = "n" + strconv.Itoa(i)
	}
	return out
}

// spoolOf writes payload to a temp file and returns it open for reading.
func spoolOf(t *testing.T, payload []byte) *os.File {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "spool")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.Close() })
	if _, err := f.Write(payload); err != nil {
		t.Fatal(err)
	}
	return f
}

func digest(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

type fixture struct {
	w       *Writer
	fake    *fakeClient
	payload []byte
	spool   *os.File
}

func newFixture(t *testing.T, nodes, n, w int, fake *fakeClient) fixture {
	t.Helper()
	payload := bytes.Repeat([]byte("cairn"), 4000)
	return fixture{
		w:       &Writer{N: n, W: w, Client: fake, Registry: newRegistry(t, nodes), Timeout: time.Second},
		fake:    fake,
		payload: payload,
		spool:   spoolOf(t, payload),
	}
}

func (f fixture) write(t *testing.T, key string) (Result, error) {
	t.Helper()
	return f.w.Write(context.Background(), key, "blob1", int64(len(f.payload)), digest(f.payload), f.spool)
}

func TestSuccessWritesTargetsInPlacementOrder(t *testing.T) {
	f := newFixture(t, 5, 3, 2, &fakeClient{})
	res, err := f.write(t, "k")
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	targets, _ := placement.Targets("k", ids(5), 3)
	if !slices.Equal(res.Succeeded, targets) || len(res.Failed) != 0 {
		t.Fatalf("Succeeded = %v, Failed = %v; want %v", res.Succeeded, res.Failed, targets)
	}
	if calls := f.fake.snapshot(); len(calls) != 3 {
		t.Fatalf("%d node writes, want 3 (no fallbacks once every target succeeded)", len(calls))
	}
}

func TestFallbackReplacesFailedTarget(t *testing.T) {
	targets, fallbacks := placement.Targets("k", ids(5), 3)
	f := newFixture(t, 5, 3, 3, &fakeClient{fail: map[string]bool{addr(targets[1]): true}})
	res, err := f.write(t, "k")
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	want := []string{targets[0], targets[2], fallbacks[0]}
	if !slices.Equal(res.Succeeded, want) {
		t.Fatalf("Succeeded = %v, want %v", res.Succeeded, want)
	}
	if len(res.Failed) != 1 || res.Failed[0].NodeID != targets[1] || !errors.Is(res.Failed[0].Err, errInjected) {
		t.Fatalf("Failed = %+v, want %s with errInjected", res.Failed, targets[1])
	}
}

func TestQuorumMetSkipsFallbacks(t *testing.T) {
	targets, fallbacks := placement.Targets("k", ids(5), 3)
	f := newFixture(t, 5, 3, 2, &fakeClient{fail: map[string]bool{addr(targets[0]): true}})
	res, err := f.write(t, "k")
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if !slices.Equal(res.Succeeded, targets[1:]) {
		t.Fatalf("Succeeded = %v, want %v", res.Succeeded, targets[1:])
	}
	for _, c := range f.fake.snapshot() {
		if c.addr == addr(fallbacks[0]) || c.addr == addr(fallbacks[1]) {
			t.Fatalf("fallback %s written although W was already met", c.addr)
		}
	}
}

func TestBelowQuorumReturnsPartialResult(t *testing.T) {
	targets, fallbacks := placement.Targets("k", ids(4), 3)
	fail := map[string]bool{addr(targets[0]): true, addr(targets[2]): true, addr(fallbacks[0]): true}
	f := newFixture(t, 4, 3, 2, &fakeClient{fail: fail})
	res, err := f.write(t, "k")
	if !errors.Is(err, ErrQuorumUnavailable) {
		t.Fatalf("Write = %v, want ErrQuorumUnavailable", err)
	}
	if !slices.Equal(res.Succeeded, []string{targets[1]}) {
		t.Fatalf("Succeeded = %v, want just %s so the caller can queue it", res.Succeeded, targets[1])
	}
	if len(res.Failed) != 3 {
		t.Fatalf("Failed = %+v, want 3 attempts", res.Failed)
	}
	if calls := f.fake.snapshot(); len(calls) != 4 {
		t.Fatalf("%d node writes, want all 4 healthy nodes tried", len(calls))
	}
}

func TestPreCheckTouchesNoNode(t *testing.T) {
	f := newFixture(t, 1, 3, 2, &fakeClient{})
	res, err := f.write(t, "k")
	if !errors.Is(err, ErrQuorumUnavailable) {
		t.Fatalf("Write = %v, want ErrQuorumUnavailable", err)
	}
	if len(res.Succeeded) != 0 || len(res.Failed) != 0 {
		t.Fatalf("Result = %+v, want empty", res)
	}
	if calls := f.fake.snapshot(); len(calls) != 0 {
		t.Fatalf("%d node writes with too few healthy nodes, want 0", len(calls))
	}
}

func TestEachNodeGetsItsOwnFullReader(t *testing.T) {
	f := newFixture(t, 4, 4, 4, &fakeClient{})
	res, err := f.write(t, "k")
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if len(res.Succeeded) != 4 {
		t.Fatalf("Succeeded = %v, want 4", res.Succeeded)
	}
	calls := f.fake.snapshot()
	if len(calls) != 4 {
		t.Fatalf("%d node writes, want 4", len(calls))
	}
	for _, c := range calls {
		if !bytes.Equal(c.body, f.payload) || c.sha != digest(f.payload) || c.id != "blob1" {
			t.Fatalf("node %s received %d bytes sha %s id %s; want %d bytes sha %s",
				c.addr, len(c.body), c.sha, c.id, len(f.payload), digest(f.payload))
		}
	}
}

func TestHungTargetIsNotRecorded(t *testing.T) {
	targets, _ := placement.Targets("k", ids(3), 3)
	f := newFixture(t, 3, 3, 2, &fakeClient{hang: map[string]bool{addr(targets[1]): true}})
	f.w.Timeout = 100 * time.Millisecond
	start := time.Now()
	res, err := f.write(t, "k")
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if !slices.Equal(res.Succeeded, []string{targets[0], targets[2]}) {
		t.Fatalf("Succeeded = %v, want %v", res.Succeeded, []string{targets[0], targets[2]})
	}
	if len(res.Failed) != 1 || res.Failed[0].NodeID != targets[1] || !errors.Is(res.Failed[0].Err, context.DeadlineExceeded) {
		t.Fatalf("Failed = %+v, want %s with DeadlineExceeded", res.Failed, targets[1])
	}
	if elapsed < f.w.Timeout || elapsed > 10*f.w.Timeout {
		t.Fatalf("Write took %s with a %s per-node timeout", elapsed, f.w.Timeout)
	}
}
