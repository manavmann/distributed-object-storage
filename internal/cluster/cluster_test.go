package cluster

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/manavmann/distributed-object-storage/internal/meta"
)

// clock is a fake time source driven by tests.
type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *clock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

const timeout = 15 * time.Second

func openMeta(t *testing.T) *meta.DB {
	t.Helper()
	db, err := meta.Open(filepath.Join(t.TempDir(), "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func newRegistry(t *testing.T, db *meta.DB) (*Registry, *clock) {
	t.Helper()
	clk := &clock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	r, err := Load(context.Background(), db, timeout, clk.now, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	return r, clk
}

func dbNode(t *testing.T, db *meta.DB, id string) meta.Node {
	t.Helper()
	nodes, err := db.ListNodes(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range nodes {
		if n.ID == id {
			return n
		}
	}
	t.Fatalf("node %s not in metadata: %+v", id, nodes)
	return meta.Node{}
}

func TestUpDownUpTransitions(t *testing.T) {
	ctx := context.Background()
	db := openMeta(t)
	r, clk := newRegistry(t, db)
	hb := Heartbeat{NodeID: "n1", Addr: "http://n1:9000", BlobCount: 3, FreeBytes: 42}
	if err := r.Heartbeat(ctx, hb); err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}
	if got := r.Healthy(); len(got) != 1 || got[0].ID != "n1" || got[0].Status != StatusUp || got[0].BlobCount != 3 || got[0].FreeBytes != 42 {
		t.Fatalf("Healthy after first heartbeat = %+v", got)
	}
	if row := dbNode(t, db, "n1"); row.Status != StatusUp || row.Addr != hb.Addr {
		t.Fatalf("first sight not persisted: %+v", row)
	}

	// Within the timeout the node stays UP.
	clk.advance(timeout)
	r.sweep(ctx)
	if len(r.Healthy()) != 1 {
		t.Fatal("node marked DOWN before timeout elapsed")
	}

	// One tick past the timeout it goes DOWN, in memory and on disk.
	clk.advance(time.Millisecond)
	r.sweep(ctx)
	if got := r.Healthy(); len(got) != 0 {
		t.Fatalf("Healthy after timeout = %+v", got)
	}
	n, ok := r.Get("n1")
	if !ok || n.Status != StatusDown || !n.StatusChangedAt.Equal(clk.now()) {
		t.Fatalf("Get after timeout = %+v, %v", n, ok)
	}
	if row := dbNode(t, db, "n1"); row.Status != StatusDown || row.StatusChangedAt == 0 {
		t.Fatalf("DOWN not persisted: %+v", row)
	}

	// A heartbeat brings it back UP.
	clk.advance(time.Second)
	if err := r.Heartbeat(ctx, hb); err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}
	n, _ = r.Get("n1")
	if n.Status != StatusUp || !n.StatusChangedAt.Equal(clk.now()) || !n.LastSeen.Equal(clk.now()) {
		t.Fatalf("Get after recovery = %+v", n)
	}
	if row := dbNode(t, db, "n1"); row.Status != StatusUp {
		t.Fatalf("UP not persisted: %+v", row)
	}
	if all := r.All(); len(all) != 1 || all[0].ID != "n1" {
		t.Fatalf("All = %+v", all)
	}
}

func TestSteadyHeartbeatDoesNotWrite(t *testing.T) {
	ctx := context.Background()
	db := openMeta(t)
	r, clk := newRegistry(t, db)
	hb := Heartbeat{NodeID: "n1", Addr: "http://n1:9000"}
	if err := r.Heartbeat(ctx, hb); err != nil {
		t.Fatal(err)
	}
	before := dbNode(t, db, "n1")
	for i := 0; i < 5; i++ {
		clk.advance(time.Second)
		hb.BlobCount++
		if err := r.Heartbeat(ctx, hb); err != nil {
			t.Fatal(err)
		}
		r.sweep(ctx)
	}
	if after := dbNode(t, db, "n1"); after != before {
		t.Fatalf("steady heartbeats touched metadata: before %+v after %+v", before, after)
	}
	if n, _ := r.Get("n1"); n.BlobCount != 5 || !n.LastSeen.Equal(clk.now()) {
		t.Fatalf("in-memory stats not refreshed: %+v", n)
	}
}

func TestAddrRefresh(t *testing.T) {
	ctx := context.Background()
	db := openMeta(t)
	r, _ := newRegistry(t, db)
	if err := r.Heartbeat(ctx, Heartbeat{NodeID: "n1", Addr: "http://old:1"}); err != nil {
		t.Fatal(err)
	}
	if err := r.Heartbeat(ctx, Heartbeat{NodeID: "n1", Addr: "http://new:2"}); err != nil {
		t.Fatal(err)
	}
	if n, _ := r.Get("n1"); n.Addr != "http://new:2" || n.Status != StatusUp {
		t.Fatalf("Get = %+v", n)
	}
	if row := dbNode(t, db, "n1"); row.Addr != "http://new:2" {
		t.Fatalf("addr change not persisted: %+v", row)
	}
}

func TestLoadThenTimeout(t *testing.T) {
	ctx := context.Background()
	db := openMeta(t)
	for _, n := range []meta.Node{
		{ID: "b", Addr: "http://b:1", Status: StatusUp},
		{ID: "a", Addr: "http://a:1", Status: StatusUp},
		{ID: "c", Addr: "http://c:1", Status: StatusDown},
	} {
		if err := db.UpsertNode(ctx, n); err != nil {
			t.Fatal(err)
		}
	}
	r, clk := newRegistry(t, db)
	healthy := r.Healthy()
	if len(healthy) != 2 || healthy[0].ID != "a" || healthy[1].ID != "b" {
		t.Fatalf("Healthy after Load = %+v", healthy)
	}
	if all := r.All(); len(all) != 3 || all[2].ID != "c" || all[2].Status != StatusDown {
		t.Fatalf("All after Load = %+v", all)
	}
	if _, ok := r.Get("zzz"); ok {
		t.Fatal("Get reported an unknown node")
	}

	// Only b heartbeats; a times out, c stays DOWN.
	clk.advance(timeout)
	if err := r.Heartbeat(ctx, Heartbeat{NodeID: "b", Addr: "http://b:1"}); err != nil {
		t.Fatal(err)
	}
	clk.advance(time.Millisecond)
	r.sweep(ctx)
	if healthy := r.Healthy(); len(healthy) != 1 || healthy[0].ID != "b" {
		t.Fatalf("Healthy after timeout = %+v", healthy)
	}
	if row := dbNode(t, db, "a"); row.Status != StatusDown {
		t.Fatalf("a not persisted DOWN: %+v", row)
	}
	if row := dbNode(t, db, "b"); row.Status != StatusUp {
		t.Fatalf("b changed: %+v", row)
	}
}

func TestInvalidHeartbeat(t *testing.T) {
	r, _ := newRegistry(t, openMeta(t))
	for _, hb := range []Heartbeat{
		{NodeID: "", Addr: "http://n1:1"},
		{NodeID: "n1", Addr: ""},
		{NodeID: "n1", Addr: "n1:9000"},
	} {
		if err := r.Heartbeat(context.Background(), hb); !errors.Is(err, ErrInvalidHeartbeat) {
			t.Errorf("Heartbeat(%+v) = %v, want ErrInvalidHeartbeat", hb, err)
		}
	}
	if all := r.All(); len(all) != 0 {
		t.Fatalf("rejected heartbeat registered a node: %+v", all)
	}
}

func TestHealthyIsACopy(t *testing.T) {
	r, _ := newRegistry(t, openMeta(t))
	if err := r.Heartbeat(context.Background(), Heartbeat{NodeID: "n1", Addr: "http://a:1"}); err != nil {
		t.Fatal(err)
	}
	r.Healthy()[0].Addr = "changed"
	if got := r.Healthy()[0].Addr; got != "http://a:1" {
		t.Fatalf("caller mutated registry: %q", got)
	}
}

// TestConcurrentAccess runs heartbeats, sweeps and readers together; it
// exists for -race.
func TestConcurrentAccess(t *testing.T) {
	ctx := context.Background()
	r, clk := newRegistry(t, openMeta(t))
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		id := "n" + string(rune('0'+i))
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				if err := r.Heartbeat(ctx, Heartbeat{NodeID: id, Addr: "http://" + id + ":1", BlobCount: j}); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for j := 0; j < 50; j++ {
			clk.advance(timeout)
			r.sweep(ctx)
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		for j := 0; j < 200; j++ {
			r.Healthy()
			r.All()
			r.Get("n1")
		}
	}()
	wg.Wait()
	if all := r.All(); len(all) != 4 {
		t.Fatalf("All = %+v", all)
	}
}
