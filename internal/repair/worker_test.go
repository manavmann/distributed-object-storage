package repair

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/manavmann/distributed-object-storage/internal/cluster"
	"github.com/manavmann/distributed-object-storage/internal/meta"
	"github.com/manavmann/distributed-object-storage/internal/nodeclient"
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
