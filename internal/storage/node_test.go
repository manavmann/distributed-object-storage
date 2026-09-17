package storage

import (
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/manavmann/distributed-object-storage/internal/metrics"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// TestStatusSetsNodeGauges writes three blobs and checks that a heartbeat
// status refresh puts the store's own count on cairn_node_blobs and a
// non-zero free-space figure on cairn_node_free_bytes.
func TestStatusSetsNodeGauges(t *testing.T) {
	m := metrics.New()
	n, err := NewNode(t.TempDir(), NodeOptions{
		ID: "n1", HeartbeatInterval: time.Minute, Metrics: m, Log: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"aaaa", "bbbb", "cccc"} {
		if _, err := n.store.Write(id, strings.NewReader("payload-"+id)); err != nil {
			t.Fatal(err)
		}
	}
	hb := n.status("http://n1:9100")
	count, err := n.store.BlobCount()
	if err != nil {
		t.Fatal(err)
	}
	if count != 3 || hb.BlobCount != count {
		t.Fatalf("BlobCount = %d, heartbeat = %d, want 3", count, hb.BlobCount)
	}
	if got := testutil.ToFloat64(m.NodeBlobs); got != float64(count) {
		t.Fatalf("cairn_node_blobs = %v, want %d", got, count)
	}
	if got := testutil.ToFloat64(m.NodeFreeBytes); got <= 0 || got != float64(hb.FreeBytes) {
		t.Fatalf("cairn_node_free_bytes = %v, heartbeat = %d", got, hb.FreeBytes)
	}
}
