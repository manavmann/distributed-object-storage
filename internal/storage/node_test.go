package storage

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"slices"
	"strconv"
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

func newTestNode(t *testing.T) *Node {
	t.Helper()
	n, err := NewNode(t.TempDir(), NodeOptions{
		ID: "n1", HeartbeatInterval: time.Minute, ScrubInterval: time.Hour, ScrubDelay: time.Millisecond,
		Metrics: metrics.New(), Log: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	return n
}

// TestScrubQuarantinesAndCounts corrupts one of three blobs and runs one
// scrub pass: the corrupt blob must be quarantined, gone from the blob
// count and reported by the next heartbeat; the two intact blobs must
// still be served.
func TestScrubQuarantinesAndCounts(t *testing.T) {
	n := newTestNode(t)
	for _, id := range []string{"aaaa", "bbbb", "cccc"} {
		if _, err := n.store.Write(id, strings.NewReader("payload-"+id)); err != nil {
			t.Fatal(err)
		}
	}
	corruptOnDisk(t, filepath.Dir(n.store.blobs), "bbbb", func(raw []byte) []byte {
		raw[len(raw)-1] ^= 0xff
		return raw
	})

	n.scrub(context.Background())

	if got := dirNames(t, n.store.quarantine); len(got) != 1 || got[0] != "bbbb" {
		t.Fatalf("quarantine/ = %v, want [bbbb]", got)
	}
	if count, err := n.store.BlobCount(); err != nil || count != 2 {
		t.Fatalf("BlobCount after scrub = %d, %v; want 2", count, err)
	}
	for _, id := range []string{"aaaa", "cccc"} {
		if got, _ := readAll(t, n.store, id); string(got) != "payload-"+id {
			t.Fatalf("Read %s after scrub = %q", id, got)
		}
	}
	hb := n.status("http://n1:9100")
	if hb.BlobCount != 2 || len(hb.ScrubFailures) != 1 || hb.ScrubFailures[0] != "bbbb" {
		t.Fatalf("heartbeat after scrub = %+v, want blob_count 2 and scrub_failures [bbbb]", hb)
	}
	if got := testutil.ToFloat64(n.metrics.NodeBlobs); got != 2 {
		t.Fatalf("cairn_node_blobs = %v, want 2", got)
	}
}

// TestScrubFailuresDrainOnAck fills the pending list past one heartbeat's
// worth: each heartbeat carries at most maxScrubReport ids, a failed post
// leaves them pending, and an acknowledged one forgets exactly the ids
// it carried.
func TestScrubFailuresDrainOnAck(t *testing.T) {
	n := newTestNode(t)
	total := maxScrubReport + 3
	for i := 0; i < total; i++ {
		n.scrubFailed("blob-" + strconv.Itoa(i))
	}
	first := n.status("http://n1:9100")
	if len(first.ScrubFailures) != maxScrubReport || first.ScrubFailures[0] != "blob-0" {
		t.Fatalf("first heartbeat carries %d ids starting at %q, want %d starting at blob-0", len(first.ScrubFailures), first.ScrubFailures[0], maxScrubReport)
	}
	// Not acknowledged: the same ids go out again.
	if again := n.status("http://n1:9100"); !slices.Equal(again.ScrubFailures, first.ScrubFailures) {
		t.Fatalf("unacknowledged ids changed: %v -> %v", first.ScrubFailures, again.ScrubFailures)
	}
	n.acked(first)
	rest := n.status("http://n1:9100")
	if len(rest.ScrubFailures) != 3 || rest.ScrubFailures[0] != "blob-"+strconv.Itoa(maxScrubReport) {
		t.Fatalf("after ack heartbeat carries %v, want the 3 ids after blob-%d", rest.ScrubFailures, maxScrubReport-1)
	}
	n.acked(rest)
	if last := n.status("http://n1:9100"); len(last.ScrubFailures) != 0 {
		t.Fatalf("after draining heartbeat still carries %v", last.ScrubFailures)
	}
}

// TestScrubLoopReportsAndStops starts the scrub and heartbeat loops with
// short intervals against a fake coordinator: the scrubber must find a
// corrupt blob on its own and the heartbeat must carry its id once, then
// drop it after the coordinator accepts; both loops must exit on cancel.
func TestScrubLoopReportsAndStops(t *testing.T) {
	posts := make(chan Heartbeat, 64)
	coord := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var hb Heartbeat
		if err := json.NewDecoder(r.Body).Decode(&hb); err != nil {
			t.Errorf("decode heartbeat: %v", err)
		}
		w.WriteHeader(http.StatusNoContent)
		posts <- hb
	}))
	defer coord.Close()
	n, err := NewNode(t.TempDir(), NodeOptions{
		ID: "n1", HeartbeatInterval: 5 * time.Millisecond, ScrubInterval: time.Millisecond, ScrubDelay: time.Millisecond,
		Metrics: metrics.New(), Log: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := n.store.Write("aaaa", strings.NewReader("payload")); err != nil {
		t.Fatal(err)
	}
	corruptOnDisk(t, filepath.Dir(n.store.blobs), "aaaa", func(raw []byte) []byte {
		raw[len(raw)-1] ^= 0xff
		return raw
	})
	ctx, cancel := context.WithCancel(context.Background())
	scrubDone := n.StartScrub(ctx)
	heartbeatDone := n.StartHeartbeat(ctx, coord.URL, "http://n1:9100")

	reported := 0
	for reported == 0 {
		select {
		case hb := <-posts:
			if len(hb.ScrubFailures) == 1 && hb.ScrubFailures[0] == "aaaa" {
				reported++
			} else if len(hb.ScrubFailures) != 0 {
				t.Fatalf("heartbeat carried %v, want [aaaa]", hb.ScrubFailures)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("no heartbeat reported the corrupt blob")
		}
	}
	// The coordinator accepted it, so the next heartbeat carries nothing.
	select {
	case hb := <-posts:
		if len(hb.ScrubFailures) != 0 {
			t.Fatalf("heartbeat after ack still carries %v", hb.ScrubFailures)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("no heartbeat after the acknowledged one")
	}
	cancel()
	for _, done := range []<-chan struct{}{scrubDone, heartbeatDone} {
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Fatal("loop did not return after cancel")
		}
	}
	if got := dirNames(t, n.store.quarantine); len(got) != 1 || got[0] != "aaaa" {
		t.Fatalf("quarantine/ = %v, want [aaaa]", got)
	}
}
