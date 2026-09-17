package integration

import (
	"bytes"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/manavmann/distributed-object-storage/internal/cluster"
	"github.com/manavmann/distributed-object-storage/internal/testcluster"
)

// scrape fetches base/metrics and returns the text exposition.
func scrape(t *testing.T, base string) string {
	t.Helper()
	resp, err := http.Get(base + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /metrics = %d: %s", resp.StatusCode, body)
	}
	return string(body)
}

// cairnSeries counts the sample lines (not comments) whose name starts
// with cairn_.
func cairnSeries(text string) int {
	n := 0
	for _, line := range strings.Split(text, "\n") {
		if strings.HasPrefix(line, "cairn_") {
			n++
		}
	}
	return n
}

// hasSample reports whether text has a sample line for name, with any
// labels, holding exactly value.
func hasSample(text, name, value string) bool {
	for _, line := range strings.Split(text, "\n") {
		if !strings.HasPrefix(line, name) {
			continue
		}
		rest := line[len(name):]
		if rest == "" || (rest[0] != ' ' && rest[0] != '{') {
			continue
		}
		if strings.HasSuffix(line, " "+value) {
			return true
		}
	}
	return false
}

// TestMetricsAfterPutGet stores and reads one object, then checks the
// coordinator's and a node's /metrics for the expected series: requests
// keyed by pattern, the replica write count, the cluster gauges and the
// node gauges, with at least ten cairn_ series on each endpoint.
func TestMetricsAfterPutGet(t *testing.T) {
	t.Parallel()
	c := testcluster.New(t, testcluster.Opts{Nodes: 3, RepairInterval: gcInterval})
	cl := c.Client()
	if err := cl.CreateBucket("bkt"); err != nil {
		t.Fatal(err)
	}
	if _, err := cl.Put("bkt", "k", bytes.Repeat([]byte("m"), 512), "text/plain"); err != nil {
		t.Fatal(err)
	}
	if _, err := cl.Get("bkt", "k"); err != nil {
		t.Fatal(err)
	}
	// objects_total is refreshed by the repair tick, so it lags the PUT.
	var text string
	testcluster.WaitFor(t, func() bool {
		text = scrape(t, cl.URL())
		return hasSample(text, "cairn_objects_total", "1")
	}, "objects_total = 1")
	for _, want := range []string{
		`cairn_http_requests_total{method="PUT",route="PUT /v1/{bucket}/{key...}",status="200"} 1`,
		`cairn_http_requests_total{method="GET",route="GET /v1/{bucket}/{key...}",status="200"} 1`,
		`cairn_http_request_duration_seconds_count{method="PUT",route="PUT /v1/{bucket}/{key...}"} 1`,
		`cairn_replica_writes_total{result="ok"} 3`,
		`cairn_nodes_up 3`,
		`cairn_nodes_total 3`,
		`cairn_under_replicated_blobs 0`,
		`cairn_pending_deletes 0`,
		`cairn_integrity_failures_total 0`,
	} {
		if !strings.Contains(text, want+"\n") {
			t.Errorf("coordinator /metrics lacks %q:\n%s", want, text)
		}
	}
	if strings.Contains(text, "/v1/bkt/k") {
		t.Errorf("coordinator /metrics labels a raw path:\n%s", text)
	}
	if n := cairnSeries(text); n < 10 {
		t.Errorf("coordinator /metrics has %d cairn_ series, want >= 10", n)
	}

	// The node gauges are set on each heartbeat, so the blob count
	// appears once the node has reported since the PUT.
	var node string
	testcluster.WaitFor(t, func() bool {
		node = scrape(t, c.NodeURL(0))
		return hasSample(node, "cairn_node_blobs", "1")
	}, "node_blobs = 1")
	for _, want := range []string{
		`cairn_http_requests_total{method="PUT",route="PUT /blobs/{id}",status="201"} 1`,
		`cairn_http_request_duration_seconds_count{method="PUT",route="PUT /blobs/{id}"} 1`,
	} {
		if !strings.Contains(node, want+"\n") {
			t.Errorf("node /metrics lacks %q:\n%s", want, node)
		}
	}
	if hasSample(node, "cairn_node_free_bytes", "0") {
		t.Errorf("node /metrics reports zero free bytes:\n%s", node)
	}
	if n := cairnSeries(node); n < 10 {
		t.Errorf("node /metrics has %d cairn_ series, want >= 10", n)
	}
}

// TestUnderReplicatedGaugeMovesAfterKill kills a holder with a long grace
// so the repair tick sees the blob under-replicated but leaves it alone,
// and checks the gauge and nodes_up follow the transition.
func TestUnderReplicatedGaugeMovesAfterKill(t *testing.T) {
	t.Parallel()
	c := testcluster.New(t, testcluster.Opts{
		Nodes: 3, RF: 3, W: 2,
		HeartbeatInterval: fastHeartbeat, HeartbeatTimeout: fastHeartbeatTimeout,
		RepairInterval: gcInterval, RepairGrace: time.Minute,
	})
	cl := c.Client()
	if err := cl.CreateBucket("bkt"); err != nil {
		t.Fatal(err)
	}
	if _, err := cl.Put("bkt", "k", []byte("under"), "text/plain"); err != nil {
		t.Fatal(err)
	}
	testcluster.WaitFor(t, func() bool {
		return hasSample(scrape(t, cl.URL()), "cairn_under_replicated_blobs", "0")
	}, "under_replicated_blobs = 0 after a tick")

	c.Kill(1)
	waitStatus(t, c, c.NodeID(1), cluster.StatusDown)
	var text string
	testcluster.WaitFor(t, func() bool {
		text = scrape(t, cl.URL())
		return hasSample(text, "cairn_under_replicated_blobs", "1")
	}, "under_replicated_blobs = 1 after kill + tick")
	if !strings.Contains(text, "\ncairn_nodes_up 2\n") || !strings.Contains(text, "\ncairn_nodes_total 3\n") {
		t.Fatalf("nodes gauges after kill:\n%s", text)
	}
	// The node may go DOWN between the tick's repair step and its gauge
	// refresh, so the grace skip can land one tick after the gauge moves.
	testcluster.WaitFor(t, func() bool {
		return strings.Contains(scrape(t, cl.URL()), `cairn_repairs_total{result="skipped"}`)
	}, "repairs_total{skipped} after a grace skip")
}
