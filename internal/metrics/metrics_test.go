package metrics

import (
	"io"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestHandlerServesEveryCollector checks that two Metrics values do not
// collide (own registry each) and that every gauge and counter appears in
// the scrape before anything has been recorded.
func TestHandlerServesEveryCollector(t *testing.T) {
	New()
	m := New()
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	body, _ := io.ReadAll(rec.Result().Body)
	for _, name := range []string{
		"cairn_nodes_up", "cairn_nodes_total", "cairn_under_replicated_blobs", "cairn_pending_deletes",
		"cairn_integrity_failures_total", "cairn_objects_total", "cairn_node_blobs", "cairn_node_free_bytes",
	} {
		if !strings.Contains(string(body), "\n"+name+" ") {
			t.Errorf("%s missing from scrape:\n%s", name, body)
		}
	}
}
