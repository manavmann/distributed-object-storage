package coordinator

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/manavmann/distributed-object-storage/internal/config"
	"github.com/manavmann/distributed-object-storage/internal/storage"
)

func TestNewWiresNodeAndServes(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	store, err := storage.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	node := httptest.NewServer(storage.NewHandler(store, log))
	t.Cleanup(node.Close)

	dataDir := filepath.Join(t.TempDir(), "coord")
	stale := filepath.Join(dataDir, "spool", "leftover")
	if err := os.MkdirAll(filepath.Dir(stale), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(stale, []byte("partial"), 0o600); err != nil {
		t.Fatal(err)
	}

	cfg := config.CoordinatorConfig{
		Addr: ":0", DataDir: dataDir,
		MaxObjectSize: 1 << 20, MaxUploads: 1, NodeTimeout: time.Second, HeartbeatTimeout: time.Minute,
	}
	c, err := New(cfg, log)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { c.Close() })

	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Fatalf("stale spool file survived New: %v", err)
	}

	srv := httptest.NewServer(c.Handler())
	t.Cleanup(srv.Close)
	hb, _ := json.Marshal(storage.Heartbeat{NodeID: "n1", Addr: node.URL})
	resp, err := http.Post(srv.URL+"/internal/heartbeat", "application/json", bytes.NewReader(hb))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("heartbeat = %d", resp.StatusCode)
	}
	nodes, err := c.meta.ListNodes(context.Background())
	if err != nil || len(nodes) != 1 || nodes[0].ID != "n1" || nodes[0].Addr != node.URL {
		t.Fatalf("ListNodes = %+v, %v", nodes, err)
	}

	req, _ := http.NewRequest(http.MethodPut, srv.URL+"/v1/bkt", nil)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create bucket = %d", resp.StatusCode)
	}
	req, _ = http.NewRequest(http.MethodPut, srv.URL+"/v1/bkt/k", bytes.NewReader([]byte("hello")))
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT = %d", resp.StatusCode)
	}
	resp, err = http.Get(srv.URL + "/v1/bkt/k")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || string(body) != "hello" {
		t.Fatalf("GET = %d %q", resp.StatusCode, body)
	}
	if _, err := c.meta.GetObject(context.Background(), "bkt", "k"); err != nil {
		t.Fatalf("object not in metadata: %v", err)
	}
	if _, err := c.meta.GetBucket(context.Background(), "bkt"); err != nil {
		t.Fatalf("bucket not in metadata: %v", err)
	}
}

// TestCloseStopsMonitor checks that Close returns once the monitor
// goroutine has exited, so nothing touches the DB after it is closed.
func TestCloseStopsMonitor(t *testing.T) {
	cfg := config.CoordinatorConfig{
		Addr: ":0", DataDir: t.TempDir(),
		MaxObjectSize: 1 << 20, MaxUploads: 1, NodeTimeout: time.Second, HeartbeatTimeout: time.Minute,
	}
	c, err := New(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case <-c.monitorDone:
	default:
		t.Fatal("monitor still running after Close")
	}
}
