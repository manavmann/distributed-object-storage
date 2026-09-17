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
	"github.com/manavmann/distributed-object-storage/internal/meta"
	"github.com/manavmann/distributed-object-storage/internal/storage"
)

func openMeta(t *testing.T, dataDir string) *meta.DB {
	t.Helper()
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		t.Fatal(err)
	}
	db, err := meta.Open(filepath.Join(dataDir, "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

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
	db := openMeta(t, dataDir)
	c, err := New(cfg, Deps{Meta: db, Log: log})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	c.Start(context.Background())
	t.Cleanup(c.Stop)

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
	nodes, err := db.ListNodes(context.Background())
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
	if _, err := db.GetObject(context.Background(), "bkt", "k"); err != nil {
		t.Fatalf("object not in metadata: %v", err)
	}
	if _, err := db.GetBucket(context.Background(), "bkt"); err != nil {
		t.Fatalf("bucket not in metadata: %v", err)
	}
}

// TestStopWaitsForMonitor checks that Stop returns once the monitor
// goroutine has exited, so nothing touches the DB after its owner closes
// it, and that Stop before Start is harmless.
func TestStopWaitsForMonitor(t *testing.T) {
	dataDir := t.TempDir()
	cfg := config.CoordinatorConfig{
		Addr: ":0", DataDir: dataDir,
		MaxObjectSize: 1 << 20, MaxUploads: 1, NodeTimeout: time.Second, HeartbeatTimeout: time.Minute,
	}
	c, err := New(cfg, Deps{Meta: openMeta(t, dataDir), Log: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	c.Stop()
	c.Start(context.Background())
	c.Stop()
	select {
	case <-c.monitorDone:
	default:
		t.Fatal("monitor still running after Stop")
	}
}
