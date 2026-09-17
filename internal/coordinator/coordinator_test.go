package coordinator

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/manavmann/distributed-object-storage/internal/cluster"
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
		Addr: ":0", DataDir: dataDir, Nodes: []cluster.Node{{ID: "n1", Addr: node.URL}},
		MaxObjectSize: 1 << 20, MaxUploads: 1, NodeTimeout: time.Second,
	}
	c, err := New(cfg, log)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { c.Close() })

	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Fatalf("stale spool file survived New: %v", err)
	}
	nodes, err := c.meta.ListNodes(context.Background())
	if err != nil || len(nodes) != 1 || nodes[0].ID != "n1" || nodes[0].Addr != node.URL {
		t.Fatalf("ListNodes = %+v, %v", nodes, err)
	}

	srv := httptest.NewServer(c.Handler())
	t.Cleanup(srv.Close)
	req, _ := http.NewRequest(http.MethodPut, srv.URL+"/v1/bkt", nil)
	resp, err := http.DefaultClient.Do(req)
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
