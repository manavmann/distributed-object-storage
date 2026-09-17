package main

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/manavmann/distributed-object-storage/internal/metrics"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/manavmann/distributed-object-storage/internal/config"
	"github.com/manavmann/distributed-object-storage/internal/storage"
)

// TestServeRoundTrip runs the coordinator against one in-process node,
// stores and reads an object, and checks a clean shutdown.
func TestServeRoundTrip(t *testing.T) {
	if version == "" {
		t.Fatal("version is empty")
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	store, err := storage.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	node := httptest.NewServer(storage.NewHandler(store, "", metrics.New(), log))
	t.Cleanup(node.Close)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	cfg := config.CoordinatorConfig{
		Addr: ln.Addr().String(), DataDir: t.TempDir(),
		MaxObjectSize: 1 << 20, MaxUploads: 2, NodeTimeout: time.Second, HeartbeatTimeout: time.Minute, RF: 1, W: 1,
		RepairInterval: time.Minute, RepairGrace: time.Minute,
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- serve(ctx, cfg, ln, log) }()

	base := "http://" + ln.Addr().String()
	resp, err := http.Get(base + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("healthz = %d", resp.StatusCode)
	}
	hb, _ := json.Marshal(storage.Heartbeat{NodeID: "n1", Addr: node.URL})
	resp, err = http.Post(base+"/internal/heartbeat", "application/json", bytes.NewReader(hb))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("heartbeat = %d", resp.StatusCode)
	}
	req, _ := http.NewRequest(http.MethodPut, base+"/v1/bkt", nil)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create bucket = %d", resp.StatusCode)
	}
	req, _ = http.NewRequest(http.MethodPut, base+"/v1/bkt/k", bytes.NewReader([]byte("payload")))
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT = %d", resp.StatusCode)
	}
	resp, err = http.Get(base + "/v1/bkt/k")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || string(body) != "payload" {
		t.Fatalf("GET = %d %q", resp.StatusCode, body)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("serve returned %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("serve did not return after cancel")
	}
}
