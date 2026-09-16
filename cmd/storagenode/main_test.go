package main

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/manavmann/distributed-object-storage/internal/config"
)

// TestServeWithoutCoordinator runs the node against an unreachable
// coordinator, exercises the API, and checks a clean shutdown.
func TestServeWithoutCoordinator(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	dead, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	coordinator := "http://" + dead.Addr().String()
	dead.Close()

	cfg := config.NodeConfig{
		NodeID: "test", Addr: ln.Addr().String(), AdvertiseAddr: "http://" + ln.Addr().String(),
		DataDir: t.TempDir(), CoordinatorURL: coordinator, HeartbeatInterval: 10 * time.Millisecond,
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- serve(ctx, cfg, ln, slog.New(slog.NewTextHandler(io.Discard, nil))) }()

	base := "http://" + ln.Addr().String()
	resp, err := http.Get(base + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("healthz = %d", resp.StatusCode)
	}

	req, _ := http.NewRequest(http.MethodPut, base+"/blobs/x", strings.NewReader("payload"))
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("PUT = %d", resp.StatusCode)
	}
	resp, err = http.Get(base + "/blobs/x")
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
	case <-time.After(15 * time.Second):
		t.Fatal("serve did not return after cancel")
	}
	if _, err := net.DialTimeout("tcp", ln.Addr().String(), time.Second); err == nil {
		t.Fatal("listener still accepting after shutdown")
	}
}

func TestServeBadDataDir(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	cfg := config.NodeConfig{
		NodeID: "test", Addr: ln.Addr().String(), AdvertiseAddr: "http://h:1",
		DataDir: "\x00", CoordinatorURL: "http://c:1", HeartbeatInterval: time.Second,
	}
	if err := serve(context.Background(), cfg, ln, slog.New(slog.NewTextHandler(io.Discard, nil))); err == nil {
		t.Fatal("serve with unusable data dir returned nil")
	}
}
