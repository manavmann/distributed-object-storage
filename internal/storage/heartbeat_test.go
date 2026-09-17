package storage

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestHeartbeatPostsAndSurvives500s(t *testing.T) {
	posts := make(chan Heartbeat, 16)
	var calls atomic.Int32
	coord := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/internal/heartbeat" {
			t.Errorf("unexpected %s %s", r.Method, r.URL.Path)
		}
		var hb Heartbeat
		if err := json.NewDecoder(r.Body).Decode(&hb); err != nil {
			t.Errorf("decode heartbeat: %v", err)
		}
		if calls.Add(1) <= 2 {
			w.WriteHeader(http.StatusInternalServerError)
		}
		posts <- hb
	}))
	defer coord.Close()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	var seq atomic.Int32
	go func() {
		defer close(done)
		RunHeartbeat(ctx, coord.Client(), coord.URL, 5*time.Millisecond, func() Heartbeat {
			return Heartbeat{NodeID: "n1", Addr: "http://n1:9100", BlobCount: int(seq.Add(1)), FreeBytes: 42}
		}, func(Heartbeat) {}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	}()

	for i := 1; i <= 3; i++ {
		select {
		case hb := <-posts:
			if hb.NodeID != "n1" || hb.Addr != "http://n1:9100" || hb.FreeBytes != 42 || hb.BlobCount != i {
				t.Fatalf("post %d = %+v", i, hb)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("only %d heartbeats received", i-1)
		}
	}
	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("RunHeartbeat did not return after cancel")
	}
}

func TestHeartbeatUnreachableCoordinator(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	url := "http://" + ln.Addr().String()
	ln.Close()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	var calls atomic.Int32
	go func() {
		defer close(done)
		RunHeartbeat(ctx, &http.Client{Timeout: time.Second}, url, time.Millisecond, func() Heartbeat {
			if calls.Add(1) == 3 {
				cancel()
			}
			return Heartbeat{NodeID: "n1"}
		}, func(Heartbeat) {}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("RunHeartbeat did not return")
	}
	if calls.Load() < 3 {
		t.Fatalf("status called %d times, want at least 3", calls.Load())
	}
}
