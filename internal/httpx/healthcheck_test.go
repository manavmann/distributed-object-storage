package httpx

import (
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHealthcheck(t *testing.T) {
	serve := func(status int) string {
		t.Helper()
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/healthz" {
				t.Errorf("path = %q, want /healthz", r.URL.Path)
			}
			w.WriteHeader(status)
		}))
		t.Cleanup(srv.Close)
		_, port, err := net.SplitHostPort(srv.Listener.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		return ":" + port
	}
	closedPort := func() string {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		addr := ln.Addr().String()
		ln.Close()
		return addr
	}

	if err := Healthcheck(serve(http.StatusOK)); err != nil {
		t.Fatalf("200: %v, want nil", err)
	}
	if err := Healthcheck(serve(http.StatusInternalServerError)); !errors.Is(err, ErrUnhealthy) {
		t.Fatalf("500: %v, want ErrUnhealthy", err)
	}
	if err := Healthcheck(closedPort()); err == nil || errors.Is(err, ErrUnhealthy) {
		t.Fatalf("closed port: %v, want a connection error", err)
	}
	if err := Healthcheck("8080"); err == nil {
		t.Fatal("no port separator: want error")
	}
}
