package nodeclient

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"github.com/manavmann/distributed-object-storage/internal/metrics"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/manavmann/distributed-object-storage/internal/httpx"
	"github.com/manavmann/distributed-object-storage/internal/storage"
)

// newNode runs a real storage node handler and returns its base URL.
func newNode(t *testing.T) string {
	t.Helper()
	store, err := storage.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(storage.NewHandler(store, "", metrics.New(), slog.New(slog.NewTextHandler(io.Discard, nil))))
	t.Cleanup(srv.Close)
	return srv.URL
}

func digest(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func TestRoundTrip(t *testing.T) {
	c, addr := New(""), newNode(t)
	ctx := context.Background()
	payload := bytes.Repeat([]byte("cairn"), 10_000)
	if err := c.Put(ctx, addr, "b1", bytes.NewReader(payload), int64(len(payload)), digest(payload)); err != nil {
		t.Fatalf("Put: %v", err)
	}

	length, sum, err := c.Head(ctx, addr, "b1")
	if err != nil {
		t.Fatalf("Head: %v", err)
	}
	if length != int64(len(payload)) || sum != digest(payload) {
		t.Fatalf("Head = (%d, %s), want (%d, %s)", length, sum, len(payload), digest(payload))
	}

	b, err := c.Get(ctx, addr, "b1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	got, err := io.ReadAll(b)
	b.Close()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, payload) || b.Length != int64(len(payload)) || b.SHA256 != digest(payload) {
		t.Fatalf("Get returned %d bytes, length %d, sha %s", len(got), b.Length, b.SHA256)
	}

	if err := c.Delete(ctx, addr, "b1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := c.Get(ctx, addr, "b1"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get after Delete = %v, want ErrNotFound", err)
	}
}

func TestNotFound(t *testing.T) {
	c, addr := New(""), newNode(t)
	ctx := context.Background()
	if _, err := c.Get(ctx, addr, "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get = %v, want ErrNotFound", err)
	}
	if _, _, err := c.Head(ctx, addr, "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Head = %v, want ErrNotFound", err)
	}
	if err := c.Delete(ctx, addr, "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Delete = %v, want ErrNotFound", err)
	}
}

func TestPutChecksumMismatch(t *testing.T) {
	c, addr := New(""), newNode(t)
	payload := []byte("hello")
	err := c.Put(context.Background(), addr, "b1", bytes.NewReader(payload), int64(len(payload)), digest([]byte("other")))
	if !errors.Is(err, ErrChecksum) {
		t.Fatalf("Put = %v, want ErrChecksum", err)
	}
	if _, err := c.Get(context.Background(), addr, "b1"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("rejected blob still served: %v", err)
	}
}

func TestPutInvalidID(t *testing.T) {
	c, addr := New(""), newNode(t)
	err := c.Put(context.Background(), addr, ".hidden", bytes.NewReader(nil), 0, digest(nil))
	if !errors.Is(err, ErrChecksum) {
		t.Fatalf("Put = %v, want ErrChecksum", err)
	}
}

// stub returns the URL of a handler that answers every request with
// status and the node error body {code, msg}.
func stub(t *testing.T, status int, code string) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		httpx.WriteError(w, r, status, code, "stub")
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

func TestErrorMapping(t *testing.T) {
	cases := []struct {
		name   string
		status int
		code   string
		want   error
	}{
		{"corrupt is integrity", http.StatusInternalServerError, "corrupt", ErrIntegrity},
		{"other 500 is unavailable", http.StatusInternalServerError, "internal", ErrUnavailable},
		{"503 is unavailable", http.StatusServiceUnavailable, "internal", ErrUnavailable},
		{"507 is unavailable", http.StatusInsufficientStorage, "no_space", ErrUnavailable},
		{"422 is checksum", http.StatusUnprocessableEntity, "checksum_mismatch", ErrChecksum},
		{"404 is not found", http.StatusNotFound, "not_found", ErrNotFound},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c, addr := New(""), stub(t, tc.status, tc.code)
			ctx := context.Background()
			if _, err := c.Get(ctx, addr, "x"); !errors.Is(err, tc.want) {
				t.Errorf("Get = %v, want %v", err, tc.want)
			}
			wantHead := tc.want
			if tc.want == ErrIntegrity {
				wantHead = ErrUnavailable // no body on HEAD, so no code to read
			}
			if _, _, err := c.Head(ctx, addr, "x"); !errors.Is(err, wantHead) {
				t.Errorf("Head = %v, want %v", err, wantHead)
			}
			if err := c.Put(ctx, addr, "x", bytes.NewReader([]byte("a")), 1, digest([]byte("a"))); !errors.Is(err, tc.want) {
				t.Errorf("Put = %v, want %v", err, tc.want)
			}
			if err := c.Delete(ctx, addr, "x"); !errors.Is(err, tc.want) {
				t.Errorf("Delete = %v, want %v", err, tc.want)
			}
		})
	}
}

func TestUnreadableErrorBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		io.WriteString(w, "<html>gateway</html>")
	}))
	t.Cleanup(srv.Close)
	if _, err := New("").Get(context.Background(), srv.URL, "x"); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Get = %v, want ErrUnavailable", err)
	}
}

func TestConnectionRefused(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := "http://" + ln.Addr().String()
	ln.Close()
	c := New("")
	if _, err := c.Get(context.Background(), addr, "x"); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Get = %v, want ErrUnavailable", err)
	}
	if err := c.Put(context.Background(), addr, "x", bytes.NewReader([]byte("a")), 1, digest([]byte("a"))); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Put = %v, want ErrUnavailable", err)
	}
}

func TestDeadline(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(func() { close(release); srv.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err := New("").Get(ctx, srv.URL, "x")
	if !errors.Is(err, ErrUnavailable) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Get = %v, want ErrUnavailable wrapping DeadlineExceeded", err)
	}
}

func TestPutTruncatedBodyIsNotStored(t *testing.T) {
	// Declaring more bytes than the reader yields makes the transport fail
	// the request; the node sees a truncated body and never acknowledges.
	c, addr := New(""), newNode(t)
	payload := []byte("short")
	err := c.Put(context.Background(), addr, "b1", bytes.NewReader(payload), int64(len(payload))+10, digest(payload))
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Put = %v, want ErrUnavailable", err)
	}
	if _, err := c.Get(context.Background(), addr, "b1"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("truncated blob served: %v", err)
	}
}

// flakyDialer fails the first dial with a dial error and delegates every
// later one to the real dialer.
type flakyDialer struct {
	real  func(ctx context.Context, network, addr string) (net.Conn, error)
	dials atomic.Int32
}

func (d *flakyDialer) dial(ctx context.Context, network, addr string) (net.Conn, error) {
	if d.dials.Add(1) == 1 {
		return nil, &net.OpError{Op: "dial", Net: network, Err: errors.New("injected")}
	}
	return d.real(ctx, network, addr)
}

func TestPutRetriesFailedDialOnce(t *testing.T) {
	c, addr := New(""), newNode(t)
	tr := c.http.Transport.(*http.Transport)
	d := &flakyDialer{real: tr.DialContext}
	tr.DialContext = d.dial
	payload := []byte("retry me")
	if err := c.Put(context.Background(), addr, "b1", bytes.NewReader(payload), int64(len(payload)), digest(payload)); err != nil {
		t.Fatalf("Put after one failed dial = %v", err)
	}
	if n := d.dials.Load(); n != 2 {
		t.Fatalf("%d dials, want 2", n)
	}
	length, sum, err := c.Head(context.Background(), addr, "b1")
	if err != nil || length != int64(len(payload)) || sum != digest(payload) {
		t.Fatalf("Head after retried Put = (%d, %s, %v)", length, sum, err)
	}
}

func TestPutDoesNotRetryNodeErrors(t *testing.T) {
	var requests atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		httpx.WriteError(w, r, http.StatusInternalServerError, "internal", "stub")
	}))
	t.Cleanup(srv.Close)
	err := New("").Put(context.Background(), srv.URL, "x", bytes.NewReader([]byte("a")), 1, digest([]byte("a")))
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Put = %v, want ErrUnavailable", err)
	}
	if n := requests.Load(); n != 1 {
		t.Fatalf("node saw %d requests, want 1", n)
	}
}
