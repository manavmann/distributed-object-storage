package storage

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/manavmann/distributed-object-storage/internal/metrics"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strconv"
	"syscall"
	"testing"

	"github.com/manavmann/distributed-object-storage/internal/httpx"
)

func newServer(t *testing.T) (*httptest.Server, *Store, string) {
	t.Helper()
	s, root := openStore(t)
	srv := httptest.NewServer(NewHandler(s, "", metrics.New(), slog.New(slog.NewTextHandler(io.Discard, nil))))
	t.Cleanup(srv.Close)
	return srv, s, root
}

// do sends method to path with body and optional header pairs and returns
// the response with its body fully read.
func do(t *testing.T, srv *httptest.Server, method, path string, body io.Reader, hdr ...string) (*http.Response, []byte) {
	t.Helper()
	req, err := http.NewRequest(method, srv.URL+path, body)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i+1 < len(hdr); i += 2 {
		req.Header.Set(hdr[i], hdr[i+1])
	}
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("%s %s: read body: %v", method, path, err)
	}
	return resp, b
}

func errBody(t *testing.T, body []byte) httpx.ErrorBody {
	t.Helper()
	var e httpx.ErrorBody
	if err := json.Unmarshal(body, &e); err != nil {
		t.Fatalf("error body %q: %v", body, err)
	}
	return e
}

func hexSum(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func TestStatusMatrix(t *testing.T) {
	srv, _, root := newServer(t)
	payload := []byte("status matrix payload")

	steps := []struct {
		name         string
		method, path string
		body         []byte
		hdr          []string
		before       func()
		status       int
		code         string
	}{
		{name: "healthz", method: "GET", path: "/healthz", status: 200},
		{name: "get missing", method: "GET", path: "/blobs/nope", status: 404, code: "not_found"},
		{name: "head missing", method: "HEAD", path: "/blobs/nope", status: 404},
		{name: "delete missing", method: "DELETE", path: "/blobs/nope", status: 404, code: "not_found"},
		{name: "get invalid id", method: "GET", path: "/blobs/.hidden", status: 404, code: "invalid_id"},
		{name: "put invalid id", method: "PUT", path: "/blobs/.hidden", body: payload, status: 422, code: "invalid_id"},
		{name: "put encoded slash", method: "PUT", path: "/blobs/a%2Fb", body: payload, status: 422, code: "invalid_id"},
		{name: "put digest mismatch", method: "PUT", path: "/blobs/bad", body: payload,
			hdr: []string{ContentSHA256Header, hexSum([]byte("other"))}, status: 422, code: "checksum_mismatch"},
		{name: "get after mismatch", method: "GET", path: "/blobs/bad", status: 404, code: "not_found"},
		{name: "put", method: "PUT", path: "/blobs/b1", body: payload, status: 201},
		{name: "put digest ok", method: "PUT", path: "/blobs/b2", body: payload,
			hdr: []string{ContentSHA256Header, hexSum(payload)}, status: 201},
		{name: "get", method: "GET", path: "/blobs/b1", status: 200},
		{name: "head", method: "HEAD", path: "/blobs/b1", status: 200},
		{name: "delete", method: "DELETE", path: "/blobs/b1", status: 204},
		{name: "get deleted", method: "GET", path: "/blobs/b1", status: 404, code: "not_found"},
		{name: "delete again", method: "DELETE", path: "/blobs/b1", status: 404, code: "not_found"},
		{name: "get corrupt", method: "GET", path: "/blobs/b2", status: 500, code: "corrupt",
			before: func() {
				corruptOnDisk(t, root, "b2", func(b []byte) []byte { b[len(b)-1] ^= 0xff; return b })
			}},
		{name: "get after corrupt", method: "GET", path: "/blobs/b2", status: 404, code: "not_found"},
		{name: "post not allowed", method: "POST", path: "/blobs/b1", body: payload, status: 405},
		{name: "unknown path", method: "GET", path: "/nope", status: 404},
	}
	for _, st := range steps {
		t.Run(st.name, func(t *testing.T) {
			if st.before != nil {
				st.before()
			}
			var body io.Reader
			if st.body != nil {
				body = bytes.NewReader(st.body)
			}
			rid := "rid-" + st.name
			resp, got := do(t, srv, st.method, st.path, body, append(st.hdr, httpx.RequestIDHeader, rid)...)
			if resp.StatusCode != st.status {
				t.Fatalf("status = %d, want %d; body %s", resp.StatusCode, st.status, got)
			}
			if resp.Header.Get(httpx.RequestIDHeader) != rid {
				t.Fatalf("request id header = %q, want %q", resp.Header.Get(httpx.RequestIDHeader), rid)
			}
			if st.code == "" {
				return
			}
			e := errBody(t, got)
			if e.Error.Code != st.code || e.RequestID != rid {
				t.Fatalf("error body = %+v, want code %s and request_id %s", e, st.code, rid)
			}
		})
	}
}

func TestPutGetBodyHash(t *testing.T) {
	srv, _, _ := newServer(t)
	payload := bytes.Repeat([]byte("cairn-body-hash-"), 4096)
	want := hexSum(payload)

	resp, body := do(t, srv, "PUT", "/blobs/h1", bytes.NewReader(payload))
	if resp.StatusCode != 201 {
		t.Fatalf("PUT status = %d: %s", resp.StatusCode, body)
	}
	var pr putResponse
	if err := json.Unmarshal(body, &pr); err != nil {
		t.Fatalf("PUT body %q: %v", body, err)
	}
	if pr.BlobID != "h1" || pr.Length != uint64(len(payload)) || pr.SHA256 != want {
		t.Fatalf("PUT body = %+v, want h1/%d/%s", pr, len(payload), want)
	}

	resp, got := do(t, srv, "GET", "/blobs/h1", nil)
	if resp.StatusCode != 200 {
		t.Fatalf("GET status = %d", resp.StatusCode)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("GET body differs: %d bytes, want %d", len(got), len(payload))
	}
	if h := resp.Header.Get(ContentSHA256Header); h != want {
		t.Fatalf("%s = %q, want %q", ContentSHA256Header, h, want)
	}
	if cl := resp.Header.Get("Content-Length"); cl != strconv.Itoa(len(payload)) {
		t.Fatalf("Content-Length = %q, want %d", cl, len(payload))
	}

	resp, got = do(t, srv, "HEAD", "/blobs/h1", nil)
	if resp.StatusCode != 200 || len(got) != 0 {
		t.Fatalf("HEAD status = %d, body %d bytes", resp.StatusCode, len(got))
	}
	if h := resp.Header.Get(ContentSHA256Header); h != want {
		t.Fatalf("HEAD %s = %q, want %q", ContentSHA256Header, h, want)
	}
	if resp.ContentLength != int64(len(payload)) {
		t.Fatalf("HEAD Content-Length = %d, want %d", resp.ContentLength, len(payload))
	}
}

// patternReader yields a deterministic byte stream without holding it.
type patternReader struct{ n int64 }

func (p *patternReader) Read(b []byte) (int, error) {
	for i := range b {
		b[i] = byte(p.n + int64(i))
	}
	p.n += int64(len(b))
	return len(b), nil
}

func TestLargePutDoesNotBuffer(t *testing.T) {
	srv, s, _ := newServer(t)
	const size = 64 << 20

	digest := sha256.New()
	if _, err := io.Copy(digest, io.LimitReader(&patternReader{}, size)); err != nil {
		t.Fatal(err)
	}
	want := hex.EncodeToString(digest.Sum(nil))

	req, err := http.NewRequest("PUT", srv.URL+"/blobs/big", io.LimitReader(&patternReader{}, size))
	if err != nil {
		t.Fatal(err)
	}
	req.ContentLength = size

	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	runtime.ReadMemStats(&after)

	if resp.StatusCode != 201 {
		t.Fatalf("status = %d: %s", resp.StatusCode, body)
	}
	const limit = 32 << 20
	heap := int64(after.HeapAlloc) - int64(before.HeapAlloc)
	total := after.TotalAlloc - before.TotalAlloc
	t.Logf("HeapAlloc delta %d, TotalAlloc delta %d", heap, total)
	if heap >= limit {
		t.Fatalf("HeapAlloc grew by %d bytes during a %d byte PUT, want < %d", heap, size, limit)
	}
	if total >= limit {
		t.Fatalf("TotalAlloc grew by %d bytes during a %d byte PUT, want < %d", total, size, limit)
	}

	b, err := s.Read("big")
	if err != nil {
		t.Fatal(err)
	}
	b.Close()
	if got := hex.EncodeToString(b.Header.SHA256[:]); got != want || b.Header.Length != size {
		t.Fatalf("stored %d bytes with digest %s, want %d / %s", b.Header.Length, got, size, want)
	}
}

func TestStatusForNoSpace(t *testing.T) {
	err := fmt.Errorf("storage: write x: %w", syscall.ENOSPC)
	if status, code := statusFor(err, 422); status != 507 || code != "no_space" {
		t.Fatalf("statusFor(ENOSPC) = %d %s, want 507 no_space", status, code)
	}
	if status, code := statusFor(fmt.Errorf("boom"), 422); status != 500 || code != "internal" {
		t.Fatalf("statusFor(other) = %d %s, want 500 internal", status, code)
	}
}

func TestBlobCountAndFreeBytes(t *testing.T) {
	s, _ := openStore(t)
	if n, err := s.BlobCount(); err != nil || n != 0 {
		t.Fatalf("BlobCount = %d, %v; want 0", n, err)
	}
	mustWrite(t, s, "a", []byte("a"))
	mustWrite(t, s, "b", []byte("b"))
	if n, err := s.BlobCount(); err != nil || n != 2 {
		t.Fatalf("BlobCount = %d, %v; want 2", n, err)
	}
	if free, err := s.FreeBytes(); err != nil || free == 0 {
		t.Fatalf("FreeBytes = %d, %v; want > 0", free, err)
	}
}

// TestBlobsRequireSecret builds a node with a cluster secret and checks
// that /blobs/* is closed without it, open with it, and that /healthz
// and /metrics never ask for it.
func TestBlobsRequireSecret(t *testing.T) {
	s, _ := openStore(t)
	srv := httptest.NewServer(NewHandler(s, "s3cret", metrics.New(), slog.New(slog.NewTextHandler(io.Discard, nil))))
	t.Cleanup(srv.Close)
	body := []byte("payload")
	id := hex.EncodeToString(bytes.Repeat([]byte{1}, 16))
	for _, tc := range []struct {
		method, path string
		body         io.Reader
	}{
		{http.MethodPut, "/blobs/" + id, bytes.NewReader(body)},
		{http.MethodGet, "/blobs/" + id, nil},
		{http.MethodHead, "/blobs/" + id, nil},
		{http.MethodDelete, "/blobs/" + id, nil},
	} {
		resp, out := do(t, srv, tc.method, tc.path, tc.body)
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("%s %s without secret = %d %s", tc.method, tc.path, resp.StatusCode, out)
		}
		if tc.method != http.MethodHead && errBody(t, out).Error.Code != "unauthorized" {
			t.Fatalf("%s %s without secret: body %s", tc.method, tc.path, out)
		}
		resp, out = do(t, srv, tc.method, tc.path, tc.body, "Authorization", "Bearer wrong")
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("%s %s with wrong secret = %d %s", tc.method, tc.path, resp.StatusCode, out)
		}
	}
	if resp, out := do(t, srv, http.MethodPut, "/blobs/"+id, bytes.NewReader(body), "Authorization", "Bearer s3cret"); resp.StatusCode != http.StatusCreated {
		t.Fatalf("PUT with secret = %d %s", resp.StatusCode, out)
	}
	if resp, out := do(t, srv, http.MethodGet, "/blobs/"+id, nil, "Authorization", "Bearer s3cret"); resp.StatusCode != http.StatusOK || !bytes.Equal(out, body) {
		t.Fatalf("GET with secret = %d %q", resp.StatusCode, out)
	}
	for _, path := range []string{"/healthz", "/metrics"} {
		if resp, out := do(t, srv, http.MethodGet, path, nil); resp.StatusCode != http.StatusOK {
			t.Fatalf("GET %s without secret = %d %s", path, resp.StatusCode, out)
		}
	}
}
