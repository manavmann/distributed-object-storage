package api

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/manavmann/distributed-object-storage/internal/cluster"
	"github.com/manavmann/distributed-object-storage/internal/httpx"
	"github.com/manavmann/distributed-object-storage/internal/meta"
	"github.com/manavmann/distributed-object-storage/internal/storage"
	"github.com/manavmann/distributed-object-storage/internal/testcluster"
)

const testMaxSize = 4 << 20

// env is one coordinator API in front of one real storage node.
type env struct {
	api      *httptest.Server
	handler  http.Handler
	meta     *meta.DB
	store    *storage.Store
	node     *httptest.Server
	spoolDir string
}

// newEnv starts a storage node on httptest, opens a metadata store in a
// temp dir, registers the node and serves the API handler on httptest.
func newEnv(t *testing.T) *env {
	t.Helper()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	store, err := storage.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	node := httptest.NewServer(storage.NewHandler(store, log))
	t.Cleanup(node.Close)

	db, err := meta.Open(filepath.Join(t.TempDir(), "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	if err := db.UpsertNode(context.Background(), meta.Node{ID: "n1", Addr: node.URL, Status: "up"}); err != nil {
		t.Fatal(err)
	}
	spoolDir := t.TempDir()
	handler := NewHandler(Config{
		Meta:          db,
		Nodes:         cluster.NewStatic([]cluster.Node{{ID: "n1", Addr: node.URL}}),
		SpoolDir:      spoolDir,
		MaxObjectSize: testMaxSize,
		MaxUploads:    2,
		NodeTimeout:   5 * time.Second,
		Log:           log,
	})
	api := httptest.NewServer(handler)
	t.Cleanup(api.Close)
	return &env{api: api, handler: handler, meta: db, store: store, node: node, spoolDir: spoolDir}
}

// do sends method to path with body and optional header pairs and returns
// the response with its body fully read.
func (e *env) do(t *testing.T, method, path string, body io.Reader, hdr ...string) (*http.Response, []byte) {
	t.Helper()
	req, err := http.NewRequest(method, e.api.URL+path, body)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i+1 < len(hdr); i += 2 {
		req.Header.Set(hdr[i], hdr[i+1])
	}
	resp, err := e.api.Client().Do(req)
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

func (e *env) createBucket(t *testing.T, name string) {
	t.Helper()
	resp, body := e.do(t, http.MethodPut, "/v1/"+name, nil)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create bucket %s = %d %s", name, resp.StatusCode, body)
	}
}

func (e *env) spoolFiles(t *testing.T) []string {
	t.Helper()
	entries, err := os.ReadDir(e.spoolDir)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, ent := range entries {
		names = append(names, ent.Name())
	}
	return names
}

func (e *env) blobCount(t *testing.T) int {
	t.Helper()
	n, err := e.store.BlobCount()
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func errCode(t *testing.T, body []byte) string {
	t.Helper()
	var eb httpx.ErrorBody
	if err := json.Unmarshal(body, &eb); err != nil {
		t.Fatalf("error body %q: %v", body, err)
	}
	return eb.Error.Code
}

func digest(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func randomBytes(n int) []byte {
	b := make([]byte, n)
	rand.New(rand.NewSource(42)).Read(b)
	return b
}

func TestRoundTrip3MiB(t *testing.T) {
	e := newEnv(t)
	e.createBucket(t, "photos")
	payload := randomBytes(3 << 20)
	want := digest(payload)

	resp, body := e.do(t, http.MethodPut, "/v1/photos/2026/cat.jpg", bytes.NewReader(payload), "Content-Type", "image/jpeg")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT = %d %s", resp.StatusCode, body)
	}
	if got := resp.Header.Get("ETag"); got != `"`+want+`"` {
		t.Fatalf("PUT ETag = %s, want %q", got, want)
	}
	var pr putResponse
	if err := json.Unmarshal(body, &pr); err != nil {
		t.Fatalf("PUT body %q: %v", body, err)
	}
	if pr.Bucket != "photos" || pr.Key != "2026/cat.jpg" || pr.Size != int64(len(payload)) || pr.SHA256 != want ||
		len(pr.Replicas) != 1 || pr.Replicas[0] != "n1" || pr.Quorum != 1 {
		t.Fatalf("PUT body = %+v", pr)
	}
	if files := e.spoolFiles(t); len(files) != 0 {
		t.Fatalf("spool not cleaned: %v", files)
	}

	resp, body = e.do(t, http.MethodGet, "/v1/photos/2026/cat.jpg", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET = %d %s", resp.StatusCode, body)
	}
	if !bytes.Equal(body, payload) {
		t.Fatalf("GET returned %d bytes with sha %s, want %d bytes %s", len(body), digest(body), len(payload), want)
	}
	if got := resp.Header.Get("ETag"); got != `"`+want+`"` {
		t.Fatalf("GET ETag = %s", got)
	}
	if got := resp.Header.Get("Content-Type"); got != "image/jpeg" {
		t.Fatalf("GET Content-Type = %q", got)
	}
	if got := resp.ContentLength; got != int64(len(payload)) {
		t.Fatalf("GET Content-Length = %d", got)
	}
	if _, err := http.ParseTime(resp.Header.Get("Last-Modified")); err != nil {
		t.Fatalf("GET Last-Modified %q: %v", resp.Header.Get("Last-Modified"), err)
	}

	resp, body = e.do(t, http.MethodHead, "/v1/photos/2026/cat.jpg", nil)
	if resp.StatusCode != http.StatusOK || len(body) != 0 {
		t.Fatalf("HEAD = %d, %d body bytes", resp.StatusCode, len(body))
	}
	if resp.ContentLength != int64(len(payload)) || resp.Header.Get("ETag") != `"`+want+`"` ||
		resp.Header.Get("Content-Type") != "image/jpeg" {
		t.Fatalf("HEAD headers = %v", resp.Header)
	}
}

func TestDefaultContentType(t *testing.T) {
	e := newEnv(t)
	e.createBucket(t, "bkt")
	if resp, body := e.do(t, http.MethodPut, "/v1/bkt/k", bytes.NewReader([]byte("x"))); resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT = %d %s", resp.StatusCode, body)
	}
	resp, _ := e.do(t, http.MethodGet, "/v1/bkt/k", nil)
	if got := resp.Header.Get("Content-Type"); got != "application/octet-stream" {
		t.Fatalf("Content-Type = %q", got)
	}
}

func TestNotFound(t *testing.T) {
	e := newEnv(t)
	e.createBucket(t, "bkt")
	for _, tc := range []struct{ method, path, code string }{
		{http.MethodGet, "/v1/nope/k", "NoSuchBucket"},
		{http.MethodHead, "/v1/nope/k", ""},
		{http.MethodPut, "/v1/nope/k", "NoSuchBucket"},
		{http.MethodDelete, "/v1/nope/k", "NoSuchBucket"},
		{http.MethodGet, "/v1/bkt/missing", "NoSuchKey"},
		{http.MethodHead, "/v1/bkt/missing", ""},
	} {
		var body io.Reader
		if tc.method == http.MethodPut {
			body = bytes.NewReader([]byte("x"))
		}
		resp, b := e.do(t, tc.method, tc.path, body)
		if resp.StatusCode != http.StatusNotFound {
			t.Errorf("%s %s = %d %s, want 404", tc.method, tc.path, resp.StatusCode, b)
			continue
		}
		if tc.code != "" && errCode(t, b) != tc.code {
			t.Errorf("%s %s code = %s, want %s", tc.method, tc.path, errCode(t, b), tc.code)
		}
	}
}

func TestDeleteIsMetadataOnly(t *testing.T) {
	e := newEnv(t)
	e.createBucket(t, "bkt")
	if resp, body := e.do(t, http.MethodPut, "/v1/bkt/k", bytes.NewReader([]byte("x"))); resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT = %d %s", resp.StatusCode, body)
	}
	if resp, body := e.do(t, http.MethodDelete, "/v1/bkt/k", nil); resp.StatusCode != http.StatusNoContent {
		t.Fatalf("DELETE = %d %s", resp.StatusCode, body)
	}
	if resp, _ := e.do(t, http.MethodGet, "/v1/bkt/k", nil); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("GET after DELETE = %d", resp.StatusCode)
	}
	if resp, _ := e.do(t, http.MethodDelete, "/v1/bkt/k", nil); resp.StatusCode != http.StatusNoContent {
		t.Fatalf("second DELETE = %d", resp.StatusCode)
	}
	if n := e.blobCount(t); n != 1 {
		t.Fatalf("node blob count after DELETE = %d, want 1 (physical delete is deferred)", n)
	}
	if n, err := e.meta.CountPending(context.Background()); err != nil || n != 1 {
		t.Fatalf("pending deletes = %d, %v; want 1", n, err)
	}
}

func TestBucketRoutes(t *testing.T) {
	e := newEnv(t)
	e.createBucket(t, "bkt")
	resp, body := e.do(t, http.MethodPut, "/v1/bkt", nil)
	if resp.StatusCode != http.StatusConflict || errCode(t, body) != "BucketAlreadyExists" {
		t.Fatalf("second create = %d %s", resp.StatusCode, body)
	}
	resp, body = e.do(t, http.MethodPut, "/v1/Bad_Name", nil)
	if resp.StatusCode != http.StatusBadRequest || errCode(t, body) != "InvalidBucket" {
		t.Fatalf("bad name = %d %s", resp.StatusCode, body)
	}
}

func TestInvalidKeys(t *testing.T) {
	e := newEnv(t)
	e.createBucket(t, "bkt")
	for _, path := range []string{
		"/v1/bkt/",
		"/v1/bkt/a%2F..%2Fb",
		"/v1/bkt/%2Fabs",
		"/v1/bkt/" + string(bytes.Repeat([]byte("k"), 1025)),
		"/v1/bkt/%ff",
	} {
		resp, body := e.do(t, http.MethodPut, path, bytes.NewReader([]byte("x")))
		if resp.StatusCode != http.StatusBadRequest || errCode(t, body) != "InvalidKey" {
			t.Errorf("PUT %s = %d %s, want 400 InvalidKey", path, resp.StatusCode, body)
		}
	}
}

// failingBody fails the test if anything reads it.
type failingBody struct{ t *testing.T }

func (f failingBody) Read([]byte) (int, error) {
	f.t.Fatal("handler read the body before rejecting the request")
	return 0, io.EOF
}

func TestLengthRequired(t *testing.T) {
	e := newEnv(t)
	e.createBucket(t, "bkt")
	req := httptest.NewRequest(http.MethodPut, "/v1/bkt/k", io.NopCloser(failingBody{t}))
	req.ContentLength = -1
	rec := httptest.NewRecorder()
	e.handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusLengthRequired || errCode(t, rec.Body.Bytes()) != "LengthRequired" {
		t.Fatalf("PUT without length = %d %s", rec.Code, rec.Body)
	}
}

func TestTooLargeBeforeBodyRead(t *testing.T) {
	e := newEnv(t)
	e.createBucket(t, "bkt")
	req := httptest.NewRequest(http.MethodPut, "/v1/bkt/k", io.NopCloser(failingBody{t}))
	req.ContentLength = testMaxSize + 1
	rec := httptest.NewRecorder()
	e.handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusRequestEntityTooLarge || errCode(t, rec.Body.Bytes()) != "EntityTooLarge" {
		t.Fatalf("oversized PUT = %d %s", rec.Code, rec.Body)
	}
	if files := e.spoolFiles(t); len(files) != 0 {
		t.Fatalf("spool created for rejected PUT: %v", files)
	}
}

func TestClientAbortLeavesNothing(t *testing.T) {
	e := newEnv(t)
	e.createBucket(t, "bkt")
	pr, pw := io.Pipe()
	req, err := http.NewRequest(http.MethodPut, e.api.URL+"/v1/bkt/k", pr)
	if err != nil {
		t.Fatal(err)
	}
	req.ContentLength = 3 << 20
	errc := make(chan error, 1)
	go func() {
		_, err := e.api.Client().Do(req)
		errc <- err
	}()
	if _, err := pw.Write(randomBytes(1 << 20)); err != nil {
		t.Fatal(err)
	}
	pw.CloseWithError(errors.New("client went away"))
	if err := <-errc; err == nil {
		t.Fatal("aborted PUT did not fail on the client side")
	}

	testcluster.WaitFor(t, 5*time.Second, "spool cleanup", func() bool { return len(e.spoolFiles(t)) == 0 })
	if _, err := e.meta.GetObject(context.Background(), "bkt", "k"); !errors.Is(err, meta.ErrNoSuchKey) {
		t.Fatalf("object exists after abort: %v", err)
	}
	if n := e.blobCount(t); n != 0 {
		t.Fatalf("node has %d blobs after abort, want 0", n)
	}
}

func TestOverwriteQueuesOldReplica(t *testing.T) {
	e := newEnv(t)
	e.createBucket(t, "bkt")
	ctx := context.Background()
	if resp, body := e.do(t, http.MethodPut, "/v1/bkt/k", bytes.NewReader([]byte("v1"))); resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT v1 = %d %s", resp.StatusCode, body)
	}
	first, err := e.meta.GetObject(ctx, "bkt", "k")
	if err != nil {
		t.Fatal(err)
	}
	if resp, body := e.do(t, http.MethodPut, "/v1/bkt/k", bytes.NewReader([]byte("v2"))); resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT v2 = %d %s", resp.StatusCode, body)
	}
	second, err := e.meta.GetObject(ctx, "bkt", "k")
	if err != nil {
		t.Fatal(err)
	}
	if first.BlobID == second.BlobID {
		t.Fatal("overwrite reused the blob id")
	}
	if reps, err := e.meta.Replicas(ctx, first.BlobID); err != nil || len(reps) != 0 {
		t.Fatalf("old blob still has replicas %v, %v", reps, err)
	}
	if n, err := e.meta.CountPending(ctx); err != nil || n != 1 {
		t.Fatalf("pending deletes = %d, %v; want 1", n, err)
	}
	if n := e.blobCount(t); n != 2 {
		t.Fatalf("node blob count = %d, want 2 (old blob not deleted inline)", n)
	}
	if _, body := e.do(t, http.MethodGet, "/v1/bkt/k", nil); string(body) != "v2" {
		t.Fatalf("GET after overwrite = %q", body)
	}
}

func TestNodeDownOnGet(t *testing.T) {
	e := newEnv(t)
	e.createBucket(t, "bkt")
	if resp, body := e.do(t, http.MethodPut, "/v1/bkt/k", bytes.NewReader([]byte("x"))); resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT = %d %s", resp.StatusCode, body)
	}
	e.node.Close()
	resp, body := e.do(t, http.MethodGet, "/v1/bkt/k", nil)
	if resp.StatusCode != http.StatusServiceUnavailable || errCode(t, body) != "NoHealthyReplica" {
		t.Fatalf("GET with node down = %d %s", resp.StatusCode, body)
	}
	if resp, _ := e.do(t, http.MethodHead, "/v1/bkt/k", nil); resp.StatusCode != http.StatusOK {
		t.Fatalf("HEAD with node down = %d, want 200 from metadata", resp.StatusCode)
	}
}

func TestNodeDownOnPut(t *testing.T) {
	e := newEnv(t)
	e.createBucket(t, "bkt")
	e.node.Close()
	resp, body := e.do(t, http.MethodPut, "/v1/bkt/k", bytes.NewReader([]byte("x")))
	if resp.StatusCode != http.StatusServiceUnavailable || errCode(t, body) != "InsufficientReplicas" {
		t.Fatalf("PUT with node down = %d %s", resp.StatusCode, body)
	}
	if _, err := e.meta.GetObject(context.Background(), "bkt", "k"); !errors.Is(err, meta.ErrNoSuchKey) {
		t.Fatalf("object committed without a replica: %v", err)
	}
	if files := e.spoolFiles(t); len(files) != 0 {
		t.Fatalf("spool not cleaned: %v", files)
	}
}

func TestNoHealthyNodesOnPut(t *testing.T) {
	e := newEnv(t)
	e.createBucket(t, "bkt")
	handler := NewHandler(Config{
		Meta: e.meta, Nodes: cluster.NewStatic(nil), SpoolDir: e.spoolDir,
		MaxObjectSize: testMaxSize, MaxUploads: 1, NodeTimeout: time.Second,
		Log: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	req := httptest.NewRequest(http.MethodPut, "/v1/bkt/k", bytes.NewReader([]byte("x")))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable || errCode(t, rec.Body.Bytes()) != "InsufficientReplicas" {
		t.Fatalf("PUT with no nodes = %d %s", rec.Code, rec.Body)
	}
}

func TestStatusFor(t *testing.T) {
	for _, tc := range []struct {
		err    error
		status int
		code   string
	}{
		{ErrInvalidBucket, 400, "InvalidBucket"},
		{ErrInvalidKey, 400, "InvalidKey"},
		{ErrLengthRequired, 411, "LengthRequired"},
		{ErrTooLarge, 413, "EntityTooLarge"},
		{ErrBodyRead, 400, "IncompleteBody"},
		{ErrInsufficientReplicas, 503, "InsufficientReplicas"},
		{ErrNoHealthyReplica, 503, "NoHealthyReplica"},
		{meta.ErrNoSuchBucket, 404, "NoSuchBucket"},
		{meta.ErrNoSuchKey, 404, "NoSuchKey"},
		{meta.ErrBucketExists, 409, "BucketAlreadyExists"},
		{meta.ErrBucketNotEmpty, 409, "BucketNotEmpty"},
		{errors.New("boom"), 500, "InternalError"},
	} {
		status, code := statusFor(tc.err)
		if status != tc.status || code != tc.code {
			t.Errorf("statusFor(%v) = %d %s, want %d %s", tc.err, status, code, tc.status, tc.code)
		}
	}
}
