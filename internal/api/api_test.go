package api_test

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
	"path/filepath"
	"testing"
	"time"

	"github.com/manavmann/distributed-object-storage/internal/api"
	"github.com/manavmann/distributed-object-storage/internal/cluster"
	"github.com/manavmann/distributed-object-storage/internal/httpx"
	"github.com/manavmann/distributed-object-storage/internal/meta"
	"github.com/manavmann/distributed-object-storage/internal/storage"
	"github.com/manavmann/distributed-object-storage/internal/testcluster"
)

// newCluster is one coordinator in front of one real storage node.
func newCluster(t *testing.T) *testcluster.Cluster {
	t.Helper()
	return testcluster.New(t, testcluster.Opts{Nodes: 1})
}

// do sends method to path with body and optional header pairs and returns
// the response with its body fully read, failing the test on a transport
// error.
func do(t *testing.T, c *testcluster.Cluster, method, path string, body io.Reader, hdr ...string) (*http.Response, []byte) {
	t.Helper()
	resp, b, err := c.Client().Do(method, path, body, hdr...)
	if err != nil {
		t.Fatal(err)
	}
	return resp, b
}

func createBucket(t *testing.T, c *testcluster.Cluster, name string) {
	t.Helper()
	if err := c.Client().CreateBucket(name); err != nil {
		t.Fatalf("create bucket %s: %v", name, err)
	}
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
	c := newCluster(t)
	createBucket(t, c, "photos")
	payload := randomBytes(3 << 20)
	want := digest(payload)

	pr, err := c.Client().Put("photos", "2026/cat.jpg", payload, "image/jpeg")
	if err != nil {
		t.Fatalf("PUT: %v", err)
	}
	if pr.ETag != `"`+want+`"` {
		t.Fatalf("PUT ETag = %s, want %q", pr.ETag, want)
	}
	if pr.Bucket != "photos" || pr.Key != "2026/cat.jpg" || pr.Size != int64(len(payload)) || pr.SHA256 != want ||
		len(pr.Replicas) != 1 || pr.Replicas[0] != c.NodeID(0) || pr.Quorum != 1 {
		t.Fatalf("PUT body = %+v", pr)
	}
	if files := c.SpoolFiles(); len(files) != 0 {
		t.Fatalf("spool not cleaned: %v", files)
	}

	resp, body := do(t, c, http.MethodGet, "/v1/photos/2026/cat.jpg", nil)
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

	resp, body = do(t, c, http.MethodHead, "/v1/photos/2026/cat.jpg", nil)
	if resp.StatusCode != http.StatusOK || len(body) != 0 {
		t.Fatalf("HEAD = %d, %d body bytes", resp.StatusCode, len(body))
	}
	if resp.ContentLength != int64(len(payload)) || resp.Header.Get("ETag") != `"`+want+`"` ||
		resp.Header.Get("Content-Type") != "image/jpeg" {
		t.Fatalf("HEAD headers = %v", resp.Header)
	}
}

func TestDefaultContentType(t *testing.T) {
	c := newCluster(t)
	createBucket(t, c, "bkt")
	if _, err := c.Client().Put("bkt", "k", []byte("x"), ""); err != nil {
		t.Fatalf("PUT: %v", err)
	}
	obj, err := c.Client().Get("bkt", "k")
	if err != nil {
		t.Fatal(err)
	}
	if obj.ContentType != "application/octet-stream" {
		t.Fatalf("Content-Type = %q", obj.ContentType)
	}
}

func TestNotFound(t *testing.T) {
	c := newCluster(t)
	createBucket(t, c, "bkt")
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
		resp, b := do(t, c, tc.method, tc.path, body)
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
	c := newCluster(t)
	createBucket(t, c, "bkt")
	cl := c.Client()
	if _, err := cl.Put("bkt", "k", []byte("x"), ""); err != nil {
		t.Fatalf("PUT: %v", err)
	}
	blobID, _ := c.Locate("bkt", "k")
	if err := cl.Delete("bkt", "k"); err != nil {
		t.Fatalf("DELETE: %v", err)
	}
	var apiErr *testcluster.APIError
	if _, err := cl.Get("bkt", "k"); !errors.As(err, &apiErr) || apiErr.Status != http.StatusNotFound {
		t.Fatalf("GET after DELETE = %v", err)
	}
	if err := cl.Delete("bkt", "k"); err != nil {
		t.Fatalf("second DELETE: %v", err)
	}
	if !c.HasBlob(0, blobID) {
		t.Fatal("blob gone from node after DELETE, want it kept (physical delete is deferred)")
	}
	if n, err := c.Meta().CountPending(context.Background()); err != nil || n != 1 {
		t.Fatalf("pending deletes = %d, %v; want 1", n, err)
	}
}

func TestBucketRoutes(t *testing.T) {
	c := newCluster(t)
	createBucket(t, c, "bkt")
	resp, body := do(t, c, http.MethodPut, "/v1/bkt", nil)
	if resp.StatusCode != http.StatusConflict || errCode(t, body) != "BucketAlreadyExists" {
		t.Fatalf("second create = %d %s", resp.StatusCode, body)
	}
	resp, body = do(t, c, http.MethodPut, "/v1/Bad_Name", nil)
	if resp.StatusCode != http.StatusBadRequest || errCode(t, body) != "InvalidBucket" {
		t.Fatalf("bad name = %d %s", resp.StatusCode, body)
	}
}

func TestInvalidKeys(t *testing.T) {
	c := newCluster(t)
	createBucket(t, c, "bkt")
	for _, path := range []string{
		"/v1/bkt/",
		"/v1/bkt/a%2F..%2Fb",
		"/v1/bkt/%2Fabs",
		"/v1/bkt/" + string(bytes.Repeat([]byte("k"), 1025)),
		"/v1/bkt/%ff",
	} {
		resp, body := do(t, c, http.MethodPut, path, bytes.NewReader([]byte("x")))
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
	c := newCluster(t)
	createBucket(t, c, "bkt")
	req := httptest.NewRequest(http.MethodPut, "/v1/bkt/k", io.NopCloser(failingBody{t}))
	req.ContentLength = -1
	rec := httptest.NewRecorder()
	c.Coordinator().Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusLengthRequired || errCode(t, rec.Body.Bytes()) != "LengthRequired" {
		t.Fatalf("PUT without length = %d %s", rec.Code, rec.Body)
	}
}

func TestTooLargeBeforeBodyRead(t *testing.T) {
	c := newCluster(t)
	createBucket(t, c, "bkt")
	req := httptest.NewRequest(http.MethodPut, "/v1/bkt/k", io.NopCloser(failingBody{t}))
	req.ContentLength = testcluster.MaxObjectSize + 1
	rec := httptest.NewRecorder()
	c.Coordinator().Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusRequestEntityTooLarge || errCode(t, rec.Body.Bytes()) != "EntityTooLarge" {
		t.Fatalf("oversized PUT = %d %s", rec.Code, rec.Body)
	}
	if files := c.SpoolFiles(); len(files) != 0 {
		t.Fatalf("spool created for rejected PUT: %v", files)
	}
}

func TestClientAbortLeavesNothing(t *testing.T) {
	c := newCluster(t)
	createBucket(t, c, "bkt")
	pr, pw := io.Pipe()
	req, err := http.NewRequest(http.MethodPut, c.Client().URL()+"/v1/bkt/k", pr)
	if err != nil {
		t.Fatal(err)
	}
	req.ContentLength = 3 << 20
	errc := make(chan error, 1)
	go func() {
		_, err := http.DefaultClient.Do(req)
		errc <- err
	}()
	if _, err := pw.Write(randomBytes(1 << 20)); err != nil {
		t.Fatal(err)
	}
	pw.CloseWithError(errors.New("client went away"))
	if err := <-errc; err == nil {
		t.Fatal("aborted PUT did not fail on the client side")
	}

	testcluster.WaitFor(t, func() bool { return len(c.SpoolFiles()) == 0 }, "spool cleanup")
	if _, err := c.Meta().GetObject(context.Background(), "bkt", "k"); !errors.Is(err, meta.ErrNoSuchKey) {
		t.Fatalf("object exists after abort: %v", err)
	}
	st, err := c.Client().Status()
	if err != nil {
		t.Fatal(err)
	}
	if n := st.Nodes[0].BlobCount; n != 0 {
		t.Fatalf("node has %d blobs after abort, want 0", n)
	}
}

func TestOverwriteQueuesOldReplica(t *testing.T) {
	c := newCluster(t)
	createBucket(t, c, "bkt")
	cl := c.Client()
	ctx := context.Background()
	if _, err := cl.Put("bkt", "k", []byte("v1"), ""); err != nil {
		t.Fatalf("PUT v1: %v", err)
	}
	first, _ := c.Locate("bkt", "k")
	if _, err := cl.Put("bkt", "k", []byte("v2"), ""); err != nil {
		t.Fatalf("PUT v2: %v", err)
	}
	second, _ := c.Locate("bkt", "k")
	if first == second {
		t.Fatal("overwrite reused the blob id")
	}
	if reps, err := c.Meta().Replicas(ctx, first); err != nil || len(reps) != 0 {
		t.Fatalf("old blob still has replicas %v, %v", reps, err)
	}
	if n, err := c.Meta().CountPending(ctx); err != nil || n != 1 {
		t.Fatalf("pending deletes = %d, %v; want 1", n, err)
	}
	if !c.HasBlob(0, first) || !c.HasBlob(0, second) {
		t.Fatalf("node holds old=%v new=%v, want both (old blob not deleted inline)", c.HasBlob(0, first), c.HasBlob(0, second))
	}
	if obj, err := cl.Get("bkt", "k"); err != nil || string(obj.Body) != "v2" {
		t.Fatalf("GET after overwrite = %q, %v", obj.Body, err)
	}
}

func TestNodeDownOnGet(t *testing.T) {
	c := newCluster(t)
	createBucket(t, c, "bkt")
	if _, err := c.Client().Put("bkt", "k", []byte("x"), ""); err != nil {
		t.Fatalf("PUT: %v", err)
	}
	c.Kill(0)
	resp, body := do(t, c, http.MethodGet, "/v1/bkt/k", nil)
	if resp.StatusCode != http.StatusServiceUnavailable || errCode(t, body) != "NoHealthyReplica" {
		t.Fatalf("GET with node down = %d %s", resp.StatusCode, body)
	}
	if resp, _ := do(t, c, http.MethodHead, "/v1/bkt/k", nil); resp.StatusCode != http.StatusOK {
		t.Fatalf("HEAD with node down = %d, want 200 from metadata", resp.StatusCode)
	}
}

func TestNodeDownOnPut(t *testing.T) {
	c := newCluster(t)
	createBucket(t, c, "bkt")
	c.Kill(0)
	resp, body := do(t, c, http.MethodPut, "/v1/bkt/k", bytes.NewReader([]byte("x")))
	if resp.StatusCode != http.StatusServiceUnavailable || errCode(t, body) != "InsufficientReplicas" {
		t.Fatalf("PUT with node down = %d %s", resp.StatusCode, body)
	}
	if _, err := c.Meta().GetObject(context.Background(), "bkt", "k"); !errors.Is(err, meta.ErrNoSuchKey) {
		t.Fatalf("object committed without a replica: %v", err)
	}
	if files := c.SpoolFiles(); len(files) != 0 {
		t.Fatalf("spool not cleaned: %v", files)
	}
}

func TestNoHealthyNodesOnPut(t *testing.T) {
	c := newCluster(t)
	createBucket(t, c, "bkt")
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	db, err := meta.Open(filepath.Join(t.TempDir(), "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	empty, err := cluster.Load(context.Background(), db, time.Minute, time.Now, log)
	if err != nil {
		t.Fatal(err)
	}
	handler := api.NewHandler(api.Config{
		Meta: c.Meta(), Nodes: empty, SpoolDir: t.TempDir(),
		MaxObjectSize: testcluster.MaxObjectSize, MaxUploads: 1, NodeTimeout: time.Second, Log: log,
	})
	req := httptest.NewRequest(http.MethodPut, "/v1/bkt/k", bytes.NewReader([]byte("x")))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable || errCode(t, rec.Body.Bytes()) != "InsufficientReplicas" {
		t.Fatalf("PUT with no nodes = %d %s", rec.Code, rec.Body)
	}
}

func TestHeartbeatRegistersNode(t *testing.T) {
	c := newCluster(t)
	// The node package's type is what real nodes send; the registry must
	// accept it verbatim.
	body, err := json.Marshal(storage.Heartbeat{NodeID: "n2", Addr: "http://n2:9000", BlobCount: 7, FreeBytes: 99})
	if err != nil {
		t.Fatal(err)
	}
	resp, out := do(t, c, http.MethodPost, "/internal/heartbeat", bytes.NewReader(body), "Content-Type", "application/json")
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("heartbeat = %d %s", resp.StatusCode, out)
	}
	nodes, err := c.Meta().ListNodes(context.Background())
	if err != nil || len(nodes) != 2 || nodes[1].ID != "n2" || nodes[1].Addr != "http://n2:9000" || nodes[1].Status != cluster.StatusUp {
		t.Fatalf("ListNodes = %+v, %v", nodes, err)
	}
	st, err := c.Client().Status()
	if err != nil {
		t.Fatal(err)
	}
	if len(st.Nodes) != 2 || st.Nodes[1].NodeID != "n2" || st.Nodes[1].BlobCount != 7 || st.Nodes[1].FreeBytes != 99 {
		t.Fatalf("status nodes = %+v", st.Nodes)
	}
}

func TestMalformedHeartbeat(t *testing.T) {
	c := newCluster(t)
	for _, body := range []string{"", "{", `{"node_id":"","addr":"http://x:1"}`, `{"node_id":"n9","addr":"x:1"}`} {
		resp, out := do(t, c, http.MethodPost, "/internal/heartbeat", bytes.NewReader([]byte(body)), "Content-Type", "application/json")
		if resp.StatusCode != http.StatusBadRequest || errCode(t, out) != "InvalidHeartbeat" {
			t.Errorf("heartbeat %q = %d %s, want 400 InvalidHeartbeat", body, resp.StatusCode, out)
		}
	}
	if nodes, err := c.Meta().ListNodes(context.Background()); err != nil || len(nodes) != 1 {
		t.Fatalf("malformed heartbeat registered a node: %+v, %v", nodes, err)
	}
}

func TestClusterStatusShape(t *testing.T) {
	c := newCluster(t)
	resp, out := do(t, c, http.MethodGet, "/cluster/status", nil)
	if resp.StatusCode != http.StatusOK || resp.Header.Get("Content-Type") != "application/json" {
		t.Fatalf("status = %d %s %s", resp.StatusCode, resp.Header.Get("Content-Type"), out)
	}
	var st map[string]json.RawMessage
	if err := json.Unmarshal(out, &st); err != nil {
		t.Fatalf("status body %s: %v", out, err)
	}
	if len(st) != 2 || st["nodes"] == nil || string(st["under_replicated"]) != "0" {
		t.Fatalf("status top-level = %s", out)
	}
	var nodes []map[string]json.RawMessage
	if err := json.Unmarshal(st["nodes"], &nodes); err != nil || len(nodes) != 1 {
		t.Fatalf("status nodes = %s: %v", st["nodes"], err)
	}
	n := nodes[0]
	for _, k := range []string{"node_id", "addr", "status", "free_bytes", "blob_count", "last_seen", "status_changed_at"} {
		if n[k] == nil {
			t.Errorf("status node lacks %q: %s", k, st["nodes"])
		}
	}
	if len(n) != 7 {
		t.Errorf("status node has %d fields, want 7: %s", len(n), st["nodes"])
	}
	if string(n["node_id"]) != `"`+c.NodeID(0)+`"` || string(n["status"]) != `"UP"` {
		t.Errorf("status node = %s", st["nodes"])
	}
	var ts time.Time
	if err := json.Unmarshal(n["last_seen"], &ts); err != nil || ts.IsZero() {
		t.Errorf("last_seen %s is not an RFC3339 time: %v", n["last_seen"], err)
	}
}
