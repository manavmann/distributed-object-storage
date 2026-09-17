package integration

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"sync"
	"testing"

	"github.com/manavmann/distributed-object-storage/internal/meta"
	"github.com/manavmann/distributed-object-storage/internal/testcluster"
)

// heldPut is a PUT whose body the test finishes when it chooses, so the
// coordinator has it inside a handler for as long as the test needs.
type heldPut struct {
	pw   *io.PipeWriter
	rest []byte
	done chan heldResult
}

// heldResult is what the held PUT's client got back.
type heldResult struct {
	status int
	etag   string
	err    error
}

// holdPut starts a PUT of body to bkt/key, sends its first byte and
// returns once the coordinator has opened a spool file for it, which
// means it is past validation and holds an upload slot. The cluster must
// have no other PUT in flight.
func holdPut(t *testing.T, c *testcluster.Cluster, key string, body []byte) *heldPut {
	t.Helper()
	pr, pw := io.Pipe()
	req, err := http.NewRequest(http.MethodPut, c.Client().URL()+"/v1/bkt/"+key, pr)
	if err != nil {
		t.Fatal(err)
	}
	req.ContentLength = int64(len(body))
	h := &heldPut{pw: pw, rest: body[1:], done: make(chan heldResult, 1)}
	go func() {
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			h.done <- heldResult{err: err}
			return
		}
		_, err = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		h.done <- heldResult{status: resp.StatusCode, etag: resp.Header.Get("ETag"), err: err}
	}()
	if _, err := pw.Write(body[:1]); err != nil {
		t.Fatal(err)
	}
	testcluster.WaitFor(t, func() bool { return len(c.SpoolFiles()) == 1 }, "held PUT to reach the coordinator")
	return h
}

// release sends the rest of the body so the PUT can finish.
func (h *heldPut) release() error {
	if _, err := h.pw.Write(h.rest); err != nil {
		return err
	}
	return h.pw.Close()
}

// freshConn sends one request on a connection of its own, never a kept
// alive one, so the answer says whether the coordinator still accepts
// connections. A response is returned with its body already read.
func freshConn(method, rawURL string, body []byte) (*http.Response, error) {
	client := &http.Client{Transport: &http.Transport{DisableKeepAlives: true}}
	defer client.CloseIdleConnections()
	req, err := http.NewRequest(method, rawURL, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if _, err := io.Copy(io.Discard, resp.Body); err != nil {
		return nil, err
	}
	return resp, nil
}

// refusing reports whether base no longer accepts connections, which is
// the first thing Shutdown does.
func refusing(base string) bool {
	_, err := freshConn(http.MethodGet, base+"/healthz", nil)
	return err != nil
}

func TestInFlightPutCompletesDuringShutdown(t *testing.T) {
	t.Parallel()
	c := testcluster.New(t, testcluster.Opts{Nodes: 1, RF: 1, W: 1})
	cl := c.Client()
	if err := cl.CreateBucket("bkt"); err != nil {
		t.Fatal(err)
	}
	body := bytes.Repeat([]byte("held across shutdown "), 1000)
	h := holdPut(t, c, "k", body)
	oldURL := cl.URL()

	// The body is finished only once the coordinator is draining, so the
	// PUT is in flight for the whole of Shutdown's wait.
	released := make(chan error, 1)
	go func() {
		if !testcluster.Poll(func() bool { return refusing(oldURL) }) {
			released <- errors.New("coordinator kept accepting connections")
			return
		}
		released <- h.release()
	}()
	c.RestartCoordinator()
	if err := <-released; err != nil {
		t.Fatal(err)
	}
	res := <-h.done
	if res.err != nil || res.status != http.StatusOK || res.etag != etagOf(body) {
		t.Fatalf("held PUT = %d %s %v, want 200 %s", res.status, res.etag, res.err, etagOf(body))
	}

	obj, err := cl.Get("bkt", "k")
	if err != nil {
		t.Fatalf("get after restart: %v", err)
	}
	if !bytes.Equal(obj.Body, body) || obj.ETag != res.etag {
		t.Fatalf("get after restart = %d bytes %s, want %d bytes %s", len(obj.Body), obj.ETag, len(body), res.etag)
	}
	if files := c.SpoolFiles(); len(files) != 0 {
		t.Fatalf("spool after restart = %v", files)
	}
	checkNoDanglingRows(t, c, 1)
}

func TestRequestsDuringShutdownRefused(t *testing.T) {
	t.Parallel()
	c := testcluster.New(t, testcluster.Opts{Nodes: 1, RF: 1, W: 1})
	cl := c.Client()
	if err := cl.CreateBucket("bkt"); err != nil {
		t.Fatal(err)
	}
	if _, err := cl.Put("bkt", "k", []byte("before"), ""); err != nil {
		t.Fatal(err)
	}
	h := holdPut(t, c, "held", []byte("hold the door"))
	oldURL := cl.URL()

	// attempt is one request made while the coordinator was draining.
	type attempt struct {
		what string
		resp *http.Response
		err  error
	}
	attempts := make(chan []attempt, 1)
	go func() {
		var made []attempt
		if !testcluster.Poll(func() bool { return refusing(oldURL) }) {
			made = append(made, attempt{what: "healthz", err: errors.New("coordinator kept accepting connections")})
		}
		for _, r := range []struct {
			what, method, path string
			body               []byte
		}{
			{"GET object", http.MethodGet, "/v1/bkt/k", nil},
			{"PUT object", http.MethodPut, "/v1/bkt/late", []byte("late")},
			{"PUT bucket", http.MethodPut, "/v1/other", nil},
			{"GET status", http.MethodGet, "/cluster/status", nil},
		} {
			resp, err := freshConn(r.method, oldURL+r.path, r.body)
			made = append(made, attempt{what: r.what, resp: resp, err: err})
		}
		attempts <- made
		if err := h.release(); err != nil {
			attempts <- []attempt{{what: "release", err: err}}
		}
	}()
	c.RestartCoordinator()
	for _, a := range <-attempts {
		if a.err == nil {
			t.Errorf("%s during shutdown answered %d, want a refused connection", a.what, a.resp.StatusCode)
		}
	}
	if res := <-h.done; res.err != nil || res.status != http.StatusOK {
		t.Fatalf("held PUT = %d %v, want 200", res.status, res.err)
	}

	// Nothing refused mid-shutdown left a trace, and the new coordinator
	// takes the same requests.
	var apiErr *testcluster.APIError
	if _, err := cl.Get("bkt", "late"); !errors.As(err, &apiErr) || apiErr.Status != http.StatusNotFound {
		t.Fatalf("get of refused PUT's key = %v, want 404", err)
	}
	if err := cl.CreateBucket("other"); err != nil {
		t.Fatalf("create bucket after restart: %v", err)
	}
	if obj, err := cl.Get("bkt", "held"); err != nil || string(obj.Body) != "hold the door" {
		t.Fatalf("get held object after restart = %q, %v", obj.Body, err)
	}
	checkNoDanglingRows(t, c, 1)
}

func TestSpoolSweptOnStart(t *testing.T) {
	t.Parallel()
	c := testcluster.New(t, testcluster.Opts{Nodes: 1, RF: 1, W: 1})
	cl := c.Client()
	if err := cl.CreateBucket("bkt"); err != nil {
		t.Fatal(err)
	}
	// What a crash mid-PUT leaves behind: a spool file nobody owns.
	stray := filepath.Join(c.SpoolDir(), "deadbeef")
	if err := os.WriteFile(stray, []byte("half an upload"), 0o600); err != nil {
		t.Fatal(err)
	}
	if files := c.SpoolFiles(); !slices.Equal(files, []string{"deadbeef"}) {
		t.Fatalf("spool before restart = %v", files)
	}

	c.RestartCoordinator()
	if files := c.SpoolFiles(); len(files) != 0 {
		t.Fatalf("spool after restart = %v, want swept", files)
	}
	if _, err := cl.Put("bkt", "k", []byte("after sweep"), ""); err != nil {
		t.Fatalf("put after sweep: %v", err)
	}
	if files := c.SpoolFiles(); len(files) != 0 {
		t.Fatalf("spool after put = %v", files)
	}
}

func TestShortBodyRejected(t *testing.T) {
	t.Parallel()
	c := testcluster.New(t, testcluster.Opts{Nodes: 1, RF: 1, W: 1})
	cl := c.Client()
	if err := cl.CreateBucket("bkt"); err != nil {
		t.Fatal(err)
	}
	u, err := url.Parse(cl.URL())
	if err != nil {
		t.Fatal(err)
	}
	// A client that promises 100 bytes, sends 10 and hangs up its writing
	// side; an HTTP client library would refuse to send this.
	conn, err := net.Dial("tcp", u.Host)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := fmt.Fprintf(conn, "PUT /v1/bkt/short HTTP/1.1\r\nHost: %s\r\nContent-Length: 100\r\n\r\nten bytes!", u.Host); err != nil {
		t.Fatal(err)
	}
	if err := conn.(*net.TCPConn).CloseWrite(); err != nil {
		t.Fatal(err)
	}
	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusBadRequest || !bytes.Contains(body, []byte(`"IncompleteBody"`)) {
		t.Fatalf("short PUT = %d %s, want 400 IncompleteBody", resp.StatusCode, body)
	}

	// No side effects: the spool entry is gone, no intent, object or
	// replica row was written, and the node never saw a blob.
	testcluster.WaitFor(t, func() bool { return len(c.SpoolFiles()) == 0 }, "spool cleanup")
	if _, err := c.Meta().GetObject(context.Background(), "bkt", "short"); !errors.Is(err, meta.ErrNoSuchKey) {
		t.Fatalf("object after short PUT: %v", err)
	}
	for _, table := range []string{"pending_uploads", "objects", "replicas", "pending_deletes"} {
		if n := count(t, c, "SELECT COUNT(*) FROM "+table); n != 0 {
			t.Fatalf("%d rows in %s after short PUT", n, table)
		}
	}
	if blobs := c.Blobs(0); len(blobs) != 0 {
		t.Fatalf("node holds %v after short PUT", blobs)
	}
	if c.MaxConcurrentWrites(0) != 0 {
		t.Fatal("node received a write for a short PUT")
	}
}

func TestUploadSemaphoreBoundsNodeWrites(t *testing.T) {
	t.Parallel()
	const nodes, puts, limit = 3, 100, 4
	c := testcluster.New(t, testcluster.Opts{Nodes: nodes, RF: 3, W: 2, MaxUploads: limit})
	cl := c.Client()
	if err := cl.CreateBucket("bkt"); err != nil {
		t.Fatal(err)
	}

	keys := make([]string, puts)
	results := make([]testcluster.PutResult, puts)
	errs := make([]error, puts)
	var wg sync.WaitGroup
	for i := 0; i < puts; i++ {
		keys[i] = "k" + strconv.Itoa(i)
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[i], errs[i] = cl.Put("bkt", keys[i], []byte("payload "+keys[i]), "")
		}()
	}
	wg.Wait()
	for i := range puts {
		if errs[i] != nil {
			t.Fatalf("put %s: %v", keys[i], errs[i])
		}
		if len(results[i].Replicas) != nodes {
			t.Fatalf("put %s landed on %v, want all %d nodes", keys[i], results[i].Replicas, nodes)
		}
	}
	// Every upload writes to all three nodes at once, so a node sees at
	// most one write per slot.
	for i := 0; i < nodes; i++ {
		peak := c.MaxConcurrentWrites(i)
		if peak == 0 || peak > limit {
			t.Errorf("%s had at most %d writes in flight, want 1..%d", c.NodeID(i), peak, limit)
		}
		t.Logf("%s peaked at %d concurrent writes", c.NodeID(i), peak)
	}
	checkLiveObjects(t, c, keys)
	checkNoDanglingRows(t, c, nodes)
}
