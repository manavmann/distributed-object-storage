package integration

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/manavmann/distributed-object-storage/internal/cluster"
	"github.com/manavmann/distributed-object-storage/internal/meta"
	"github.com/manavmann/distributed-object-storage/internal/testcluster"
)

// etagOf is the ETag the coordinator reports for body.
func etagOf(body []byte) string {
	sum := sha256.Sum256(body)
	return `"` + hex.EncodeToString(sum[:]) + `"`
}

// count runs a COUNT(*) query against the raw metadata connection.
func count(t *testing.T, c *testcluster.Cluster, query string, args ...any) int {
	t.Helper()
	var n int
	if err := c.RawDB().QueryRowContext(context.Background(), query, args...).Scan(&n); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	return n
}

// liveObject is bkt/key from metadata, or false if it is deleted.
func liveObject(t *testing.T, c *testcluster.Cluster, key string) (meta.Object, bool) {
	t.Helper()
	obj, err := c.Meta().GetObject(context.Background(), "bkt", key)
	if errors.Is(err, meta.ErrNoSuchKey) {
		return meta.Object{}, false
	}
	if err != nil {
		t.Fatal(err)
	}
	return obj, true
}

// checkNoDanglingRows asserts every metadata row is consistent with the
// others and with the nodes' disks: no replica without its object, no
// queued delete for a live blob, no upload intent left behind, and every
// replica row backed by a file.
func checkNoDanglingRows(t *testing.T, c *testcluster.Cluster, nodes int) {
	t.Helper()
	if n := count(t, c, `SELECT COUNT(*) FROM replicas WHERE blob_id NOT IN (SELECT blob_id FROM objects)`); n != 0 {
		t.Fatalf("%d replica rows for blobs no object references", n)
	}
	if n := count(t, c, `SELECT COUNT(*) FROM pending_deletes WHERE blob_id IN (SELECT blob_id FROM objects)`); n != 0 {
		t.Fatalf("%d queued deletes for live blobs", n)
	}
	if n := count(t, c, `SELECT COUNT(*) FROM pending_uploads`); n != 0 {
		t.Fatalf("%d upload intents left after every PUT returned", n)
	}
	rows, err := c.RawDB().QueryContext(context.Background(), `SELECT blob_id, node_id FROM replicas`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var blobID, nodeID string
		if err := rows.Scan(&blobID, &nodeID); err != nil {
			t.Fatal(err)
		}
		if !c.HasBlob(nodeIndex(t, c, nodes, nodeID), blobID) {
			t.Fatalf("replica row %s on %s has no file on disk", blobID, nodeID)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
}

// getMatches reads bkt/key and reports whether the body is the version
// metadata names and hashes to its ETag.
func getMatches(cl *testcluster.Client, key string, want meta.Object) error {
	obj, err := cl.Get("bkt", key)
	if err != nil {
		return fmt.Errorf("get %s: %w", key, err)
	}
	if obj.ETag != `"`+want.SHA256+`"` || etagOf(obj.Body) != obj.ETag || obj.Size != want.Size {
		return fmt.Errorf("get %s = %d bytes etag %s, metadata says %d bytes sha %s", key, obj.Size, obj.ETag, want.Size, want.SHA256)
	}
	return nil
}

// checkLiveObject asserts getMatches for one key.
func checkLiveObject(t *testing.T, c *testcluster.Cluster, key string, want meta.Object) {
	t.Helper()
	if err := getMatches(c.Client(), key, want); err != nil {
		t.Fatal(err)
	}
}

// checkLiveObjects asserts every key is live and reads back as the
// version metadata names, fanning the reads out over eight goroutines.
func checkLiveObjects(t *testing.T, c *testcluster.Cluster, keys []string) {
	t.Helper()
	objs := make([]meta.Object, len(keys))
	for i, key := range keys {
		obj, ok := liveObject(t, c, key)
		if !ok {
			t.Fatalf("%s is gone", key)
		}
		objs[i] = obj
	}
	errs := make([]error, len(keys))
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := w; i < len(keys); i += 8 {
				errs[i] = getMatches(c.Client(), keys[i], objs[i])
			}
		}()
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			t.Error(err)
		}
	}
}

// waitGCSettled waits until the delete queue is empty and each node's
// blobs/ holds nothing but the live blobs metadata says it holds.
func waitGCSettled(t *testing.T, c *testcluster.Cluster, nodes int) {
	t.Helper()
	testcluster.WaitFor(t, func() bool {
		if pendingCount(t, c) != 0 {
			return false
		}
		for i := 0; i < nodes; i++ {
			for _, id := range c.Blobs(i) {
				if count(t, c, `SELECT COUNT(*) FROM replicas WHERE blob_id = ? AND node_id = ?`, id, c.NodeID(i)) == 0 {
					return false
				}
			}
		}
		return true
	}, "GC to settle with no orphan files")
}

func TestConcurrentPutsSameKey(t *testing.T) {
	t.Parallel()
	const nodes, puts = 3, 32
	c := gcCluster(t, nodes, testcluster.Opts{})
	cl := c.Client()

	results := make([]testcluster.PutResult, puts)
	errs := make([]error, puts)
	var wg sync.WaitGroup
	for i := 0; i < puts; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			body := []byte("version " + strconv.Itoa(i) + " of k")
			results[i], errs[i] = cl.Put("bkt", "k", body, "")
		}()
	}
	wg.Wait()
	etags := make(map[string]bool, puts)
	written := make(map[string]int, nodes)
	for i := range results {
		if errs[i] != nil {
			t.Fatalf("put %d: %v", i, errs[i])
		}
		etags[results[i].ETag] = true
		for _, id := range results[i].Replicas {
			written[id]++
		}
	}
	if len(etags) != puts {
		t.Fatalf("%d distinct ETags from %d PUTs", len(etags), puts)
	}

	obj, ok := liveObject(t, c, "k")
	if !ok || !etags[`"`+obj.SHA256+`"`] {
		t.Fatalf("live object = %+v (found %v), not one of the versions written", obj, ok)
	}
	checkLiveObject(t, c, "k", obj)
	blobID, holders := c.Locate("bkt", "k")
	if len(holders) < 2 {
		t.Fatalf("live blob %s on %v, want a quorum", blobID, holders)
	}
	checkNoDanglingRows(t, c, nodes)

	waitGCSettled(t, c, nodes)
	for i := 0; i < nodes; i++ {
		id := c.NodeID(i)
		var want []string
		if slices.Contains(holders, id) {
			want = []string{blobID}
		}
		if got := c.Blobs(i); !slices.Equal(got, want) {
			t.Fatalf("%s blobs/ = %v, want %v", id, got, want)
		}
		if got := c.QuarantineCount(i); got != written[id]-len(want) {
			t.Fatalf("%s quarantined %d copies, want %d (wrote %d, holds %d)", id, got, written[id]-len(want), written[id], len(want))
		}
	}
}

func TestPutDeleteInterleavings(t *testing.T) {
	t.Parallel()
	const nodes, rounds, perRound = 3, 6, 4
	c := gcCluster(t, nodes, testcluster.Opts{})
	cl := c.Client()

	for r := 0; r < rounds; r++ {
		var wg sync.WaitGroup
		errs := make([]error, 2*perRound)
		for i := 0; i < perRound; i++ {
			wg.Add(2)
			go func() {
				defer wg.Done()
				body := []byte(fmt.Sprintf("round %d put %d", r, i))
				_, errs[i] = cl.Put("bkt", "k", body, "")
			}()
			go func() {
				defer wg.Done()
				errs[perRound+i] = cl.Delete("bkt", "k")
			}()
		}
		wg.Wait()
		for i, err := range errs {
			if err != nil {
				t.Fatalf("round %d op %d: %v", r, i, err)
			}
		}
		checkNoDanglingRows(t, c, nodes)
		if obj, ok := liveObject(t, c, "k"); ok {
			if _, holders := c.Locate("bkt", "k"); len(holders) < 2 {
				t.Fatalf("round %d: live blob %s on %v, want a quorum", r, obj.BlobID, holders)
			}
			checkLiveObject(t, c, "k", obj)
		} else {
			var apiErr *testcluster.APIError
			if _, err := cl.Get("bkt", "k"); !errors.As(err, &apiErr) || apiErr.Status != http.StatusNotFound {
				t.Fatalf("round %d: get of deleted key = %v, want 404", r, err)
			}
			if n := count(t, c, `SELECT COUNT(*) FROM objects`); n != 0 {
				t.Fatalf("round %d: %d objects while k is deleted", r, n)
			}
		}
	}
	waitGCSettled(t, c, nodes)
	checkNoDanglingRows(t, c, nodes)
	if obj, ok := liveObject(t, c, "k"); ok {
		checkLiveObject(t, c, "k", obj)
	}
}

// opResult is one operation of the mixed-load test: which key it hit,
// what came back and when it ran, so a failure can be matched against
// the kill windows.
type opResult struct {
	op         string
	key        string
	status     int
	err        error
	etag       string
	bodyETag   string
	start, end time.Time
}

// window is one Kill→Restart→UP cycle of the flapping node.
type window struct{ start, end time.Time }

func (w window) overlaps(r opResult) bool {
	return !r.start.After(w.end) && !r.end.Before(w.start)
}

func TestMixedOpsWithNodeFlapping(t *testing.T) {
	t.Parallel()
	// keys is coprime with the four-way op mix, so every key sees every op.
	const nodes, ops, workers, keys = 4, 200, 8, 15
	c := testcluster.New(t, testcluster.Opts{
		Nodes: nodes, RF: 3, W: 2,
		HeartbeatInterval: fastHeartbeat, HeartbeatTimeout: fastHeartbeatTimeout,
		RepairInterval: gcInterval, RepairGrace: time.Minute,
	})
	cl := c.Client()
	if err := cl.CreateBucket("bkt"); err != nil {
		t.Fatal(err)
	}

	// Every ETag ever issued for a key; a GET must return one of them.
	var mu sync.Mutex
	issued := make(map[string]map[string]bool, keys)
	record := func(key, etag string) {
		mu.Lock()
		defer mu.Unlock()
		if issued[key] == nil {
			issued[key] = make(map[string]bool)
		}
		issued[key][etag] = true
	}

	results := make([]opResult, ops)
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := w; i < ops; i += workers {
				key := "k" + strconv.Itoa(i%keys)
				res := opResult{key: key, start: time.Now()}
				var apiErr *testcluster.APIError
				switch i % 4 {
				case 0:
					res.op = "PUT"
					body := []byte(fmt.Sprintf("%s op %d", key, i))
					record(key, etagOf(body))
					pr, err := cl.Put("bkt", key, body, "")
					res.status, res.err, res.etag = http.StatusOK, err, pr.ETag
				case 1, 2:
					res.op = "GET"
					obj, err := cl.Get("bkt", key)
					res.status, res.err, res.etag, res.bodyETag = http.StatusOK, err, obj.ETag, etagOf(obj.Body)
				case 3:
					res.op = "DELETE"
					res.status, res.err = http.StatusNoContent, cl.Delete("bkt", key)
				}
				if errors.As(res.err, &apiErr) {
					res.status = apiErr.Status
				}
				res.end = time.Now()
				results[i] = res
			}
		}()
	}
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	// Flap node 0 until the load is finished; every window ends with the
	// node back UP.
	var windows []window
flap:
	for {
		select {
		case <-done:
			break flap
		default:
		}
		w := window{start: time.Now()}
		c.Kill(0)
		waitStatus(t, c, c.NodeID(0), cluster.StatusDown)
		c.Restart(0)
		waitStatus(t, c, c.NodeID(0), cluster.StatusUp)
		w.end = time.Now()
		windows = append(windows, w)
	}

	unavailable := 0
	for i, r := range results {
		switch {
		case r.err == nil:
			if r.op == "GET" && (r.bodyETag != r.etag || !issued[r.key][r.etag]) {
				t.Errorf("op %d GET %s: body hashes to %s, ETag %s, issued %v", i, r.key, r.bodyETag, r.etag, issued[r.key])
			}
		case r.op == "GET" && r.status == http.StatusNotFound:
		case r.status == http.StatusServiceUnavailable:
			unavailable++
			if !slices.ContainsFunc(windows, func(w window) bool { return w.overlaps(r) }) {
				t.Errorf("op %d %s %s: 503 outside every kill window (%v-%v): %v", i, r.op, r.key, r.start, r.end, r.err)
			}
		default:
			t.Errorf("op %d %s %s: %v", i, r.op, r.key, r.err)
		}
	}
	t.Logf("%d ops over %d kill windows, %d answered 503", ops, len(windows), unavailable)
	for k := 0; k < keys; k++ {
		key := "k" + strconv.Itoa(k)
		obj, ok := liveObject(t, c, key)
		if !ok {
			continue
		}
		if !issued[key][`"`+obj.SHA256+`"`] {
			t.Errorf("%s: live sha %s was never written", key, obj.SHA256)
		}
		checkLiveObject(t, c, key, obj)
	}
}
