package integration

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/manavmann/distributed-object-storage/internal/cluster"
	"github.com/manavmann/distributed-object-storage/internal/testcluster"
)

// putLoad runs workers goroutines that PUT distinct objects under prefix
// until stop is closed. committed maps each key whose PUT returned 200 to
// the holders the PUT reported; failed holds the error of every other.
func putLoad(cl *testcluster.Client, workers int, prefix string, stop <-chan struct{}) (committed map[string][]string, failed map[string]error) {
	var mu sync.Mutex
	committed = make(map[string][]string)
	failed = make(map[string]error)
	var wg sync.WaitGroup
	var next atomic.Int64
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				i := next.Add(1)
				key := prefix + strconv.FormatInt(i, 10)
				pr, err := cl.Put("bkt", key, []byte(fmt.Sprintf("%s payload %d", key, i)), "")
				mu.Lock()
				if err == nil {
					committed[key] = pr.Replicas
				} else {
					failed[key] = err
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	return committed, failed
}

// waitLoad blocks until at least n PUTs of the load have completed,
// judging by the object count metadata reports.
func waitLoad(t *testing.T, c *testcluster.Cluster, n int) {
	t.Helper()
	testcluster.WaitFor(t, func() bool {
		return count(t, c, `SELECT COUNT(*) FROM objects`) >= n
	}, strconv.Itoa(n)+" objects committed")
}

func TestNodeRestartKeepsIdentityAndData(t *testing.T) {
	t.Parallel()
	const nodes, objects = 3, 10
	c := testcluster.New(t, testcluster.Opts{
		Nodes: nodes, RF: 3, W: 2,
		HeartbeatInterval: fastHeartbeat, HeartbeatTimeout: fastHeartbeatTimeout,
	})
	cl := c.Client()
	if err := cl.CreateBucket("bkt"); err != nil {
		t.Fatal(err)
	}
	etags := make(map[string]string, objects)
	for i := 0; i < objects; i++ {
		key := "obj-" + strconv.Itoa(i)
		pr, err := cl.Put("bkt", key, []byte("data "+key), "")
		if err != nil {
			t.Fatal(err)
		}
		if len(pr.Replicas) != nodes {
			t.Fatalf("put %s landed on %v, want all %d nodes", key, pr.Replicas, nodes)
		}
		etags[key] = pr.ETag
	}
	id := c.NodeID(0)
	before, _ := nodeStatus(t, c, id)
	blobs := c.Blobs(0)
	if len(blobs) != objects {
		t.Fatalf("%s holds %v, want %d blobs", id, blobs, objects)
	}

	c.Kill(0)
	waitStatus(t, c, id, cluster.StatusDown)
	c.Restart(0)
	after := waitStatus(t, c, id, cluster.StatusUp)
	if after.NodeID != before.NodeID || after.Addr == before.Addr {
		t.Fatalf("restarted node = %+v, before %+v", after, before)
	}
	if st, err := cl.Status(); err != nil || len(st.Nodes) != nodes {
		t.Fatalf("status after restart = %+v, %v; want the same %d nodes", st, err, nodes)
	}
	if got := c.Blobs(0); !slices.Equal(got, blobs) {
		t.Fatalf("%s blobs/ after restart = %v, before %v", id, got, blobs)
	}
	testcluster.WaitFor(t, func() bool { return blobCounts(t, c)[id] == objects }, "restarted node reports its blob count")

	// With the other holders dead every read must come off the restarted
	// node, so its data is what is served, not just what is on disk. The
	// reader skips a holder it cannot reach, so there is no need to wait
	// for the monitor to mark them DOWN.
	c.Kill(1)
	c.Kill(2)
	for key, etag := range etags {
		obj, err := cl.Get("bkt", key)
		if err != nil {
			t.Fatalf("get %s from the restarted node: %v", key, err)
		}
		if obj.Replica != id || obj.ETag != etag || etagOf(obj.Body) != etag {
			t.Fatalf("get %s = replica %s etag %s (body %s), want %s %s", key, obj.Replica, obj.ETag, etagOf(obj.Body), id, etag)
		}
	}
}

// loadResult is what putLoad returned once the load was stopped.
type loadResult struct {
	committed map[string][]string
	failed    map[string]error
}

// startLoad runs putLoad in the background; the returned channel yields
// its result once stop is closed and the workers have drained.
func startLoad(cl *testcluster.Client, workers int, stop <-chan struct{}) <-chan loadResult {
	done := make(chan loadResult, 1)
	go func() {
		committed, failed := putLoad(cl, workers, "obj-", stop)
		done <- loadResult{committed, failed}
	}()
	return done
}

// keysOf lists m's keys in a fixed order.
func keysOf(m map[string][]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}

func TestCoordinatorRestartUnderLoad(t *testing.T) {
	t.Parallel()
	const nodes, workers = 3, 4
	c := gcCluster(t, nodes, testcluster.Opts{})
	cl := c.Client()

	stop := make(chan struct{})
	loadDone := startLoad(cl, workers, stop)
	waitLoad(t, c, 8)
	c.RestartCoordinator()
	waitLoad(t, c, 16)
	close(stop)
	load := <-loadDone

	testcluster.WaitFor(t, func() bool {
		st, err := cl.Status()
		if err != nil || len(st.Nodes) != nodes {
			return false
		}
		for _, n := range st.Nodes {
			if n.Status != cluster.StatusUp {
				return false
			}
		}
		return true
	}, "nodes UP after coordinator restart")
	if len(load.committed) < 16 {
		t.Fatalf("only %d PUTs committed", len(load.committed))
	}
	for key, err := range load.failed {
		var apiErr *testcluster.APIError
		if errors.As(err, &apiErr) {
			t.Errorf("put %s refused with %v; only a cut connection may fail a PUT during restart", key, err)
		}
	}
	for key, holders := range load.committed {
		if _, now := c.Locate("bkt", key); !sameSet(now, holders) {
			t.Errorf("%s holders = %v, PUT reported %v", key, now, holders)
		}
	}
	checkLiveObjects(t, c, keysOf(load.committed))
	// A PUT whose connection was cut may or may not have committed; if it
	// did it must be as sound as any other.
	for key := range load.failed {
		if obj, ok := liveObject(t, c, key); ok {
			checkLiveObject(t, c, key, obj)
		}
	}

	// Nothing was under-replicated, so the worker must not have touched a
	// single candidate once it started ticking again. The new coordinator
	// has a fresh registry, so the gauge appears only after its first
	// tick, and any candidate that tick saw would have left a
	// repairs_total sample.
	checkNoDanglingRows(t, c, nodes)
	if got := underReplicated(t, c); got != 0 {
		t.Fatalf("under_replicated = %d after restart, want 0", got)
	}
	var text string
	testcluster.WaitFor(t, func() bool {
		text = scrape(t, cl.URL())
		return hasSample(text, "cairn_under_replicated_blobs", "0")
	}, "first repair tick after restart")
	if strings.Contains(text, "cairn_repairs_total{") {
		t.Fatalf("repairs ran after a clean restart:\n%s", text)
	}
}

func TestKillHolderUnderWriteLoadConverges(t *testing.T) {
	t.Parallel()
	const nodes, workers = 4, 4
	c := testcluster.New(t, testcluster.Opts{
		Nodes: nodes, RF: 3, W: 2,
		HeartbeatInterval: fastHeartbeat, HeartbeatTimeout: fastHeartbeatTimeout,
		RepairInterval: gcInterval, RepairGrace: time.Millisecond,
	})
	if err := c.Client().CreateBucket("bkt"); err != nil {
		t.Fatal(err)
	}
	cl := c.Client()

	// The kill lands mid-load and the writes go on while the coordinator
	// still thinks the node is UP, so some PUTs commit at 2/3 with the
	// dead node among their targets.
	stop := make(chan struct{})
	loadDone := startLoad(cl, workers, stop)
	waitLoad(t, c, 12)
	victim := c.NodeID(0)
	c.Kill(0)
	waitLoad(t, c, 20)
	close(stop)
	load := <-loadDone
	for key, err := range load.failed {
		t.Errorf("put %s: %v", key, err)
	}
	waitStatus(t, c, victim, cluster.StatusDown)

	// Converged: no object has other than three UP holders.
	const offRF = `SELECT COUNT(*) FROM objects o WHERE (
		SELECT COUNT(*) FROM replicas r JOIN nodes n ON n.node_id = r.node_id
		WHERE r.blob_id = o.blob_id AND n.status = ?) != 3`
	testcluster.WaitFor(t, func() bool {
		return underReplicated(t, c) == 0 && count(t, c, offRF, cluster.StatusUp) == 0
	}, "every object back at RF 3")
	short := 0
	for key, reported := range load.committed {
		if len(reported) < 3 {
			short++
		}
		blobID, _ := c.Locate("bkt", key)
		up := upHolders(t, c, blobID)
		if len(up) != 3 || slices.Contains(up, victim) {
			t.Errorf("%s: UP holders %v", key, up)
		}
		for _, id := range up {
			if !c.HasBlob(nodeIndex(t, c, nodes, id), blobID) {
				t.Errorf("%s: metadata says %s holds %s but it is not on disk", key, id, blobID)
			}
		}
		for _, id := range reported {
			if id != victim && !slices.Contains(up, id) {
				t.Errorf("%s: %s acknowledged the PUT but is no longer an UP holder (%v)", key, id, up)
			}
		}
	}
	checkLiveObjects(t, c, keysOf(load.committed))
	t.Logf("%d PUTs committed, %d of them short of RF before repair", len(load.committed), short)
}
