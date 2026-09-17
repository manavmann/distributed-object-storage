package integration

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"testing"
	"time"

	"github.com/manavmann/distributed-object-storage/internal/cluster"
	"github.com/manavmann/distributed-object-storage/internal/meta"
	"github.com/manavmann/distributed-object-storage/internal/nodeclient"
	"github.com/manavmann/distributed-object-storage/internal/testcluster"
)

// gcInterval is the repair tick used by the GC tests: fast enough that
// WaitFor sees queued deletes drain within its budget.
const gcInterval = 50 * time.Millisecond

// gcCluster starts n nodes with a fast repair tick and an empty bucket.
func gcCluster(t *testing.T, n int, opts testcluster.Opts) *testcluster.Cluster {
	t.Helper()
	opts.Nodes = n
	opts.RF, opts.W = 3, 2
	opts.RepairInterval = gcInterval
	c := testcluster.New(t, opts)
	if err := c.Client().CreateBucket("bkt"); err != nil {
		t.Fatal(err)
	}
	return c
}

// pendingCount is the size of pending_deletes right now.
func pendingCount(t *testing.T, c *testcluster.Cluster) int {
	t.Helper()
	n, err := c.Meta().CountPending(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return n
}

// blobOnAny reports whether any of nodes 0..n-1 still has blob id on disk.
func blobOnAny(c *testcluster.Cluster, n int, id string) bool {
	for i := 0; i < n; i++ {
		if c.HasBlob(i, id) {
			return true
		}
	}
	return false
}

func TestDeleteReclaimsAllCopies(t *testing.T) {
	t.Parallel()
	const nodes = 3
	c := gcCluster(t, nodes, testcluster.Opts{})
	cl := c.Client()
	if _, err := cl.Put("bkt", "k", []byte("bye"), ""); err != nil {
		t.Fatal(err)
	}
	blobID, holders := c.Locate("bkt", "k")
	if len(holders) != nodes {
		t.Fatalf("blob %s on %v, want all %d nodes", blobID, holders, nodes)
	}
	if err := cl.Delete("bkt", "k"); err != nil {
		t.Fatal(err)
	}
	testcluster.WaitFor(t, func() bool {
		return pendingCount(t, c) == 0 && !blobOnAny(c, nodes, blobID)
	}, "every copy reclaimed and the queue empty")
	for i := 0; i < nodes; i++ {
		if !c.Quarantined(i, blobID) {
			t.Fatalf("%s reclaimed %s without quarantining it", c.NodeID(i), blobID)
		}
	}
}

func TestOverwriteReclaimsOldCopiesOnly(t *testing.T) {
	t.Parallel()
	const nodes = 3
	c := gcCluster(t, nodes, testcluster.Opts{})
	cl := c.Client()
	if _, err := cl.Put("bkt", "k", []byte("v1"), ""); err != nil {
		t.Fatal(err)
	}
	first, _ := c.Locate("bkt", "k")
	if _, err := cl.Put("bkt", "k", []byte("v2"), ""); err != nil {
		t.Fatal(err)
	}
	second, holders := c.Locate("bkt", "k")
	testcluster.WaitFor(t, func() bool {
		return pendingCount(t, c) == 0 && !blobOnAny(c, nodes, first)
	}, "old copies reclaimed")
	for i := 0; i < nodes; i++ {
		if !c.HasBlob(i, second) {
			t.Fatalf("%s lost the current blob %s (holders %v)", c.NodeID(i), second, holders)
		}
	}
	if obj, err := cl.Get("bkt", "k"); err != nil || string(obj.Body) != "v2" {
		t.Fatalf("get after gc = %q, %v", obj.Body, err)
	}
}

func TestDeleteWithKilledHolderReclaimedAfterRestart(t *testing.T) {
	t.Parallel()
	const nodes = 3
	c := gcCluster(t, nodes, testcluster.Opts{
		HeartbeatInterval: fastHeartbeat, HeartbeatTimeout: fastHeartbeatTimeout,
	})
	cl := c.Client()
	if _, err := cl.Put("bkt", "k", []byte("held"), ""); err != nil {
		t.Fatal(err)
	}
	blobID, _ := c.Locate("bkt", "k")
	c.Kill(0)
	waitStatus(t, c, c.NodeID(0), cluster.StatusDown)
	if err := cl.Delete("bkt", "k"); err != nil {
		t.Fatal(err)
	}
	testcluster.WaitFor(t, func() bool {
		return pendingCount(t, c) == 1 && !c.HasBlob(1, blobID) && !c.HasBlob(2, blobID)
	}, "live copies reclaimed, dead node's row kept")
	if !c.HasBlob(0, blobID) {
		t.Fatal("killed node's copy vanished from disk")
	}

	c.Restart(0)
	waitStatus(t, c, c.NodeID(0), cluster.StatusUp)
	testcluster.WaitFor(t, func() bool {
		return pendingCount(t, c) == 0 && !c.HasBlob(0, blobID)
	}, "restarted node's copy reclaimed")
}

func TestFailedDeleteRetries(t *testing.T) {
	t.Parallel()
	const nodes = 3
	c := gcCluster(t, nodes, testcluster.Opts{})
	cl := c.Client()
	ctx := context.Background()
	if _, err := cl.Put("bkt", "k", []byte("sticky"), ""); err != nil {
		t.Fatal(err)
	}
	blobID, _ := c.Locate("bkt", "k")
	c.FailDeletes(0, true)
	if err := cl.Delete("bkt", "k"); err != nil {
		t.Fatal(err)
	}
	testcluster.WaitFor(t, func() bool {
		rows, err := c.Meta().PendingDeletesForNodes(ctx, []string{c.NodeID(0)}, 10)
		if err != nil {
			t.Fatal(err)
		}
		return pendingCount(t, c) == 1 && len(rows) == 1 && rows[0].BlobID == blobID && rows[0].Attempts >= 2
	}, "failing delete retried with attempts counted")
	if !c.HasBlob(0, blobID) {
		t.Fatal("copy removed although the node refused every DELETE")
	}

	c.FailDeletes(0, false)
	testcluster.WaitFor(t, func() bool {
		return pendingCount(t, c) == 0 && !c.HasBlob(0, blobID)
	}, "delete succeeds once the fault clears")
}

func TestFailedQuorumStrayReclaimed(t *testing.T) {
	t.Parallel()
	const nodes = 3
	c := gcCluster(t, nodes, testcluster.Opts{})
	cl := c.Client()
	c.FailWrites(1, true)
	c.FailWrites(2, true)
	var apiErr *testcluster.APIError
	if _, err := cl.Put("bkt", "k", []byte("stray"), ""); !errors.As(err, &apiErr) || apiErr.Status != 503 {
		t.Fatalf("put with 2 of 3 nodes failing = %v, want 503", err)
	}
	testcluster.WaitFor(t, func() bool {
		return pendingCount(t, c) == 0 && blobCounts(t, c)[c.NodeID(0)] == 0
	}, "stray copy reclaimed")
}

// seedBlob writes body to node i as blob id, bypassing the coordinator, as
// a PUT that crashed mid-replication would have left it.
func seedBlob(t *testing.T, c *testcluster.Cluster, i int, id string, body []byte) {
	t.Helper()
	sum := sha256.Sum256(body)
	if err := nodeclient.New("").Put(context.Background(), c.NodeURL(i), id, bytes.NewReader(body), int64(len(body)), hex.EncodeToString(sum[:])); err != nil {
		t.Fatalf("seed %s on %s: %v", id, c.NodeID(i), err)
	}
}

func TestStaleUploadIntentSweptOnRestart(t *testing.T) {
	t.Parallel()
	const nodes = 2
	c := gcCluster(t, nodes, testcluster.Opts{})
	ctx := context.Background()

	// Two uploads that never committed: one whose coordinator died long
	// ago, one that is younger than 2× the node timeout and so might still
	// have a node write in flight.
	const staleID, freshID = "stale-upload", "fresh-upload"
	seedBlob(t, c, 0, staleID, []byte("orphan"))
	seedBlob(t, c, 0, freshID, []byte("in flight"))
	intents := []meta.PendingUpload{
		{BlobID: staleID, Bucket: "bkt", Key: "old", StartedAt: time.Now().Add(-time.Hour).UnixMilli()},
		{BlobID: freshID, Bucket: "bkt", Key: "new", StartedAt: time.Now().UnixMilli()},
	}
	for _, u := range intents {
		if err := c.Meta().BeginUpload(ctx, u); err != nil {
			t.Fatal(err)
		}
	}

	c.RestartCoordinator()

	testcluster.WaitFor(t, func() bool {
		return pendingCount(t, c) == 0 && !c.HasBlob(0, staleID)
	}, "stale upload reclaimed and the queue drained")
	if !c.Quarantined(0, staleID) {
		t.Fatalf("%s reclaimed %s without quarantining it", c.NodeID(0), staleID)
	}
	if !c.HasBlob(0, freshID) {
		t.Fatalf("fresh upload %s was reclaimed", freshID)
	}
	left, err := c.Meta().PendingUploads(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(left) != 1 || left[0] != intents[1] {
		t.Fatalf("intents after restart = %+v; want only %+v", left, intents[1])
	}
}
