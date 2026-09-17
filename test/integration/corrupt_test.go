package integration

import (
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/manavmann/distributed-object-storage/internal/testcluster"
)

// corruptCluster is a repair cluster with one object on three of its four
// nodes, so a corrupt copy can be re-replicated onto the spare or back
// onto the node that quarantined it.
func corruptCluster(t *testing.T) (c *testcluster.Cluster, blobID string, holders []int) {
	t.Helper()
	c = repairCluster(t, time.Millisecond, 5*time.Second)
	blobID, ids, _ := putOnThree(t, c, "k")
	for _, id := range ids {
		holders = append(holders, nodeIndex(t, c, 4, id))
	}
	return c, blobID, holders
}

// waitFullyReplicated waits until three UP nodes hold blobID according to
// metadata and each of them has it on disk.
func waitFullyReplicated(t *testing.T, c *testcluster.Cluster, blobID string) []string {
	t.Helper()
	var up []string
	testcluster.WaitFor(t, func() bool {
		if underReplicated(t, c) != 0 {
			return false
		}
		up = upHolders(t, c, blobID)
		if len(up) != 3 {
			return false
		}
		for _, id := range up {
			if !c.HasBlob(nodeIndex(t, c, 4, id), blobID) {
				return false
			}
		}
		return true
	}, "three verified holders")
	return up
}

// blobCount is node id's blob_count as last heartbeated to the coordinator.
func blobCount(t *testing.T, c *testcluster.Cluster, id string) int {
	t.Helper()
	n, ok := nodeStatus(t, c, id)
	if !ok {
		t.Fatalf("node %s not in status", id)
	}
	return n.BlobCount
}

func TestOneCorruptServedDroppedRepaired(t *testing.T) {
	t.Parallel()
	c, blobID, holders := corruptCluster(t)
	cl := c.Client()
	bad := holders[0]
	c.CorruptBlob(bad, blobID)

	// Reads shuffle the UP holders, so the first GET may or may not visit
	// the corrupt copy; every GET must still return the right bytes.
	obj, err := cl.Get("bkt", "k")
	if err != nil || string(obj.Body) != "payload for k" || obj.Replica == c.NodeID(bad) {
		t.Fatalf("GET with one corrupt copy = %q from %s, %v", obj.Body, obj.Replica, err)
	}
	testcluster.WaitFor(t, func() bool {
		if obj, err := cl.Get("bkt", "k"); err != nil || string(obj.Body) != "payload for k" {
			t.Fatalf("GET while corrupt copy undiscovered = %q, %v", obj.Body, err)
		}
		// The quarantined file outlives the repair that may already have
		// put a fresh copy back on this node, so it is the durable sign
		// that the read hit the corrupt copy.
		return c.Quarantined(bad, blobID)
	}, "corrupt copy discovered")

	waitFullyReplicated(t, c, blobID)
	if !c.Quarantined(bad, blobID) {
		t.Fatalf("repair removed the quarantined copy on %s", c.NodeID(bad))
	}
	if obj, err := cl.Get("bkt", "k"); err != nil || string(obj.Body) != "payload for k" {
		t.Fatalf("GET after repair = %q, %v", obj.Body, err)
	}
}

func TestTwoCorruptServedFromThirdAndRepaired(t *testing.T) {
	t.Parallel()
	c, blobID, holders := corruptCluster(t)
	cl := c.Client()
	bad, good := holders[:2], holders[2]
	for _, i := range bad {
		c.CorruptBlob(i, blobID)
	}
	obj, err := cl.Get("bkt", "k")
	if err != nil || string(obj.Body) != "payload for k" || obj.Replica != c.NodeID(good) {
		t.Fatalf("GET with two corrupt copies = %q from %s, %v; want the intact copy on %s", obj.Body, obj.Replica, err, c.NodeID(good))
	}
	// A corrupt copy is only found by a read that visits it. Keep reading
	// until both have been quarantined; no read may serve wrong bytes.
	testcluster.WaitFor(t, func() bool {
		if obj, err := cl.Get("bkt", "k"); err != nil || string(obj.Body) != "payload for k" {
			t.Fatalf("GET with corrupt copies = %q, %v", obj.Body, err)
		}
		return c.Quarantined(bad[0], blobID) && c.Quarantined(bad[1], blobID)
	}, "both corrupt copies discovered")

	up := waitFullyReplicated(t, c, blobID)
	if !slices.Contains(up, c.NodeID(good)) {
		t.Fatalf("UP holders after repair = %v, intact holder %s missing", up, c.NodeID(good))
	}
	if obj, err := cl.Get("bkt", "k"); err != nil || string(obj.Body) != "payload for k" {
		t.Fatalf("GET after repair = %q, %v", obj.Body, err)
	}
}

func TestAllCorruptNoSource(t *testing.T) {
	t.Parallel()
	c, blobID, holders := corruptCluster(t)
	cl := c.Client()
	for _, i := range holders {
		id := c.NodeID(i)
		testcluster.WaitFor(t, func() bool { return blobCount(t, c, id) == 1 }, id+" reports one blob")
	}
	for _, i := range holders {
		c.CorruptBlob(i, blobID)
	}

	// One GET visits every UP holder, so every copy is quarantined and
	// every replica row dropped by the time the 503 comes back.
	var apiErr *testcluster.APIError
	if _, err := cl.Get("bkt", "k"); !errors.As(err, &apiErr) || apiErr.Status != 503 || apiErr.Code != "NoHealthyReplica" {
		t.Fatalf("GET with every copy corrupt = %v, want 503 NoHealthyReplica", err)
	}
	if _, h := c.Locate("bkt", "k"); len(h) != 0 {
		t.Fatalf("holders after all-corrupt GET = %v, want none", h)
	}
	for _, i := range holders {
		if c.HasBlob(i, blobID) || !c.Quarantined(i, blobID) {
			t.Fatalf("%s: committed=%v quarantined=%v, want quarantined", c.NodeID(i), c.HasBlob(i, blobID), c.Quarantined(i, blobID))
		}
	}
	worker := c.Coordinator().Worker()
	testcluster.WaitFor(t, func() bool { return worker.SkippedNoSource() >= 1 }, "repair_skipped no_source")
	for _, i := range holders {
		id := c.NodeID(i)
		testcluster.WaitFor(t, func() bool { return blobCount(t, c, id) == 0 }, id+" blob_count decremented")
	}
	if got := underReplicated(t, c); got != 1 {
		t.Fatalf("under_replicated = %d, want 1", got)
	}
	l, err := cl.List("bkt", "", "", 0)
	if err != nil || len(l.Objects) != 1 || l.Objects[0].Key != "k" {
		t.Fatalf("list = %+v, %v; want the object still listed", l, err)
	}
	if _, err := cl.Head("bkt", "k"); !errors.As(err, &apiErr) || apiErr.Status != 503 {
		t.Fatalf("HEAD with every copy corrupt = %v, want 503", err)
	}
	for i := 0; i < 4; i++ {
		if c.HasBlob(i, blobID) {
			t.Fatalf("%s got a copy of %s with no source to read from", c.NodeID(i), blobID)
		}
	}
}
