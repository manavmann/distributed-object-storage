package integration

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/manavmann/distributed-object-storage/internal/cluster"
	"github.com/manavmann/distributed-object-storage/internal/meta"
	"github.com/manavmann/distributed-object-storage/internal/placement"
	"github.com/manavmann/distributed-object-storage/internal/testcluster"
)

// nodeIDs lists the ids of a cluster of n nodes, as the harness names them.
func nodeIDs(c *testcluster.Cluster, n int) []string {
	ids := make([]string, n)
	for i := range ids {
		ids[i] = c.NodeID(i)
	}
	return ids
}

// nodeIndex maps a harness node id back to its index.
func nodeIndex(t *testing.T, c *testcluster.Cluster, n int, id string) int {
	t.Helper()
	for i := 0; i < n; i++ {
		if c.NodeID(i) == id {
			return i
		}
	}
	t.Fatalf("no node %s", id)
	return -1
}

// blobCounts is the per-node blob_count from /cluster/status, by node id.
func blobCounts(t *testing.T, c *testcluster.Cluster) map[string]int {
	t.Helper()
	st, err := c.Client().Status()
	if err != nil {
		t.Fatal(err)
	}
	out := make(map[string]int, len(st.Nodes))
	for _, n := range st.Nodes {
		out[n.NodeID] = n.BlobCount
	}
	return out
}

func TestReplicasMatchPlacement(t *testing.T) {
	t.Parallel()
	const nodes = 5
	c := testcluster.New(t, testcluster.Opts{Nodes: nodes, RF: 3, W: 2})
	cl := c.Client()
	if err := cl.CreateBucket("bkt"); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"a", "dir/b", "photos/2026/c.jpg"} {
		pr, err := cl.Put("bkt", key, bytes.Repeat([]byte(key), 100), "")
		if err != nil {
			t.Fatalf("put %s: %v", key, err)
		}
		want, _ := placement.Targets(key, nodeIDs(c, nodes), 3)
		if !sameSet(pr.Replicas, want) || pr.Quorum != "3/3" {
			t.Fatalf("put %s = replicas %v quorum %s, placement says %v", key, pr.Replicas, pr.Quorum, want)
		}
		blobID, holders := c.Locate("bkt", key)
		if !sameSet(holders, want) {
			t.Fatalf("metadata holds %s on %v, placement says %v", blobID, holders, want)
		}
		for i := 0; i < nodes; i++ {
			onDisk, placed := c.HasBlob(i, blobID), slices.Contains(want, c.NodeID(i))
			if onDisk != placed {
				t.Fatalf("%s has blob %s on disk = %v, placed = %v", c.NodeID(i), blobID, onDisk, placed)
			}
		}
	}
}

func TestFailedTargetFallsBack(t *testing.T) {
	t.Parallel()
	const nodes = 4
	c := testcluster.New(t, testcluster.Opts{Nodes: nodes, RF: 3, W: 3})
	cl := c.Client()
	if err := cl.CreateBucket("bkt"); err != nil {
		t.Fatal(err)
	}
	targets, fallbacks := placement.Targets("k", nodeIDs(c, nodes), 3)
	bad := nodeIndex(t, c, nodes, targets[0])
	c.FailWrites(bad, true)

	pr, err := cl.Put("bkt", "k", []byte("fallback"), "")
	if err != nil {
		t.Fatalf("put with a failing target: %v", err)
	}
	want := []string{targets[1], targets[2], fallbacks[0]}
	if !sameSet(pr.Replicas, want) || pr.Quorum != "3/3" {
		t.Fatalf("put = replicas %v quorum %s, want %v", pr.Replicas, pr.Quorum, want)
	}
	blobID, holders := c.Locate("bkt", "k")
	if !sameSet(holders, want) {
		t.Fatalf("metadata holds %s on %v, want %v", blobID, holders, want)
	}
	if c.HasBlob(bad, blobID) {
		t.Fatalf("failed target %s has the blob on disk", targets[0])
	}
	if obj, err := cl.Get("bkt", "k"); err != nil || string(obj.Body) != "fallback" {
		t.Fatalf("get = %q, %v", obj.Body, err)
	}
}

func TestQuorumWithDownNodes(t *testing.T) {
	t.Parallel()
	const nodes = 4
	c := testcluster.New(t, testcluster.Opts{
		Nodes: nodes, RF: 3, W: 2,
		HeartbeatInterval: 20 * time.Millisecond, HeartbeatTimeout: 100 * time.Millisecond,
	})
	cl := c.Client()
	if err := cl.CreateBucket("bkt"); err != nil {
		t.Fatal(err)
	}

	c.Kill(0)
	c.Kill(1)
	waitStatus(t, c, c.NodeID(0), cluster.StatusDown)
	waitStatus(t, c, c.NodeID(1), cluster.StatusDown)
	pr, err := cl.Put("bkt", "two", []byte("two up"), "")
	if err != nil {
		t.Fatalf("put with 2 of 4 nodes down: %v", err)
	}
	survivors := []string{c.NodeID(2), c.NodeID(3)}
	if !sameSet(pr.Replicas, survivors) || pr.Quorum != "2/3" {
		t.Fatalf("put = replicas %v quorum %s, want %v and 2/3", pr.Replicas, pr.Quorum, survivors)
	}
	if _, holders := c.Locate("bkt", "two"); !sameSet(holders, survivors) {
		t.Fatalf("metadata holds on %v, want %v", holders, survivors)
	}
	testcluster.WaitFor(t, func() bool { return blobCounts(t, c)[c.NodeID(3)] == 1 }, "blob count reported")

	c.Kill(2)
	waitStatus(t, c, c.NodeID(2), cluster.StatusDown)
	var apiErr *testcluster.APIError
	if _, err := cl.Put("bkt", "three", []byte("one up"), ""); !errors.As(err, &apiErr) || apiErr.Status != 503 || apiErr.Code != "InsufficientReplicas" {
		t.Fatalf("put with 3 of 4 nodes down = %v, want 503 InsufficientReplicas", err)
	}
	if _, err := c.Meta().GetObject(context.Background(), "bkt", "three"); !errors.Is(err, meta.ErrNoSuchKey) {
		t.Fatalf("object committed without a quorum: %v", err)
	}
	if n, err := c.Meta().CountPending(context.Background()); err != nil || n != 0 {
		t.Fatalf("pending deletes = %d, %v; want 0 (no node was written)", n, err)
	}
	if got := blobCounts(t, c)[c.NodeID(3)]; got != 1 {
		t.Fatalf("survivor holds %d blobs after the refused put, want 1 (untouched)", got)
	}
	if files := c.SpoolFiles(); len(files) != 0 {
		t.Fatalf("spool not cleaned: %v", files)
	}
}

func TestHungNodeIsNotRecorded(t *testing.T) {
	t.Parallel()
	const timeout = 300 * time.Millisecond
	c := testcluster.New(t, testcluster.Opts{Nodes: 3, RF: 3, W: 2, NodeRequestTimeout: timeout})
	cl := c.Client()
	if err := cl.CreateBucket("bkt"); err != nil {
		t.Fatal(err)
	}
	c.HangWrites(1, true)
	start := time.Now()
	pr, err := cl.Put("bkt", "k", []byte("hung"), "")
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("put with a hung node: %v", err)
	}
	want := []string{c.NodeID(0), c.NodeID(2)}
	if !sameSet(pr.Replicas, want) || pr.Quorum != "2/3" {
		t.Fatalf("put = replicas %v quorum %s, want %v and 2/3", pr.Replicas, pr.Quorum, want)
	}
	if elapsed < timeout || elapsed > 10*timeout {
		t.Fatalf("put took %s with a %s node timeout", elapsed, timeout)
	}
	blobID, holders := c.Locate("bkt", "k")
	if !sameSet(holders, want) {
		t.Fatalf("metadata holds %s on %v, want %v", blobID, holders, want)
	}
	if c.HasBlob(1, blobID) {
		t.Fatal("hung node committed the blob")
	}
}

func TestOverwriteQueuesAllOldReplicas(t *testing.T) {
	t.Parallel()
	c := testcluster.New(t, testcluster.Opts{Nodes: 3, RF: 3, W: 2})
	cl := c.Client()
	ctx := context.Background()
	if err := cl.CreateBucket("bkt"); err != nil {
		t.Fatal(err)
	}
	if _, err := cl.Put("bkt", "k", []byte("v1"), ""); err != nil {
		t.Fatal(err)
	}
	first, _ := c.Locate("bkt", "k")
	if _, err := cl.Put("bkt", "k", []byte("v2"), ""); err != nil {
		t.Fatal(err)
	}
	second, holders := c.Locate("bkt", "k")
	if first == second || len(holders) != 3 {
		t.Fatalf("overwrite: blob %s -> %s on %v", first, second, holders)
	}
	if reps, err := c.Meta().Replicas(ctx, first); err != nil || len(reps) != 0 {
		t.Fatalf("old blob still has replicas %v, %v", reps, err)
	}
	if n, err := c.Meta().CountPending(ctx); err != nil || n != 3 {
		t.Fatalf("pending deletes = %d, %v; want 3", n, err)
	}
	for i := 0; i < 3; i++ {
		if !c.HasBlob(i, first) || !c.HasBlob(i, second) {
			t.Fatalf("%s holds old=%v new=%v, want both (old blob not deleted inline)", c.NodeID(i), c.HasBlob(i, first), c.HasBlob(i, second))
		}
	}
	if obj, err := cl.Get("bkt", "k"); err != nil || string(obj.Body) != "v2" {
		t.Fatalf("get after overwrite = %q, %v", obj.Body, err)
	}
}

func TestFailedQuorumQueuesStrayCopy(t *testing.T) {
	t.Parallel()
	c := testcluster.New(t, testcluster.Opts{Nodes: 3, RF: 3, W: 2})
	cl := c.Client()
	ctx := context.Background()
	if err := cl.CreateBucket("bkt"); err != nil {
		t.Fatal(err)
	}
	c.FailWrites(1, true)
	c.FailWrites(2, true)
	var apiErr *testcluster.APIError
	if _, err := cl.Put("bkt", "k", []byte("stray"), ""); !errors.As(err, &apiErr) || apiErr.Status != 503 || apiErr.Code != "InsufficientReplicas" {
		t.Fatalf("put with 2 of 3 nodes failing = %v, want 503 InsufficientReplicas", err)
	}
	if _, err := c.Meta().GetObject(ctx, "bkt", "k"); !errors.Is(err, meta.ErrNoSuchKey) {
		t.Fatalf("object committed below quorum: %v", err)
	}
	if n, err := c.Meta().CountPending(ctx); err != nil || n != 1 {
		t.Fatalf("pending deletes = %d, %v; want 1 for the copy that landed", n, err)
	}
	testcluster.WaitFor(t, func() bool { return blobCounts(t, c)[c.NodeID(0)] == 1 }, "stray copy on "+c.NodeID(0))
	counts := blobCounts(t, c)
	for i := 1; i < 3; i++ {
		if counts[c.NodeID(i)] != 0 {
			t.Fatalf("%s holds %d blobs after a failed write, want 0", c.NodeID(i), counts[c.NodeID(i)])
		}
	}
	if files := c.SpoolFiles(); len(files) != 0 {
		t.Fatalf("spool not cleaned: %v", files)
	}
}
