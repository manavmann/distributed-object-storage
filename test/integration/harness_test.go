// Package integration exercises the coordinator and storage nodes together
// through internal/testcluster.
package integration

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/manavmann/distributed-object-storage/internal/cluster"
	"github.com/manavmann/distributed-object-storage/internal/nodeclient"
	"github.com/manavmann/distributed-object-storage/internal/testcluster"
)

// nodeStatus returns node id's entry in /cluster/status, or false if the
// coordinator does not list it.
func nodeStatus(t *testing.T, c *testcluster.Cluster, id string) (testcluster.NodeStatus, bool) {
	t.Helper()
	st, err := c.Client().Status()
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range st.Nodes {
		if n.NodeID == id {
			return n, true
		}
	}
	return testcluster.NodeStatus{}, false
}

func waitStatus(t *testing.T, c *testcluster.Cluster, id, want string) testcluster.NodeStatus {
	t.Helper()
	var got testcluster.NodeStatus
	testcluster.WaitFor(t, func() bool {
		n, ok := nodeStatus(t, c, id)
		got = n
		return ok && n.Status == want
	}, id+" "+want)
	return got
}

func TestStartPutGet(t *testing.T) {
	t.Parallel()
	c := testcluster.New(t, testcluster.Opts{Nodes: 3})
	cl := c.Client()
	if err := cl.CreateBucket("bkt"); err != nil {
		t.Fatal(err)
	}
	payload := bytes.Repeat([]byte("cairn"), 1000)
	pr, err := cl.Put("bkt", "dir/obj", payload, "text/plain")
	if err != nil {
		t.Fatal(err)
	}
	if pr.Size != int64(len(payload)) || len(pr.Replicas) != 3 || pr.Quorum != "3/3" || pr.ETag != `"`+pr.SHA256+`"` {
		t.Fatalf("put = %+v", pr)
	}
	blobID, holders := c.Locate("bkt", "dir/obj")
	if !sameSet(holders, pr.Replicas) {
		t.Fatalf("Locate = %s on %v, put said %v", blobID, holders, pr.Replicas)
	}
	for i := 0; i < 3; i++ {
		if !c.HasBlob(i, blobID) {
			t.Fatalf("%s does not hold the blob", c.NodeID(i))
		}
	}
	obj, err := cl.Get("bkt", "dir/obj")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(obj.Body, payload) || obj.ContentType != "text/plain" || obj.ETag != pr.ETag {
		t.Fatalf("get = %d bytes %s %s", len(obj.Body), obj.ContentType, obj.ETag)
	}
	head, err := cl.Head("bkt", "dir/obj")
	if err != nil {
		t.Fatal(err)
	}
	if head.Size != int64(len(payload)) || head.Body != nil {
		t.Fatalf("head = %+v", head)
	}
	l, err := cl.List("bkt", "dir/", "", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(l.Objects) != 1 || l.Objects[0].Key != "dir/obj" || l.Truncated {
		t.Fatalf("list = %+v", l)
	}
	if err := cl.Delete("bkt", "dir/obj"); err != nil {
		t.Fatal(err)
	}
	var apiErr *testcluster.APIError
	if _, err := cl.Get("bkt", "dir/obj"); !errors.As(err, &apiErr) || apiErr.Status != 404 || apiErr.Code != "NoSuchKey" {
		t.Fatalf("get after delete = %v", err)
	}
	st, err := cl.Status()
	if err != nil {
		t.Fatal(err)
	}
	if len(st.Nodes) != 3 {
		t.Fatalf("status lists %d nodes", len(st.Nodes))
	}
}

func TestKillAndRestartNode(t *testing.T) {
	t.Parallel()
	c := testcluster.New(t, testcluster.Opts{
		Nodes: 2, HeartbeatInterval: 20 * time.Millisecond, HeartbeatTimeout: 100 * time.Millisecond,
	})
	cl := c.Client()
	if err := cl.CreateBucket("bkt"); err != nil {
		t.Fatal(err)
	}
	if _, err := cl.Put("bkt", "k", []byte("survives"), ""); err != nil {
		t.Fatal(err)
	}
	blobID, holders := c.Locate("bkt", "k")
	victim := -1
	for i := 0; i < 2; i++ {
		if c.NodeID(i) == holders[0] {
			victim = i
		}
	}
	if victim < 0 || !c.HasBlob(victim, blobID) {
		t.Fatalf("holder %s of %s not found among nodes", holders[0], blobID)
	}
	id := c.NodeID(victim)
	before, _ := nodeStatus(t, c, id)
	oldAddr := c.NodeURL(victim)

	c.Kill(victim)
	if c.NodeURL(victim) != "" {
		t.Fatal("killed node still has a URL")
	}
	down := waitStatus(t, c, id, cluster.StatusDown)
	if down.Addr != oldAddr {
		t.Fatalf("DOWN addr = %s, want %s", down.Addr, oldAddr)
	}
	if !c.HasBlob(victim, blobID) {
		t.Fatal("blob gone from disk after Kill")
	}
	if obj, err := cl.Get("bkt", "k"); err != nil || string(obj.Body) != "survives" || obj.Replica == id {
		t.Fatalf("get with holder down = %+v, %v; want the other holder to serve it", obj, err)
	}

	c.Restart(victim)
	newAddr := c.NodeURL(victim)
	if newAddr == "" || newAddr == oldAddr {
		t.Fatalf("restart addr = %q, old %q", newAddr, oldAddr)
	}
	up := waitStatus(t, c, id, cluster.StatusUp)
	if up.Addr != newAddr || up.NodeID != before.NodeID {
		t.Fatalf("UP node = %+v, want %s at %s", up, id, newAddr)
	}
	obj, err := cl.Get("bkt", "k")
	if err != nil {
		t.Fatal(err)
	}
	if string(obj.Body) != "survives" {
		t.Fatalf("get after restart = %q", obj.Body)
	}
	if st, _ := cl.Status(); len(st.Nodes) != 2 {
		t.Fatalf("restart registered a new node: %+v", st.Nodes)
	}
}

func TestCoordinatorRestartKeepsObjects(t *testing.T) {
	t.Parallel()
	c := testcluster.New(t, testcluster.Opts{Nodes: 2})
	cl := c.Client()
	if err := cl.CreateBucket("bkt"); err != nil {
		t.Fatal(err)
	}
	if _, err := cl.Put("bkt", "k", []byte("durable"), ""); err != nil {
		t.Fatal(err)
	}
	blobID, holders := c.Locate("bkt", "k")
	oldURL := cl.URL()

	c.RestartCoordinator()
	if cl.URL() == oldURL {
		t.Fatal("coordinator restarted on the same URL")
	}
	testcluster.WaitFor(t, func() bool {
		st, err := cl.Status()
		if err != nil || len(st.Nodes) != 2 {
			return false
		}
		for _, n := range st.Nodes {
			if n.Status != cluster.StatusUp {
				return false
			}
		}
		return true
	}, "nodes UP after coordinator restart")
	if id, h := c.Locate("bkt", "k"); id != blobID || !sameSet(h, holders) {
		t.Fatalf("Locate after restart = %s on %v, want %s on %v", id, h, blobID, holders)
	}
	obj, err := cl.Get("bkt", "k")
	if err != nil {
		t.Fatal(err)
	}
	if string(obj.Body) != "durable" {
		t.Fatalf("get after restart = %q", obj.Body)
	}
	if l, err := cl.List("bkt", "", "", 0); err != nil || len(l.Objects) != 1 {
		t.Fatalf("list after restart = %+v, %v", l, err)
	}
	if _, err := cl.Put("bkt", "k2", []byte("after"), ""); err != nil {
		t.Fatalf("put after restart: %v", err)
	}
}

func TestFaultFlags(t *testing.T) {
	t.Parallel()
	c := testcluster.New(t, testcluster.Opts{Nodes: 1, NodeRequestTimeout: 100 * time.Millisecond, RF: 1, W: 1})
	cl := c.Client()
	if err := cl.CreateBucket("bkt"); err != nil {
		t.Fatal(err)
	}
	var apiErr *testcluster.APIError

	c.FailWrites(0, true)
	if _, err := cl.Put("bkt", "k", []byte("x"), ""); !errors.As(err, &apiErr) || apiErr.Code != "InsufficientReplicas" {
		t.Fatalf("put with FailWrites = %v", err)
	}
	c.FailWrites(0, false)
	pr, err := cl.Put("bkt", "k", []byte("x"), "")
	if err != nil {
		t.Fatalf("put after FailWrites off: %v", err)
	}
	blobID, _ := c.Locate("bkt", "k")
	if !c.HasBlob(0, blobID) {
		t.Fatal("blob missing after successful put")
	}

	c.HangWrites(0, true)
	start := time.Now()
	if _, err := cl.Put("bkt", "k", []byte("y"), ""); !errors.As(err, &apiErr) || apiErr.Code != "InsufficientReplicas" {
		t.Fatalf("put with HangWrites = %v", err)
	}
	if elapsed := time.Since(start); elapsed < 100*time.Millisecond {
		t.Fatalf("hung put returned after %s, before the node timeout", elapsed)
	}
	c.HangWrites(0, false)
	if obj, err := cl.Get("bkt", "k"); err != nil || string(obj.Body) != "x" || obj.ETag != pr.ETag {
		t.Fatalf("object changed by a failed put: %+v, %v", obj, err)
	}

	nc := nodeclient.New()
	ctx := context.Background()
	c.FailDeletes(0, true)
	if err := nc.Delete(ctx, c.NodeURL(0), blobID); !errors.Is(err, nodeclient.ErrUnavailable) {
		t.Fatalf("delete with FailDeletes = %v", err)
	}
	if !c.HasBlob(0, blobID) {
		t.Fatal("blob removed despite FailDeletes")
	}
	c.FailDeletes(0, false)
	if err := nc.Delete(ctx, c.NodeURL(0), blobID); err != nil {
		t.Fatalf("delete after FailDeletes off: %v", err)
	}
	if c.HasBlob(0, blobID) {
		t.Fatal("blob still committed after delete")
	}

	// A corrupted blob is refused by the node and never served.
	pr2, err := cl.Put("bkt", "c", []byte("intact"), "")
	if err != nil {
		t.Fatal(err)
	}
	id2, _ := c.Locate("bkt", "c")
	c.CorruptBlob(0, id2)
	if _, err := cl.Get("bkt", "c"); !errors.As(err, &apiErr) || apiErr.Code != "NoHealthyReplica" {
		t.Fatalf("get of corrupt blob = %v, want NoHealthyReplica (etag %s)", err, pr2.ETag)
	}
	if c.HasBlob(0, id2) {
		t.Fatal("corrupt blob still under blobs/")
	}
}

// sameSet reports whether a and b hold the same node ids in any order.
func sameSet(a, b []string) bool {
	a, b = slices.Clone(a), slices.Clone(b)
	slices.Sort(a)
	slices.Sort(b)
	return slices.Equal(a, b)
}
