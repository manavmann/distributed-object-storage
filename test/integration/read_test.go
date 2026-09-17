package integration

import (
	"bytes"
	"errors"
	"net/http"
	"slices"
	"testing"
	"time"

	"github.com/manavmann/distributed-object-storage/internal/cluster"
	"github.com/manavmann/distributed-object-storage/internal/placement"
	"github.com/manavmann/distributed-object-storage/internal/testcluster"
)

// putReplicated stores payload as bkt/k on a 3-node cluster and returns the
// cluster, its blob id and the indexes of the nodes holding it.
func putReplicated(t *testing.T, payload []byte) (*testcluster.Cluster, string, []int) {
	t.Helper()
	c := testcluster.New(t, testcluster.Opts{Nodes: 3, RF: 3, W: 3})
	cl := c.Client()
	if err := cl.CreateBucket("bkt"); err != nil {
		t.Fatal(err)
	}
	if _, err := cl.Put("bkt", "k", payload, "text/plain"); err != nil {
		t.Fatal(err)
	}
	blobID, holders := c.Locate("bkt", "k")
	if len(holders) != 3 {
		t.Fatalf("holders = %v, want all 3 nodes", holders)
	}
	idx := make([]int, 0, len(holders))
	for _, id := range holders {
		idx = append(idx, nodeIndex(t, c, 3, id))
	}
	return c, blobID, idx
}

func TestReadFailsOverToAnotherHolder(t *testing.T) {
	t.Parallel()
	payload := bytes.Repeat([]byte("failover"), 500)
	c, _, holders := putReplicated(t, payload)
	cl := c.Client()

	c.Kill(holders[0])
	for _, method := range []string{http.MethodGet, http.MethodHead} {
		var obj testcluster.Object
		var err error
		if method == http.MethodGet {
			obj, err = cl.Get("bkt", "k")
		} else {
			obj, err = cl.Head("bkt", "k")
		}
		if err != nil {
			t.Fatalf("%s with one holder killed: %v", method, err)
		}
		if obj.Replica == "" || obj.Replica == c.NodeID(holders[0]) {
			t.Fatalf("%s served from %q, want a live holder", method, obj.Replica)
		}
		if obj.Size != int64(len(payload)) || obj.ContentType != "text/plain" {
			t.Fatalf("%s = %+v", method, obj)
		}
		if method == http.MethodGet && !bytes.Equal(obj.Body, payload) {
			t.Fatalf("GET body = %d bytes, want %d", len(obj.Body), len(payload))
		}
	}
}

func TestReadWithAllHoldersDown(t *testing.T) {
	t.Parallel()
	c, _, holders := putReplicated(t, []byte("gone"))
	cl := c.Client()
	for _, i := range holders {
		c.Kill(i)
	}
	var apiErr *testcluster.APIError
	if _, err := cl.Get("bkt", "k"); !errors.As(err, &apiErr) || apiErr.Status != 503 || apiErr.Code != "NoHealthyReplica" {
		t.Fatalf("GET with every holder killed = %v, want 503 NoHealthyReplica", err)
	}
	// A HEAD response carries no body, so only the status can be checked.
	if _, err := cl.Head("bkt", "k"); !errors.As(err, &apiErr) || apiErr.Status != 503 {
		t.Fatalf("HEAD with every holder killed = %v, want 503", err)
	}
	// Metadata still knows the object; nothing was dropped for mere unreachability.
	if _, h := c.Locate("bkt", "k"); len(h) != 3 {
		t.Fatalf("holders after failed reads = %v, want all 3 still recorded", h)
	}
}

func TestCorruptReplicaIsDroppedAndBypassed(t *testing.T) {
	t.Parallel()
	payload := bytes.Repeat([]byte("intact"), 1000)
	c, blobID, holders := putReplicated(t, payload)
	cl := c.Client()
	// Reads shuffle the UP holders, so corrupt every copy but one: whatever
	// order a read picks, it must pass through a corrupt copy or land on the
	// intact one, and never serve corrupt bytes.
	bad := holders[:2]
	for _, i := range bad {
		c.CorruptBlob(i, blobID)
	}
	obj, err := cl.Get("bkt", "k")
	if err != nil {
		t.Fatalf("GET with corrupt copies: %v", err)
	}
	if obj.Replica != c.NodeID(holders[2]) || !bytes.Equal(obj.Body, payload) {
		t.Fatalf("GET served from %s (%d bytes), want the intact copy on %s", obj.Replica, len(obj.Body), c.NodeID(holders[2]))
	}
	// Whichever corrupt copies the read visited have been quarantined on
	// the node and dropped from metadata; the rest are still recorded and
	// still on disk. Read until every corrupt copy has been discovered.
	testcluster.WaitFor(t, func() bool {
		if _, err := cl.Get("bkt", "k"); err != nil {
			return false
		}
		_, h := c.Locate("bkt", "k")
		return len(h) == 1
	}, "both corrupt replicas dropped")
	if _, h := c.Locate("bkt", "k"); len(h) != 1 || h[0] != c.NodeID(holders[2]) {
		t.Fatalf("holders after corruption = %v, want only %s", h, c.NodeID(holders[2]))
	}
	for _, i := range bad {
		if c.HasBlob(i, blobID) || !c.Quarantined(i, blobID) {
			t.Fatalf("%s: blob committed=%v quarantined=%v, want moved to quarantine", c.NodeID(i), c.HasBlob(i, blobID), c.Quarantined(i, blobID))
		}
	}
	if !c.HasBlob(holders[2], blobID) || c.Quarantined(holders[2], blobID) {
		t.Fatalf("intact copy on %s was touched", c.NodeID(holders[2]))
	}
}

func TestLocateEndpoint(t *testing.T) {
	t.Parallel()
	const nodes = 4
	c := testcluster.New(t, testcluster.Opts{Nodes: nodes, RF: 3, W: 2})
	cl := c.Client()
	if err := cl.CreateBucket("bkt"); err != nil {
		t.Fatal(err)
	}
	payload := []byte("locate me")
	pr, err := cl.Put("bkt", "dir/k", payload, "")
	if err != nil {
		t.Fatal(err)
	}
	loc, err := cl.Locate("bkt", "dir/k")
	if err != nil {
		t.Fatalf("Locate: %v", err)
	}
	blobID, holders := c.Locate("bkt", "dir/k")
	if loc.BlobID != blobID || loc.Size != int64(len(payload)) || loc.SHA256 != pr.SHA256 {
		t.Fatalf("locate = %+v, metadata says blob %s size %d sha %s", loc, blobID, len(payload), pr.SHA256)
	}
	var got []string
	for _, rep := range loc.Replicas {
		got = append(got, rep.NodeID)
		i := nodeIndex(t, c, nodes, rep.NodeID)
		if rep.Addr != c.NodeURL(i) || rep.Status != cluster.StatusUp {
			t.Fatalf("replica %+v, want addr %s status UP", rep, c.NodeURL(i))
		}
		if _, err := time.Parse(time.RFC3339Nano, rep.CreatedAt); err != nil {
			t.Fatalf("replica created_at %q: %v", rep.CreatedAt, err)
		}
	}
	if !sameSet(got, holders) {
		t.Fatalf("locate replicas %v, metadata says %v", got, holders)
	}
	want := placement.Rank("dir/k", nodeIDs(c, nodes))
	if !slices.Equal(loc.Placement, want) {
		t.Fatalf("placement = %v, want %v", loc.Placement, want)
	}
	if !sameSet(got, want[:3]) {
		t.Fatalf("replicas %v disagree with the top-3 placement %v", got, want[:3])
	}

	var apiErr *testcluster.APIError
	if _, err := cl.Locate("bkt", "missing"); !errors.As(err, &apiErr) || apiErr.Status != 404 || apiErr.Code != "NoSuchKey" {
		t.Fatalf("locate unknown key = %v, want 404 NoSuchKey", err)
	}
	if _, err := cl.Locate("nope", "dir/k"); !errors.As(err, &apiErr) || apiErr.Status != 404 || apiErr.Code != "NoSuchBucket" {
		t.Fatalf("locate unknown bucket = %v, want 404 NoSuchBucket", err)
	}
	resp, _, err := cl.Do(http.MethodGet, "/cluster/locate?bucket=bkt", nil)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("locate without key = %d, want 400", resp.StatusCode)
	}
}
