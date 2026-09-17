package integration

import (
	"context"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/manavmann/distributed-object-storage/internal/cluster"
	"github.com/manavmann/distributed-object-storage/internal/placement"
	"github.com/manavmann/distributed-object-storage/internal/testcluster"
)

// repairCluster starts four nodes with RF=3, W=2, a fast repair tick and
// heartbeat timeout, the given grace and node timeout, and an empty
// bucket. Four nodes at RF=3 leave one spare to repair onto.
func repairCluster(t *testing.T, grace, nodeTimeout time.Duration) *testcluster.Cluster {
	t.Helper()
	c := testcluster.New(t, testcluster.Opts{
		Nodes: 4, RF: 3, W: 2, NodeRequestTimeout: nodeTimeout,
		HeartbeatInterval: fastHeartbeat, HeartbeatTimeout: fastHeartbeatTimeout,
		RepairInterval: gcInterval, RepairGrace: grace,
	})
	if err := c.Client().CreateBucket("bkt"); err != nil {
		t.Fatal(err)
	}
	return c
}

// underReplicated is the under_replicated field of /cluster/status.
func underReplicated(t *testing.T, c *testcluster.Cluster) int {
	t.Helper()
	st, err := c.Client().Status()
	if err != nil {
		t.Fatal(err)
	}
	return st.UnderReplicated
}

// upHolders lists the UP nodes metadata says hold blobID.
func upHolders(t *testing.T, c *testcluster.Cluster, blobID string) []string {
	t.Helper()
	reps, err := c.Meta().Replicas(context.Background(), blobID)
	if err != nil {
		t.Fatal(err)
	}
	var up []string
	for _, r := range reps {
		if r.Status == cluster.StatusUp {
			up = append(up, r.NodeID)
		}
	}
	return up
}

// putOnThree stores bkt/key and returns its blob, its three holders and
// the index of the spare node, the one that does not hold it.
func putOnThree(t *testing.T, c *testcluster.Cluster, key string) (blobID string, holders []string, spare int) {
	t.Helper()
	if _, err := c.Client().Put("bkt", key, []byte("payload for "+key), ""); err != nil {
		t.Fatal(err)
	}
	blobID, holders = c.Locate("bkt", key)
	if len(holders) != 3 {
		t.Fatalf("blob %s on %v, want 3 holders", blobID, holders)
	}
	spare = -1
	for i := 0; i < 4; i++ {
		if !slices.Contains(holders, c.NodeID(i)) {
			spare = i
		}
	}
	return blobID, holders, spare
}

// killNode kills node id and waits for the coordinator to mark it DOWN.
func killNode(t *testing.T, c *testcluster.Cluster, id string) (index int, down testcluster.NodeStatus) {
	t.Helper()
	index = nodeIndex(t, c, 4, id)
	c.Kill(index)
	return index, waitStatus(t, c, id, cluster.StatusDown)
}

// gcProbe stores and deletes a throwaway object and waits for its copies
// to be reclaimed. The worker is sequential, so by then every tick that
// started before the call, including its repair step, has finished.
func gcProbe(t *testing.T, c *testcluster.Cluster, key string) {
	t.Helper()
	if _, err := c.Client().Put("bkt", key, []byte("probe"), ""); err != nil {
		t.Fatal(err)
	}
	blobID, _ := c.Locate("bkt", key)
	if err := c.Client().Delete("bkt", key); err != nil {
		t.Fatal(err)
	}
	testcluster.WaitFor(t, func() bool {
		for i := 0; i < 4; i++ {
			if c.NodeURL(i) != "" && c.HasBlob(i, blobID) {
				return false
			}
		}
		return true
	}, "probe "+key+" reclaimed")
}

func TestRepairAfterGraceOntoExpectedNode(t *testing.T) {
	t.Parallel()
	const grace = 800 * time.Millisecond
	c := repairCluster(t, grace, 5*time.Second)
	blobID, holders, spare := putOnThree(t, c, "k")
	killed, down := killNode(t, c, holders[0])
	if got := underReplicated(t, c); got != 1 {
		t.Fatalf("under_replicated = %d after killing a holder, want 1", got)
	}
	var healthy []string
	for i := 0; i < 4; i++ {
		if i != killed {
			healthy = append(healthy, c.NodeID(i))
		}
	}
	ranked := placement.Rank("k", healthy)
	want := ranked[slices.IndexFunc(ranked, func(id string) bool { return !slices.Contains(holders, id) })]
	if want != c.NodeID(spare) {
		t.Fatalf("expected target %s, but the only non-holder is %s", want, c.NodeID(spare))
	}

	// Inside the grace period nothing may change. The check stops short of
	// the boundary so a tick that starts right at it cannot race with it.
	testcluster.WaitFor(t, func() bool {
		since := time.Since(down.StatusChangedAt)
		if since < grace-200*time.Millisecond && (c.HasBlob(spare, blobID) || len(upHolders(t, c, blobID)) != 2) {
			t.Fatalf("repaired %s onto %s %s after node_down, grace is %s", blobID, c.NodeID(spare), since, grace)
		}
		return since >= grace
	}, "grace to elapse")

	testcluster.WaitFor(t, func() bool {
		return underReplicated(t, c) == 0 && c.HasBlob(spare, blobID)
	}, "repair onto "+want)
	wantUp := append(slices.Clone(holders[1:]), want)
	if up := upHolders(t, c, blobID); !sameSet(up, wantUp) {
		t.Fatalf("UP holders after repair = %v, want %v", up, wantUp)
	}
	if obj, err := c.Client().Get("bkt", "k"); err != nil || string(obj.Body) != "payload for k" {
		t.Fatalf("get after repair = %q, %v", obj.Body, err)
	}
}

func TestRestartWithinGraceNoRepair(t *testing.T) {
	t.Parallel()
	c := repairCluster(t, time.Minute, 5*time.Second)
	blobID, holders, spare := putOnThree(t, c, "k")
	killed, _ := killNode(t, c, holders[0])
	if got := underReplicated(t, c); got != 1 {
		t.Fatalf("under_replicated = %d while a holder is DOWN, want 1", got)
	}
	gcProbe(t, c, "probe-down")
	if up := upHolders(t, c, blobID); c.HasBlob(spare, blobID) || len(up) != 2 {
		t.Fatalf("repaired %s inside the grace period (UP holders %v)", blobID, up)
	}

	c.Restart(killed)
	waitStatus(t, c, c.NodeID(killed), cluster.StatusUp)
	gcProbe(t, c, "probe-up")
	if got := underReplicated(t, c); got != 0 {
		t.Fatalf("under_replicated = %d after the holder came back, want 0", got)
	}
	if c.HasBlob(spare, blobID) {
		t.Fatalf("%s copied onto %s although its holder came back within grace", blobID, c.NodeID(spare))
	}
	if _, now := c.Locate("bkt", "k"); !sameSet(now, holders) {
		t.Fatalf("holders changed from %v to %v", holders, now)
	}
}

func TestRepairVsDeleteHangWrites(t *testing.T) {
	t.Parallel()
	c := repairCluster(t, time.Millisecond, nodeTimeout)
	cl := c.Client()
	blobID, holders, spare := putOnThree(t, c, "k")
	c.HangWrites(spare, true)
	killed, _ := killNode(t, c, holders[0])
	testcluster.WaitFor(t, func() bool { return underReplicated(t, c) == 1 }, "blob seen as under-replicated")

	// The repair copy is hung on the spare; delete the object under it.
	if err := cl.Delete("bkt", "k"); err != nil {
		t.Fatal(err)
	}
	c.HangWrites(spare, false)
	gcProbe(t, c, "probe")
	if reps, err := c.Meta().Replicas(context.Background(), blobID); err != nil || len(reps) != 0 {
		t.Fatalf("zombie replica rows for deleted %s: %+v, %v", blobID, reps, err)
	}
	// Only the killed holder's queued delete may remain.
	testcluster.WaitFor(t, func() bool { return pendingCount(t, c) == 1 }, "live copies reclaimed")
	for i := 0; i < 4; i++ {
		if i != killed && c.HasBlob(i, blobID) {
			t.Fatalf("%s still serves deleted %s", c.NodeID(i), blobID)
		}
	}
	if got := underReplicated(t, c); got != 0 {
		t.Fatalf("under_replicated = %d for a deleted object, want 0", got)
	}
}

func TestNoSourceSkipped(t *testing.T) {
	t.Parallel()
	// The grace outlasts one monitor sweep, so even if the three holders
	// are marked DOWN on different sweeps no repair runs while any is UP.
	c := repairCluster(t, 1500*time.Millisecond, 5*time.Second)
	blobID, holders, spare := putOnThree(t, c, "k")
	for _, id := range holders {
		c.Kill(nodeIndex(t, c, 4, id))
	}
	for _, id := range holders {
		waitStatus(t, c, id, cluster.StatusDown)
	}
	worker := c.Coordinator().Worker()
	testcluster.WaitFor(t, func() bool { return worker.SkippedNoSource() >= 2 }, "no_source skips counted")
	if c.HasBlob(spare, blobID) {
		t.Fatalf("%s appeared on %s with no UP source", blobID, c.NodeID(spare))
	}
	if _, now := c.Locate("bkt", "k"); !sameSet(now, holders) {
		t.Fatalf("holders changed from %v to %v", holders, now)
	}
	if got := underReplicated(t, c); got != 1 {
		t.Fatalf("under_replicated = %d, want 1", got)
	}
}

func TestTwentyObjectsRepaired(t *testing.T) {
	t.Parallel()
	const objects = 20
	c := repairCluster(t, time.Millisecond, 5*time.Second)
	cl := c.Client()
	for i := 0; i < objects; i++ {
		if _, err := cl.Put("bkt", "obj-"+strconv.Itoa(i), []byte("object "+strconv.Itoa(i)), ""); err != nil {
			t.Fatal(err)
		}
	}
	if got := underReplicated(t, c); got != 0 {
		t.Fatalf("under_replicated = %d before any failure, want 0", got)
	}
	killNode(t, c, c.NodeID(0))
	testcluster.WaitFor(t, func() bool { return underReplicated(t, c) == 0 }, "every object repaired")
	for i := 0; i < objects; i++ {
		key := "obj-" + strconv.Itoa(i)
		blobID, _ := c.Locate("bkt", key)
		up := upHolders(t, c, blobID)
		if len(up) != 3 || slices.Contains(up, c.NodeID(0)) {
			t.Fatalf("%s: UP holders %v, want the 3 live nodes", key, up)
		}
		for _, id := range up {
			if !c.HasBlob(nodeIndex(t, c, 4, id), blobID) {
				t.Fatalf("%s: metadata says %s holds %s but it is not on disk", key, id, blobID)
			}
		}
		if obj, err := cl.Get("bkt", key); err != nil || string(obj.Body) != "object "+strconv.Itoa(i) {
			t.Fatalf("get %s = %q, %v", key, obj.Body, err)
		}
	}
}

// TestOverReplicationTrimmed kills a holder, lets repair copy the blob
// onto the spare, then brings the holder back: four UP nodes now hold the
// blob. The worker must trim exactly one copy, the one placement ranks
// last, and the UP holder count must never dip below RF on the way.
func TestOverReplicationTrimmed(t *testing.T) {
	t.Parallel()
	c := repairCluster(t, time.Millisecond, 5*time.Second)
	blobID, holders, spare := putOnThree(t, c, "k")
	killed, _ := killNode(t, c, holders[0])
	testcluster.WaitFor(t, func() bool {
		return underReplicated(t, c) == 0 && c.HasBlob(spare, blobID)
	}, "repair onto "+c.NodeID(spare))

	c.Restart(killed)
	waitStatus(t, c, c.NodeID(killed), cluster.StatusUp)
	var all []string
	for i := 0; i < 4; i++ {
		all = append(all, c.NodeID(i))
	}
	ranked := placement.Rank("k", all)
	victim := ranked[len(ranked)-1]
	victimIndex := nodeIndex(t, c, 4, victim)

	testcluster.WaitFor(t, func() bool {
		up := upHolders(t, c, blobID)
		if len(up) < 3 {
			t.Fatalf("UP holders dropped to %v while trimming", up)
		}
		return len(up) == 3 && !c.HasBlob(victimIndex, blobID)
	}, "trim of "+victim)
	wantUp := slices.DeleteFunc(slices.Clone(all), func(id string) bool { return id == victim })
	if up := upHolders(t, c, blobID); !sameSet(up, wantUp) {
		t.Fatalf("UP holders after trim = %v, want %v", up, wantUp)
	}
	if !c.Quarantined(victimIndex, blobID) {
		t.Fatalf("trimmed copy on %s was not quarantined", victim)
	}
	testcluster.WaitFor(t, func() bool { return pendingCount(t, c) == 0 }, "trim delete reclaimed")
	gcProbe(t, c, "probe")
	if _, now := c.Locate("bkt", "k"); !sameSet(now, wantUp) {
		t.Fatalf("holders after settling = %v, want %v", now, wantUp)
	}
	if got := underReplicated(t, c); got != 0 {
		t.Fatalf("under_replicated = %d after trim, want 0", got)
	}
	if obj, err := c.Client().Get("bkt", "k"); err != nil || string(obj.Body) != "payload for k" {
		t.Fatalf("get after trim = %q, %v", obj.Body, err)
	}
}
