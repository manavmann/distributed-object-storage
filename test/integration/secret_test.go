package integration

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/manavmann/distributed-object-storage/internal/nodeclient"
	"github.com/manavmann/distributed-object-storage/internal/testcluster"
)

// TestClusterSecret runs a cluster whose coordinator and nodes share a
// CAIRN_CLUSTER_SECRET and checks the data path still works end to end,
// while a caller without the secret is shut out of the internal endpoints.
func TestClusterSecret(t *testing.T) {
	t.Parallel()
	const secret = "s3cret"
	c := testcluster.New(t, testcluster.Opts{Nodes: 3, ClusterSecret: secret})
	cl := c.Client()
	if err := cl.CreateBucket("bkt"); err != nil {
		t.Fatal(err)
	}
	payload := bytes.Repeat([]byte("cairn"), 1000)
	pr, err := cl.Put("bkt", "k", payload, "text/plain")
	if err != nil {
		t.Fatal(err)
	}
	if len(pr.Replicas) != 3 || pr.Quorum != "3/3" {
		t.Fatalf("put = %+v", pr)
	}
	obj, err := cl.Get("bkt", "k")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(obj.Body, payload) || obj.ETag != pr.ETag {
		t.Fatalf("get = %d bytes %s", len(obj.Body), obj.ETag)
	}
	loc, err := cl.Locate("bkt", "k")
	if err != nil {
		t.Fatal(err)
	}
	if loc.SHA256 != pr.SHA256 || len(loc.Replicas) != 3 {
		t.Fatalf("locate = %+v", loc)
	}
	st, err := cl.Status()
	if err != nil {
		t.Fatal(err)
	}
	if len(st.Nodes) != 3 {
		t.Fatalf("status lists %d nodes", len(st.Nodes))
	}

	// The secret really is on: a bare caller gets 401 from both sides.
	resp, out, err := cl.Do(http.MethodPost, "/internal/heartbeat", bytes.NewReader([]byte(`{"node_id":"x","addr":"http://x:1"}`)), "Content-Type", "application/json")
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("bare heartbeat = %d %s", resp.StatusCode, out)
	}
	ctx := context.Background()
	if _, err := nodeclient.New("").Get(ctx, c.NodeURL(0), loc.BlobID); !errors.Is(err, nodeclient.ErrUnavailable) {
		t.Fatalf("bare node get = %v, want ErrUnavailable", err)
	}
	if b, err := nodeclient.New(secret).Get(ctx, c.NodeURL(0), loc.BlobID); err != nil {
		t.Fatalf("node get with secret: %v", err)
	} else {
		b.Close()
	}
}
