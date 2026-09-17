package replication

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"

	"github.com/manavmann/distributed-object-storage/internal/cluster"
	"github.com/manavmann/distributed-object-storage/internal/meta"
	"github.com/manavmann/distributed-object-storage/internal/nodeclient"
)

// ErrNoHealthyReplica means no node could serve a verified copy of the
// object's blob.
var ErrNoHealthyReplica = errors.New("replication: no healthy replica")

// Getter is the one node operation a Reader needs. nodeclient.Client
// implements it; tests use a fake.
type Getter interface {
	Get(ctx context.Context, addr, id string) (*nodeclient.Blob, error)
}

// Reader opens an object's blob from whichever replica can serve it.
type Reader struct {
	Meta   *meta.DB
	Client Getter
	Log    *slog.Logger
}

// Open returns a verified stream of obj's blob and the id of the node
// serving it. Replicas are tried in a random order among UP nodes first,
// then DOWN nodes, each at most once. A node that reports its copy corrupt
// loses its replica row on the spot (event integrity_failure); a node that
// is unreachable, lacks the blob, or holds bytes that do not match obj is
// skipped. If no node had the blob at all, the object is re-read once in
// case it was overwritten meanwhile, and the new blob is tried the same
// way. Otherwise the error wraps ErrNoHealthyReplica.
func (r *Reader) Open(ctx context.Context, obj meta.Object) (*nodeclient.Blob, string, error) {
	blob, from, allMissing, err := r.open(ctx, obj)
	if err != nil || blob != nil {
		return blob, from, err
	}
	if allMissing {
		fresh, err := r.Meta.GetObject(ctx, obj.Bucket, obj.Key)
		if err != nil {
			return nil, "", err
		}
		if fresh.BlobID != obj.BlobID {
			obj = fresh
			blob, from, _, err := r.open(ctx, obj)
			if err != nil || blob != nil {
				return blob, from, err
			}
		}
	}
	return nil, "", fmt.Errorf("%w: blob %s", ErrNoHealthyReplica, obj.BlobID)
}

// open makes one pass over obj's replicas. allMissing reports that every
// replica (vacuously, none) answered not found; a nil blob with a nil
// error means the pass found nothing to serve.
func (r *Reader) open(ctx context.Context, obj meta.Object) (*nodeclient.Blob, string, bool, error) {
	replicas, err := r.Meta.Replicas(ctx, obj.BlobID)
	if err != nil {
		return nil, "", false, err
	}
	allMissing := true
	for _, rep := range order(replicas) {
		blob, err := r.Client.Get(ctx, rep.Addr, obj.BlobID)
		switch {
		case errors.Is(err, nodeclient.ErrIntegrity):
			allMissing = false
			r.Log.Warn("integrity_failure", "blob_id", obj.BlobID, "node_id", rep.NodeID, "err", err)
			if derr := r.Meta.DropReplica(ctx, obj.BlobID, rep.NodeID); derr != nil {
				return nil, "", false, derr
			}
			continue
		case errors.Is(err, nodeclient.ErrNotFound):
			continue
		case err != nil:
			allMissing = false
			continue
		}
		allMissing = false
		if blob.Length != obj.Size || blob.SHA256 != obj.SHA256 {
			r.Log.Warn("replica_mismatch", "blob_id", obj.BlobID, "node_id", rep.NodeID,
				"length", blob.Length, "sha256", blob.SHA256, "want_length", obj.Size, "want_sha256", obj.SHA256)
			blob.Close()
			continue
		}
		return blob, rep.NodeID, false, nil
	}
	return nil, "", allMissing, nil
}

// order returns replicas with the UP nodes first in random order, then
// the rest in the order given.
func order(replicas []meta.Replica) []meta.Replica {
	out := make([]meta.Replica, 0, len(replicas))
	for _, rep := range replicas {
		if rep.Status == cluster.StatusUp {
			out = append(out, rep)
		}
	}
	rand.Shuffle(len(out), func(i, j int) { out[i], out[j] = out[j], out[i] })
	for _, rep := range replicas {
		if rep.Status != cluster.StatusUp {
			out = append(out, rep)
		}
	}
	return out
}
