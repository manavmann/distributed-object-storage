// Package replication writes one blob to a quorum of storage nodes.
//
// A Writer asks placement for the N nodes that should hold a key, writes
// to all of them in parallel, then walks the remaining healthy nodes one
// at a time until W copies are acknowledged. It reports which nodes
// acknowledged so the caller can record exactly those, and nothing else,
// in metadata. It knows nothing about HTTP handlers or the metadata store.
package replication

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"time"

	"github.com/manavmann/distributed-object-storage/internal/cluster"
	"github.com/manavmann/distributed-object-storage/internal/placement"
)

// ErrQuorumUnavailable means fewer than W nodes acknowledged the write,
// or fewer than W were healthy to begin with.
var ErrQuorumUnavailable = errors.New("replication: write quorum unavailable")

// Putter is the one node operation a Writer needs. nodeclient.Client
// implements it; tests use a fake.
type Putter interface {
	Put(ctx context.Context, addr, id string, r io.Reader, size int64, sha256 string) error
}

// Writer replicates blobs. N is how many copies to try for, W how many
// must succeed (1 <= W <= N). Timeout bounds each write to one node.
type Writer struct {
	N        int
	W        int
	Client   Putter
	Registry *cluster.Registry
	Timeout  time.Duration
}

// Attempt is one write to one node that did not succeed.
type Attempt struct {
	NodeID string
	Err    error
}

// Result reports every node that acknowledged the write, in placement
// order, and every attempt that failed.
type Result struct {
	Succeeded []string
	Failed    []Attempt
}

// Write stores the first size bytes of spool, whose hex digest is sha256,
// as blobID on the nodes placement chooses for key. Each node gets its own
// SectionReader over the spool, so writes never share a read position.
// If fewer than W nodes are healthy, no node is contacted. If fewer than
// W acknowledge, the error wraps ErrQuorumUnavailable and the Result still
// names the nodes that did, so the caller can queue those copies for
// deletion.
func (w *Writer) Write(ctx context.Context, key, blobID string, size int64, sha256 string, spool *os.File) (Result, error) {
	healthy := w.Registry.Healthy()
	if len(healthy) < w.W {
		return Result{}, fmt.Errorf("%w: %d healthy nodes, need %d", ErrQuorumUnavailable, len(healthy), w.W)
	}
	addrs := make(map[string]string, len(healthy))
	ids := make([]string, 0, len(healthy))
	for _, n := range healthy {
		addrs[n.ID] = n.Addr
		ids = append(ids, n.ID)
	}
	targets, fallbacks := placement.Targets(key, ids, w.N)

	put := func(id string) Attempt {
		nodeCtx, cancel := context.WithTimeout(ctx, w.Timeout)
		defer cancel()
		err := w.Client.Put(nodeCtx, addrs[id], blobID, io.NewSectionReader(spool, 0, size), size, sha256)
		return Attempt{NodeID: id, Err: err}
	}

	// Every target goroutine reports exactly once on the buffered channel,
	// and all of them are drained before this function moves on, so none
	// outlives the call.
	attempts := make(chan Attempt, len(targets))
	for _, id := range targets {
		go func() { attempts <- put(id) }()
	}
	ok := make(map[string]bool, w.N)
	var res Result
	record := func(a Attempt) {
		if a.Err != nil {
			res.Failed = append(res.Failed, a)
			return
		}
		ok[a.NodeID] = true
	}
	for range targets {
		record(<-attempts)
	}
	for _, id := range fallbacks {
		if len(ok) >= w.W {
			break
		}
		record(put(id))
	}
	for _, id := range slices.Concat(targets, fallbacks) {
		if ok[id] {
			res.Succeeded = append(res.Succeeded, id)
		}
	}
	if len(res.Succeeded) < w.W {
		return res, fmt.Errorf("%w: %d of %d nodes acknowledged, need %d", ErrQuorumUnavailable, len(res.Succeeded), len(targets)+len(fallbacks), w.W)
	}
	return res, nil
}
