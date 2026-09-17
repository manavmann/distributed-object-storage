package storage

import (
	"context"
	"github.com/manavmann/distributed-object-storage/internal/events"
	"github.com/manavmann/distributed-object-storage/internal/metrics"
	"log/slog"
	"net/http"
	"slices"
	"sync"
	"time"
)

// maxScrubReport caps how many scrub failures one heartbeat carries, so
// the body stays inside the coordinator's heartbeat size limit; the rest
// wait for the next heartbeat.
const maxScrubReport = 64

// NodeOptions configures a Node.
type NodeOptions struct {
	// ID is what the node calls itself in heartbeats.
	ID string
	// HeartbeatInterval is the time between heartbeats and the deadline
	// for posting one.
	HeartbeatInterval time.Duration
	// ScrubInterval is the time between scrub passes over blobs/ and
	// ScrubDelay the pause before each blob within a pass.
	ScrubInterval time.Duration
	ScrubDelay    time.Duration
	// Metrics receives cairn_node_blobs and cairn_node_free_bytes with
	// every heartbeat and serves /metrics.
	Metrics *metrics.Metrics
	Log     *slog.Logger
}

// Node is a storage node minus its listener: an open Store, the handler
// that serves it, the heartbeat loop that reports it and the scrubber
// that verifies it.
type Node struct {
	id            string
	interval      time.Duration
	scrubInterval time.Duration
	scrubDelay    time.Duration
	store         *Store
	handler       http.Handler
	metrics       *metrics.Metrics
	log           *slog.Logger

	// mu guards scrubFailures, the quarantined blob ids not yet accepted
	// by the coordinator. The scrubber appends; the heartbeat loop
	// reports a prefix and drops it once acknowledged.
	mu            sync.Mutex
	scrubFailures []string
}

// NewNode opens dir as a Store and builds the node API over it.
func NewNode(dir string, opts NodeOptions) (*Node, error) {
	store, err := Open(dir)
	if err != nil {
		return nil, err
	}
	return &Node{
		id:            opts.ID,
		interval:      opts.HeartbeatInterval,
		scrubInterval: opts.ScrubInterval,
		scrubDelay:    opts.ScrubDelay,
		store:         store,
		handler:       NewHandler(store, opts.Metrics, opts.Log),
		metrics:       opts.Metrics,
		log:           opts.Log,
	}, nil
}

// Handler is the node API.
func (n *Node) Handler() http.Handler {
	return n.handler
}

// StartHeartbeat reports the node to coordinatorURL as advertiseAddr every
// interval until ctx is done. The returned channel closes once the loop
// has exited.
func (n *Node) StartHeartbeat(ctx context.Context, coordinatorURL, advertiseAddr string) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		RunHeartbeat(ctx, &http.Client{Timeout: n.interval}, coordinatorURL, n.interval,
			func() Heartbeat { return n.status(advertiseAddr) }, n.acked, n.log)
	}()
	return done
}

// StartScrub verifies every committed blob once per scrub interval, one
// blob per scrub delay, until ctx is done. A corrupt blob is quarantined
// by the store and reported in the next heartbeat. The returned channel
// closes once the loop has exited.
func (n *Node) StartScrub(ctx context.Context) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		t := time.NewTicker(n.scrubInterval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
			}
			n.scrub(ctx)
		}
	}()
	return done
}

// scrub is one pass of the scrubber.
func (n *Node) scrub(ctx context.Context) {
	if err := n.store.Scrub(ctx, n.scrubDelay, n.scrubFailed); err != nil {
		n.log.Error(events.NodeScrubError, "err", err)
	}
}

// scrubFailed records that the scrubber quarantined blob id.
func (n *Node) scrubFailed(id string) {
	n.log.Warn(events.ScrubFailure, "blob_id", id)
	n.mu.Lock()
	defer n.mu.Unlock()
	n.scrubFailures = append(n.scrubFailures, id)
}

// status is what each heartbeat reports, and what the node gauges show.
// Counting failures are logged and reported as zero rather than skipping
// the heartbeat.
func (n *Node) status(advertiseAddr string) Heartbeat {
	count, err := n.store.BlobCount()
	if err != nil {
		n.log.Warn(events.NodeBlobCount, "err", err)
	}
	free, err := n.store.FreeBytes()
	if err != nil {
		n.log.Warn(events.NodeFreeBytes, "err", err)
	}
	n.metrics.NodeBlobs.Set(float64(count))
	n.metrics.NodeFreeBytes.Set(float64(free))
	n.mu.Lock()
	failures := slices.Clone(n.scrubFailures[:min(len(n.scrubFailures), maxScrubReport)])
	n.mu.Unlock()
	return Heartbeat{NodeID: n.id, Addr: advertiseAddr, BlobCount: count, FreeBytes: free, ScrubFailures: failures}
}

// acked forgets the scrub failures hb carried: the coordinator has
// dropped their replica rows. They are always the oldest pending ids,
// because status reports a prefix and scrubFailed only appends.
func (n *Node) acked(hb Heartbeat) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.scrubFailures = slices.Delete(n.scrubFailures, 0, len(hb.ScrubFailures))
}
