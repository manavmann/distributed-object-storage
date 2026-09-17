package storage

import (
	"context"
	"log/slog"
	"net/http"
	"time"
)

// NodeOptions configures a Node.
type NodeOptions struct {
	// ID is what the node calls itself in heartbeats.
	ID string
	// HeartbeatInterval is the time between heartbeats and the deadline
	// for posting one.
	HeartbeatInterval time.Duration
	Log               *slog.Logger
}

// Node is a storage node minus its listener: an open Store, the handler
// that serves it and the heartbeat loop that reports it.
type Node struct {
	id       string
	interval time.Duration
	store    *Store
	handler  http.Handler
	log      *slog.Logger
}

// NewNode opens dir as a Store and builds the node API over it.
func NewNode(dir string, opts NodeOptions) (*Node, error) {
	store, err := Open(dir)
	if err != nil {
		return nil, err
	}
	return &Node{
		id:       opts.ID,
		interval: opts.HeartbeatInterval,
		store:    store,
		handler:  NewHandler(store, opts.Log),
		log:      opts.Log,
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
			func() Heartbeat { return n.status(advertiseAddr) }, n.log)
	}()
	return done
}

// status is what each heartbeat reports. Counting failures are logged and
// reported as zero rather than skipping the heartbeat.
func (n *Node) status(advertiseAddr string) Heartbeat {
	count, err := n.store.BlobCount()
	if err != nil {
		n.log.Warn("node.blob_count", "err", err)
	}
	free, err := n.store.FreeBytes()
	if err != nil {
		n.log.Warn("node.free_bytes", "err", err)
	}
	return Heartbeat{NodeID: n.id, Addr: advertiseAddr, BlobCount: count, FreeBytes: free}
}
