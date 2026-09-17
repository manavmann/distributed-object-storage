// Package cluster answers "which storage nodes can take traffic right now".
// The answer is driven by heartbeats: a node is UP while it keeps posting
// to /internal/heartbeat and DOWN once the monitor sees it go quiet for
// longer than the timeout. Metadata is written only on first sight, on an
// address change and on a status transition, never per heartbeat.
package cluster

import (
	"context"
	"errors"
	"fmt"
	"github.com/manavmann/distributed-object-storage/internal/events"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/manavmann/distributed-object-storage/internal/meta"
	"github.com/manavmann/distributed-object-storage/internal/metrics"
)

// ErrInvalidHeartbeat is wrapped by every heartbeat the registry rejects.
var ErrInvalidHeartbeat = errors.New("cluster: invalid heartbeat")

// Node status values as stored in metadata and reported by /cluster/status.
const (
	StatusUp   = "UP"
	StatusDown = "DOWN"
)

// Heartbeat is the body a storage node POSTs to /internal/heartbeat. It
// mirrors storage.Heartbeat field for field; the two packages share only
// the wire format.
type Heartbeat struct {
	NodeID    string `json:"node_id"`
	Addr      string `json:"addr"`
	BlobCount int    `json:"blob_count"`
	FreeBytes uint64 `json:"free_bytes"`
}

// Node is the registry's view of one storage node.
type Node struct {
	ID string
	// Addr is the node's base URL, e.g. http://10.0.0.5:9100.
	Addr            string
	Status          string
	FreeBytes       uint64
	BlobCount       int
	LastSeen        time.Time
	StatusChangedAt time.Time
}

// Registry is the placement view of the cluster. It is safe for
// concurrent use.
type Registry struct {
	db      *meta.DB
	log     *slog.Logger
	metrics *metrics.Metrics
	now     func() time.Time
	timeout time.Duration

	mu    sync.RWMutex
	nodes map[string]*Node
}

// Load seeds a registry from meta.ListNodes. Every node keeps its persisted
// status and gets lastSeen=now(), so a node that was UP has one timeout to
// heartbeat again before the monitor marks it DOWN. now and timeout are
// injectable so tests can drive a fake clock. The nodes_up and
// nodes_total gauges on m are set here and on every status transition.
func Load(ctx context.Context, db *meta.DB, timeout time.Duration, now func() time.Time, m *metrics.Metrics, log *slog.Logger) (*Registry, error) {
	rows, err := db.ListNodes(ctx)
	if err != nil {
		return nil, err
	}
	r := &Registry{db: db, log: log, metrics: m, now: now, timeout: timeout, nodes: make(map[string]*Node, len(rows))}
	seen := now()
	for _, row := range rows {
		r.nodes[row.ID] = &Node{
			ID: row.ID, Addr: row.Addr, Status: row.Status,
			FreeBytes: uint64(row.FreeBytes), BlobCount: int(row.BlobCount),
			LastSeen: seen, StatusChangedAt: time.UnixMilli(row.StatusChangedAt),
		}
	}
	r.refreshGauges()
	return r, nil
}

// Heartbeat records hb: it refreshes the node's address, stats and
// lastSeen, persists the row on first sight or an address change, and
// marks a node that was not UP as UP (persisted, logged as node_up).
func (r *Registry) Heartbeat(ctx context.Context, hb Heartbeat) error {
	if hb.NodeID == "" {
		return fmt.Errorf("%w: empty node_id", ErrInvalidHeartbeat)
	}
	if !strings.HasPrefix(hb.Addr, "http://") && !strings.HasPrefix(hb.Addr, "https://") {
		return fmt.Errorf("%w: addr %q is not an http(s) URL", ErrInvalidHeartbeat, hb.Addr)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.now()
	n, known := r.nodes[hb.NodeID]
	if !known {
		n = &Node{ID: hb.NodeID, Status: StatusUp, StatusChangedAt: now}
	}
	persistRow := !known || n.Addr != hb.Addr
	wasDown := known && n.Status != StatusUp
	if persistRow {
		if err := r.db.UpsertNode(ctx, meta.Node{
			ID: hb.NodeID, Addr: hb.Addr, Status: StatusUp,
			FreeBytes: int64(hb.FreeBytes), BlobCount: int64(hb.BlobCount),
		}); err != nil {
			return err
		}
	}
	if wasDown {
		if err := r.db.SetNodeStatus(ctx, hb.NodeID, StatusUp); err != nil {
			return err
		}
		n.Status = StatusUp
		n.StatusChangedAt = now
		r.log.Info(events.NodeUp, "node_id", hb.NodeID, "addr", hb.Addr)
	}
	n.Addr = hb.Addr
	n.FreeBytes = hb.FreeBytes
	n.BlobCount = hb.BlobCount
	n.LastSeen = now
	r.nodes[hb.NodeID] = n
	if !known || wasDown {
		r.refreshGauges()
	}
	return nil
}

// Monitor sweeps every tick until ctx is done, marking UP nodes that have
// not been heard from within the timeout as DOWN.
func (r *Registry) Monitor(ctx context.Context, tick time.Duration) {
	t := time.NewTicker(tick)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			r.sweep(ctx)
		}
	}
}

// sweep is one pass of the monitor. A persist failure leaves the node UP
// in memory so the next pass retries; the error is logged, not dropped.
func (r *Registry) sweep(ctx context.Context) {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.now()
	for _, n := range r.nodes {
		if n.Status != StatusUp || now.Sub(n.LastSeen) <= r.timeout {
			continue
		}
		if err := r.db.SetNodeStatus(ctx, n.ID, StatusDown); err != nil {
			r.log.Error(events.NodeDownPersistFailed, "node_id", n.ID, "err", err)
			continue
		}
		n.Status = StatusDown
		n.StatusChangedAt = now
		r.log.Warn(events.NodeDown, "node_id", n.ID, "addr", n.Addr, "last_seen", n.LastSeen)
		r.refreshGauges()
	}
}

// Healthy returns the UP nodes sorted by id.
func (r *Registry) Healthy() []Node {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]Node, 0, len(r.nodes))
	for _, n := range r.nodes {
		if n.Status == StatusUp {
			out = append(out, *n)
		}
	}
	sortByID(out)
	return out
}

// All returns every known node sorted by id.
func (r *Registry) All() []Node {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]Node, 0, len(r.nodes))
	for _, n := range r.nodes {
		out = append(out, *n)
	}
	sortByID(out)
	return out
}

// Get returns the node with that id and whether it is known at all.
func (r *Registry) Get(id string) (Node, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	n, ok := r.nodes[id]
	if !ok {
		return Node{}, false
	}
	return *n, true
}

func sortByID(nodes []Node) {
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].ID < nodes[j].ID })
}

// refreshGauges sets nodes_up and nodes_total from the map. The caller
// holds mu.
func (r *Registry) refreshGauges() {
	up := 0
	for _, n := range r.nodes {
		if n.Status == StatusUp {
			up++
		}
	}
	r.metrics.NodesUp.Set(float64(up))
	r.metrics.NodesTotal.Set(float64(len(r.nodes)))
}
