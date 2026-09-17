// Package repair is the coordinator's background worker. One goroutine
// ticks every Interval and works through the metadata store's queues; it
// does nothing between ticks and never touches a blob that metadata has
// not told it to. Its first duty is garbage collection: draining
// pending_deletes by asking the holding node to quarantine each copy.
package repair

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/manavmann/distributed-object-storage/internal/cluster"
	"github.com/manavmann/distributed-object-storage/internal/meta"
	"github.com/manavmann/distributed-object-storage/internal/nodeclient"
)

// warnAttempts is the failure count at which a queued delete is logged at
// warn level instead of info: something about that node or blob is stuck.
const warnAttempts = 5

// Worker drains pending_deletes. Interval is the tick period and also the
// unit of retry back-off: a row that has failed k times is left alone
// until k×Interval after it was queued. Grace is the DOWN time a node is
// allowed before its blobs are re-replicated; it is consumed by the
// re-replication step. BatchSize bounds the rows one tick fetches, and
// Timeout bounds each request to a node.
type Worker struct {
	DB        *meta.DB
	Client    *nodeclient.Client
	Registry  *cluster.Registry
	Interval  time.Duration
	Grace     time.Duration
	BatchSize int
	Timeout   time.Duration
	Log       *slog.Logger
}

// Run ticks every Interval until ctx is done. It is the worker's only
// goroutine; all work happens inside tick.
func (w *Worker) Run(ctx context.Context) {
	t := time.NewTicker(w.Interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			w.tick(ctx)
		}
	}
}

// tick is one pass of the worker: step 1 garbage-collects pending_deletes.
// Re-replication of under-replicated objects (step 2) and the sweep of
// stale pending_uploads (step 3) follow it once they exist.
func (w *Worker) tick(ctx context.Context) {
	w.gc(ctx)
}

// gc fetches one batch of queued deletes whose node is UP and asks each
// node to drop its copy. A copy the node no longer has counts as deleted.
// Any other failure bumps the row's attempt count; the row is retried on
// a later tick.
func (w *Worker) gc(ctx context.Context) {
	healthy := w.Registry.Healthy()
	ids := make([]string, len(healthy))
	addrs := make(map[string]string, len(healthy))
	for i, n := range healthy {
		ids[i] = n.ID
		addrs[n.ID] = n.Addr
	}
	rows, err := w.DB.PendingDeletesForNodes(ctx, ids, w.BatchSize)
	if err != nil {
		w.Log.Error("gc_fetch_failed", "err", err)
		return
	}
	now := time.Now()
	for _, p := range rows {
		if ctx.Err() != nil {
			return
		}
		notBefore := time.UnixMilli(p.EnqueuedAt).Add(time.Duration(p.Attempts) * w.Interval)
		if p.Attempts > 0 && now.Before(notBefore) {
			continue
		}
		w.deleteCopy(ctx, p, addrs[p.NodeID])
	}
}

func (w *Worker) deleteCopy(ctx context.Context, p meta.PendingDelete, addr string) {
	reqCtx, cancel := context.WithTimeout(ctx, w.Timeout)
	err := w.Client.Delete(reqCtx, addr, p.BlobID)
	cancel()
	if err != nil && !errors.Is(err, nodeclient.ErrNotFound) {
		attempts, berr := w.DB.BumpAttempts(ctx, p.BlobID, p.NodeID)
		if berr != nil {
			w.Log.Error("gc_bump_failed", "blob_id", p.BlobID, "node_id", p.NodeID, "err", berr)
			return
		}
		level := slog.LevelInfo
		if attempts >= warnAttempts {
			level = slog.LevelWarn
		}
		w.Log.Log(ctx, level, "gc_delete_failed", "blob_id", p.BlobID, "node_id", p.NodeID, "attempts", attempts, "err", err)
		return
	}
	if rerr := w.DB.RemovePending(ctx, p.BlobID, p.NodeID); rerr != nil {
		w.Log.Error("gc_remove_failed", "blob_id", p.BlobID, "node_id", p.NodeID, "err", rerr)
		return
	}
	w.Log.Info("gc_deleted", "blob_id", p.BlobID, "node_id", p.NodeID, "already_gone", err != nil)
}
