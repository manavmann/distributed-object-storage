// Package repair is the coordinator's background worker. One goroutine
// ticks every Interval and works through the metadata store's queues; it
// does nothing between ticks and never touches a blob that metadata has
// not told it to. Step 1 is garbage collection: draining pending_deletes
// by asking the holding node to quarantine each copy. Step 2 is
// re-replication: copying each blob that fewer than RF UP nodes hold onto
// the node placement ranks first among the healthy non-holders.
package repair

import (
	"context"
	"errors"
	"log/slog"
	"math/rand/v2"
	"slices"
	"sync/atomic"
	"time"

	"github.com/manavmann/distributed-object-storage/internal/cluster"
	"github.com/manavmann/distributed-object-storage/internal/meta"
	"github.com/manavmann/distributed-object-storage/internal/nodeclient"
	"github.com/manavmann/distributed-object-storage/internal/placement"
)

// warnAttempts is the failure count at which a queued delete is logged at
// warn level instead of info: something about that node or blob is stuck.
const warnAttempts = 5

// Worker drains pending_deletes and re-replicates under-replicated blobs.
// Interval is the tick period and also the unit of retry back-off: a row
// that has failed k times is left alone until k×Interval after it was
// queued. Grace is the DOWN time a node is allowed before its blobs are
// re-replicated. RF is how many UP holders every blob should have.
// BatchSize bounds the rows one tick fetches for each step, and Timeout
// bounds each request to a node.
type Worker struct {
	DB        *meta.DB
	Client    *nodeclient.Client
	Registry  *cluster.Registry
	Interval  time.Duration
	Grace     time.Duration
	RF        int
	BatchSize int
	Timeout   time.Duration
	Log       *slog.Logger

	skippedNoSource atomic.Int64
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

// SkippedNoSource is how many re-replication candidates have been skipped
// because no UP node held a copy to read from.
func (w *Worker) SkippedNoSource() int64 {
	return w.skippedNoSource.Load()
}

// tick is one pass of the worker: step 1 garbage-collects pending_deletes,
// step 2 re-replicates under-replicated blobs. Stale pending_uploads are
// swept into pending_deletes by the coordinator at startup, not here.
func (w *Worker) tick(ctx context.Context) {
	w.gc(ctx)
	w.rereplicate(ctx)
}

// healthy is the registry's UP nodes as the ids placement ranks and the
// addresses requests go to.
func (w *Worker) healthy() (ids []string, addrs map[string]string) {
	nodes := w.Registry.Healthy()
	ids = make([]string, len(nodes))
	addrs = make(map[string]string, len(nodes))
	for i, n := range nodes {
		ids[i] = n.ID
		addrs[n.ID] = n.Addr
	}
	return ids, addrs
}

// gc fetches one batch of queued deletes whose node is UP and asks each
// node to drop its copy. A copy the node no longer has counts as deleted.
// Any other failure bumps the row's attempt count; the row is retried on
// a later tick.
func (w *Worker) gc(ctx context.Context) {
	ids, addrs := w.healthy()
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

// rereplicate fetches one batch of blobs with fewer than RF UP holders and
// copies each one, sequentially, onto one more node. A blob is skipped
// while any of its DOWN holders went DOWN less than Grace ago (it may be
// coming back) and when no UP holder can serve as the source. Failure on
// one blob is logged and the loop moves on to the next.
func (w *Worker) rereplicate(ctx context.Context) {
	cands, err := w.DB.UnderReplicated(ctx, w.RF, w.BatchSize)
	if err != nil {
		w.Log.Error("repair_fetch_failed", "err", err)
		return
	}
	if len(cands) == 0 {
		return
	}
	ids, addrs := w.healthy()
	now := time.Now()
	for _, c := range cands {
		if ctx.Err() != nil {
			return
		}
		if c.LastDownChange != 0 && now.Sub(time.UnixMilli(c.LastDownChange)) < w.Grace {
			w.Log.Info("repair_skipped", "blob_id", c.BlobID, "reason", "grace")
			continue
		}
		var sources []meta.Replica
		holding := make(map[string]bool, len(c.Holders))
		for _, h := range c.Holders {
			holding[h.NodeID] = true
			if h.Status == cluster.StatusUp {
				sources = append(sources, h)
			}
		}
		if len(sources) == 0 {
			w.skippedNoSource.Add(1)
			w.Log.Warn("repair_skipped", "blob_id", c.BlobID, "reason", "no_source")
			continue
		}
		ranked := placement.Rank(c.Key, ids)
		i := slices.IndexFunc(ranked, func(id string) bool { return !holding[id] })
		if i < 0 {
			w.Log.Info("repair_skipped", "blob_id", c.BlobID, "reason", "no_target")
			continue
		}
		target := ranked[i]
		source := sources[rand.IntN(len(sources))]
		w.copy(ctx, c, source, target, addrs[target])
	}
}

// copy streams blob c from source to target and, if the object still
// references the blob, records the new replica. If the object went away
// while the copy was in flight the fresh copy is queued for deletion. A
// source that reports its copy corrupt has quarantined it, so its replica
// row is dropped, as a failover read would, and the blob waits for the
// next tick to pick another source.
func (w *Worker) copy(ctx context.Context, c meta.UnderReplicated, source meta.Replica, target, targetAddr string) {
	start := time.Now()
	w.Log.Info("repair_started", "blob_id", c.BlobID, "source", source.NodeID, "target", target)
	reqCtx, cancel := context.WithTimeout(ctx, w.Timeout)
	defer cancel()
	blob, err := w.Client.Get(reqCtx, source.Addr, c.BlobID)
	if errors.Is(err, nodeclient.ErrIntegrity) {
		w.Log.Warn("integrity_failure", "blob_id", c.BlobID, "node_id", source.NodeID, "err", err)
		if derr := w.DB.DropReplica(ctx, c.BlobID, source.NodeID); derr != nil {
			w.Log.Error("repair_failed", "blob_id", c.BlobID, "source", source.NodeID, "target", target, "err", derr)
		}
		return
	}
	if err != nil {
		w.Log.Error("repair_failed", "blob_id", c.BlobID, "source", source.NodeID, "target", target, "err", err)
		return
	}
	defer blob.Close()
	if err := w.Client.Put(reqCtx, targetAddr, c.BlobID, blob, blob.Length, blob.SHA256); err != nil {
		w.Log.Error("repair_failed", "blob_id", c.BlobID, "source", source.NodeID, "target", target, "err", err)
		return
	}
	added, err := w.DB.AddReplicaIfLive(ctx, c.BlobID, target)
	if err != nil {
		w.Log.Error("repair_commit_failed", "blob_id", c.BlobID, "target", target, "err", err)
		return
	}
	if !added {
		if err := w.DB.EnqueueDeletes(ctx, c.BlobID, []string{target}); err != nil {
			w.Log.Error("repair_enqueue_failed", "blob_id", c.BlobID, "target", target, "err", err)
			return
		}
		w.Log.Info("repair_skipped", "blob_id", c.BlobID, "target", target, "reason", "stale")
		return
	}
	w.Log.Info("repair_completed", "blob_id", c.BlobID, "source", source.NodeID, "target", target, "duration", time.Since(start))
}
