// Package repair is the coordinator's background worker. One goroutine
// ticks every Interval and works through the metadata store's queues; it
// does nothing between ticks and never touches a blob that metadata has
// not told it to. Step 1 is garbage collection: draining pending_deletes
// by asking the holding node to quarantine each copy. Step 2 is
// re-replication: copying each blob that fewer than RF UP nodes hold onto
// the node placement ranks first among the healthy non-holders. Step 3 is
// trimming: for each blob that more than RF UP nodes hold, the copy on the
// UP holder placement ranks last is forgotten and queued for deletion.
package repair

import (
	"context"
	"errors"
	"github.com/manavmann/distributed-object-storage/internal/events"
	"log/slog"
	"math/rand/v2"
	"slices"
	"sync/atomic"
	"time"

	"github.com/manavmann/distributed-object-storage/internal/cluster"
	"github.com/manavmann/distributed-object-storage/internal/meta"
	"github.com/manavmann/distributed-object-storage/internal/metrics"
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
// bounds each request to a node. Metrics receives repairs_total and
// integrity_failures_total as they happen and the queue gauges once per
// tick.
type Worker struct {
	DB        *meta.DB
	Client    *nodeclient.Client
	Registry  *cluster.Registry
	Interval  time.Duration
	Grace     time.Duration
	RF        int
	BatchSize int
	Timeout   time.Duration
	Metrics   *metrics.Metrics
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
// step 2 re-replicates under-replicated blobs, step 3 trims
// over-replicated ones, and the queue gauges are refreshed from what is
// left. Stale pending_uploads are swept into pending_deletes by the
// coordinator at startup, not here.
func (w *Worker) tick(ctx context.Context) {
	w.gc(ctx)
	w.rereplicate(ctx)
	w.trim(ctx)
	w.refreshGauges(ctx)
}

// refreshGauges sets under_replicated_blobs, pending_deletes and
// objects_total from metadata. A count that fails is logged and its gauge
// keeps the previous value.
func (w *Worker) refreshGauges(ctx context.Context) {
	if n, err := w.DB.CountUnderReplicated(ctx, w.RF); err != nil {
		w.Log.Error(events.GaugeRefreshFailed, "gauge", "under_replicated_blobs", "err", err)
	} else {
		w.Metrics.UnderReplicatedBlobs.Set(float64(n))
	}
	if n, err := w.DB.CountPending(ctx); err != nil {
		w.Log.Error(events.GaugeRefreshFailed, "gauge", "pending_deletes", "err", err)
	} else {
		w.Metrics.PendingDeletes.Set(float64(n))
	}
	if n, err := w.DB.CountObjects(ctx); err != nil {
		w.Log.Error(events.GaugeRefreshFailed, "gauge", "objects_total", "err", err)
	} else {
		w.Metrics.ObjectsTotal.Set(float64(n))
	}
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
		w.Log.Error(events.GCFetchFailed, "err", err)
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
			w.Log.Error(events.GCBumpFailed, "blob_id", p.BlobID, "node_id", p.NodeID, "err", berr)
			return
		}
		level := slog.LevelInfo
		if attempts >= warnAttempts {
			level = slog.LevelWarn
		}
		w.Log.Log(ctx, level, events.GCDeleteFailed, "blob_id", p.BlobID, "node_id", p.NodeID, "attempts", attempts, "err", err)
		return
	}
	if rerr := w.DB.RemovePending(ctx, p.BlobID, p.NodeID); rerr != nil {
		w.Log.Error(events.GCRemoveFailed, "blob_id", p.BlobID, "node_id", p.NodeID, "err", rerr)
		return
	}
	w.Log.Info(events.GCDeleted, "blob_id", p.BlobID, "node_id", p.NodeID, "already_gone", err != nil)
}

// rereplicate fetches one batch of blobs with fewer than RF UP holders and
// copies each one, sequentially, onto one more node. A blob is skipped
// while any of its DOWN holders went DOWN less than Grace ago (it may be
// coming back) and when no UP holder can serve as the source. Failure on
// one blob is logged and the loop moves on to the next.
func (w *Worker) rereplicate(ctx context.Context) {
	cands, err := w.DB.UnderReplicated(ctx, w.RF, w.BatchSize)
	if err != nil {
		w.Log.Error(events.RepairFetchFailed, "err", err)
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
			w.Metrics.Repairs.WithLabelValues(metrics.ResultSkipped).Inc()
			w.Log.Info(events.RepairSkipped, "blob_id", c.BlobID, "reason", "grace")
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
			w.Metrics.Repairs.WithLabelValues(metrics.ResultSkipped).Inc()
			w.Log.Warn(events.RepairSkipped, "blob_id", c.BlobID, "reason", "no_source")
			continue
		}
		ranked := placement.Rank(c.Key, ids)
		i := slices.IndexFunc(ranked, func(id string) bool { return !holding[id] })
		if i < 0 {
			w.Metrics.Repairs.WithLabelValues(metrics.ResultSkipped).Inc()
			w.Log.Info(events.RepairSkipped, "blob_id", c.BlobID, "reason", "no_target")
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
	w.Log.Info(events.RepairStarted, "blob_id", c.BlobID, "source", source.NodeID, "target", target)
	reqCtx, cancel := context.WithTimeout(ctx, w.Timeout)
	defer cancel()
	blob, err := w.Client.Get(reqCtx, source.Addr, c.BlobID)
	if errors.Is(err, nodeclient.ErrIntegrity) {
		w.Metrics.IntegrityFailures.Inc()
		w.Metrics.Repairs.WithLabelValues(metrics.ResultFailed).Inc()
		w.Log.Warn(events.IntegrityFailure, "blob_id", c.BlobID, "node_id", source.NodeID, "err", err)
		if derr := w.DB.DropReplica(ctx, c.BlobID, source.NodeID); derr != nil {
			w.Log.Error(events.RepairFailed, "blob_id", c.BlobID, "source", source.NodeID, "target", target, "err", derr)
		}
		return
	}
	if err != nil {
		w.Metrics.Repairs.WithLabelValues(metrics.ResultFailed).Inc()
		w.Log.Error(events.RepairFailed, "blob_id", c.BlobID, "source", source.NodeID, "target", target, "err", err)
		return
	}
	defer blob.Close()
	if err := w.Client.Put(reqCtx, targetAddr, c.BlobID, blob, blob.Length, blob.SHA256); err != nil {
		w.Metrics.Repairs.WithLabelValues(metrics.ResultFailed).Inc()
		w.Log.Error(events.RepairFailed, "blob_id", c.BlobID, "source", source.NodeID, "target", target, "err", err)
		return
	}
	added, err := w.DB.AddReplicaIfLive(ctx, c.BlobID, target)
	if err != nil {
		w.Metrics.Repairs.WithLabelValues(metrics.ResultFailed).Inc()
		w.Log.Error(events.RepairCommitFailed, "blob_id", c.BlobID, "target", target, "err", err)
		return
	}
	if !added {
		if err := w.DB.EnqueueDeletes(ctx, c.BlobID, []string{target}); err != nil {
			w.Metrics.Repairs.WithLabelValues(metrics.ResultFailed).Inc()
			w.Log.Error(events.RepairEnqueueFailed, "blob_id", c.BlobID, "target", target, "err", err)
			return
		}
		w.Metrics.Repairs.WithLabelValues(metrics.ResultSkipped).Inc()
		w.Log.Info(events.RepairSkipped, "blob_id", c.BlobID, "target", target, "reason", "stale")
		return
	}
	w.Metrics.Repairs.WithLabelValues(metrics.ResultCompleted).Inc()
	w.Log.Info(events.RepairCompleted, "blob_id", c.BlobID, "source", source.NodeID, "target", target, "duration", time.Since(start))
}

// trim fetches one batch of blobs with more than RF UP holders and, for
// each, drops the replica on the UP holder placement ranks last, queueing
// that copy for the next tick's gc. The drop is conditional: metadata
// re-checks inside the transaction that the blob still has more than RF
// UP holders, so a holder going DOWN in between never leaves the blob
// short.
func (w *Worker) trim(ctx context.Context) {
	cands, err := w.DB.OverReplicated(ctx, w.RF, w.BatchSize)
	if err != nil {
		w.Log.Error(events.TrimFetchFailed, "err", err)
		return
	}
	for _, c := range cands {
		if ctx.Err() != nil {
			return
		}
		var up []string
		for _, h := range c.Holders {
			if h.Status == cluster.StatusUp {
				up = append(up, h.NodeID)
			}
		}
		ranked := placement.Rank(c.Key, up)
		victim := ranked[len(ranked)-1]
		trimmed, err := w.DB.TrimReplica(ctx, c.BlobID, victim, w.RF)
		if err != nil {
			w.Log.Error(events.TrimFailed, "blob_id", c.BlobID, "node_id", victim, "err", err)
			continue
		}
		if !trimmed {
			w.Log.Info(events.TrimSkipped, "blob_id", c.BlobID, "node_id", victim, "reason", "not_over_replicated")
			continue
		}
		w.Log.Info(events.TrimEnqueued, "blob_id", c.BlobID, "node_id", victim, "up_holders", len(up)-1)
	}
}
