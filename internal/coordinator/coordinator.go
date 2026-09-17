// Package coordinator wires the coordinator's parts together: metadata
// store, node registry, health monitor, repair/GC worker and the HTTP API.
// It owns the lifetimes of the parts it creates; the metadata store is
// handed in and stays with its owner.
package coordinator

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/manavmann/distributed-object-storage/internal/api"
	"github.com/manavmann/distributed-object-storage/internal/cluster"
	"github.com/manavmann/distributed-object-storage/internal/config"
	"github.com/manavmann/distributed-object-storage/internal/meta"
	"github.com/manavmann/distributed-object-storage/internal/nodeclient"
	"github.com/manavmann/distributed-object-storage/internal/repair"
)

const (
	// monitorTick is how often the health monitor looks for silent nodes.
	monitorTick = time.Second
	// batchSize is how many queued deletes, and how many under-replicated
	// blobs, one repair tick works through.
	batchSize = 64
)

// Deps are the parts the coordinator uses but does not own.
type Deps struct {
	Meta *meta.DB
	Log  *slog.Logger
}

// Coordinator is a control plane minus its listener.
type Coordinator struct {
	nodes   *cluster.Registry
	worker  *repair.Worker
	handler http.Handler

	stop context.CancelFunc
	done sync.WaitGroup
}

// New prepares DATA_DIR/spool, loads the node registry from metadata and
// builds the API handler and repair worker. Spool files left by a previous
// run are removed: their uploads were never committed. Nothing runs until
// Start.
func New(cfg config.CoordinatorConfig, deps Deps) (*Coordinator, error) {
	spoolDir := filepath.Join(cfg.DataDir, "spool")
	if err := os.RemoveAll(spoolDir); err != nil {
		return nil, fmt.Errorf("coordinator: clear spool: %w", err)
	}
	if err := os.MkdirAll(spoolDir, 0o755); err != nil {
		return nil, fmt.Errorf("coordinator: spool dir: %w", err)
	}
	nodes, err := cluster.Load(context.Background(), deps.Meta, cfg.HeartbeatTimeout, time.Now, deps.Log)
	if err != nil {
		return nil, err
	}
	return &Coordinator{
		nodes: nodes,
		worker: &repair.Worker{
			DB:        deps.Meta,
			Client:    nodeclient.New(),
			Registry:  nodes,
			Interval:  cfg.RepairInterval,
			Grace:     cfg.RepairGrace,
			RF:        cfg.RF,
			BatchSize: batchSize,
			Timeout:   cfg.NodeTimeout,
			Log:       deps.Log,
		},
		handler: api.NewHandler(api.Config{
			Meta:          deps.Meta,
			Nodes:         nodes,
			SpoolDir:      spoolDir,
			MaxObjectSize: cfg.MaxObjectSize,
			MaxUploads:    cfg.MaxUploads,
			NodeTimeout:   cfg.NodeTimeout,
			RF:            cfg.RF,
			W:             cfg.W,
			Log:           deps.Log,
		}),
	}, nil
}

// Handler is the public HTTP API.
func (c *Coordinator) Handler() http.Handler {
	return c.handler
}

// Start runs the health monitor and the repair worker, each on its own
// goroutine, until ctx is done or Stop is called.
func (c *Coordinator) Start(ctx context.Context) {
	ctx, c.stop = context.WithCancel(ctx)
	c.done.Add(2)
	go func() {
		defer c.done.Done()
		c.nodes.Monitor(ctx, monitorTick)
	}()
	go func() {
		defer c.done.Done()
		c.worker.Run(ctx)
	}()
}

// Stop halts the monitor and the worker and returns once both have
// exited, so nothing touches the metadata store afterwards. It is a no-op
// before Start.
func (c *Coordinator) Stop() {
	if c.stop == nil {
		return
	}
	c.stop()
	c.done.Wait()
}

// Worker is the repair/GC worker, for tests that read its counters.
func (c *Coordinator) Worker() *repair.Worker {
	return c.worker
}
