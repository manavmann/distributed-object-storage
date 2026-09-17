// Package coordinator wires the coordinator's parts together: metadata
// store, node registry, health monitor and the HTTP API. It owns the
// lifetimes of the parts it creates; the metadata store is handed in and
// stays with its owner.
package coordinator

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/manavmann/distributed-object-storage/internal/api"
	"github.com/manavmann/distributed-object-storage/internal/cluster"
	"github.com/manavmann/distributed-object-storage/internal/config"
	"github.com/manavmann/distributed-object-storage/internal/meta"
)

// monitorTick is how often the health monitor looks for silent nodes.
const monitorTick = time.Second

// Deps are the parts the coordinator uses but does not own.
type Deps struct {
	Meta *meta.DB
	Log  *slog.Logger
}

// Coordinator is a control plane minus its listener.
type Coordinator struct {
	nodes   *cluster.Registry
	handler http.Handler

	stopMonitor context.CancelFunc
	monitorDone chan struct{}
}

// New prepares DATA_DIR/spool, loads the node registry from metadata and
// builds the API handler. Spool files left by a previous run are removed:
// their uploads were never committed. Nothing runs until Start.
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
		handler: api.NewHandler(api.Config{
			Meta:          deps.Meta,
			Nodes:         nodes,
			SpoolDir:      spoolDir,
			MaxObjectSize: cfg.MaxObjectSize,
			MaxUploads:    cfg.MaxUploads,
			NodeTimeout:   cfg.NodeTimeout,
			Log:           deps.Log,
		}),
	}, nil
}

// Handler is the public HTTP API.
func (c *Coordinator) Handler() http.Handler {
	return c.handler
}

// Start runs the health monitor until ctx is done or Stop is called.
func (c *Coordinator) Start(ctx context.Context) {
	ctx, c.stopMonitor = context.WithCancel(ctx)
	c.monitorDone = make(chan struct{})
	go func() {
		defer close(c.monitorDone)
		c.nodes.Monitor(ctx, monitorTick)
	}()
}

// Stop halts the health monitor and returns once it has exited, so nothing
// touches the metadata store afterwards. It is a no-op before Start.
func (c *Coordinator) Stop() {
	if c.stopMonitor == nil {
		return
	}
	c.stopMonitor()
	<-c.monitorDone
}
