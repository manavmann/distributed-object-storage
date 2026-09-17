// Package coordinator wires the coordinator's parts together: metadata
// store, node registry, health monitor and the HTTP API. It owns their
// lifetimes.
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

// Coordinator is a running control plane minus its listener.
type Coordinator struct {
	meta        *meta.DB
	handler     http.Handler
	stopMonitor context.CancelFunc
	monitorDone chan struct{}
}

// New opens DATA_DIR/meta.db, prepares DATA_DIR/spool, loads the node
// registry from metadata, starts the health monitor and builds the API
// handler. Spool files left by a previous run are removed: their uploads
// were never committed.
func New(cfg config.CoordinatorConfig, log *slog.Logger) (*Coordinator, error) {
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return nil, fmt.Errorf("coordinator: data dir: %w", err)
	}
	spoolDir := filepath.Join(cfg.DataDir, "spool")
	if err := os.RemoveAll(spoolDir); err != nil {
		return nil, fmt.Errorf("coordinator: clear spool: %w", err)
	}
	if err := os.MkdirAll(spoolDir, 0o755); err != nil {
		return nil, fmt.Errorf("coordinator: spool dir: %w", err)
	}
	db, err := meta.Open(filepath.Join(cfg.DataDir, "meta.db"))
	if err != nil {
		return nil, err
	}
	nodes, err := cluster.Load(context.Background(), db, cfg.HeartbeatTimeout, time.Now, log)
	if err != nil {
		db.Close()
		return nil, err
	}
	ctx, stop := context.WithCancel(context.Background())
	c := &Coordinator{
		meta: db,
		handler: api.NewHandler(api.Config{
			Meta:          db,
			Nodes:         nodes,
			SpoolDir:      spoolDir,
			MaxObjectSize: cfg.MaxObjectSize,
			MaxUploads:    cfg.MaxUploads,
			NodeTimeout:   cfg.NodeTimeout,
			Log:           log,
		}),
		stopMonitor: stop,
		monitorDone: make(chan struct{}),
	}
	go func() {
		defer close(c.monitorDone)
		nodes.Monitor(ctx, monitorTick)
	}()
	return c, nil
}

// Handler is the public HTTP API.
func (c *Coordinator) Handler() http.Handler {
	return c.handler
}

// Close stops the health monitor and releases the metadata store.
func (c *Coordinator) Close() error {
	c.stopMonitor()
	<-c.monitorDone
	return c.meta.Close()
}
