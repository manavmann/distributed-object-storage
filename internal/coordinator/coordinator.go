// Package coordinator wires the coordinator's parts together: metadata
// store, node registry and the HTTP API. It owns their lifetimes.
package coordinator

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"

	"github.com/manavmann/distributed-object-storage/internal/api"
	"github.com/manavmann/distributed-object-storage/internal/cluster"
	"github.com/manavmann/distributed-object-storage/internal/config"
	"github.com/manavmann/distributed-object-storage/internal/meta"
)

// Coordinator is a running control plane minus its listener.
type Coordinator struct {
	meta    *meta.DB
	handler http.Handler
}

// New opens DATA_DIR/meta.db, prepares DATA_DIR/spool, records every
// configured node in metadata so replica rows can reference it, and
// builds the API handler. Spool files left by a previous run are removed:
// their uploads were never committed.
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
	for _, n := range cfg.Nodes {
		if err := db.UpsertNode(context.Background(), meta.Node{ID: n.ID, Addr: n.Addr, Status: "up"}); err != nil {
			db.Close()
			return nil, err
		}
	}
	return &Coordinator{
		meta: db,
		handler: api.NewHandler(api.Config{
			Meta:          db,
			Nodes:         cluster.NewStatic(cfg.Nodes),
			SpoolDir:      spoolDir,
			MaxObjectSize: cfg.MaxObjectSize,
			MaxUploads:    cfg.MaxUploads,
			NodeTimeout:   cfg.NodeTimeout,
			Log:           log,
		}),
	}, nil
}

// Handler is the public HTTP API.
func (c *Coordinator) Handler() http.Handler {
	return c.handler
}

// Close releases the metadata store.
func (c *Coordinator) Close() error {
	return c.meta.Close()
}
