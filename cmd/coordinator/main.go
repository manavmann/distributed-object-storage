// Command coordinator runs the Cairn control plane: HTTP API, metadata, placement.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/manavmann/distributed-object-storage/internal/config"
	"github.com/manavmann/distributed-object-storage/internal/coordinator"
)

const (
	version         = "0.1.0"
	shutdownTimeout = 30 * time.Second
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	cfg, err := config.LoadCoordinator(os.Getenv)
	if err != nil {
		log.Error("coordinator.config", "err", err)
		os.Exit(2)
	}
	ln, err := net.Listen("tcp", cfg.Addr)
	if err != nil {
		log.Error("coordinator.listen", "addr", cfg.Addr, "err", err)
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := serve(ctx, cfg, ln, log); err != nil {
		log.Error("coordinator.exit", "err", err)
		os.Exit(1)
	}
}

// serve runs the coordinator on ln until ctx is done, then drains in-flight
// requests and closes the metadata store.
func serve(ctx context.Context, cfg config.CoordinatorConfig, ln net.Listener, log *slog.Logger) error {
	c, err := coordinator.New(cfg, log)
	if err != nil {
		return err
	}
	defer c.Close()
	srv := &http.Server{
		Handler:           c.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}
	log.Info("coordinator.start", "version", version, "addr", ln.Addr().String(),
		"data_dir", cfg.DataDir, "nodes", len(cfg.Nodes))

	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(ln) }()

	select {
	case err := <-serveErr:
		return fmt.Errorf("serve: %w", err)
	case <-ctx.Done():
	}
	log.Info("coordinator.shutdown")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	err = srv.Shutdown(shutdownCtx)
	if serr := <-serveErr; !errors.Is(serr, http.ErrServerClosed) {
		return fmt.Errorf("serve: %w", serr)
	}
	return err
}
