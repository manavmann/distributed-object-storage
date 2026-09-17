// Command storagenode serves checksummed blobs from local disk for a Cairn cluster.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"github.com/manavmann/distributed-object-storage/internal/events"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/manavmann/distributed-object-storage/internal/config"
	"github.com/manavmann/distributed-object-storage/internal/httpx"
	"github.com/manavmann/distributed-object-storage/internal/metrics"
	"github.com/manavmann/distributed-object-storage/internal/storage"
)

const (
	version         = "0.1.0"
	shutdownTimeout = 30 * time.Second
)

func main() {
	healthcheck := flag.Bool("healthcheck", false, "probe the running node's /healthz on CAIRN_ADDR and exit 0 or 1")
	flag.Parse()
	log := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	cfg, err := config.LoadNode(os.Getenv)
	if err != nil {
		log.Error(events.NodeConfig, "err", err)
		os.Exit(2)
	}
	if *healthcheck {
		if err := httpx.Healthcheck(cfg.Addr); err != nil {
			log.Error(events.NodeHealthcheck, "err", err)
			os.Exit(1)
		}
		return
	}
	ln, err := net.Listen("tcp", cfg.Addr)
	if err != nil {
		log.Error(events.NodeListen, "addr", cfg.Addr, "err", err)
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := serve(ctx, cfg, ln, log); err != nil {
		log.Error(events.NodeExit, "err", err)
		os.Exit(1)
	}
}

// serve runs the node on ln until ctx is done, then drains in-flight
// requests and waits for the heartbeat loop before returning.
func serve(ctx context.Context, cfg config.NodeConfig, ln net.Listener, log *slog.Logger) error {
	node, err := storage.NewNode(cfg.DataDir, storage.NodeOptions{
		ID: cfg.NodeID, HeartbeatInterval: cfg.HeartbeatInterval, Metrics: metrics.New(), Log: log,
	})
	if err != nil {
		return err
	}
	// No WriteTimeout: a blob GET streams the whole file, so the only
	// write deadline is the client giving up.
	srv := &http.Server{
		Handler:           node.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
	log.Info(events.NodeStart, "version", version, "node_id", cfg.NodeID, "addr", ln.Addr().String(),
		"data_dir", cfg.DataDir, "coordinator", cfg.CoordinatorURL)

	hbCtx, stopHeartbeat := context.WithCancel(ctx)
	heartbeatDone := node.StartHeartbeat(hbCtx, cfg.CoordinatorURL, cfg.AdvertiseAddr)

	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(ln) }()

	select {
	case err := <-serveErr:
		stopHeartbeat()
		<-heartbeatDone
		return fmt.Errorf("serve: %w", err)
	case <-ctx.Done():
	}
	log.Info(events.NodeShutdown)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	err = srv.Shutdown(shutdownCtx)
	stopHeartbeat()
	<-heartbeatDone
	if serr := <-serveErr; !errors.Is(serr, http.ErrServerClosed) {
		return fmt.Errorf("serve: %w", serr)
	}
	return err
}
