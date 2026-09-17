package testcluster

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/manavmann/distributed-object-storage/internal/blob"
	"github.com/manavmann/distributed-object-storage/internal/cluster"
	"github.com/manavmann/distributed-object-storage/internal/config"
	"github.com/manavmann/distributed-object-storage/internal/coordinator"
	"github.com/manavmann/distributed-object-storage/internal/meta"
	"github.com/manavmann/distributed-object-storage/internal/storage"
)

// MaxObjectSize is the largest PUT body the cluster accepts.
const MaxObjectSize = 64 << 20

// Opts sizes and times a cluster. Zero values take the defaults below.
type Opts struct {
	// Nodes is how many storage nodes to start. Default 1.
	Nodes int
	// HeartbeatInterval is how often each node reports. Default 50ms.
	HeartbeatInterval time.Duration
	// HeartbeatTimeout is the silence after which the coordinator marks a
	// node DOWN. Default 1m, so a killed node only goes DOWN in tests that
	// ask for a shorter timeout.
	HeartbeatTimeout time.Duration
	// NodeRequestTimeout bounds one coordinator request to a node. Default 5s.
	NodeRequestTimeout time.Duration
	// RF and W are the PUT replication factor and write quorum. Defaults
	// 3 and 2, the production defaults; a cluster with fewer than W nodes
	// refuses every PUT, so small clusters set both explicitly.
	RF int
	W  int
}

// Cluster is a running coordinator plus its nodes. Its methods must be
// called from the test goroutine; the fault switches are the exception and
// may be flipped at any time.
type Cluster struct {
	t     *testing.T
	log   *slog.Logger
	opts  Opts
	cfg   config.CoordinatorConfig
	meta  *meta.DB
	coord *coordinator.Coordinator
	srv   *httptest.Server
	nodes []*node
}

// node is one storage node and the harness state around it. dir and id
// outlive Kill so Restart brings the same node back.
type node struct {
	id     string
	dir    string
	faults faults

	srv           *httptest.Server
	stopHeartbeat context.CancelFunc
	heartbeatDone <-chan struct{}
}

// New starts opts.Nodes storage nodes and a coordinator on ephemeral ports
// and returns once every node is UP in /cluster/status. Everything is torn
// down by t.Cleanup.
func New(t *testing.T, opts Opts) *Cluster {
	t.Helper()
	if opts.Nodes == 0 {
		opts.Nodes = 1
	}
	if opts.HeartbeatInterval == 0 {
		opts.HeartbeatInterval = 50 * time.Millisecond
	}
	if opts.HeartbeatTimeout == 0 {
		opts.HeartbeatTimeout = time.Minute
	}
	if opts.NodeRequestTimeout == 0 {
		opts.NodeRequestTimeout = 5 * time.Second
	}
	if opts.RF == 0 {
		opts.RF = 3
	}
	if opts.W == 0 {
		opts.W = 2
	}
	c := &Cluster{
		t:    t,
		log:  slog.New(slog.NewTextHandler(io.Discard, nil)),
		opts: opts,
		cfg: config.CoordinatorConfig{
			Addr:             ":0",
			DataDir:          t.TempDir(),
			MaxObjectSize:    MaxObjectSize,
			MaxUploads:       16,
			NodeTimeout:      opts.NodeRequestTimeout,
			HeartbeatTimeout: opts.HeartbeatTimeout,
			RF:               opts.RF,
			W:                opts.W,
		},
	}
	if err := c.cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.close)
	c.startCoordinator()
	for i := 0; i < opts.Nodes; i++ {
		n := &node{id: "n" + strconv.Itoa(i), dir: t.TempDir()}
		c.nodes = append(c.nodes, n)
		c.startNode(n)
	}
	WaitFor(t, func() bool {
		st, err := c.Client().Status()
		if err != nil || len(st.Nodes) != opts.Nodes {
			return false
		}
		for _, n := range st.Nodes {
			if n.Status != cluster.StatusUp {
				return false
			}
		}
		return true
	}, "all nodes UP")
	return c
}

// close tears the cluster down in dependency order: nodes stop reporting,
// then the coordinator stops serving and monitoring, then metadata closes.
func (c *Cluster) close() {
	for _, n := range c.nodes {
		c.stopNode(n)
	}
	c.stopCoordinator()
	if err := c.meta.Close(); err != nil {
		c.t.Errorf("close meta: %v", err)
	}
}

func (c *Cluster) startCoordinator() {
	c.t.Helper()
	db, err := meta.Open(filepath.Join(c.cfg.DataDir, "meta.db"))
	if err != nil {
		c.t.Fatal(err)
	}
	c.meta = db
	coord, err := coordinator.New(c.cfg, coordinator.Deps{Meta: db, Log: c.log})
	if err != nil {
		c.t.Fatal(err)
	}
	c.coord = coord
	c.srv = httptest.NewServer(coord.Handler())
	coord.Start(context.Background())
}

func (c *Cluster) stopCoordinator() {
	c.srv.Close()
	c.coord.Stop()
}

// startNode opens n's directory, serves it and starts its heartbeat loop
// against the current coordinator.
func (c *Cluster) startNode(n *node) {
	c.t.Helper()
	sn, err := storage.NewNode(n.dir, storage.NodeOptions{
		ID: n.id, HeartbeatInterval: c.opts.HeartbeatInterval, Log: c.log,
	})
	if err != nil {
		c.t.Fatal(err)
	}
	n.srv = httptest.NewServer(n.faults.wrap(sn.Handler()))
	c.startHeartbeat(n, sn)
}

func (c *Cluster) startHeartbeat(n *node, sn *storage.Node) {
	ctx, cancel := context.WithCancel(context.Background())
	n.stopHeartbeat = cancel
	n.heartbeatDone = sn.StartHeartbeat(ctx, c.srv.URL, n.srv.URL)
}

func (c *Cluster) stopHeartbeat(n *node) {
	n.stopHeartbeat()
	<-n.heartbeatDone
}

// stopNode is a no-op for a node that is already down.
func (c *Cluster) stopNode(n *node) {
	if n.srv == nil {
		return
	}
	c.stopHeartbeat(n)
	n.srv.CloseClientConnections()
	n.srv.Close()
	n.srv = nil
}

// Kill takes node i off the network: it stops heartbeating, in-flight
// requests are cut and its port stops answering. Its directory and id are
// kept for Restart.
func (c *Cluster) Kill(i int) {
	c.stopNode(c.nodes[i])
}

// Restart brings node i back with the same id and data on a new port. The
// first heartbeat goes out immediately; wait on Status to see it UP.
func (c *Cluster) Restart(i int) {
	c.t.Helper()
	if c.nodes[i].srv != nil {
		c.t.Fatalf("Restart(%d): node is running", i)
	}
	c.startNode(c.nodes[i])
}

// RestartCoordinator stops the coordinator, closes and reopens metadata and
// starts a new coordinator on a new port. Live nodes are pointed at the
// new address, as a stable coordinator hostname would.
func (c *Cluster) RestartCoordinator() {
	c.t.Helper()
	c.stopCoordinator()
	if err := c.meta.Close(); err != nil {
		c.t.Fatal(err)
	}
	c.startCoordinator()
	for _, n := range c.nodes {
		if n.srv == nil {
			continue
		}
		c.stopHeartbeat(n)
		sn, err := storage.NewNode(n.dir, storage.NodeOptions{
			ID: n.id, HeartbeatInterval: c.opts.HeartbeatInterval, Log: c.log,
		})
		if err != nil {
			c.t.Fatal(err)
		}
		c.startHeartbeat(n, sn)
	}
}

// Client talks to the current coordinator.
func (c *Cluster) Client() *Client {
	return &Client{c: c}
}

// Coordinator is the running control plane, for tests that drive its
// handler directly.
func (c *Cluster) Coordinator() *coordinator.Coordinator {
	return c.coord
}

// Meta is the coordinator's metadata store.
func (c *Cluster) Meta() *meta.DB {
	return c.meta
}

// NodeID is the id node i reports in heartbeats.
func (c *Cluster) NodeID(i int) string {
	return c.nodes[i].id
}

// NodeURL is the base URL node i currently serves on, or "" while killed.
func (c *Cluster) NodeURL(i int) string {
	if c.nodes[i].srv == nil {
		return ""
	}
	return c.nodes[i].srv.URL
}

// Locate answers, from metadata, which blob backs bucket/key and which
// nodes hold acknowledged copies of it.
func (c *Cluster) Locate(bucket, key string) (blobID string, nodeIDs []string) {
	c.t.Helper()
	ctx := context.Background()
	obj, err := c.meta.GetObject(ctx, bucket, key)
	if err != nil {
		c.t.Fatalf("Locate %s/%s: %v", bucket, key, err)
	}
	reps, err := c.meta.Replicas(ctx, obj.BlobID)
	if err != nil {
		c.t.Fatalf("Locate %s/%s: replicas: %v", bucket, key, err)
	}
	for _, r := range reps {
		nodeIDs = append(nodeIDs, r.NodeID)
	}
	return obj.BlobID, nodeIDs
}

// blobPath is where node i keeps committed blob id.
func (c *Cluster) blobPath(i int, id string) string {
	return filepath.Join(c.nodes[i].dir, "blobs", id)
}

// HasBlob reports whether node i has blob id committed on disk. It works
// whether or not the node is running.
func (c *Cluster) HasBlob(i int, id string) bool {
	c.t.Helper()
	_, err := os.Stat(c.blobPath(i, id))
	if err == nil {
		return true
	}
	if errors.Is(err, os.ErrNotExist) {
		return false
	}
	c.t.Fatalf("HasBlob(%d, %s): %v", i, id, err)
	return false
}

// Quarantined reports whether node i has moved blob id into quarantine/,
// under its own name or a numbered suffix.
func (c *Cluster) Quarantined(i int, id string) bool {
	c.t.Helper()
	entries, err := os.ReadDir(filepath.Join(c.nodes[i].dir, "quarantine"))
	if err != nil {
		c.t.Fatalf("Quarantined(%d, %s): %v", i, id, err)
	}
	for _, e := range entries {
		if e.Name() == id || strings.HasPrefix(e.Name(), id+".") {
			return true
		}
	}
	return false
}

// CorruptBlob flips the first payload byte of blob id on node i, so the
// node's checksum verification fails on the next read.
func (c *Cluster) CorruptBlob(i int, id string) {
	c.t.Helper()
	f, err := os.OpenFile(c.blobPath(i, id), os.O_RDWR, 0)
	if err != nil {
		c.t.Fatalf("CorruptBlob(%d, %s): %v", i, id, err)
	}
	defer f.Close()
	var b [1]byte
	if _, err := f.ReadAt(b[:], blob.HeaderSize); err != nil {
		c.t.Fatalf("CorruptBlob(%d, %s): read: %v", i, id, err)
	}
	b[0] ^= 0xff
	if _, err := f.WriteAt(b[:], blob.HeaderSize); err != nil {
		c.t.Fatalf("CorruptBlob(%d, %s): write: %v", i, id, err)
	}
}

// SpoolFiles lists the coordinator's in-flight upload spool. It is empty
// whenever no PUT is in progress.
func (c *Cluster) SpoolFiles() []string {
	c.t.Helper()
	entries, err := os.ReadDir(filepath.Join(c.cfg.DataDir, "spool"))
	if err != nil {
		c.t.Fatalf("SpoolFiles: %v", err)
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	return names
}
