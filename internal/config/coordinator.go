package config

import (
	"fmt"
	"strconv"
	"time"

	"github.com/manavmann/distributed-object-storage/internal/cluster"
)

// CoordinatorConfig is the coordinator's configuration.
//
//	variable               default     meaning
//	CAIRN_ADDR             :9000       listen address
//	CAIRN_DATA_DIR         ./data      holds meta.db and spool/
//	CAIRN_NODES            (required)  static node list, "id=host:port,..."
//	CAIRN_MAX_OBJECT_SIZE  67108864    largest accepted PUT body, bytes
//	CAIRN_MAX_UPLOADS      16          PUTs spooling or replicating at once
//	CAIRN_NODE_TIMEOUT     30s         deadline for one request to a node
type CoordinatorConfig struct {
	Addr          string
	DataDir       string
	Nodes         []cluster.Node
	MaxObjectSize int64
	MaxUploads    int
	NodeTimeout   time.Duration
}

// LoadCoordinator builds a CoordinatorConfig from getenv (normally
// os.Getenv), applying the defaults above, and validates it.
func LoadCoordinator(getenv func(string) string) (CoordinatorConfig, error) {
	get := func(key, def string) string {
		if v := getenv(key); v != "" {
			return v
		}
		return def
	}
	c := CoordinatorConfig{
		Addr:    get("CAIRN_ADDR", ":9000"),
		DataDir: get("CAIRN_DATA_DIR", "./data"),
	}
	reg, err := cluster.ParseStatic(getenv("CAIRN_NODES"))
	if err != nil {
		return CoordinatorConfig{}, fmt.Errorf("%w: %w", ErrInvalidConfig, err)
	}
	c.Nodes = reg.Healthy()
	maxSize := get("CAIRN_MAX_OBJECT_SIZE", "67108864")
	c.MaxObjectSize, err = strconv.ParseInt(maxSize, 10, 64)
	if err != nil {
		return CoordinatorConfig{}, fmt.Errorf("%w: CAIRN_MAX_OBJECT_SIZE %q: %w", ErrInvalidConfig, maxSize, err)
	}
	maxUploads := get("CAIRN_MAX_UPLOADS", "16")
	c.MaxUploads, err = strconv.Atoi(maxUploads)
	if err != nil {
		return CoordinatorConfig{}, fmt.Errorf("%w: CAIRN_MAX_UPLOADS %q: %w", ErrInvalidConfig, maxUploads, err)
	}
	timeout := get("CAIRN_NODE_TIMEOUT", "30s")
	c.NodeTimeout, err = time.ParseDuration(timeout)
	if err != nil {
		return CoordinatorConfig{}, fmt.Errorf("%w: CAIRN_NODE_TIMEOUT %q: %w", ErrInvalidConfig, timeout, err)
	}
	if err := c.Validate(); err != nil {
		return CoordinatorConfig{}, err
	}
	return c, nil
}

// Validate reports the first field that cannot be used as-is.
func (c CoordinatorConfig) Validate() error {
	if c.Addr == "" {
		return fmt.Errorf("%w: CAIRN_ADDR is empty", ErrInvalidConfig)
	}
	if c.DataDir == "" {
		return fmt.Errorf("%w: CAIRN_DATA_DIR is empty", ErrInvalidConfig)
	}
	if len(c.Nodes) == 0 {
		return fmt.Errorf("%w: CAIRN_NODES is empty", ErrInvalidConfig)
	}
	for _, n := range c.Nodes {
		if n.ID == "" {
			return fmt.Errorf("%w: CAIRN_NODES has a node with an empty id", ErrInvalidConfig)
		}
		if err := checkURL(n.Addr); err != nil {
			return fmt.Errorf("%w: CAIRN_NODES node %s addr %q: %w", ErrInvalidConfig, n.ID, n.Addr, err)
		}
	}
	if c.MaxObjectSize <= 0 {
		return fmt.Errorf("%w: CAIRN_MAX_OBJECT_SIZE must be positive, got %d", ErrInvalidConfig, c.MaxObjectSize)
	}
	if c.MaxUploads <= 0 {
		return fmt.Errorf("%w: CAIRN_MAX_UPLOADS must be positive, got %d", ErrInvalidConfig, c.MaxUploads)
	}
	if c.NodeTimeout <= 0 {
		return fmt.Errorf("%w: CAIRN_NODE_TIMEOUT must be positive, got %s", ErrInvalidConfig, c.NodeTimeout)
	}
	return nil
}
