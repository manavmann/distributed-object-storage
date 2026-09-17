package config

import (
	"fmt"
	"strconv"
	"time"
)

// CoordinatorConfig is the coordinator's configuration.
//
//	variable                 default   meaning
//	CAIRN_ADDR               :9000     listen address
//	CAIRN_DATA_DIR           ./data    holds meta.db and spool/
//	CAIRN_MAX_OBJECT_SIZE    67108864  largest accepted PUT body, bytes
//	CAIRN_MAX_UPLOADS        16        PUTs spooling or replicating at once
//	CAIRN_NODE_TIMEOUT       30s       deadline for one request to a node
//	CAIRN_HEARTBEAT_TIMEOUT  15s       silence after which a node is marked DOWN
type CoordinatorConfig struct {
	Addr             string
	DataDir          string
	MaxObjectSize    int64
	MaxUploads       int
	NodeTimeout      time.Duration
	HeartbeatTimeout time.Duration
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
	var err error
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
	hbTimeout := get("CAIRN_HEARTBEAT_TIMEOUT", "15s")
	c.HeartbeatTimeout, err = time.ParseDuration(hbTimeout)
	if err != nil {
		return CoordinatorConfig{}, fmt.Errorf("%w: CAIRN_HEARTBEAT_TIMEOUT %q: %w", ErrInvalidConfig, hbTimeout, err)
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
	if c.MaxObjectSize <= 0 {
		return fmt.Errorf("%w: CAIRN_MAX_OBJECT_SIZE must be positive, got %d", ErrInvalidConfig, c.MaxObjectSize)
	}
	if c.MaxUploads <= 0 {
		return fmt.Errorf("%w: CAIRN_MAX_UPLOADS must be positive, got %d", ErrInvalidConfig, c.MaxUploads)
	}
	if c.NodeTimeout <= 0 {
		return fmt.Errorf("%w: CAIRN_NODE_TIMEOUT must be positive, got %s", ErrInvalidConfig, c.NodeTimeout)
	}
	if c.HeartbeatTimeout <= 0 {
		return fmt.Errorf("%w: CAIRN_HEARTBEAT_TIMEOUT must be positive, got %s", ErrInvalidConfig, c.HeartbeatTimeout)
	}
	return nil
}
