// Package config reads process configuration from CAIRN_* environment
// variables and validates it before anything is opened or listened on.
package config

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"time"
)

// ErrInvalidConfig is wrapped by every validation failure.
var ErrInvalidConfig = errors.New("config: invalid")

// NodeConfig is a storage node's configuration.
//
//	variable                  default                  meaning
//	CAIRN_NODE_ID             hostname                 id reported in heartbeats
//	CAIRN_ADDR                :9100                    listen address
//	CAIRN_ADVERTISE_ADDR      http://localhost:9100    URL the coordinator reaches us at
//	CAIRN_DATA_DIR            ./data                   root of blobs/ and quarantine/
//	CAIRN_COORDINATOR_URL     http://localhost:9000    coordinator base URL
//	CAIRN_HEARTBEAT_INTERVAL  5s                       time between heartbeats
//	CAIRN_SCRUB_INTERVAL      1h                       time between scrub passes over blobs/
//	CAIRN_SCRUB_DELAY         10ms                     pause before each blob within a pass
type NodeConfig struct {
	NodeID            string
	Addr              string
	AdvertiseAddr     string
	DataDir           string
	CoordinatorURL    string
	HeartbeatInterval time.Duration
	ScrubInterval     time.Duration
	ScrubDelay        time.Duration
}

// LoadNode builds a NodeConfig from getenv (normally os.Getenv), applying
// the defaults above to unset or empty variables, and validates it.
func LoadNode(getenv func(string) string) (NodeConfig, error) {
	get := func(key, def string) string {
		if v := getenv(key); v != "" {
			return v
		}
		return def
	}
	hostname, err := os.Hostname()
	if err != nil {
		return NodeConfig{}, fmt.Errorf("%w: hostname: %w", ErrInvalidConfig, err)
	}
	c := NodeConfig{
		NodeID:         get("CAIRN_NODE_ID", hostname),
		Addr:           get("CAIRN_ADDR", ":9100"),
		AdvertiseAddr:  get("CAIRN_ADVERTISE_ADDR", "http://localhost:9100"),
		DataDir:        get("CAIRN_DATA_DIR", "./data"),
		CoordinatorURL: get("CAIRN_COORDINATOR_URL", "http://localhost:9000"),
	}
	for _, d := range []struct {
		name, def string
		dst       *time.Duration
	}{
		{"CAIRN_HEARTBEAT_INTERVAL", "5s", &c.HeartbeatInterval},
		{"CAIRN_SCRUB_INTERVAL", "1h", &c.ScrubInterval},
		{"CAIRN_SCRUB_DELAY", "10ms", &c.ScrubDelay},
	} {
		v := get(d.name, d.def)
		*d.dst, err = time.ParseDuration(v)
		if err != nil {
			return NodeConfig{}, fmt.Errorf("%w: %s %q: %w", ErrInvalidConfig, d.name, v, err)
		}
	}
	if err := c.Validate(); err != nil {
		return NodeConfig{}, err
	}
	return c, nil
}

// Validate reports the first field that cannot be used as-is.
func (c NodeConfig) Validate() error {
	if c.NodeID == "" {
		return fmt.Errorf("%w: CAIRN_NODE_ID is empty", ErrInvalidConfig)
	}
	if c.Addr == "" {
		return fmt.Errorf("%w: CAIRN_ADDR is empty", ErrInvalidConfig)
	}
	if c.DataDir == "" {
		return fmt.Errorf("%w: CAIRN_DATA_DIR is empty", ErrInvalidConfig)
	}
	for _, d := range []struct {
		name string
		val  time.Duration
	}{
		{"CAIRN_HEARTBEAT_INTERVAL", c.HeartbeatInterval},
		{"CAIRN_SCRUB_INTERVAL", c.ScrubInterval},
		{"CAIRN_SCRUB_DELAY", c.ScrubDelay},
	} {
		if d.val <= 0 {
			return fmt.Errorf("%w: %s must be positive, got %s", ErrInvalidConfig, d.name, d.val)
		}
	}
	for _, u := range []struct{ name, val string }{
		{"CAIRN_ADVERTISE_ADDR", c.AdvertiseAddr},
		{"CAIRN_COORDINATOR_URL", c.CoordinatorURL},
	} {
		if err := checkURL(u.val); err != nil {
			return fmt.Errorf("%w: %s %q: %w", ErrInvalidConfig, u.name, u.val, err)
		}
	}
	return nil
}

// checkURL requires an absolute http(s) URL with a host.
func checkURL(s string) error {
	u, err := url.Parse(s)
	if err != nil {
		return err
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return errors.New("scheme must be http or https")
	}
	if u.Host == "" {
		return errors.New("missing host")
	}
	return nil
}
