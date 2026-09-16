package config

import (
	"errors"
	"os"
	"testing"
	"time"
)

func envFrom(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

func TestLoadNodeTable(t *testing.T) {
	hostname, err := os.Hostname()
	if err != nil {
		t.Fatal(err)
	}
	full := map[string]string{
		"CAIRN_NODE_ID":            "n1",
		"CAIRN_ADDR":               "127.0.0.1:0",
		"CAIRN_ADVERTISE_ADDR":     "http://10.0.0.1:9100",
		"CAIRN_DATA_DIR":           "/var/cairn",
		"CAIRN_COORDINATOR_URL":    "https://coord.example:9000",
		"CAIRN_HEARTBEAT_INTERVAL": "250ms",
	}
	with := func(k, v string) map[string]string {
		m := map[string]string{}
		for kk, vv := range full {
			m[kk] = vv
		}
		m[k] = v
		return m
	}
	tests := []struct {
		name string
		env  map[string]string
		want NodeConfig
		bad  bool
	}{
		{name: "defaults", env: nil, want: NodeConfig{
			NodeID: hostname, Addr: ":9100", AdvertiseAddr: "http://localhost:9100",
			DataDir: "./data", CoordinatorURL: "http://localhost:9000", HeartbeatInterval: 5 * time.Second,
		}},
		{name: "all set", env: full, want: NodeConfig{
			NodeID: "n1", Addr: "127.0.0.1:0", AdvertiseAddr: "http://10.0.0.1:9100",
			DataDir: "/var/cairn", CoordinatorURL: "https://coord.example:9000", HeartbeatInterval: 250 * time.Millisecond,
		}},
		{name: "empty value uses default", env: with("CAIRN_ADDR", ""), want: NodeConfig{
			NodeID: "n1", Addr: ":9100", AdvertiseAddr: "http://10.0.0.1:9100",
			DataDir: "/var/cairn", CoordinatorURL: "https://coord.example:9000", HeartbeatInterval: 250 * time.Millisecond,
		}},
		{name: "bad interval", env: with("CAIRN_HEARTBEAT_INTERVAL", "soon"), bad: true},
		{name: "zero interval", env: with("CAIRN_HEARTBEAT_INTERVAL", "0s"), bad: true},
		{name: "negative interval", env: with("CAIRN_HEARTBEAT_INTERVAL", "-1s"), bad: true},
		{name: "coordinator no scheme", env: with("CAIRN_COORDINATOR_URL", "localhost:9000"), bad: true},
		{name: "coordinator bad scheme", env: with("CAIRN_COORDINATOR_URL", "ftp://x"), bad: true},
		{name: "coordinator no host", env: with("CAIRN_COORDINATOR_URL", "http://"), bad: true},
		{name: "advertise no scheme", env: with("CAIRN_ADVERTISE_ADDR", "10.0.0.1:9100"), bad: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := LoadNode(envFrom(tc.env))
			if tc.bad {
				if !errors.Is(err, ErrInvalidConfig) {
					t.Fatalf("err = %v, want ErrInvalidConfig", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("LoadNode: %v", err)
			}
			if got != tc.want {
				t.Fatalf("got %+v\nwant %+v", got, tc.want)
			}
		})
	}
}

func TestValidateEmptyFields(t *testing.T) {
	base := NodeConfig{
		NodeID: "n", Addr: ":1", AdvertiseAddr: "http://h:1", DataDir: "d",
		CoordinatorURL: "http://c:1", HeartbeatInterval: time.Second,
	}
	if err := base.Validate(); err != nil {
		t.Fatalf("base: %v", err)
	}
	for name, mut := range map[string]func(*NodeConfig){
		"node id":  func(c *NodeConfig) { c.NodeID = "" },
		"addr":     func(c *NodeConfig) { c.Addr = "" },
		"data dir": func(c *NodeConfig) { c.DataDir = "" },
	} {
		c := base
		mut(&c)
		if err := c.Validate(); !errors.Is(err, ErrInvalidConfig) {
			t.Errorf("%s: err = %v, want ErrInvalidConfig", name, err)
		}
	}
}
