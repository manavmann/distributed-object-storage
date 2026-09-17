package config

import (
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestLoadCoordinatorTable(t *testing.T) {
	full := map[string]string{
		"CAIRN_ADDR":               "127.0.0.1:0",
		"CAIRN_DATA_DIR":           "/var/cairn",
		"CAIRN_MAX_OBJECT_SIZE":    "1024",
		"CAIRN_MAX_UPLOADS":        "2",
		"CAIRN_NODE_TIMEOUT":       "5s",
		"CAIRN_HEARTBEAT_TIMEOUT":  "6s",
		"CAIRN_REPLICATION_FACTOR": "5",
		"CAIRN_WRITE_QUORUM":       "4",
		"CAIRN_REPAIR_INTERVAL":    "2s",
		"CAIRN_REPAIR_GRACE":       "10s",
		"CAIRN_CLUSTER_SECRET":     "hunter2",
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
		want CoordinatorConfig
		bad  bool
	}{
		{name: "defaults", env: nil, want: CoordinatorConfig{
			Addr: ":9000", DataDir: "./data",
			MaxObjectSize: 64 << 20, MaxUploads: 16, NodeTimeout: 30 * time.Second, HeartbeatTimeout: 15 * time.Second,
			RF: 3, W: 2, RepairInterval: 30 * time.Second, RepairGrace: 60 * time.Second,
		}},
		{name: "all set", env: full, want: CoordinatorConfig{
			Addr: "127.0.0.1:0", DataDir: "/var/cairn",
			MaxObjectSize: 1024, MaxUploads: 2, NodeTimeout: 5 * time.Second, HeartbeatTimeout: 6 * time.Second,
			RF: 5, W: 4, RepairInterval: 2 * time.Second, RepairGrace: 10 * time.Second, ClusterSecret: "hunter2",
		}},
		{name: "bad size", env: with("CAIRN_MAX_OBJECT_SIZE", "big"), bad: true},
		{name: "zero size", env: with("CAIRN_MAX_OBJECT_SIZE", "0"), bad: true},
		{name: "bad uploads", env: with("CAIRN_MAX_UPLOADS", "many"), bad: true},
		{name: "zero uploads", env: with("CAIRN_MAX_UPLOADS", "0"), bad: true},
		{name: "bad timeout", env: with("CAIRN_NODE_TIMEOUT", "soon"), bad: true},
		{name: "negative timeout", env: with("CAIRN_NODE_TIMEOUT", "-1s"), bad: true},
		{name: "bad heartbeat timeout", env: with("CAIRN_HEARTBEAT_TIMEOUT", "soon"), bad: true},
		{name: "zero heartbeat timeout", env: with("CAIRN_HEARTBEAT_TIMEOUT", "0s"), bad: true},
		{name: "bad rf", env: with("CAIRN_REPLICATION_FACTOR", "three"), bad: true},
		{name: "zero rf", env: with("CAIRN_REPLICATION_FACTOR", "0"), bad: true},
		{name: "bad quorum", env: with("CAIRN_WRITE_QUORUM", "two"), bad: true},
		{name: "zero quorum", env: with("CAIRN_WRITE_QUORUM", "0"), bad: true},
		{name: "quorum above rf", env: with("CAIRN_WRITE_QUORUM", "6"), bad: true},
		{name: "bad repair interval", env: with("CAIRN_REPAIR_INTERVAL", "often"), bad: true},
		{name: "zero repair interval", env: with("CAIRN_REPAIR_INTERVAL", "0s"), bad: true},
		{name: "bad repair grace", env: with("CAIRN_REPAIR_GRACE", "soon"), bad: true},
		{name: "negative repair grace", env: with("CAIRN_REPAIR_GRACE", "-1s"), bad: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := LoadCoordinator(envFrom(tc.env))
			if tc.bad {
				if !errors.Is(err, ErrInvalidConfig) {
					t.Fatalf("err = %v, want ErrInvalidConfig", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("LoadCoordinator: %v", err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("got %+v\nwant %+v", got, tc.want)
			}
		})
	}
}

func TestCoordinatorValidate(t *testing.T) {
	good := CoordinatorConfig{Addr: ":1", DataDir: "d", MaxObjectSize: 1, MaxUploads: 1, NodeTimeout: time.Second, HeartbeatTimeout: time.Second, RF: 1, W: 1, RepairInterval: time.Second, RepairGrace: time.Second}
	if err := good.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	for _, bad := range []CoordinatorConfig{
		{Addr: "", DataDir: "d", MaxObjectSize: 1, MaxUploads: 1, NodeTimeout: time.Second, HeartbeatTimeout: time.Second, RF: 1, W: 1},
		{Addr: ":1", DataDir: "", MaxObjectSize: 1, MaxUploads: 1, NodeTimeout: time.Second, HeartbeatTimeout: time.Second, RF: 1, W: 1},
		{Addr: ":1", DataDir: "d", MaxObjectSize: 1, MaxUploads: 1, NodeTimeout: time.Second, HeartbeatTimeout: 0, RF: 1, W: 1},
		{Addr: ":1", DataDir: "d", MaxObjectSize: 1, MaxUploads: 1, NodeTimeout: time.Second, HeartbeatTimeout: time.Second, RF: 0, W: 0},
		{Addr: ":1", DataDir: "d", MaxObjectSize: 1, MaxUploads: 1, NodeTimeout: time.Second, HeartbeatTimeout: time.Second, RF: 2, W: 3},
		{Addr: ":1", DataDir: "d", MaxObjectSize: 1, MaxUploads: 1, NodeTimeout: time.Second, HeartbeatTimeout: time.Second, RF: 1, W: 1, RepairInterval: 0, RepairGrace: time.Second},
		{Addr: ":1", DataDir: "d", MaxObjectSize: 1, MaxUploads: 1, NodeTimeout: time.Second, HeartbeatTimeout: time.Second, RF: 1, W: 1, RepairInterval: time.Second, RepairGrace: 0},
	} {
		if err := bad.Validate(); !errors.Is(err, ErrInvalidConfig) {
			t.Errorf("Validate(%+v) = %v, want ErrInvalidConfig", bad, err)
		}
	}
}
