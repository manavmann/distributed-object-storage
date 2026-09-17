package config

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/manavmann/distributed-object-storage/internal/cluster"
)

func TestLoadCoordinatorTable(t *testing.T) {
	full := map[string]string{
		"CAIRN_ADDR":            "127.0.0.1:0",
		"CAIRN_DATA_DIR":        "/var/cairn",
		"CAIRN_NODES":           "n1=10.0.0.1:9100,n2=10.0.0.2:9100",
		"CAIRN_MAX_OBJECT_SIZE": "1024",
		"CAIRN_MAX_UPLOADS":     "2",
		"CAIRN_NODE_TIMEOUT":    "5s",
	}
	with := func(k, v string) map[string]string {
		m := map[string]string{}
		for kk, vv := range full {
			m[kk] = vv
		}
		m[k] = v
		return m
	}
	twoNodes := []cluster.Node{{ID: "n1", Addr: "http://10.0.0.1:9100"}, {ID: "n2", Addr: "http://10.0.0.2:9100"}}
	tests := []struct {
		name string
		env  map[string]string
		want CoordinatorConfig
		bad  bool
	}{
		{name: "defaults", env: map[string]string{"CAIRN_NODES": "n1=localhost:9100"}, want: CoordinatorConfig{
			Addr: ":9000", DataDir: "./data", Nodes: []cluster.Node{{ID: "n1", Addr: "http://localhost:9100"}},
			MaxObjectSize: 64 << 20, MaxUploads: 16, NodeTimeout: 30 * time.Second,
		}},
		{name: "all set", env: full, want: CoordinatorConfig{
			Addr: "127.0.0.1:0", DataDir: "/var/cairn", Nodes: twoNodes,
			MaxObjectSize: 1024, MaxUploads: 2, NodeTimeout: 5 * time.Second,
		}},
		{name: "nodes required", env: nil, bad: true},
		{name: "nodes malformed", env: with("CAIRN_NODES", "n1"), bad: true},
		{name: "bad size", env: with("CAIRN_MAX_OBJECT_SIZE", "big"), bad: true},
		{name: "zero size", env: with("CAIRN_MAX_OBJECT_SIZE", "0"), bad: true},
		{name: "bad uploads", env: with("CAIRN_MAX_UPLOADS", "many"), bad: true},
		{name: "zero uploads", env: with("CAIRN_MAX_UPLOADS", "0"), bad: true},
		{name: "bad timeout", env: with("CAIRN_NODE_TIMEOUT", "soon"), bad: true},
		{name: "negative timeout", env: with("CAIRN_NODE_TIMEOUT", "-1s"), bad: true},
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

func TestCoordinatorValidateNodes(t *testing.T) {
	good := CoordinatorConfig{Addr: ":1", DataDir: "d", Nodes: []cluster.Node{{ID: "n1", Addr: "http://h:1"}},
		MaxObjectSize: 1, MaxUploads: 1, NodeTimeout: time.Second}
	if err := good.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	for _, bad := range []CoordinatorConfig{
		{Addr: ":1", DataDir: "d", MaxObjectSize: 1, MaxUploads: 1, NodeTimeout: time.Second},
		{Addr: ":1", DataDir: "d", Nodes: []cluster.Node{{ID: "", Addr: "http://h:1"}}, MaxObjectSize: 1, MaxUploads: 1, NodeTimeout: time.Second},
		{Addr: ":1", DataDir: "d", Nodes: []cluster.Node{{ID: "n1", Addr: "h:1"}}, MaxObjectSize: 1, MaxUploads: 1, NodeTimeout: time.Second},
		{Addr: "", DataDir: "d", Nodes: good.Nodes, MaxObjectSize: 1, MaxUploads: 1, NodeTimeout: time.Second},
		{Addr: ":1", DataDir: "", Nodes: good.Nodes, MaxObjectSize: 1, MaxUploads: 1, NodeTimeout: time.Second},
	} {
		if err := bad.Validate(); !errors.Is(err, ErrInvalidConfig) {
			t.Errorf("Validate(%+v) = %v, want ErrInvalidConfig", bad, err)
		}
	}
}
