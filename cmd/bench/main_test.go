package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/manavmann/distributed-object-storage/internal/testcluster"
)

func TestPercentiles(t *testing.T) {
	ms := func(n int) time.Duration { return time.Duration(n) * time.Millisecond }
	// 1..100 ms in shuffled order: nearest-rank p is exactly p ms.
	var hundred []time.Duration
	for i := 100; i >= 1; i-- {
		hundred = append(hundred, ms(i))
	}
	cases := []struct {
		name string
		in   []time.Duration
		want pcts
	}{
		{"empty", nil, pcts{}},
		{"one", []time.Duration{ms(7)}, pcts{ms(7), ms(7), ms(7), ms(7)}},
		{"two", []time.Duration{ms(9), ms(1)}, pcts{ms(1), ms(9), ms(9), ms(9)}},
		{"hundred", hundred, pcts{ms(50), ms(95), ms(99), ms(100)}},
		{"ten", []time.Duration{ms(10), ms(1), ms(2), ms(3), ms(4), ms(5), ms(6), ms(7), ms(8), ms(9)},
			pcts{ms(5), ms(10), ms(10), ms(10)}},
	}
	for _, c := range cases {
		before := append([]time.Duration(nil), c.in...)
		if got := percentiles(c.in); got != c.want {
			t.Errorf("%s: percentiles = %+v, want %+v", c.name, got, c.want)
		}
		for i := range before {
			if c.in[i] != before[i] {
				t.Fatalf("%s: percentiles reordered its input", c.name)
			}
		}
	}
}

func TestParseSize(t *testing.T) {
	good := map[string]int64{
		"0": 0, "4096": 4096, "512B": 512, "4KiB": 4096, "1MiB": 1 << 20, "32MiB": 32 << 20, "1GiB": 1 << 30, " 2KiB ": 2048,
	}
	for in, want := range good {
		got, err := parseSize(in)
		if err != nil || got != want {
			t.Errorf("parseSize(%q) = %d, %v; want %d", in, got, err, want)
		}
	}
	for _, in := range []string{"", "KiB", "4kb", "4K", "1.5MiB", "-1", "4 KiB", "MiB4"} {
		if _, err := parseSize(in); !errors.Is(err, ErrConfig) {
			t.Errorf("parseSize(%q) err = %v, want ErrConfig", in, err)
		}
	}
}

func TestRunRejectsBadConfig(t *testing.T) {
	bad := []config{
		{Op: "delete", Concurrency: 1, Duration: time.Second, Keys: 1},
		{Op: "put", Concurrency: 0, Duration: time.Second},
		{Op: "put", Concurrency: 1, Duration: 0},
		{Op: "get", Concurrency: 1, Duration: time.Second, Keys: 0},
	}
	for _, cfg := range bad {
		if _, err := run(context.Background(), cfg); !errors.Is(err, ErrConfig) {
			t.Errorf("run(%+v) err = %v, want ErrConfig", cfg, err)
		}
	}
}

// TestBenchAgainstCluster runs a 2s mixed bench against the harness and
// expects work done with no errors.
func TestBenchAgainstCluster(t *testing.T) {
	c := testcluster.New(t, testcluster.Opts{Nodes: 3})
	cfg := config{
		URL: c.Client().URL(), Bucket: "bench", Op: "mixed", Size: 4 << 10,
		Concurrency: 4, Duration: 2 * time.Second, Warmup: 200 * time.Millisecond, Keys: 8,
	}
	r, err := run(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Errors) != 0 {
		t.Fatalf("errors = %v, want none", r.Errors)
	}
	if r.Ops == 0 || r.OpsPerSec <= 0 || r.MiBPerSec <= 0 {
		t.Fatalf("no work measured: %+v", r)
	}
	if r.P50 <= 0 || r.P50 > r.P95 || r.P95 > r.P99 || r.P99 > r.Max {
		t.Fatalf("percentiles out of order: %+v", r)
	}
	// The window covers the requests in flight at the deadline, so it is
	// only bounded below.
	if r.Duration < 2 {
		t.Fatalf("measured window = %.2fs, want >= 2s", r.Duration)
	}
	// A second run against the same bucket must not trip over it existing.
	if _, err := run(context.Background(), cfg); err != nil {
		t.Fatalf("second run: %v", err)
	}
	l, err := c.Client().List("bench", "bench/seed-", "", 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(l.Objects) != cfg.Keys {
		t.Fatalf("seeded %d keys, want %d", len(l.Objects), cfg.Keys)
	}
}
