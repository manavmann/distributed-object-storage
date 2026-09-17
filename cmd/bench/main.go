// Command bench is a small load generator for the Cairn coordinator API.
// Workers loop PUT, GET or a mix of both until the deadline and the run
// reports throughput, latency percentiles and errors by API code.
package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"math/big"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/manavmann/distributed-object-storage/internal/httpx"
)

// ErrConfig is returned for a flag combination bench cannot run.
var ErrConfig = errors.New("bench: invalid config")

// config is one bench run, as set by the flags.
type config struct {
	URL         string
	Bucket      string
	Op          string
	Size        int64
	Concurrency int
	Duration    time.Duration
	Warmup      time.Duration
	Keys        int
}

// report is what one run measured. Errors is keyed by the API error code
// (or "HTTP <status>" when the body is not an error document, or
// "transport" when no response arrived). Latencies are milliseconds.
type report struct {
	Op          string         `json:"op"`
	Size        int64          `json:"size"`
	Concurrency int            `json:"concurrency"`
	Duration    float64        `json:"duration_s"`
	Ops         int            `json:"ops"`
	OpsPerSec   float64        `json:"ops_per_sec"`
	MiBPerSec   float64        `json:"mib_per_sec"`
	P50         float64        `json:"p50_ms"`
	P95         float64        `json:"p95_ms"`
	P99         float64        `json:"p99_ms"`
	Max         float64        `json:"max_ms"`
	Errors      map[string]int `json:"errors"`
}

// sample is one completed request.
type sample struct {
	end     time.Time
	latency time.Duration
	err     string
}

func main() {
	var cfg config
	var size string
	var asJSON bool
	flag.StringVar(&cfg.URL, "url", "http://localhost:8080", "coordinator base URL")
	flag.StringVar(&cfg.Bucket, "bucket", "bench", "bucket to use (created if missing)")
	flag.StringVar(&cfg.Op, "op", "put", "put, get or mixed")
	flag.StringVar(&size, "size", "4KiB", "object size, e.g. 4KiB, 1MiB, 32MiB")
	flag.IntVar(&cfg.Concurrency, "concurrency", 1, "number of workers")
	flag.DurationVar(&cfg.Duration, "duration", 10*time.Second, "measured run length")
	flag.DurationVar(&cfg.Warmup, "warmup", 0, "unmeasured run-in before the measured window")
	flag.IntVar(&cfg.Keys, "keys", 100, "objects to pre-populate for get and mixed")
	flag.BoolVar(&asJSON, "json", false, "print the report as one JSON object")
	flag.Parse()
	var err error
	if cfg.Size, err = parseSize(size); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	r, err := run(context.Background(), cfg)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if asJSON {
		json.NewEncoder(os.Stdout).Encode(r)
		return
	}
	fmt.Printf("op=%s size=%d concurrency=%d duration=%.1fs\n", r.Op, r.Size, r.Concurrency, r.Duration)
	fmt.Printf("ops=%d ops/s=%.1f MiB/s=%.2f\n", r.Ops, r.OpsPerSec, r.MiBPerSec)
	fmt.Printf("latency ms: p50=%.2f p95=%.2f p99=%.2f max=%.2f\n", r.P50, r.P95, r.P99, r.Max)
	if len(r.Errors) == 0 {
		fmt.Println("errors: none")
		return
	}
	for _, code := range sortedKeys(r.Errors) {
		fmt.Printf("errors %s=%d\n", code, r.Errors[code])
	}
}

// parseSize reads a byte count with an optional B, KiB, MiB or GiB suffix.
func parseSize(s string) (int64, error) {
	units := []struct {
		suffix string
		mult   int64
	}{{"GiB", 1 << 30}, {"MiB", 1 << 20}, {"KiB", 1 << 10}, {"B", 1}}
	mult := int64(1)
	num := strings.TrimSpace(s)
	for _, u := range units {
		if strings.HasSuffix(num, u.suffix) {
			mult, num = u.mult, strings.TrimSuffix(num, u.suffix)
			break
		}
	}
	n, err := strconv.ParseInt(num, 10, 64)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("%w: size %q", ErrConfig, s)
	}
	return n * mult, nil
}

// run executes cfg and returns its report. It fails only when the run
// cannot start (bad config, bucket or pre-population); request failures
// during the run are counted in report.Errors.
func run(ctx context.Context, cfg config) (report, error) {
	if cfg.Op != "put" && cfg.Op != "get" && cfg.Op != "mixed" {
		return report{}, fmt.Errorf("%w: op %q", ErrConfig, cfg.Op)
	}
	if cfg.Concurrency < 1 || cfg.Duration <= 0 || cfg.Warmup < 0 {
		return report{}, fmt.Errorf("%w: concurrency, duration and warmup must be positive", ErrConfig)
	}
	if cfg.Op != "put" && cfg.Keys < 1 {
		return report{}, fmt.Errorf("%w: %s needs -keys >= 1", ErrConfig, cfg.Op)
	}
	body := make([]byte, cfg.Size)
	if _, err := rand.Read(body); err != nil {
		return report{}, err
	}
	b := &bench{cfg: cfg, body: body, http: &http.Client{
		Transport: &http.Transport{MaxIdleConnsPerHost: cfg.Concurrency},
	}}
	if err := b.createBucket(ctx); err != nil {
		return report{}, err
	}
	if cfg.Op != "put" {
		for i := 0; i < cfg.Keys; i++ {
			if s := b.put(ctx, b.key("seed", i)); s.err != "" {
				return report{}, fmt.Errorf("bench: pre-populate %s: %s", b.key("seed", i), s.err)
			}
		}
	}
	start := time.Now()
	deadline := start.Add(cfg.Warmup + cfg.Duration)
	results := make([][]sample, cfg.Concurrency)
	var wg sync.WaitGroup
	for w := 0; w < cfg.Concurrency; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			results[w] = b.worker(ctx, w, deadline)
		}(w)
	}
	wg.Wait()
	b.http.CloseIdleConnections()
	measured := start.Add(cfg.Warmup)
	// Requests in flight at the deadline run to completion rather than
	// being cancelled on the server, so the window ends when they do.
	window := time.Since(measured)
	var all []sample
	for _, rs := range results {
		for _, s := range rs {
			if !s.end.Before(measured) {
				all = append(all, s)
			}
		}
	}
	return summarize(cfg, all, window), nil
}

// bench is the shared state of one run's workers.
type bench struct {
	cfg  config
	body []byte
	http *http.Client
}

func (b *bench) key(prefix string, n int) string {
	return "bench/" + prefix + "-" + strconv.Itoa(n)
}

// worker starts ops until deadline and lets the last one finish. mixed
// alternates PUT and GET.
func (b *bench) worker(ctx context.Context, w int, deadline time.Time) []sample {
	var out []sample
	prefix := "w" + strconv.Itoa(w)
	for n := 0; time.Now().Before(deadline); n++ {
		var s sample
		switch {
		case b.cfg.Op == "put" || (b.cfg.Op == "mixed" && n%2 == 0):
			s = b.put(ctx, b.key(prefix, n))
		default:
			s = b.get(ctx, b.key("seed", randInt(b.cfg.Keys)))
		}
		out = append(out, s)
	}
	return out
}

func (b *bench) put(ctx context.Context, key string) sample {
	return b.do(ctx, http.MethodPut, key, bytes.NewReader(b.body), http.StatusOK)
}

func (b *bench) get(ctx context.Context, key string) sample {
	return b.do(ctx, http.MethodGet, key, nil, http.StatusOK)
}

// do sends one object request and classifies the outcome. Response bodies
// are drained so the connection returns to the pool.
func (b *bench) do(ctx context.Context, method, key string, body io.Reader, want int) sample {
	start := time.Now()
	req, err := http.NewRequestWithContext(ctx, method, b.cfg.URL+"/v1/"+b.cfg.Bucket+"/"+key, body)
	if err != nil {
		return sample{end: time.Now(), err: "transport"}
	}
	resp, err := b.http.Do(req)
	if err != nil {
		return sample{end: time.Now(), latency: time.Since(start), err: "transport"}
	}
	defer resp.Body.Close()
	s := sample{}
	if resp.StatusCode != want {
		raw, _ := io.ReadAll(resp.Body)
		s.err = errorCode(resp.StatusCode, raw)
	} else if _, err := io.Copy(io.Discard, resp.Body); err != nil {
		s.err = "transport"
	}
	s.end = time.Now()
	s.latency = s.end.Sub(start)
	return s
}

// errorCode is the API code from an error body, or the bare status.
func errorCode(status int, body []byte) string {
	var eb httpx.ErrorBody
	if json.Unmarshal(body, &eb) == nil && eb.Error.Code != "" {
		return eb.Error.Code
	}
	return "HTTP " + strconv.Itoa(status)
}

// createBucket creates the bucket, treating an existing one as success.
func (b *bench) createBucket(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, b.cfg.URL+"/v1/"+b.cfg.Bucket, nil)
	if err != nil {
		return err
	}
	resp, err := b.http.Do(req)
	if err != nil {
		return fmt.Errorf("bench: create bucket: %w", err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusConflict {
		return fmt.Errorf("bench: create bucket: %s", errorCode(resp.StatusCode, raw))
	}
	return nil
}

// summarize turns the measured samples into a report over window.
func summarize(cfg config, all []sample, window time.Duration) report {
	r := report{
		Op: cfg.Op, Size: cfg.Size, Concurrency: cfg.Concurrency,
		Duration: window.Seconds(), Errors: map[string]int{},
	}
	var lat []time.Duration
	for _, s := range all {
		if s.err != "" {
			r.Errors[s.err]++
			continue
		}
		r.Ops++
		lat = append(lat, s.latency)
	}
	p := percentiles(lat)
	r.P50, r.P95, r.P99, r.Max = ms(p.p50), ms(p.p95), ms(p.p99), ms(p.max)
	r.OpsPerSec = float64(r.Ops) / window.Seconds()
	r.MiBPerSec = float64(r.Ops) * float64(cfg.Size) / (1 << 20) / window.Seconds()
	return r
}

// pcts is the latency distribution of a run.
type pcts struct{ p50, p95, p99, max time.Duration }

// percentiles is nearest-rank: p is the smallest sample such that at
// least p% of the samples are <= it. Empty input yields zeros.
func percentiles(lat []time.Duration) pcts {
	if len(lat) == 0 {
		return pcts{}
	}
	sorted := append([]time.Duration(nil), lat...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	rank := func(p float64) time.Duration {
		i := int(math.Ceil(p/100*float64(len(sorted)))) - 1
		return sorted[max(i, 0)]
	}
	return pcts{p50: rank(50), p95: rank(95), p99: rank(99), max: sorted[len(sorted)-1]}
}

func ms(d time.Duration) float64 {
	return float64(d) / float64(time.Millisecond)
}

func sortedKeys(m map[string]int) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// randInt is a uniform [0, n) from crypto/rand, which needs no seeding
// or locking across workers.
func randInt(n int) int {
	v, err := rand.Int(rand.Reader, big.NewInt(int64(n)))
	if err != nil {
		return 0
	}
	return int(v.Int64())
}
