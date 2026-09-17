// Package metrics holds every Prometheus collector Cairn exports. One
// Metrics value is built per process and registered on its own registry,
// never the default one, so tests can build as many as they like. Every
// label has a small fixed value set: routes are mux patterns, methods are
// HTTP verbs, results are constants below.
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Values of the result label on ReplicaWrites and Repairs.
const (
	ResultOK        = "ok"
	ResultFailed    = "failed"
	ResultCompleted = "completed"
	ResultSkipped   = "skipped"
)

// Metrics is the process's collectors. Coordinator collectors sit at zero
// on a node and node collectors at zero on the coordinator.
type Metrics struct {
	reg *prometheus.Registry

	// HTTPRequests counts responses by route pattern, method and status.
	HTTPRequests *prometheus.CounterVec
	// HTTPDuration observes request latency by route pattern and method.
	HTTPDuration *prometheus.HistogramVec

	// ReplicaWrites counts per-node copies a PUT attempted, by result.
	ReplicaWrites *prometheus.CounterVec
	// NodesUp and NodesTotal are the registry's UP and known node counts.
	NodesUp    prometheus.Gauge
	NodesTotal prometheus.Gauge
	// UnderReplicatedBlobs is how many live blobs fewer than RF UP nodes hold.
	UnderReplicatedBlobs prometheus.Gauge
	// PendingDeletes is the length of the pending_deletes queue.
	PendingDeletes prometheus.Gauge
	// Repairs counts re-replication candidates by result.
	Repairs *prometheus.CounterVec
	// IntegrityFailures counts copies a node reported corrupt.
	IntegrityFailures prometheus.Counter
	// ObjectsTotal is the number of objects in metadata.
	ObjectsTotal prometheus.Gauge

	// NodeBlobs and NodeFreeBytes are what a storage node reports in its
	// heartbeat.
	NodeBlobs     prometheus.Gauge
	NodeFreeBytes prometheus.Gauge
}

// New builds and registers every collector on a fresh registry.
func New() *Metrics {
	m := &Metrics{
		reg: prometheus.NewRegistry(),
		HTTPRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "cairn_http_requests_total", Help: "HTTP responses by route pattern, method and status.",
		}, []string{"route", "method", "status"}),
		HTTPDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "cairn_http_request_duration_seconds", Help: "HTTP request latency by route pattern and method.",
			Buckets: prometheus.DefBuckets,
		}, []string{"route", "method"}),
		ReplicaWrites: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "cairn_replica_writes_total", Help: "Per-node copies attempted by object PUTs, by result.",
		}, []string{"result"}),
		NodesUp:    prometheus.NewGauge(prometheus.GaugeOpts{Name: "cairn_nodes_up", Help: "Storage nodes currently UP."}),
		NodesTotal: prometheus.NewGauge(prometheus.GaugeOpts{Name: "cairn_nodes_total", Help: "Storage nodes known to the coordinator."}),
		UnderReplicatedBlobs: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "cairn_under_replicated_blobs", Help: "Live blobs held by fewer than RF UP nodes.",
		}),
		PendingDeletes: prometheus.NewGauge(prometheus.GaugeOpts{Name: "cairn_pending_deletes", Help: "Copies queued for deletion."}),
		Repairs: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "cairn_repairs_total", Help: "Re-replication candidates processed, by result.",
		}, []string{"result"}),
		IntegrityFailures: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "cairn_integrity_failures_total", Help: "Copies a storage node reported corrupt.",
		}),
		ObjectsTotal:  prometheus.NewGauge(prometheus.GaugeOpts{Name: "cairn_objects_total", Help: "Objects in metadata."}),
		NodeBlobs:     prometheus.NewGauge(prometheus.GaugeOpts{Name: "cairn_node_blobs", Help: "Blobs committed on this node."}),
		NodeFreeBytes: prometheus.NewGauge(prometheus.GaugeOpts{Name: "cairn_node_free_bytes", Help: "Free bytes on this node's data volume."}),
	}
	m.reg.MustRegister(
		m.HTTPRequests, m.HTTPDuration, m.ReplicaWrites, m.NodesUp, m.NodesTotal,
		m.UnderReplicatedBlobs, m.PendingDeletes, m.Repairs, m.IntegrityFailures, m.ObjectsTotal,
		m.NodeBlobs, m.NodeFreeBytes,
	)
	return m
}

// Handler serves the registry in the Prometheus text format.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{})
}
