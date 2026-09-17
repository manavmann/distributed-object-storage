// Package events is the one list of log event names. Every slog call in
// the coordinator and the storage node names its event with one of these
// constants, so an operator can grep a name here and find where it is
// emitted.
package events

// Coordinator API.
const (
	ReplicaWriteFailed = "replica_write_failed"
	OrphanBlob         = "api.orphan_blob"
	StreamAborted      = "api.stream_aborted"
	SpoolRemove        = "api.spool_remove"
	UploadIntentSwept  = "upload_intent_swept"
)

// Node registry and health monitor.
const (
	NodeUp                = "node_up"
	NodeDown              = "node_down"
	NodeDownPersistFailed = "node_down_persist_failed"
)

// Replica reads.
const (
	IntegrityFailure = "integrity_failure"
	ReplicaMismatch  = "replica_mismatch"
)

// Repair/GC worker.
const (
	GCFetchFailed       = "gc_fetch_failed"
	GCBumpFailed        = "gc_bump_failed"
	GCDeleteFailed      = "gc_delete_failed"
	GCRemoveFailed      = "gc_remove_failed"
	GCDeleted           = "gc_deleted"
	RepairFetchFailed   = "repair_fetch_failed"
	RepairSkipped       = "repair_skipped"
	RepairStarted       = "repair_started"
	RepairFailed        = "repair_failed"
	RepairCommitFailed  = "repair_commit_failed"
	RepairEnqueueFailed = "repair_enqueue_failed"
	RepairCompleted     = "repair_completed"
	TrimFetchFailed     = "trim_fetch_failed"
	TrimFailed          = "trim_failed"
	TrimSkipped         = "trim_skipped"
	TrimEnqueued        = "trim_enqueued"
	GaugeRefreshFailed  = "gauge_refresh_failed"
)

// HTTP plumbing shared by both binaries.
const (
	HTTPRequest       = "http.request"
	HTTPWriteJSON     = "http.write_json"
	HTTPStreamAborted = "http.stream_aborted"
	HTTPStoreError    = "http.store_error"
)

// Storage node.
const (
	NodeHeartbeatFailed = "node.heartbeat_failed"
	NodeBlobCount       = "node.blob_count"
	NodeFreeBytes       = "node.free_bytes"
	ScrubFailure        = "scrub_failure"
	NodeScrubError      = "node.scrub_error"
)

// Process lifecycle, coordinator binary.
const (
	CoordinatorConfig      = "coordinator.config"
	CoordinatorHealthcheck = "coordinator.healthcheck"
	CoordinatorListen      = "coordinator.listen"
	CoordinatorExit        = "coordinator.exit"
	CoordinatorStart       = "coordinator.start"
	CoordinatorShutdown    = "coordinator.shutdown"
)

// Process lifecycle, storage node binary.
const (
	NodeConfig      = "node.config"
	NodeHealthcheck = "node.healthcheck"
	NodeListen      = "node.listen"
	NodeExit        = "node.exit"
	NodeStart       = "node.start"
	NodeShutdown    = "node.shutdown"
)
