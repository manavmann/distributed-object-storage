// Package api is the coordinator's public HTTP API. Handlers never buffer
// a client body in memory: a PUT is spooled to disk while it is hashed,
// then streamed from the spool to a quorum of storage nodes, and only the
// nodes that acknowledged the checksummed write make it into metadata.
package api

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/manavmann/distributed-object-storage/internal/cluster"
	"github.com/manavmann/distributed-object-storage/internal/httpx"
	"github.com/manavmann/distributed-object-storage/internal/meta"
	"github.com/manavmann/distributed-object-storage/internal/nodeclient"
	"github.com/manavmann/distributed-object-storage/internal/replication"
)

// Config is everything a Server needs.
type Config struct {
	Meta  *meta.DB
	Nodes *cluster.Registry
	// SpoolDir holds one file per PUT in flight; it must exist.
	SpoolDir string
	// MaxObjectSize is the largest body a PUT accepts, in bytes.
	MaxObjectSize int64
	// MaxUploads bounds how many PUTs may be spooling or replicating at once.
	MaxUploads int
	// NodeTimeout is the deadline for one request to a storage node.
	NodeTimeout time.Duration
	// RF is how many copies a PUT tries to write; W how many must be
	// acknowledged before it commits. 1 <= W <= RF.
	RF  int
	W   int
	Log *slog.Logger
}

// Server holds the handler state.
type Server struct {
	meta        *meta.DB
	nodes       *cluster.Registry
	client      *nodeclient.Client
	writer      *replication.Writer
	spoolDir    string
	maxSize     int64
	nodeTimeout time.Duration
	uploads     chan struct{}
	log         *slog.Logger
}

// putResponse is the body of a successful object PUT. Quorum is "k/N":
// k nodes acknowledged out of the N the PUT aimed for.
type putResponse struct {
	Bucket   string   `json:"bucket"`
	Key      string   `json:"key"`
	Size     int64    `json:"size"`
	SHA256   string   `json:"sha256"`
	Replicas []string `json:"replicas"`
	Quorum   string   `json:"quorum"`
}

// NewHandler serves the coordinator API:
//
//	PUT    /v1/{bucket}           201 created | 400 | 409 exists
//	GET    /v1/{bucket}           200 {name,created_at,object_count} | 400 | 404
//	GET    /v1/{bucket}/          200 listing; ?prefix=&limit=&start_after= | 400 | 404
//	PUT    /v1/{bucket}/{key...}  200 stored  | 400 | 404 no bucket | 411 | 413 | 503 InsufficientReplicas
//	GET    /v1/{bucket}/{key...}  200 payload | 400 | 404 | 503 NoHealthyReplica
//	HEAD   /v1/{bucket}/{key...}  200 headers from metadata | 400 | 404
//	DELETE /v1/{bucket}/{key...}  204 | 400 | 404 no bucket
//	GET    /healthz               200
//	POST   /internal/heartbeat    204 | 400 InvalidHeartbeat
//	GET    /cluster/status        200 {nodes:[...],under_replicated}
//
// Every response carries X-Request-ID and every request is logged.
func NewHandler(cfg Config) http.Handler {
	client := nodeclient.New()
	s := &Server{
		meta:        cfg.Meta,
		nodes:       cfg.Nodes,
		client:      client,
		writer:      &replication.Writer{N: cfg.RF, W: cfg.W, Client: client, Registry: cfg.Nodes, Timeout: cfg.NodeTimeout},
		spoolDir:    cfg.SpoolDir,
		maxSize:     cfg.MaxObjectSize,
		nodeTimeout: cfg.NodeTimeout,
		uploads:     make(chan struct{}, cfg.MaxUploads),
		log:         cfg.Log,
	}
	mux := http.NewServeMux()
	mux.HandleFunc("PUT /v1/{bucket}", s.handlePutBucket)
	mux.HandleFunc("GET /v1/{bucket}", s.handleGetBucket)
	mux.HandleFunc("PUT /v1/{bucket}/{key...}", s.handlePutObject)
	mux.HandleFunc("GET /v1/{bucket}/{key...}", s.handleGetObject)
	mux.HandleFunc("DELETE /v1/{bucket}/{key...}", s.handleDeleteObject)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		httpx.WriteJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("POST /internal/heartbeat", s.handleHeartbeat)
	mux.HandleFunc("GET /cluster/status", s.handleClusterStatus)
	return httpx.RequestID(httpx.Logging(cfg.Log, mux))
}

// statusNode is one entry of the /cluster/status response.
type statusNode struct {
	NodeID          string    `json:"node_id"`
	Addr            string    `json:"addr"`
	Status          string    `json:"status"`
	FreeBytes       uint64    `json:"free_bytes"`
	BlobCount       int       `json:"blob_count"`
	LastSeen        time.Time `json:"last_seen"`
	StatusChangedAt time.Time `json:"status_changed_at"`
}

// statusResponse is the body of GET /cluster/status.
type statusResponse struct {
	Nodes           []statusNode `json:"nodes"`
	UnderReplicated int          `json:"under_replicated"`
}

func (s *Server) handleHeartbeat(w http.ResponseWriter, r *http.Request) {
	var hb cluster.Heartbeat
	if err := json.NewDecoder(io.LimitReader(r.Body, 4096)).Decode(&hb); err != nil {
		s.writeError(w, r, fmt.Errorf("%w: %w", cluster.ErrInvalidHeartbeat, err))
		return
	}
	if err := s.nodes.Heartbeat(r.Context(), hb); err != nil {
		s.writeError(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleClusterStatus(w http.ResponseWriter, r *http.Request) {
	all := s.nodes.All()
	resp := statusResponse{Nodes: make([]statusNode, 0, len(all))}
	for _, n := range all {
		resp.Nodes = append(resp.Nodes, statusNode{
			NodeID: n.ID, Addr: n.Addr, Status: n.Status, FreeBytes: n.FreeBytes, BlobCount: n.BlobCount,
			LastSeen: n.LastSeen.UTC(), StatusChangedAt: n.StatusChangedAt.UTC(),
		})
	}
	httpx.WriteJSON(w, http.StatusOK, resp)
}

func (s *Server) handlePutBucket(w http.ResponseWriter, r *http.Request) {
	bucket := r.PathValue("bucket")
	if err := validateBucket(bucket); err != nil {
		s.writeError(w, r, err)
		return
	}
	if err := s.meta.CreateBucket(r.Context(), bucket); err != nil {
		s.writeError(w, r, err)
		return
	}
	httpx.WriteJSON(w, http.StatusCreated, map[string]string{"bucket": bucket})
}

func (s *Server) handlePutObject(w http.ResponseWriter, r *http.Request) {
	bucket, key := r.PathValue("bucket"), r.PathValue("key")
	if err := validateBucket(bucket); err != nil {
		s.writeError(w, r, err)
		return
	}
	if err := validateKey(key); err != nil {
		s.writeError(w, r, err)
		return
	}
	// Both checks happen before a single body byte is read.
	if r.ContentLength < 0 {
		s.writeError(w, r, ErrLengthRequired)
		return
	}
	if r.ContentLength > s.maxSize {
		s.writeError(w, r, fmt.Errorf("%w: %d bytes exceeds %d", ErrTooLarge, r.ContentLength, s.maxSize))
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, s.maxSize)

	select {
	case s.uploads <- struct{}{}:
		defer func() { <-s.uploads }()
	case <-r.Context().Done():
		return
	}

	blobID := newBlobID()
	spoolPath := filepath.Join(s.spoolDir, blobID)
	spool, err := os.OpenFile(spoolPath, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		s.writeError(w, r, fmt.Errorf("api: create spool: %w", err))
		return
	}
	defer s.removeSpool(r, spoolPath)
	defer spool.Close()

	digest := sha256.New()
	size, err := io.Copy(io.MultiWriter(spool, digest), r.Body)
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			s.writeError(w, r, fmt.Errorf("%w: %w", ErrTooLarge, err))
			return
		}
		s.writeError(w, r, fmt.Errorf("%w: %w", ErrBodyRead, err))
		return
	}
	if size != r.ContentLength {
		s.writeError(w, r, fmt.Errorf("%w: got %d of %d declared bytes", ErrBodyRead, size, r.ContentLength))
		return
	}
	sum := hex.EncodeToString(digest.Sum(nil))

	res, err := s.writer.Write(r.Context(), key, blobID, size, sum, spool)
	for _, a := range res.Failed {
		s.log.Warn("replica_write_failed", "request_id", httpx.RequestIDFrom(r.Context()),
			"blob_id", blobID, "node_id", a.NodeID, "err", a.Err)
	}
	if err != nil {
		// Copies that did land are unreferenced; queue them for the GC
		// worker rather than deleting inline.
		s.queueStrays(r, blobID, res.Succeeded)
		s.writeError(w, r, fmt.Errorf("%w: %w", ErrInsufficientReplicas, err))
		return
	}

	contentType := r.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	obj := meta.Object{Bucket: bucket, Key: key, BlobID: blobID, Size: size, SHA256: sum, ContentType: contentType}
	if err := s.meta.CommitObject(r.Context(), obj, res.Succeeded); err != nil {
		s.queueStrays(r, blobID, res.Succeeded)
		s.writeError(w, r, err)
		return
	}
	w.Header().Set("ETag", `"`+sum+`"`)
	httpx.WriteJSON(w, http.StatusOK, putResponse{
		Bucket: bucket, Key: key, Size: size, SHA256: sum, Replicas: res.Succeeded,
		Quorum: strconv.Itoa(len(res.Succeeded)) + "/" + strconv.Itoa(s.writer.N),
	})
}

// queueStrays puts blobID on nodeIDs into pending_deletes: the copies were
// written but the object was never committed. A failure to queue is
// logged, not returned, because the response has already been decided.
func (s *Server) queueStrays(r *http.Request, blobID string, nodeIDs []string) {
	if len(nodeIDs) == 0 {
		return
	}
	// The client may already be gone, so the queueing must not die with
	// its request.
	if err := s.meta.EnqueueDeletes(context.WithoutCancel(r.Context()), blobID, nodeIDs); err != nil {
		s.log.Error("api.orphan_blob", "request_id", httpx.RequestIDFrom(r.Context()),
			"blob_id", blobID, "node_ids", nodeIDs, "err", err)
	}
}

// bucketResponse is the body of GET /v1/{bucket}.
type bucketResponse struct {
	Name        string `json:"name"`
	CreatedAt   string `json:"created_at"`
	ObjectCount int    `json:"object_count"`
}

func (s *Server) handleGetBucket(w http.ResponseWriter, r *http.Request) {
	bucket := r.PathValue("bucket")
	if err := validateBucket(bucket); err != nil {
		s.writeError(w, r, err)
		return
	}
	b, err := s.meta.GetBucket(r.Context(), bucket)
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	// Counted by paging through the listing so meta needs no count method.
	count, startAfter := 0, ""
	for {
		objs, truncated, err := s.meta.ListObjects(r.Context(), bucket, "", startAfter, maxListLimit)
		if err != nil {
			s.writeError(w, r, err)
			return
		}
		count += len(objs)
		if !truncated {
			break
		}
		startAfter = objs[len(objs)-1].Key
	}
	httpx.WriteJSON(w, http.StatusOK, bucketResponse{
		Name: b.Name, CreatedAt: time.UnixMilli(b.CreatedAt).UTC().Format(time.RFC3339Nano), ObjectCount: count,
	})
}

// listEntry is one object in a listing.
type listEntry struct {
	Key          string `json:"key"`
	Size         int64  `json:"size"`
	ETag         string `json:"etag"`
	ContentType  string `json:"content_type"`
	LastModified string `json:"last_modified"`
}

// listResponse is the body of GET /v1/{bucket}/. Objects is never null.
type listResponse struct {
	Objects        []listEntry `json:"objects"`
	Truncated      bool        `json:"truncated"`
	NextStartAfter string      `json:"next_start_after"`
}

func (s *Server) handleListObjects(w http.ResponseWriter, r *http.Request, bucket string) {
	lq, err := parseListQuery(r.URL.Query())
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	objs, truncated, err := s.meta.ListObjects(r.Context(), bucket, lq.Prefix, lq.StartAfter, lq.Limit)
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	resp := listResponse{Objects: make([]listEntry, 0, len(objs)), Truncated: truncated}
	for _, o := range objs {
		resp.Objects = append(resp.Objects, listEntry{
			Key:          o.Key,
			Size:         o.Size,
			ETag:         `"` + o.SHA256 + `"`,
			ContentType:  o.ContentType,
			LastModified: time.UnixMilli(o.CreatedAt).UTC().Format(time.RFC3339Nano),
		})
	}
	if truncated {
		resp.NextStartAfter = objs[len(objs)-1].Key
	}
	httpx.WriteJSON(w, http.StatusOK, resp)
}

func (s *Server) handleGetObject(w http.ResponseWriter, r *http.Request) {
	bucket, key := r.PathValue("bucket"), r.PathValue("key")
	if err := validateBucket(bucket); err != nil {
		s.writeError(w, r, err)
		return
	}
	// GET /v1/{bucket}/ is the listing; the empty key is invalid elsewhere.
	if key == "" {
		s.handleListObjects(w, r, bucket)
		return
	}
	if err := validateKey(key); err != nil {
		s.writeError(w, r, err)
		return
	}
	obj, err := s.meta.GetObject(r.Context(), bucket, key)
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	if r.Method == http.MethodHead {
		setObjectHeaders(w, obj)
		w.WriteHeader(http.StatusOK)
		return
	}
	replicas, err := s.meta.Replicas(r.Context(), obj.BlobID)
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	if len(replicas) == 0 {
		s.writeError(w, r, fmt.Errorf("%w: blob %s has no replicas", ErrNoHealthyReplica, obj.BlobID))
		return
	}
	rep := replicas[0]
	nodeCtx, cancel := context.WithTimeout(r.Context(), s.nodeTimeout)
	defer cancel()
	blob, err := s.client.Get(nodeCtx, rep.Addr, obj.BlobID)
	if err != nil {
		s.writeError(w, r, fmt.Errorf("%w: node %s: %w", ErrNoHealthyReplica, rep.NodeID, err))
		return
	}
	defer blob.Close()
	if blob.Length != obj.Size || blob.SHA256 != obj.SHA256 {
		s.writeError(w, r, fmt.Errorf("%w: node %s holds %d bytes %s, metadata says %d bytes %s",
			ErrNoHealthyReplica, rep.NodeID, blob.Length, blob.SHA256, obj.Size, obj.SHA256))
		return
	}
	setObjectHeaders(w, obj)
	w.WriteHeader(http.StatusOK)
	if _, err := io.Copy(w, blob); err != nil {
		s.log.Warn("api.stream_aborted", "request_id", httpx.RequestIDFrom(r.Context()), "err", err)
	}
}

func (s *Server) handleDeleteObject(w http.ResponseWriter, r *http.Request) {
	bucket, key := r.PathValue("bucket"), r.PathValue("key")
	if err := validateBucket(bucket); err != nil {
		s.writeError(w, r, err)
		return
	}
	if err := validateKey(key); err != nil {
		s.writeError(w, r, err)
		return
	}
	if err := s.meta.DeleteObject(r.Context(), bucket, key); err != nil {
		s.writeError(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// setObjectHeaders writes the headers GET and HEAD share, all from metadata.
func setObjectHeaders(w http.ResponseWriter, obj meta.Object) {
	h := w.Header()
	h.Set("Content-Length", strconv.FormatInt(obj.Size, 10))
	h.Set("Content-Type", obj.ContentType)
	h.Set("ETag", `"`+obj.SHA256+`"`)
	h.Set("Last-Modified", time.UnixMilli(obj.CreatedAt).UTC().Format(http.TimeFormat))
}

// newBlobID returns 16 random bytes as 32 hex characters.
func newBlobID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("api: crypto/rand: " + err.Error())
	}
	return hex.EncodeToString(b[:])
}

// removeSpool deletes a spool file; a failure is logged, not returned,
// because the response has already been decided by then.
func (s *Server) removeSpool(r *http.Request, path string) {
	if err := os.Remove(path); err != nil {
		s.log.Error("api.spool_remove", "request_id", httpx.RequestIDFrom(r.Context()), "path", path, "err", err)
	}
}
