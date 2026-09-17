package storage

import (
	"encoding/hex"
	"errors"
	"github.com/manavmann/distributed-object-storage/internal/events"
	"io"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/manavmann/distributed-object-storage/internal/httpx"
	"github.com/manavmann/distributed-object-storage/internal/metrics"
)

// ContentSHA256Header carries the hex SHA-256 of a blob's payload. A PUT may
// send it to have the node reject a body whose digest differs; every GET and
// HEAD response includes it.
const ContentSHA256Header = "X-Content-SHA256"

// putResponse is the body of a successful PUT.
type putResponse struct {
	BlobID string `json:"blob_id"`
	Length uint64 `json:"length"`
	SHA256 string `json:"sha256"`
}

// server binds a Store to the node API.
type server struct {
	store *Store
	log   *slog.Logger
}

// NewHandler serves the node API over store:
//
//	PUT    /blobs/{id}  201 stored | 422 bad id or digest mismatch | 507 disk full | 500
//	GET    /blobs/{id}  200 verified payload | 404 unknown | 500 corrupt or I/O
//	HEAD   /blobs/{id}  200 headers only | 404 | 500
//	DELETE /blobs/{id}  204 quarantined | 404 | 500
//	GET    /healthz     200
//	GET    /metrics     200 Prometheus text format
//
// Every response carries X-Request-ID and every request is logged and
// counted under its route pattern.
func NewHandler(store *Store, m *metrics.Metrics, log *slog.Logger) http.Handler {
	s := &server{store: store, log: log}
	mux := http.NewServeMux()
	mux.HandleFunc("PUT /blobs/{id}", s.handlePut)
	mux.HandleFunc("GET /blobs/{id}", s.handleGet)
	mux.HandleFunc("DELETE /blobs/{id}", s.handleDelete)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		httpx.WriteJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.Handle("GET /metrics", m.Handler())
	return httpx.RequestID(httpx.Logging(log, httpx.Metrics(m, mux)))
}

func (s *server) handlePut(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	h, err := s.store.Write(id, r.Body)
	if err != nil {
		s.writeError(w, r, err, http.StatusUnprocessableEntity)
		return
	}
	sum := hex.EncodeToString(h.SHA256[:])
	if want := r.Header.Get(ContentSHA256Header); want != "" && want != sum {
		if err := s.store.Delete(id); err != nil {
			s.writeError(w, r, err, http.StatusUnprocessableEntity)
			return
		}
		httpx.WriteError(w, r, http.StatusUnprocessableEntity, "checksum_mismatch",
			"payload SHA-256 "+sum+" does not match "+ContentSHA256Header)
		return
	}
	httpx.WriteJSON(w, http.StatusCreated, putResponse{BlobID: id, Length: h.Length, SHA256: sum})
}

func (s *server) handleGet(w http.ResponseWriter, r *http.Request) {
	b, err := s.store.Read(r.PathValue("id"))
	if err != nil {
		s.writeError(w, r, err, http.StatusNotFound)
		return
	}
	defer b.Close()
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.FormatUint(b.Header.Length, 10))
	w.Header().Set(ContentSHA256Header, hex.EncodeToString(b.Header.SHA256[:]))
	w.WriteHeader(http.StatusOK)
	if r.Method == http.MethodHead {
		return
	}
	if _, err := io.Copy(w, b); err != nil {
		s.log.Warn(events.HTTPStreamAborted, "request_id", httpx.RequestIDFrom(r.Context()), "err", err)
	}
}

func (s *server) handleDelete(w http.ResponseWriter, r *http.Request) {
	if err := s.store.Delete(r.PathValue("id")); err != nil {
		s.writeError(w, r, err, http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// writeError maps a Store error to a response. invalidID is the status
// for ErrInvalidID, which is 422 when the client chose the id (PUT) and 404
// when it is merely looking one up.
func (s *server) writeError(w http.ResponseWriter, r *http.Request, err error, invalidID int) {
	status, code := statusFor(err, invalidID)
	if status == http.StatusInternalServerError {
		s.log.Error(events.HTTPStoreError, "request_id", httpx.RequestIDFrom(r.Context()), "err", err)
	}
	httpx.WriteError(w, r, status, code, err.Error())
}

// statusFor is the single place a Store error becomes an HTTP status.
func statusFor(err error, invalidID int) (int, string) {
	switch {
	case errors.Is(err, ErrInvalidID):
		return invalidID, "invalid_id"
	case errors.Is(err, ErrNotFound):
		return http.StatusNotFound, "not_found"
	case errors.Is(err, ErrCorrupt):
		return http.StatusInternalServerError, "corrupt"
	case isNoSpace(err):
		return http.StatusInsufficientStorage, "no_space"
	default:
		return http.StatusInternalServerError, "internal"
	}
}
