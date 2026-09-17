package api

import (
	"errors"
	"log/slog"
	"net/http"

	"github.com/manavmann/distributed-object-storage/internal/httpx"
	"github.com/manavmann/distributed-object-storage/internal/meta"
)

var (
	// ErrInvalidBucket means the bucket name fails validateBucket.
	ErrInvalidBucket = errors.New("api: invalid bucket name")
	// ErrInvalidKey means the object key fails validateKey.
	ErrInvalidKey = errors.New("api: invalid object key")
	// ErrLengthRequired means a PUT arrived without a Content-Length.
	ErrLengthRequired = errors.New("api: Content-Length required")
	// ErrTooLarge means the declared or actual body exceeds the limit.
	ErrTooLarge = errors.New("api: object too large")
	// ErrBodyRead means the client's body ended early or could not be read.
	ErrBodyRead = errors.New("api: reading request body")
	// ErrInsufficientReplicas means a PUT could not be stored on enough
	// nodes to be committed.
	ErrInsufficientReplicas = errors.New("api: insufficient replicas")
	// ErrNoHealthyReplica means no node could serve the object's blob.
	ErrNoHealthyReplica = errors.New("api: no healthy replica")
)

// statusFor is the single place an error becomes an HTTP status and code.
func statusFor(err error) (int, string) {
	switch {
	case errors.Is(err, ErrInvalidBucket):
		return http.StatusBadRequest, "InvalidBucket"
	case errors.Is(err, ErrInvalidKey):
		return http.StatusBadRequest, "InvalidKey"
	case errors.Is(err, ErrLengthRequired):
		return http.StatusLengthRequired, "LengthRequired"
	case errors.Is(err, ErrTooLarge):
		return http.StatusRequestEntityTooLarge, "EntityTooLarge"
	case errors.Is(err, ErrBodyRead):
		return http.StatusBadRequest, "IncompleteBody"
	case errors.Is(err, ErrInsufficientReplicas):
		return http.StatusServiceUnavailable, "InsufficientReplicas"
	case errors.Is(err, ErrNoHealthyReplica):
		return http.StatusServiceUnavailable, "NoHealthyReplica"
	case errors.Is(err, meta.ErrNoSuchBucket):
		return http.StatusNotFound, "NoSuchBucket"
	case errors.Is(err, meta.ErrNoSuchKey):
		return http.StatusNotFound, "NoSuchKey"
	case errors.Is(err, meta.ErrBucketExists):
		return http.StatusConflict, "BucketAlreadyExists"
	case errors.Is(err, meta.ErrBucketNotEmpty):
		return http.StatusConflict, "BucketNotEmpty"
	default:
		return http.StatusInternalServerError, "InternalError"
	}
}

// writeError maps err to a response. 5xx responses are logged at Error
// level with the full chain; 4xx are only visible in the access log.
func (s *Server) writeError(w http.ResponseWriter, r *http.Request, err error) {
	status, code := statusFor(err)
	if status >= 500 {
		s.log.LogAttrs(r.Context(), slog.LevelError, "api.error",
			slog.String("request_id", httpx.RequestIDFrom(r.Context())),
			slog.String("code", code), slog.String("err", err.Error()))
	}
	httpx.WriteError(w, r, status, code, err.Error())
}
