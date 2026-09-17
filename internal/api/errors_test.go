package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/manavmann/distributed-object-storage/internal/cluster"
	"github.com/manavmann/distributed-object-storage/internal/httpx"
	"github.com/manavmann/distributed-object-storage/internal/meta"
	"github.com/manavmann/distributed-object-storage/internal/replication"
)

func TestStatusFor(t *testing.T) {
	for _, tc := range []struct {
		err    error
		status int
		code   string
	}{
		{ErrInvalidBucket, 400, "InvalidBucket"},
		{ErrInvalidKey, 400, "InvalidKey"},
		{ErrInvalidArgument, 400, "InvalidArgument"},
		{ErrLengthRequired, 411, "LengthRequired"},
		{ErrTooLarge, 413, "EntityTooLarge"},
		{ErrBodyRead, 400, "IncompleteBody"},
		{cluster.ErrInvalidHeartbeat, 400, "InvalidHeartbeat"},
		{ErrInsufficientReplicas, 503, "InsufficientReplicas"},
		{replication.ErrNoHealthyReplica, 503, "NoHealthyReplica"},
		{meta.ErrNoSuchBucket, 404, "NoSuchBucket"},
		{meta.ErrNoSuchKey, 404, "NoSuchKey"},
		{meta.ErrBucketExists, 409, "BucketAlreadyExists"},
		{meta.ErrBucketNotEmpty, 409, "BucketNotEmpty"},
		{errors.New("boom"), 500, "InternalError"},
	} {
		status, code := statusFor(tc.err)
		if status != tc.status || code != tc.code {
			t.Errorf("statusFor(%v) = %d %s, want %d %s", tc.err, status, code, tc.status, tc.code)
		}
	}
}

// TestErrorEnvelope drives every public code through writeError and
// checks the JSON envelope and request id header.
func TestErrorEnvelope(t *testing.T) {
	s := &Server{log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	for _, tc := range []struct {
		err    error
		status int
		code   string
	}{
		{ErrInvalidBucket, 400, "InvalidBucket"},
		{ErrInvalidKey, 400, "InvalidKey"},
		{ErrInvalidArgument, 400, "InvalidArgument"},
		{ErrBodyRead, 400, "IncompleteBody"},
		{meta.ErrNoSuchBucket, 404, "NoSuchBucket"},
		{meta.ErrNoSuchKey, 404, "NoSuchKey"},
		{meta.ErrBucketExists, 409, "BucketAlreadyExists"},
		{meta.ErrBucketNotEmpty, 409, "BucketNotEmpty"},
		{ErrLengthRequired, 411, "LengthRequired"},
		{ErrTooLarge, 413, "EntityTooLarge"},
		{errors.New("boom"), 500, "InternalError"},
		{ErrInsufficientReplicas, 503, "InsufficientReplicas"},
		{replication.ErrNoHealthyReplica, 503, "NoHealthyReplica"},
	} {
		h := httpx.RequestID(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			s.writeError(w, r, fmt.Errorf("wrapped: %w", tc.err))
		}))
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
		if rec.Code != tc.status {
			t.Errorf("%s: status %d, want %d", tc.code, rec.Code, tc.status)
		}
		if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
			t.Errorf("%s: Content-Type %q", tc.code, ct)
		}
		var raw map[string]json.RawMessage
		if err := json.Unmarshal(rec.Body.Bytes(), &raw); err != nil || len(raw) != 2 {
			t.Errorf("%s: body %s: %v", tc.code, rec.Body.Bytes(), err)
			continue
		}
		var eb httpx.ErrorBody
		if err := json.Unmarshal(rec.Body.Bytes(), &eb); err != nil {
			t.Errorf("%s: %v", tc.code, err)
			continue
		}
		if eb.Error.Code != tc.code || !strings.Contains(eb.Error.Message, tc.err.Error()) {
			t.Errorf("%s: error = %+v", tc.code, eb.Error)
		}
		if eb.RequestID == "" || eb.RequestID != rec.Header().Get(httpx.RequestIDHeader) {
			t.Errorf("%s: request_id %q, header %q", tc.code, eb.RequestID, rec.Header().Get(httpx.RequestIDHeader))
		}
	}
}
