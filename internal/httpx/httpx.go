// Package httpx holds the HTTP plumbing shared by the coordinator and the
// storage nodes: request ids, access logging and the JSON response shape.
package httpx

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"github.com/manavmann/distributed-object-storage/internal/events"
	"github.com/manavmann/distributed-object-storage/internal/metrics"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// RequestIDHeader carries the request id on both request and response.
const RequestIDHeader = "X-Request-ID"

type ctxKey struct{}

// RequestID honours an incoming X-Request-ID, or generates a 16-hex-char id,
// echoes it on the response and stores it in the request context.
func RequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get(RequestIDHeader)
		if id == "" {
			var b [8]byte
			if _, err := rand.Read(b[:]); err != nil {
				panic("httpx: crypto/rand: " + err.Error())
			}
			id = hex.EncodeToString(b[:])
		}
		w.Header().Set(RequestIDHeader, id)
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), ctxKey{}, id)))
	})
}

// RequestIDFrom returns the id RequestID stored in ctx, or "".
func RequestIDFrom(ctx context.Context) string {
	id, _ := ctx.Value(ctxKey{}).(string)
	return id
}

// Logging logs one line per request at Info level with the fields
// request_id, method, path, status, duration_ms and bytes.
func Logging(log *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &recorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)
		log.LogAttrs(r.Context(), slog.LevelInfo, events.HTTPRequest,
			slog.String("request_id", RequestIDFrom(r.Context())),
			slog.String("method", r.Method),
			slog.String("path", r.URL.Path),
			slog.Int("status", rec.status),
			slog.Int64("duration_ms", time.Since(start).Milliseconds()),
			slog.Int64("bytes", rec.bytes),
		)
	})
}

// recorder captures the status and body size written through it.
type recorder struct {
	http.ResponseWriter
	status int
	bytes  int64
}

func (r *recorder) WriteHeader(status int) {
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

func (r *recorder) Write(p []byte) (int, error) {
	n, err := r.ResponseWriter.Write(p)
	r.bytes += int64(n)
	return n, err
}

// WriteJSON writes v as a JSON body with the given status.
func WriteJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		slog.Warn(events.HTTPWriteJSON, "err", err)
	}
}

// ErrorBody is the JSON shape of every error response.
type ErrorBody struct {
	Error     ErrorDetail `json:"error"`
	RequestID string      `json:"request_id"`
}

// ErrorDetail is the machine-readable code and human-readable message.
type ErrorDetail struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// WriteError writes {"error":{"code","message"},"request_id"} with status.
func WriteError(w http.ResponseWriter, r *http.Request, status int, code, msg string) {
	WriteJSON(w, status, ErrorBody{
		Error:     ErrorDetail{Code: code, Message: msg},
		RequestID: RequestIDFrom(r.Context()),
	})
}

// BearerAuth requires every request to carry "Authorization: Bearer
// <secret>" and answers 401 otherwise. An empty secret turns the check
// off: next then serves every request as before.
func BearerAuth(secret string, next http.Handler) http.Handler {
	if secret == "" {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		if !ok || subtle.ConstantTimeCompare([]byte(got), []byte(secret)) != 1 {
			w.Header().Set("WWW-Authenticate", "Bearer")
			WriteError(w, r, http.StatusUnauthorized, "unauthorized", "missing or wrong cluster secret")
			return
		}
		next.ServeHTTP(w, r)
	})
}

// SetBearer adds the header BearerAuth checks to req. An empty secret
// adds nothing.
func SetBearer(req *http.Request, secret string) {
	if secret != "" {
		req.Header.Set("Authorization", "Bearer "+secret)
	}
}

// Metrics records each response in m.HTTPRequests and m.HTTPDuration
// under the mux pattern that matched, never the raw path, so the label
// set stays as small as the route table. It must wrap the mux directly
// (inside RequestID), because the mux writes the pattern onto the very
// *Request it receives and a middleware that clones the request would
// hide it. A request no pattern matched is recorded under route "".
func Metrics(m *metrics.Metrics, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &recorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)
		m.HTTPRequests.WithLabelValues(r.Pattern, r.Method, strconv.Itoa(rec.status)).Inc()
		m.HTTPDuration.WithLabelValues(r.Pattern, r.Method).Observe(time.Since(start).Seconds())
	})
}
