package httpx

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"regexp"
	"testing"
)

func TestRequestIDHonoursHeader(t *testing.T) {
	var got string
	h := RequestID(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = RequestIDFrom(r.Context())
	}))
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set(RequestIDHeader, "abc-123")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if got != "abc-123" {
		t.Fatalf("context id = %q, want abc-123", got)
	}
	if v := rec.Header().Get(RequestIDHeader); v != "abc-123" {
		t.Fatalf("response header = %q, want abc-123", v)
	}
}

func TestRequestIDGenerates16Hex(t *testing.T) {
	var got string
	h := RequestID(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = RequestIDFrom(r.Context())
	}))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if !regexp.MustCompile(`^[0-9a-f]{16}$`).MatchString(got) {
		t.Fatalf("generated id = %q, want 16 hex chars", got)
	}
	if v := rec.Header().Get(RequestIDHeader); v != got {
		t.Fatalf("response header = %q, want %q", v, got)
	}
}

func TestRequestIDFromMissing(t *testing.T) {
	if id := RequestIDFrom(httptest.NewRequest(http.MethodGet, "/", nil).Context()); id != "" {
		t.Fatalf("id = %q, want empty", id)
	}
}

func TestLoggingFields(t *testing.T) {
	var buf bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&buf, nil))
	h := RequestID(Logging(log, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTeapot)
		w.Write([]byte("hello"))
	})))
	req := httptest.NewRequest(http.MethodPost, "/blobs/x", nil)
	req.Header.Set(RequestIDHeader, "rid1")
	h.ServeHTTP(httptest.NewRecorder(), req)

	var line map[string]any
	if err := json.Unmarshal(buf.Bytes(), &line); err != nil {
		t.Fatalf("log line %q: %v", buf.String(), err)
	}
	want := map[string]any{
		"msg": "http.request", "request_id": "rid1", "method": "POST",
		"path": "/blobs/x", "status": float64(418), "bytes": float64(5),
	}
	for k, v := range want {
		if line[k] != v {
			t.Errorf("%s = %v, want %v", k, line[k], v)
		}
	}
	if _, ok := line["duration_ms"].(float64); !ok {
		t.Errorf("duration_ms missing or not a number: %v", line["duration_ms"])
	}
}

func TestLoggingDefaultStatus(t *testing.T) {
	var buf bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&buf, nil))
	h := Logging(log, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	}))
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))
	var line map[string]any
	if err := json.Unmarshal(buf.Bytes(), &line); err != nil {
		t.Fatal(err)
	}
	if line["status"] != float64(200) {
		t.Fatalf("status = %v, want 200", line["status"])
	}
}

func TestWriteError(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	RequestID(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		WriteError(w, r, http.StatusNotFound, "not_found", "no such blob")
	})).ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("content-type = %q", ct)
	}
	var body ErrorBody
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("body %q: %v", rec.Body.String(), err)
	}
	if body.Error.Code != "not_found" || body.Error.Message != "no such blob" {
		t.Fatalf("error = %+v", body.Error)
	}
	if body.RequestID != rec.Header().Get(RequestIDHeader) || body.RequestID == "" {
		t.Fatalf("request_id = %q, header = %q", body.RequestID, rec.Header().Get(RequestIDHeader))
	}
}

func TestWriteJSON(t *testing.T) {
	rec := httptest.NewRecorder()
	WriteJSON(rec, http.StatusCreated, map[string]int{"n": 1})
	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d", rec.Code)
	}
	if got := rec.Body.String(); got != "{\"n\":1}\n" {
		t.Fatalf("body = %q", got)
	}
}
