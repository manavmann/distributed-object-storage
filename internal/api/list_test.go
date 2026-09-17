package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"testing"

	"github.com/manavmann/distributed-object-storage/internal/httpx"
	"github.com/manavmann/distributed-object-storage/internal/meta"
)

// seed commits one metadata-only object per key on node n1, in the order
// given, so listing order must come from the store and not insertion.
func (e *env) seed(t *testing.T, bucket string, keys []string) {
	t.Helper()
	ctx := context.Background()
	for i, k := range keys {
		obj := meta.Object{Bucket: bucket, Key: k, BlobID: fmt.Sprintf("%032x", i), Size: int64(i), SHA256: digest([]byte(k)), ContentType: "text/plain"}
		if err := e.meta.CommitObject(ctx, obj, []string{"n1"}); err != nil {
			t.Fatalf("seed %q: %v", k, err)
		}
	}
}

func (e *env) list(t *testing.T, bucket string, q url.Values) (int, listResponse, []byte) {
	t.Helper()
	path := "/v1/" + bucket + "/"
	if len(q) > 0 {
		path += "?" + q.Encode()
	}
	resp, body := e.do(t, http.MethodGet, path, nil)
	var lr listResponse
	if resp.StatusCode == http.StatusOK {
		if err := json.Unmarshal(body, &lr); err != nil {
			t.Fatalf("GET %s: %v: %s", path, err, body)
		}
	}
	return resp.StatusCode, lr, body
}

// walk follows next_start_after until truncated is false and returns
// every key seen plus the number of pages.
func (e *env) walk(t *testing.T, bucket, prefix string, limit int) ([]string, int) {
	t.Helper()
	var keys []string
	pages, startAfter := 0, ""
	for {
		q := url.Values{"limit": {fmt.Sprint(limit)}}
		if prefix != "" {
			q.Set("prefix", prefix)
		}
		if startAfter != "" {
			q.Set("start_after", startAfter)
		}
		status, lr, body := e.list(t, bucket, q)
		if status != http.StatusOK {
			t.Fatalf("page %d = %d %s", pages, status, body)
		}
		pages++
		if len(lr.Objects) > limit {
			t.Fatalf("page %d returned %d objects, limit %d", pages, len(lr.Objects), limit)
		}
		for _, o := range lr.Objects {
			keys = append(keys, o.Key)
		}
		if !lr.Truncated {
			if lr.NextStartAfter != "" {
				t.Fatalf("last page has next_start_after %q", lr.NextStartAfter)
			}
			return keys, pages
		}
		if lr.NextStartAfter != lr.Objects[len(lr.Objects)-1].Key {
			t.Fatalf("next_start_after %q != last key %q", lr.NextStartAfter, lr.Objects[len(lr.Objects)-1].Key)
		}
		startAfter = lr.NextStartAfter
	}
}

func TestListPaginationWalk(t *testing.T) {
	e := newEnv(t)
	e.createBucket(t, "bkt")
	const n = 2500
	want := make([]string, n)
	for i := range want {
		want[i] = fmt.Sprintf("obj-%04d", i)
	}
	shuffled := slices.Clone(want)
	rand.New(rand.NewSource(7)).Shuffle(n, func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })
	e.seed(t, "bkt", shuffled)

	for _, limit := range []int{1000, 7} {
		got, pages := e.walk(t, "bkt", "", limit)
		if wantPages := (n + limit - 1) / limit; pages != wantPages {
			t.Errorf("limit %d: %d pages, want %d", limit, pages, wantPages)
		}
		if !slices.Equal(got, want) {
			t.Errorf("limit %d: walk returned %d keys, sorted=%v, want %d distinct sorted keys",
				limit, len(got), slices.IsSorted(got), n)
		}
	}
	// A prefix walk pages the same way over the subset.
	var wantPrefixed []string
	for _, k := range want {
		if strings.HasPrefix(k, "obj-1") {
			wantPrefixed = append(wantPrefixed, k)
		}
	}
	if got, _ := e.walk(t, "bkt", "obj-1", 7); !slices.Equal(got, wantPrefixed) {
		t.Errorf("prefix walk returned %d keys, want %d", len(got), len(wantPrefixed))
	}

	// Entry shape comes straight from metadata.
	status, lr, body := e.list(t, "bkt", url.Values{"limit": {"1"}, "start_after": {"obj-0041"}})
	if status != http.StatusOK || len(lr.Objects) != 1 {
		t.Fatalf("single page = %d %s", status, body)
	}
	o := lr.Objects[0]
	if o.Key != "obj-0042" || o.ETag != `"`+digest([]byte("obj-0042"))+`"` || o.ContentType != "text/plain" || o.LastModified == "" {
		t.Errorf("entry = %+v", o)
	}

	// GET /v1/{bucket} counts by paging through the same listing.
	resp, body := e.do(t, http.MethodGet, "/v1/bkt", nil)
	var br bucketResponse
	if resp.StatusCode != http.StatusOK || json.Unmarshal(body, &br) != nil {
		t.Fatalf("GET bucket = %d %s", resp.StatusCode, body)
	}
	if br.Name != "bkt" || br.ObjectCount != n || br.CreatedAt == "" {
		t.Errorf("bucket = %+v, want name bkt count %d", br, n)
	}
}

func TestListPrefixBoundaries(t *testing.T) {
	e := newEnv(t)
	e.createBucket(t, "bkt")
	e.seed(t, "bkt", []string{"b/1", "a0", "a/2", "b", "a", "ab", "a/", "a/1"})
	for _, tc := range []struct {
		prefix string
		want   []string
	}{
		{"", []string{"a", "a/", "a/1", "a/2", "a0", "ab", "b", "b/1"}},
		{"a", []string{"a", "a/", "a/1", "a/2", "a0", "ab"}},
		{"a/", []string{"a/", "a/1", "a/2"}},
		{"a/1", []string{"a/1"}},
		{"a/3", []string{}},
		{"b", []string{"b", "b/1"}},
		{"b/", []string{"b/1"}},
		{"c", []string{}},
	} {
		q := url.Values{}
		if tc.prefix != "" {
			q.Set("prefix", tc.prefix)
		}
		status, lr, body := e.list(t, "bkt", q)
		if status != http.StatusOK {
			t.Errorf("prefix %q = %d %s", tc.prefix, status, body)
			continue
		}
		var got []string
		for _, o := range lr.Objects {
			got = append(got, o.Key)
		}
		if !slices.Equal(got, tc.want) || lr.Truncated {
			t.Errorf("prefix %q = %v truncated=%v, want %v", tc.prefix, got, lr.Truncated, tc.want)
		}
		if !strings.Contains(string(body), `"objects":[`) {
			t.Errorf("prefix %q: objects is not an array: %s", tc.prefix, body)
		}
	}
}

func TestListBadLimit(t *testing.T) {
	e := newEnv(t)
	e.createBucket(t, "bkt")
	for _, tc := range []struct {
		limit  string
		status int
	}{
		{"", http.StatusOK},
		{"1", http.StatusOK},
		{"1000", http.StatusOK},
		{"0", http.StatusBadRequest},
		{"-1", http.StatusBadRequest},
		{"1001", http.StatusBadRequest},
		{"abc", http.StatusBadRequest},
		{"1.5", http.StatusBadRequest},
		{" 5", http.StatusBadRequest},
		{"9999999999999999999999", http.StatusBadRequest},
	} {
		q := url.Values{}
		if tc.limit != "" {
			q.Set("limit", tc.limit)
		}
		status, _, body := e.list(t, "bkt", q)
		if status != tc.status {
			t.Errorf("limit %q = %d %s, want %d", tc.limit, status, body, tc.status)
			continue
		}
		if status == http.StatusBadRequest && errCode(t, body) != "InvalidArgument" {
			t.Errorf("limit %q code = %s, want InvalidArgument", tc.limit, errCode(t, body))
		}
	}
	for _, q := range []url.Values{
		{"prefix": {"/abs"}},
		{"prefix": {"a/../b"}},
		{"start_after": {"/abs"}},
		{"start_after": {strings.Repeat("k", 1025)}},
	} {
		status, _, body := e.list(t, "bkt", q)
		if status != http.StatusBadRequest || errCode(t, body) != "InvalidArgument" {
			t.Errorf("%v = %d %s, want 400 InvalidArgument", q, status, body)
		}
	}
	if status, _, body := e.list(t, "nope", nil); status != http.StatusNotFound || errCode(t, body) != "NoSuchBucket" {
		t.Errorf("list missing bucket = %d %s", status, body)
	}
	if resp, body := e.do(t, http.MethodGet, "/v1/nope", nil); resp.StatusCode != http.StatusNotFound || errCode(t, body) != "NoSuchBucket" {
		t.Errorf("get missing bucket = %d %s", resp.StatusCode, body)
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
		{ErrNoHealthyReplica, 503, "NoHealthyReplica"},
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

func TestValidateKeyTable(t *testing.T) {
	for _, tc := range []struct {
		key string
		ok  bool
	}{
		{"k", true},
		{"a/b/c", true},
		{"a//b", true},
		{"./a", true},
		{"a/./b", true},
		{"a/.../b", true},
		{"..a", true},
		{"a..", true},
		{"trailing/", true},
		{"ünïcode/🙂", true},
		{strings.Repeat("k", 1024), true},
		{"", false},
		{"/abs", false},
		{"..", false},
		{"../a", false},
		{"a/..", false},
		{"a/../b", false},
		{"\xff", false},
		{"a\xffb", false},
		{strings.Repeat("k", 1025), false},
	} {
		err := validateKey(tc.key)
		if (err == nil) != tc.ok {
			t.Errorf("validateKey(%q) = %v, want ok=%v", tc.key, err, tc.ok)
		}
		if err != nil && !errors.Is(err, ErrInvalidKey) {
			t.Errorf("validateKey(%q) = %v, not ErrInvalidKey", tc.key, err)
		}
	}
	for _, tc := range []struct {
		name string
		ok   bool
	}{
		{"abc", true},
		{"a-1", true},
		{"123", true},
		{strings.Repeat("a", 63), true},
		{"ab", false},
		{strings.Repeat("a", 64), false},
		{"-ab", false},
		{"ab-", false},
		{"Abc", false},
		{"a_b", false},
		{"a.b", false},
		{"a/b", false},
		{"", false},
	} {
		err := validateBucket(tc.name)
		if (err == nil) != tc.ok {
			t.Errorf("validateBucket(%q) = %v, want ok=%v", tc.name, err, tc.ok)
		}
		if err != nil && !errors.Is(err, ErrInvalidBucket) {
			t.Errorf("validateBucket(%q) = %v, not ErrInvalidBucket", tc.name, err)
		}
	}
}
