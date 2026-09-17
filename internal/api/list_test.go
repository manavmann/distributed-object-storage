package api_test

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"testing"

	"github.com/manavmann/distributed-object-storage/internal/meta"
	"github.com/manavmann/distributed-object-storage/internal/testcluster"
)

// seed commits one metadata-only object per key on node 0, in the order
// given, so listing order must come from the store and not insertion.
func seed(t *testing.T, c *testcluster.Cluster, bucket string, keys []string) {
	t.Helper()
	ctx := context.Background()
	for i, k := range keys {
		obj := meta.Object{Bucket: bucket, Key: k, BlobID: fmt.Sprintf("%032x", i), Size: int64(i), SHA256: digest([]byte(k)), ContentType: "text/plain"}
		if err := c.Meta().CommitObject(ctx, obj, []string{c.NodeID(0)}); err != nil {
			t.Fatalf("seed %q: %v", k, err)
		}
	}
}

// list sends the raw query q so malformed parameters can be exercised.
func list(t *testing.T, c *testcluster.Cluster, bucket string, q url.Values) (int, testcluster.Listing, []byte) {
	t.Helper()
	path := "/v1/" + bucket + "/"
	if len(q) > 0 {
		path += "?" + q.Encode()
	}
	resp, body := do(t, c, http.MethodGet, path, nil)
	var lr testcluster.Listing
	if resp.StatusCode == http.StatusOK {
		if err := json.Unmarshal(body, &lr); err != nil {
			t.Fatalf("GET %s: %v: %s", path, err, body)
		}
	}
	return resp.StatusCode, lr, body
}

// walk follows next_start_after until truncated is false and returns
// every key seen plus the number of pages.
func walk(t *testing.T, c *testcluster.Cluster, bucket, prefix string, limit int) ([]string, int) {
	t.Helper()
	var keys []string
	pages, startAfter := 0, ""
	for {
		lr, err := c.Client().List(bucket, prefix, startAfter, limit)
		if err != nil {
			t.Fatalf("page %d: %v", pages, err)
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
	c := newCluster(t)
	createBucket(t, c, "bkt")
	const n = 2500
	want := make([]string, n)
	for i := range want {
		want[i] = fmt.Sprintf("obj-%04d", i)
	}
	shuffled := slices.Clone(want)
	rand.New(rand.NewSource(7)).Shuffle(n, func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })
	seed(t, c, "bkt", shuffled)

	for _, limit := range []int{1000, 7} {
		got, pages := walk(t, c, "bkt", "", limit)
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
	if got, _ := walk(t, c, "bkt", "obj-1", 7); !slices.Equal(got, wantPrefixed) {
		t.Errorf("prefix walk returned %d keys, want %d", len(got), len(wantPrefixed))
	}

	// Entry shape comes straight from metadata.
	status, lr, body := list(t, c, "bkt", url.Values{"limit": {"1"}, "start_after": {"obj-0041"}})
	if status != http.StatusOK || len(lr.Objects) != 1 {
		t.Fatalf("single page = %d %s", status, body)
	}
	o := lr.Objects[0]
	if o.Key != "obj-0042" || o.ETag != `"`+digest([]byte("obj-0042"))+`"` || o.ContentType != "text/plain" || o.LastModified == "" {
		t.Errorf("entry = %+v", o)
	}

	// GET /v1/{bucket} counts by paging through the same listing.
	resp, body := do(t, c, http.MethodGet, "/v1/bkt", nil)
	var br struct {
		Name        string `json:"name"`
		CreatedAt   string `json:"created_at"`
		ObjectCount int    `json:"object_count"`
	}
	if resp.StatusCode != http.StatusOK || json.Unmarshal(body, &br) != nil {
		t.Fatalf("GET bucket = %d %s", resp.StatusCode, body)
	}
	if br.Name != "bkt" || br.ObjectCount != n || br.CreatedAt == "" {
		t.Errorf("bucket = %+v, want name bkt count %d", br, n)
	}
}

func TestListPrefixBoundaries(t *testing.T) {
	c := newCluster(t)
	createBucket(t, c, "bkt")
	seed(t, c, "bkt", []string{"b/1", "a0", "a/2", "b", "a", "ab", "a/", "a/1"})
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
		status, lr, body := list(t, c, "bkt", q)
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
	c := newCluster(t)
	createBucket(t, c, "bkt")
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
		status, _, body := list(t, c, "bkt", q)
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
		status, _, body := list(t, c, "bkt", q)
		if status != http.StatusBadRequest || errCode(t, body) != "InvalidArgument" {
			t.Errorf("%v = %d %s, want 400 InvalidArgument", q, status, body)
		}
	}
	if status, _, body := list(t, c, "nope", nil); status != http.StatusNotFound || errCode(t, body) != "NoSuchBucket" {
		t.Errorf("list missing bucket = %d %s", status, body)
	}
	if resp, body := do(t, c, http.MethodGet, "/v1/nope", nil); resp.StatusCode != http.StatusNotFound || errCode(t, body) != "NoSuchBucket" {
		t.Errorf("get missing bucket = %d %s", resp.StatusCode, body)
	}
}
