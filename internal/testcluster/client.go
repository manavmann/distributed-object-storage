package testcluster

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/manavmann/distributed-object-storage/internal/httpx"
)

// Client is a typed client for the coordinator API. It always addresses
// the cluster's current coordinator, so it stays valid across
// RestartCoordinator.
type Client struct {
	c *Cluster
}

// APIError is a non-2xx response, decoded from the coordinator's error
// body. Message is the raw body when it is not an error document.
type APIError struct {
	Status  int
	Code    string
	Message string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("api: %d %s: %s", e.Status, e.Code, e.Message)
}

// PutResult is the body of a successful object PUT plus its ETag. Quorum
// is "k/N": k of the N intended copies were acknowledged.
type PutResult struct {
	Bucket   string   `json:"bucket"`
	Key      string   `json:"key"`
	Size     int64    `json:"size"`
	SHA256   string   `json:"sha256"`
	Replicas []string `json:"replicas"`
	Quorum   string   `json:"quorum"`
	ETag     string   `json:"-"`
}

// Object is what GET and HEAD report. Body is nil for HEAD.
type Object struct {
	Body         []byte
	Size         int64
	ETag         string
	ContentType  string
	LastModified time.Time
}

// ListEntry is one object in a Listing.
type ListEntry struct {
	Key          string `json:"key"`
	Size         int64  `json:"size"`
	ETag         string `json:"etag"`
	ContentType  string `json:"content_type"`
	LastModified string `json:"last_modified"`
}

// Listing is the body of GET /v1/{bucket}/.
type Listing struct {
	Objects        []ListEntry `json:"objects"`
	Truncated      bool        `json:"truncated"`
	NextStartAfter string      `json:"next_start_after"`
}

// NodeStatus is one entry of Status.
type NodeStatus struct {
	NodeID          string    `json:"node_id"`
	Addr            string    `json:"addr"`
	Status          string    `json:"status"`
	FreeBytes       uint64    `json:"free_bytes"`
	BlobCount       int       `json:"blob_count"`
	LastSeen        time.Time `json:"last_seen"`
	StatusChangedAt time.Time `json:"status_changed_at"`
}

// Status is the body of GET /cluster/status.
type Status struct {
	Nodes           []NodeStatus `json:"nodes"`
	UnderReplicated int          `json:"under_replicated"`
}

// URL is the coordinator's current base URL.
func (cl *Client) URL() string {
	return cl.c.srv.URL
}

// Do sends method to path with body and optional header pairs and returns
// the response with its body fully read. Any status is returned as-is; it
// is the escape hatch for requests the typed methods cannot express.
func (cl *Client) Do(method, path string, body io.Reader, hdr ...string) (*http.Response, []byte, error) {
	req, err := http.NewRequest(method, cl.URL()+path, body)
	if err != nil {
		return nil, nil, err
	}
	for i := 0; i+1 < len(hdr); i += 2 {
		req.Header.Set(hdr[i], hdr[i+1])
	}
	resp, err := cl.c.srv.Client().Do(req)
	if err != nil {
		return nil, nil, fmt.Errorf("%s %s: %w", method, path, err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, nil, fmt.Errorf("%s %s: read body: %w", method, path, err)
	}
	return resp, b, nil
}

// call is Do plus the status check: anything but want becomes an APIError.
func (cl *Client) call(method, path string, body io.Reader, want int, hdr ...string) (*http.Response, []byte, error) {
	resp, b, err := cl.Do(method, path, body, hdr...)
	if err != nil {
		return nil, nil, err
	}
	if resp.StatusCode != want {
		return nil, nil, errorFrom(resp, b)
	}
	return resp, b, nil
}

func errorFrom(resp *http.Response, body []byte) error {
	e := &APIError{Status: resp.StatusCode, Message: string(body)}
	var eb httpx.ErrorBody
	if json.Unmarshal(body, &eb) == nil && eb.Error.Code != "" {
		e.Code, e.Message = eb.Error.Code, eb.Error.Message
	}
	return e
}

func objectPath(bucket, key string) string {
	return "/v1/" + bucket + "/" + key
}

// CreateBucket creates bucket.
func (cl *Client) CreateBucket(bucket string) error {
	_, _, err := cl.call(http.MethodPut, "/v1/"+bucket, nil, http.StatusCreated)
	return err
}

// Put stores body as bucket/key with contentType ("" for the default).
func (cl *Client) Put(bucket, key string, body []byte, contentType string) (PutResult, error) {
	var hdr []string
	if contentType != "" {
		hdr = []string{"Content-Type", contentType}
	}
	resp, b, err := cl.call(http.MethodPut, objectPath(bucket, key), bytes.NewReader(body), http.StatusOK, hdr...)
	if err != nil {
		return PutResult{}, err
	}
	var pr PutResult
	if err := json.Unmarshal(b, &pr); err != nil {
		return PutResult{}, fmt.Errorf("put response %q: %w", b, err)
	}
	pr.ETag = resp.Header.Get("ETag")
	return pr, nil
}

// Get reads bucket/key.
func (cl *Client) Get(bucket, key string) (Object, error) {
	resp, b, err := cl.call(http.MethodGet, objectPath(bucket, key), nil, http.StatusOK)
	if err != nil {
		return Object{}, err
	}
	obj, err := objectFrom(resp)
	obj.Body = b
	return obj, err
}

// Head reads bucket/key's headers from metadata.
func (cl *Client) Head(bucket, key string) (Object, error) {
	resp, _, err := cl.call(http.MethodHead, objectPath(bucket, key), nil, http.StatusOK)
	if err != nil {
		return Object{}, err
	}
	return objectFrom(resp)
}

func objectFrom(resp *http.Response) (Object, error) {
	size, err := strconv.ParseInt(resp.Header.Get("Content-Length"), 10, 64)
	if err != nil {
		return Object{}, fmt.Errorf("Content-Length %q: %w", resp.Header.Get("Content-Length"), err)
	}
	mod, err := http.ParseTime(resp.Header.Get("Last-Modified"))
	if err != nil {
		return Object{}, fmt.Errorf("Last-Modified %q: %w", resp.Header.Get("Last-Modified"), err)
	}
	return Object{
		Size: size, ETag: resp.Header.Get("ETag"), ContentType: resp.Header.Get("Content-Type"), LastModified: mod,
	}, nil
}

// Delete removes bucket/key from metadata.
func (cl *Client) Delete(bucket, key string) error {
	_, _, err := cl.call(http.MethodDelete, objectPath(bucket, key), nil, http.StatusNoContent)
	return err
}

// List pages bucket's objects. prefix, startAfter and limit are omitted
// from the query when zero.
func (cl *Client) List(bucket, prefix, startAfter string, limit int) (Listing, error) {
	q := url.Values{}
	if prefix != "" {
		q.Set("prefix", prefix)
	}
	if startAfter != "" {
		q.Set("start_after", startAfter)
	}
	if limit > 0 {
		q.Set("limit", strconv.Itoa(limit))
	}
	path := "/v1/" + bucket + "/"
	if len(q) > 0 {
		path += "?" + q.Encode()
	}
	_, b, err := cl.call(http.MethodGet, path, nil, http.StatusOK)
	if err != nil {
		return Listing{}, err
	}
	var l Listing
	if err := json.Unmarshal(b, &l); err != nil {
		return Listing{}, fmt.Errorf("listing %q: %w", b, err)
	}
	return l, nil
}

// Status is the coordinator's view of the cluster.
func (cl *Client) Status() (Status, error) {
	_, b, err := cl.call(http.MethodGet, "/cluster/status", nil, http.StatusOK)
	if err != nil {
		return Status{}, err
	}
	var st Status
	if err := json.Unmarshal(b, &st); err != nil {
		return Status{}, fmt.Errorf("status %q: %w", b, err)
	}
	return st, nil
}
