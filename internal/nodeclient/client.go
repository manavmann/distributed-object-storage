// Package nodeclient is the coordinator's HTTP client for the storage node
// blob API. Every failure is wrapped in one of four sentinels so callers
// can decide between "the node is gone", "the node has the wrong bytes"
// and "the node never had it" without looking at HTTP details.
package nodeclient

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/manavmann/distributed-object-storage/internal/httpx"
	"github.com/manavmann/distributed-object-storage/internal/storage"
)

// DialTimeout bounds how long a connection attempt to a node may take.
const DialTimeout = 2 * time.Second

var (
	// ErrNotFound means the node has no committed blob with that id.
	ErrNotFound = errors.New("nodeclient: blob not found")
	// ErrIntegrity means the node found the blob corrupt and quarantined it.
	ErrIntegrity = errors.New("nodeclient: integrity failure")
	// ErrChecksum means the node rejected the write: the digest it computed
	// differs from the one we sent, or the id is malformed.
	ErrChecksum = errors.New("nodeclient: checksum rejected")
	// ErrUnavailable means the node could not be reached, timed out or
	// answered with a 5xx other than an integrity failure.
	ErrUnavailable = errors.New("nodeclient: node unavailable")
)

// Client talks to storage nodes over one shared connection pool. It is
// safe for concurrent use.
type Client struct {
	http *http.Client
}

// New returns a client. Each call names its node by base URL (an absolute
// http(s) URL with no path); deadlines come from the ctx of each call.
func New() *Client {
	return &Client{
		http: &http.Client{
			Transport: &http.Transport{
				DialContext:         (&net.Dialer{Timeout: DialTimeout}).DialContext,
				MaxIdleConnsPerHost: 4,
			},
		},
	}
}

// Blob is a verified payload streamed from a node. The caller must Close it.
type Blob struct {
	io.ReadCloser
	Length int64
	SHA256 string
}

// Put streams size bytes of r to the node at addr as blob id. sha256 is
// the hex digest of those bytes; the node rejects the write with
// ErrChecksum if what it received hashes differently.
func (c *Client) Put(ctx context.Context, addr, id string, r io.Reader, size int64, sha256 string) error {
	req, err := c.request(ctx, http.MethodPut, addr, id, r)
	if err != nil {
		return err
	}
	req.ContentLength = size
	req.Header.Set(storage.ContentSHA256Header, sha256)
	resp, err := c.do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		return errorFrom(resp)
	}
	var body struct {
		SHA256 string `json:"sha256"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return fmt.Errorf("%w: decode put response: %w", ErrUnavailable, err)
	}
	if body.SHA256 != sha256 {
		return fmt.Errorf("%w: node stored %s, sent %s", ErrChecksum, body.SHA256, sha256)
	}
	return nil
}

// Get opens blob id on the node at addr for streaming. The node verifies
// the whole payload before sending the first byte.
func (c *Client) Get(ctx context.Context, addr, id string) (*Blob, error) {
	req, err := c.request(ctx, http.MethodGet, addr, id, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		defer resp.Body.Close()
		return nil, errorFrom(resp)
	}
	length, err := strconv.ParseInt(resp.Header.Get("Content-Length"), 10, 64)
	if err != nil {
		resp.Body.Close()
		return nil, fmt.Errorf("%w: bad Content-Length: %w", ErrUnavailable, err)
	}
	return &Blob{ReadCloser: resp.Body, Length: length, SHA256: resp.Header.Get(storage.ContentSHA256Header)}, nil
}

// Head returns the length and digest of blob id without its payload. A
// HEAD response has no body, so a corrupt blob surfaces as ErrUnavailable
// here and as ErrIntegrity only from Get.
func (c *Client) Head(ctx context.Context, addr, id string) (length int64, sha256 string, err error) {
	req, err := c.request(ctx, http.MethodHead, addr, id, nil)
	if err != nil {
		return 0, "", err
	}
	resp, err := c.do(req)
	if err != nil {
		return 0, "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, "", errorFrom(resp)
	}
	length, err = strconv.ParseInt(resp.Header.Get("Content-Length"), 10, 64)
	if err != nil {
		return 0, "", fmt.Errorf("%w: bad Content-Length: %w", ErrUnavailable, err)
	}
	return length, resp.Header.Get(storage.ContentSHA256Header), nil
}

// Delete asks the node at addr to stop serving blob id.
func (c *Client) Delete(ctx context.Context, addr, id string) error {
	req, err := c.request(ctx, http.MethodDelete, addr, id, nil)
	if err != nil {
		return err
	}
	resp, err := c.do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		return errorFrom(resp)
	}
	return nil
}

func (c *Client) request(ctx context.Context, method, addr, id string, body io.Reader) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, method, addr+"/blobs/"+id, body)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	return req, nil
}

// do sends req; transport failures (refused, dial or deadline exceeded,
// reset mid-stream) all become ErrUnavailable.
func (c *Client) do(req *http.Request) (*http.Response, error) {
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	return resp, nil
}

// errorFrom is the single place a non-success node response becomes a
// sentinel. The status decides; the JSON body only refines the message,
// except that a 5xx whose code is "corrupt" is an integrity failure.
func errorFrom(resp *http.Response) error {
	var body httpx.ErrorBody
	var detail string
	if err := json.NewDecoder(io.LimitReader(resp.Body, 4096)).Decode(&body); err != nil {
		detail = "unreadable error body: " + err.Error()
	} else {
		detail = body.Error.Code + ": " + body.Error.Message
	}
	switch {
	case resp.StatusCode == http.StatusNotFound:
		return fmt.Errorf("%w: %s", ErrNotFound, detail)
	case resp.StatusCode == http.StatusUnprocessableEntity:
		return fmt.Errorf("%w: %s", ErrChecksum, detail)
	case body.Error.Code == "corrupt":
		return fmt.Errorf("%w: %s", ErrIntegrity, detail)
	default:
		return fmt.Errorf("%w: node returned %s: %s", ErrUnavailable, resp.Status, detail)
	}
}
