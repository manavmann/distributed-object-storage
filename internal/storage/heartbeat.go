package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/manavmann/distributed-object-storage/internal/events"
	"github.com/manavmann/distributed-object-storage/internal/httpx"
	"log/slog"
	"net/http"
	"time"
)

// Heartbeat is what a node reports to the coordinator every interval.
type Heartbeat struct {
	NodeID    string `json:"node_id"`
	Addr      string `json:"addr"`
	BlobCount int    `json:"blob_count"`
	FreeBytes uint64 `json:"free_bytes"`
	// ScrubFailures are the ids of blobs the scrubber found corrupt and
	// quarantined since the last acknowledged heartbeat.
	ScrubFailures []string `json:"scrub_failures,omitempty"`
}

// RunHeartbeat POSTs status() to coordinatorURL/internal/heartbeat once
// immediately and then every interval until ctx is done, calling acked
// with each heartbeat the coordinator accepted. A failed post is logged
// and the loop carries on; the coordinator's health monitor, not the
// node, decides what a missed heartbeat means. secret, when non-empty,
// is sent as a bearer token on every post.
func RunHeartbeat(ctx context.Context, client *http.Client, coordinatorURL, secret string, interval time.Duration, status func() Heartbeat, acked func(Heartbeat), log *slog.Logger) {
	url := coordinatorURL + "/internal/heartbeat"
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		hb := status()
		if err := postHeartbeat(ctx, client, url, secret, hb); err != nil {
			log.Warn(events.NodeHeartbeatFailed, "coordinator", url, "err", err)
		} else {
			acked(hb)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

func postHeartbeat(ctx context.Context, client *http.Client, url, secret string, hb Heartbeat) error {
	body, err := json.Marshal(hb)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	httpx.SetBearer(req, secret)
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("coordinator returned %s", resp.Status)
	}
	return nil
}
