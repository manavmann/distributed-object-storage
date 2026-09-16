package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
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
}

// RunHeartbeat POSTs status() to coordinatorURL/internal/heartbeat once
// immediately and then every interval until ctx is done. A failed post is
// logged and the loop carries on; the coordinator's health monitor, not the
// node, decides what a missed heartbeat means.
func RunHeartbeat(ctx context.Context, client *http.Client, coordinatorURL string, interval time.Duration, status func() Heartbeat, log *slog.Logger) {
	url := coordinatorURL + "/internal/heartbeat"
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		if err := postHeartbeat(ctx, client, url, status()); err != nil {
			log.Warn("node.heartbeat_failed", "coordinator", url, "err", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

func postHeartbeat(ctx context.Context, client *http.Client, url string, hb Heartbeat) error {
	body, err := json.Marshal(hb)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
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
