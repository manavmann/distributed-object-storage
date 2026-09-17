package httpx

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"
)

// ErrUnhealthy is returned by Healthcheck when /healthz answers but not 200.
var ErrUnhealthy = errors.New("httpx: unhealthy")

// healthcheckTimeout bounds one probe; container healthchecks run it often.
const healthcheckTimeout = 3 * time.Second

// Healthcheck GETs /healthz on the loopback port of listenAddr (the
// server's own CAIRN_ADDR, e.g. ":8080") and returns nil only on 200. It
// is what `-healthcheck` runs inside the container, which has no curl.
func Healthcheck(listenAddr string) error {
	_, port, err := net.SplitHostPort(listenAddr)
	if err != nil {
		return fmt.Errorf("healthcheck: listen addr %q: %w", listenAddr, err)
	}
	client := &http.Client{Timeout: healthcheckTimeout}
	resp, err := client.Get("http://" + net.JoinHostPort("127.0.0.1", port) + "/healthz")
	if err != nil {
		return fmt.Errorf("healthcheck: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%w: /healthz returned %d", ErrUnhealthy, resp.StatusCode)
	}
	return nil
}
