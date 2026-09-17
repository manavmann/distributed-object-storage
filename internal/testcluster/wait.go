// Package testcluster holds helpers shared by tests that stand up real
// coordinator and node processes in-process.
package testcluster

import (
	"testing"
	"time"
)

// WaitFor polls cond every 10ms until it returns true or timeout passes,
// then fails the test with msg. It is the replacement for time.Sleep in
// tests.
func WaitFor(t *testing.T, timeout time.Duration, msg string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if cond() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for %s", timeout, msg)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
