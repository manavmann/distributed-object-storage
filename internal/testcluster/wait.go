// Package testcluster stands up a whole Cairn cluster in one test process:
// K storage nodes and a coordinator, each on its own httptest server and
// t.TempDir, with switches for killing and restarting processes and for
// injecting node faults.
package testcluster

import (
	"testing"
	"time"
)

const (
	waitTimeout = 10 * time.Second
	waitPoll    = 20 * time.Millisecond
)

// WaitFor polls cond every 20ms for up to 10s and fails the test with msg
// if it never returns true. It is the replacement for time.Sleep in tests.
func WaitFor(t *testing.T, cond func() bool, msg string) {
	t.Helper()
	if !Poll(cond) {
		t.Fatalf("timed out after %s waiting for %s", waitTimeout, msg)
	}
}

// Poll is WaitFor for a goroutine that may not fail the test: it reports
// whether cond returned true within the same budget.
func Poll(cond func() bool) bool {
	deadline := time.Now().Add(waitTimeout)
	for {
		if cond() {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(waitPoll)
	}
}
