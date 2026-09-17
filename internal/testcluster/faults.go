package testcluster

import (
	"io"
	"net/http"
	"sync/atomic"

	"github.com/manavmann/distributed-object-storage/internal/httpx"
)

// faults are the switches a test flips on one node. They sit in front of
// the real node handler, so a tripped switch never touches the store.
// They also count the PUTs in flight, and the most that ever were, so a
// test can check the coordinator's upload limit from the node's side.
type faults struct {
	failWrites  atomic.Bool
	failDeletes atomic.Bool
	hangWrites  atomic.Bool

	inflightWrites atomic.Int64
	peakWrites     atomic.Int64
}

// wrap returns next guarded by the switches. A failed request gets a 500
// so the coordinator sees the node as unavailable. A hung PUT drains its
// body, then blocks until the client gives up or the connection is cut.
func (f *faults) wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			f.trackWrite()
			defer f.inflightWrites.Add(-1)
		}
		switch {
		case r.Method == http.MethodPut && f.hangWrites.Load():
			io.Copy(io.Discard, r.Body)
			<-r.Context().Done()
			return
		case r.Method == http.MethodPut && f.failWrites.Load(),
			r.Method == http.MethodDelete && f.failDeletes.Load():
			httpx.WriteError(w, r, http.StatusInternalServerError, "fault_injected", "testcluster: fault injected")
			return
		}
		next.ServeHTTP(w, r)
	})
}

// trackWrite counts one more PUT in flight and raises the high-water
// mark if it is a new peak.
func (f *faults) trackWrite() {
	n := f.inflightWrites.Add(1)
	for {
		peak := f.peakWrites.Load()
		if n <= peak || f.peakWrites.CompareAndSwap(peak, n) {
			return
		}
	}
}

// FailWrites makes every PUT to node i answer 500 while on.
func (c *Cluster) FailWrites(i int, on bool) { c.nodes[i].faults.failWrites.Store(on) }

// FailDeletes makes every DELETE to node i answer 500 while on.
func (c *Cluster) FailDeletes(i int, on bool) { c.nodes[i].faults.failDeletes.Store(on) }

// HangWrites makes every PUT to node i block until the caller gives up
// while on. Requests already hung stay hung when it is switched off.
func (c *Cluster) HangWrites(i int, on bool) { c.nodes[i].faults.hangWrites.Store(on) }

// MaxConcurrentWrites is the most PUTs node i has ever had in flight at
// once, counted across kills and restarts.
func (c *Cluster) MaxConcurrentWrites(i int) int {
	return int(c.nodes[i].faults.peakWrites.Load())
}
