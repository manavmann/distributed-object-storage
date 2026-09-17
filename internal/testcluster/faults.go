package testcluster

import (
	"io"
	"net/http"
	"sync/atomic"

	"github.com/manavmann/distributed-object-storage/internal/httpx"
)

// faults are the switches a test flips on one node. They sit in front of
// the real node handler, so a tripped switch never touches the store.
type faults struct {
	failWrites  atomic.Bool
	failDeletes atomic.Bool
	hangWrites  atomic.Bool
}

// wrap returns next guarded by the switches. A failed request gets a 500
// so the coordinator sees the node as unavailable. A hung PUT drains its
// body, then blocks until the client gives up or the connection is cut.
func (f *faults) wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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

// FailWrites makes every PUT to node i answer 500 while on.
func (c *Cluster) FailWrites(i int, on bool) { c.nodes[i].faults.failWrites.Store(on) }

// FailDeletes makes every DELETE to node i answer 500 while on.
func (c *Cluster) FailDeletes(i int, on bool) { c.nodes[i].faults.failDeletes.Store(on) }

// HangWrites makes every PUT to node i block until the caller gives up
// while on. Requests already hung stay hung when it is switched off.
func (c *Cluster) HangWrites(i int, on bool) { c.nodes[i].faults.hangWrites.Store(on) }
