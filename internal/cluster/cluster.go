// Package cluster answers "which storage nodes can take traffic right now".
// The only implementation so far is a static list from CAIRN_NODES; the
// health monitor will replace it with one backed by heartbeats.
package cluster

import (
	"errors"
	"fmt"
	"strings"
)

// ErrInvalidNodes is wrapped by every CAIRN_NODES parse failure.
var ErrInvalidNodes = errors.New("cluster: invalid CAIRN_NODES")

// Node is a storage node the coordinator can address.
type Node struct {
	ID string
	// Addr is the node's base URL, e.g. http://10.0.0.5:9100.
	Addr string
}

// Registry is the placement view of the cluster.
type Registry interface {
	// Healthy returns the nodes that may take reads and writes, in a
	// stable order.
	Healthy() []Node
	// Get returns node id and whether it is known at all.
	Get(id string) (Node, bool)
}

// StaticRegistry treats every configured node as healthy, always.
type StaticRegistry struct {
	nodes []Node
}

// NewStatic returns a registry over nodes in the given order.
func NewStatic(nodes []Node) *StaticRegistry {
	return &StaticRegistry{nodes: append([]Node(nil), nodes...)}
}

// ParseStatic parses "id=host:port,id=host:port,...". Each host:port
// becomes http://host:port. Ids must be unique and non-empty.
func ParseStatic(s string) (*StaticRegistry, error) {
	if strings.TrimSpace(s) == "" {
		return nil, fmt.Errorf("%w: empty", ErrInvalidNodes)
	}
	var nodes []Node
	seen := map[string]bool{}
	for _, entry := range strings.Split(s, ",") {
		entry = strings.TrimSpace(entry)
		id, hostport, ok := strings.Cut(entry, "=")
		if !ok || id == "" || hostport == "" {
			return nil, fmt.Errorf("%w: entry %q is not id=host:port", ErrInvalidNodes, entry)
		}
		if strings.Contains(hostport, "/") {
			return nil, fmt.Errorf("%w: entry %q: address must be host:port, not a URL", ErrInvalidNodes, entry)
		}
		if seen[id] {
			return nil, fmt.Errorf("%w: duplicate node id %q", ErrInvalidNodes, id)
		}
		seen[id] = true
		nodes = append(nodes, Node{ID: id, Addr: "http://" + hostport})
	}
	return NewStatic(nodes), nil
}

// Healthy returns every configured node.
func (r *StaticRegistry) Healthy() []Node {
	return append([]Node(nil), r.nodes...)
}

// Get returns the configured node with that id.
func (r *StaticRegistry) Get(id string) (Node, bool) {
	for _, n := range r.nodes {
		if n.ID == id {
			return n, true
		}
	}
	return Node{}, false
}
