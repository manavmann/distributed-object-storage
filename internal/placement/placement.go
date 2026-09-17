// Package placement decides which storage nodes hold a given key using
// rendezvous (highest-random-weight, HRW) hashing.
//
// Every node is scored against the key independently and the nodes are
// ordered by score. The top n are the replica targets; the remainder are
// the fallback order for repair. HRW is used instead of a consistent hash
// ring because it needs no state: there are no virtual nodes to build,
// no ring to keep in sync, and adding or removing a node moves only the
// keys that scored that node into their top n (a ring moves the arcs
// adjacent to the changed node, which is uneven unless heavily virtualised).
// The cluster is small (single-digit to low double-digit nodes) so the
// O(nodes) score per key is far cheaper than maintaining a ring.
//
// The score is FNV-1a 64 over nodeID + "\x00" + key (the same function as
// hash/fnv) passed through the splitmix64 finalizer. FNV is chosen over
// hash/maphash because maphash is seeded per process: the coordinator, the
// repair worker and any offline tooling must compute identical rankings
// across restarts and across binaries, and a process-local seed would make
// placement irreproducible. The separator makes ("ab","c") and ("a","bc")
// hash differently. The finalizer is needed because FNV-1a's last step is
// a multiply, which leaves the high bits nearly untouched by the final
// input bytes; HRW compares whole 64-bit values, so without it keys that
// differ only in a trailing digit (object-000123, object-000124) collapse
// onto the same winner and top-1 shares skew by 30-70%. With it every key
// and node-id shape measured lands within a few percent of uniform.
//
// Placement is only a suggestion. The metadata store records which nodes
// actually acknowledged a write; readers never recompute placement.
package placement

import (
	"slices"
	"strings"
)

// FNV-1a 64-bit parameters, identical to hash/fnv.
const (
	fnvOffset64 = 14695981039346656037
	fnvPrime64  = 1099511628211
)

type scored struct {
	id    string
	score uint64
}

// score returns splitmix64(fnv64a(nodeID + "\x00" + key)) without building
// the concatenated string.
func score(nodeID, key string) uint64 {
	h := uint64(fnvOffset64)
	for i := 0; i < len(nodeID); i++ {
		h ^= uint64(nodeID[i])
		h *= fnvPrime64
	}
	h *= fnvPrime64 // separator "\x00": the xor with 0 is a no-op
	for i := 0; i < len(key); i++ {
		h ^= uint64(key[i])
		h *= fnvPrime64
	}
	return mix64(h)
}

// mix64 is the splitmix64 finalizer: a bijection on uint64 that spreads
// every input bit across the whole word.
func mix64(h uint64) uint64 {
	h ^= h >> 30
	h *= 0xbf58476d1ce4e5b9
	h ^= h >> 27
	h *= 0x94d049bb133111eb
	h ^= h >> 31
	return h
}

// Rank orders nodeIDs for key by descending HRW score, ties broken by
// ascending id. It never mutates nodeIDs; the only allocations are the
// scored copy and the returned slice.
func Rank(key string, nodeIDs []string) []string {
	s := make([]scored, len(nodeIDs))
	for i, id := range nodeIDs {
		s[i] = scored{id: id, score: score(id, key)}
	}
	slices.SortFunc(s, func(a, b scored) int {
		if a.score != b.score {
			if a.score > b.score {
				return -1
			}
			return 1
		}
		return strings.Compare(a.id, b.id)
	})
	out := make([]string, len(s))
	for i := range s {
		out[i] = s[i].id
	}
	return out
}

// Targets splits Rank(key, nodeIDs) into the first n nodes and the rest.
// n is clamped to [0, len(nodeIDs)].
func Targets(key string, nodeIDs []string, n int) (targets, rest []string) {
	ranked := Rank(key, nodeIDs)
	n = max(0, min(n, len(ranked)))
	return ranked[:n], ranked[n:]
}
