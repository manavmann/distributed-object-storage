package placement

import (
	"fmt"
	"hash/fnv"
	"math/rand"
	"slices"
	"testing"
)

func nodes(n int) []string {
	ids := make([]string, n)
	for i := range ids {
		ids[i] = fmt.Sprintf("node-%02d", i)
	}
	return ids
}

func keys(n int) []string {
	ks := make([]string, n)
	for i := range ks {
		ks[i] = fmt.Sprintf("bucket/object-%06d", i)
	}
	return ks
}

// TestScoreMatchesHashFNV pins the inline FNV-1a stage to hash/fnv so the
// two can never drift apart.
func TestScoreMatchesHashFNV(t *testing.T) {
	cases := [][2]string{{"", ""}, {"a", ""}, {"", "a"}, {"node-01", "bucket/key"}, {"ab", "c"}, {"a", "bc"}}
	for _, c := range cases {
		h := fnv.New64a()
		h.Write([]byte(c[0] + "\x00" + c[1]))
		if got, want := score(c[0], c[1]), mix64(h.Sum64()); got != want {
			t.Fatalf("score(%q,%q)=%d, hash/fnv=%d", c[0], c[1], got, want)
		}
	}
	if score("ab", "c") == score("a", "bc") {
		t.Fatal("separator does not distinguish (ab,c) from (a,bc)")
	}
}

func TestRankDeterministicAndDoesNotMutate(t *testing.T) {
	ids := nodes(8)
	const key = "bucket/some-object"
	want := Rank(key, ids)
	if len(want) != len(ids) {
		t.Fatalf("Rank returned %d ids, want %d", len(want), len(ids))
	}
	rng := rand.New(rand.NewSource(1))
	for i := 0; i < 1000; i++ {
		in := slices.Clone(ids)
		rng.Shuffle(len(in), func(a, b int) { in[a], in[b] = in[b], in[a] })
		snapshot := slices.Clone(in)
		got := Rank(key, in)
		if !slices.Equal(got, want) {
			t.Fatalf("call %d: Rank(%v)=%v, want %v", i, in, got, want)
		}
		if !slices.Equal(in, snapshot) {
			t.Fatalf("call %d: Rank mutated its input: %v -> %v", i, snapshot, in)
		}
	}
}

func TestRankTieBreakByID(t *testing.T) {
	// Duplicate ids score identically, so the tie-break must keep them
	// adjacent and in id order relative to each other.
	got := Rank("k", []string{"b", "a", "b", "a"})
	for i := 1; i < len(got); i++ {
		if score(got[i-1], "k") == score(got[i], "k") && got[i-1] > got[i] {
			t.Fatalf("tie not broken by ascending id: %v", got)
		}
	}
}

func TestRemoveNodeOutsideTop3LeavesTop3Unchanged(t *testing.T) {
	ids := nodes(6)
	for _, key := range keys(10_000) {
		before := Rank(key, ids)
		removed := before[len(before)-1] // lowest ranked, never in top-3
		without := slices.DeleteFunc(slices.Clone(ids), func(id string) bool { return id == removed })
		after := Rank(key, without)
		if !slices.Equal(before[:3], after[:3]) {
			t.Fatalf("key %q: removing %s changed top-3 %v -> %v", key, removed, before[:3], after[:3])
		}
	}
}

func TestRemoveTop3NodeChangesExactlyOneSlot(t *testing.T) {
	ids := nodes(6)
	for i, key := range keys(10_000) {
		before := Rank(key, ids)
		removed := before[i%3]
		without := slices.DeleteFunc(slices.Clone(ids), func(id string) bool { return id == removed })
		after := Rank(key, without)
		// Survivors keep their order and the old 4th moves up: the new
		// top-3 is the old top-4 with the removed node elided.
		want := slices.DeleteFunc(slices.Clone(before[:4]), func(id string) bool { return id == removed })
		if !slices.Equal(after[:3], want) {
			t.Fatalf("key %q: removing %s gave top-3 %v, want %v", key, removed, after[:3], want)
		}
		if slices.Contains(after, removed) {
			t.Fatalf("key %q: removed node %s still ranked", key, removed)
		}
	}
}

func TestDistributionWithin20Percent(t *testing.T) {
	for _, ids := range [][]string{
		nodes(4),
		{"storage-a.local:9001", "storage-b.local:9001", "storage-c.local:9001", "storage-d.local:9001"},
		{"3f2504e0-4f89-11d3-9a0c-0305e82c3301", "b7e23ec2-9c8e-4a6d-8a2f-2a1e0f6c9d11", "9a1c3d2e-7b4f-4c5d-9e6f-1a2b3c4d5e6f", "c0ffee00-1234-5678-9abc-def012345678"},
	} {
		checkDistribution(t, ids, keys(10_000))
	}
}

// checkDistribution requires each node's top-1 and top-3 share over ks to
// be within ±20% of uniform.
func checkDistribution(t *testing.T, ids, ks []string) {
	t.Helper()
	top1 := map[string]int{}
	top3 := map[string]int{}
	for _, key := range ks {
		targets, rest := Targets(key, ids, 3)
		if len(targets) != 3 || len(rest) != 1 {
			t.Fatalf("Targets returned %d/%d, want 3/1", len(targets), len(rest))
		}
		top1[targets[0]]++
		for _, id := range targets {
			top3[id]++
		}
	}
	check := func(name string, counts map[string]int, expected float64) {
		lo, hi := int(expected*0.8), int(expected*1.2)
		for _, id := range ids {
			if c := counts[id]; c < lo || c > hi {
				t.Errorf("%s: %s got %d keys, want within [%d,%d] (nodes %v)", name, id, c, lo, hi, ids)
			}
		}
	}
	check("top-1", top1, float64(len(ks))/4)
	check("top-3", top3, float64(len(ks))*3/4)
}

func TestTargetsClampsN(t *testing.T) {
	ids := nodes(3)
	ranked := Rank("k", ids)
	for _, n := range []int{-1, 0, 2, 3, 10} {
		targets, rest := Targets("k", ids, n)
		if want := max(0, min(n, 3)); len(targets) != want || len(rest) != 3-want {
			t.Fatalf("n=%d: got %d/%d, want %d/%d", n, len(targets), len(rest), want, 3-want)
		}
		if !slices.Equal(append(slices.Clone(targets), rest...), ranked) {
			t.Fatalf("n=%d: targets+rest %v %v != rank %v", n, targets, rest, ranked)
		}
	}
}

func TestRankEmpty(t *testing.T) {
	if got := Rank("k", nil); len(got) != 0 {
		t.Fatalf("Rank(nil)=%v, want empty", got)
	}
}

func TestRankAllocations(t *testing.T) {
	ids := nodes(16)
	allocs := testing.AllocsPerRun(100, func() { Rank("bucket/object", ids) })
	if allocs > 2 {
		t.Fatalf("Rank allocates %.0f times per call, want <= 2 (scored copy + output)", allocs)
	}
}

func benchmarkRank(b *testing.B, n int) {
	ids := nodes(n)
	ks := keys(1024)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Rank(ks[i%len(ks)], ids)
	}
}

func BenchmarkRank4(b *testing.B)  { benchmarkRank(b, 4) }
func BenchmarkRank16(b *testing.B) { benchmarkRank(b, 16) }
