package q1

import (
	"fmt"
	"testing"
)

func TestRingDistribution(t *testing.T) {
	r := newRing(200)
	for _, name := range []string{"a", "b", "c"} {
		r.Add(name, 1)
	}

	const keys = 12000
	counts := make(map[string]int, 3)
	for i := 0; i < keys; i++ {
		candidates := r.Candidates(fmt.Sprintf("key:%d", i), 1)
		if len(candidates) != 1 {
			t.Fatalf("expected one candidate, got %v", candidates)
		}
		counts[candidates[0]]++
	}

	for _, name := range []string{"a", "b", "c"} {
		ratio := float64(counts[name]) / float64(keys)
		if ratio < 0.22 || ratio > 0.45 {
			t.Errorf("backend %s received %.2f%% of keys, want roughly one third (counts=%v)", name, ratio*100, counts)
		}
	}
}

func TestRingCandidatesAreDistinctAndOrdered(t *testing.T) {
	r := newRing(100)
	for _, name := range []string{"a", "b", "c"} {
		r.Add(name, 1)
	}

	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key:%d", i)
		one := r.Candidates(key, 1)
		three := r.Candidates(key, 3)
		if len(one) != 1 || len(three) != 3 {
			t.Fatalf("unexpected candidate counts: one=%v three=%v", one, three)
		}
		if one[0] != three[0] {
			t.Fatalf("primary mismatch for %s: %v vs %v", key, one, three)
		}
		seen := map[string]bool{}
		for _, name := range three {
			if seen[name] {
				t.Fatalf("duplicate candidate in %v", three)
			}
			seen[name] = true
		}
	}

	if got := len(r.Candidates("key", 10)); got != 3 {
		t.Fatalf("expected all 3 distinct backends, got %d", got)
	}
}

func TestRingMinimalMovement(t *testing.T) {
	r := newRing(200)
	for _, name := range []string{"a", "b", "c"} {
		r.Add(name, 1)
	}

	const keys = 5000
	before := make(map[string]string, keys)
	for i := 0; i < keys; i++ {
		key := fmt.Sprintf("key:%d", i)
		before[key] = r.Candidates(key, 1)[0]
	}

	r.Remove("b")
	moved := 0
	for key, old := range before {
		if r.Candidates(key, 1)[0] != old {
			moved++
		}
	}
	ratio := float64(moved) / float64(keys)
	if ratio > 0.50 {
		t.Fatalf("removing one of three backends moved %.1f%% of keys; expected roughly 33%%", ratio*100)
	}
}

func TestRingAddsBackendWithMinimalMovement(t *testing.T) {
	r := newRing(200)
	for _, name := range []string{"a", "b", "c"} {
		r.Add(name, 1)
	}

	const keys = 5000
	before := make(map[string]string, keys)
	for i := 0; i < keys; i++ {
		key := fmt.Sprintf("key:%d", i)
		before[key] = r.Candidates(key, 1)[0]
	}

	r.Add("d", 1)
	moved := 0
	for key, old := range before {
		if r.Candidates(key, 1)[0] != old {
			moved++
		}
	}
	ratio := float64(moved) / float64(keys)
	if ratio > 0.40 {
		t.Fatalf("adding one of four backends moved %.1f%% of keys; expected roughly 25%%", ratio*100)
	}
}

func TestRingEmpty(t *testing.T) {
	r := newRing(10)
	if got := r.Candidates("key", 1); got != nil {
		t.Fatalf("expected nil candidates for empty ring, got %v", got)
	}
}
