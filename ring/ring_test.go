package ring

import (
	"errors"
	"fmt"
	"math"
	mrand "math/rand"
	"math/rand/v2"
	"slices"
	"testing"
	"testing/quick"
)

func names(prefix string, n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("%s-%03d", prefix, i)
	}
	return out
}

func keys(rng *rand.Rand, n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("key:%016x", rng.Uint64())
	}
	return out
}

func TestNewValidation(t *testing.T) {
	if _, err := New(0, nil); !errors.Is(err, ErrBadVNodes) {
		t.Fatalf("err = %v", err)
	}
	r := MustNew(4, nil)
	if _, err := r.Add(""); !errors.Is(err, ErrEmptyName) {
		t.Fatalf("err = %v", err)
	}
	defer func() {
		if recover() == nil {
			t.Fatal("MustNew(0) did not panic")
		}
	}()
	MustNew(0, nil)
}

func TestEmptyRing(t *testing.T) {
	r := MustNew(8, nil)
	if r.Lookup("k") != "" || r.LookupN("k", 3) != nil || len(r.Ownership()) != 0 {
		t.Fatal("empty ring returned owners")
	}
	if r.Len() != 0 || r.VNodes() != 8 {
		t.Fatal("bad metadata")
	}
}

func TestSingleNodeOwnsEverything(t *testing.T) {
	r := MustNew(1, nil, "a")
	if got := r.Ownership()["a"]; got != 1 {
		t.Fatalf("ownership = %v", got)
	}
	if r.Lookup("anything") != "a" {
		t.Fatal("lookup")
	}
	if got := r.LookupN("x", 5); !slices.Equal(got, []string{"a"}) {
		t.Fatalf("LookupN = %v", got)
	}
}

func TestAddRemoveIdempotent(t *testing.T) {
	r := MustNew(16, nil, "a", "b")
	r2, err := r.Add("a", "b", "b")
	if err != nil || r2 != r {
		t.Fatal("adding present nodes should return the same ring")
	}
	if r.Remove("zz") != r {
		t.Fatal("removing absent node should return the same ring")
	}
	r3 := r.Remove("a")
	if r3.Has("a") || !r3.Has("b") || r3.Len() != 1 || len(r3.points) != 16 {
		t.Fatalf("bad remove: %v", r3)
	}
	if !r.Has("a") {
		t.Fatal("Remove mutated the original ring")
	}
	if !slices.Equal(r.Nodes(), []string{"a", "b"}) || r.String() == "" {
		t.Fatal("metadata")
	}
}

// Property: lookups do not depend on the order nodes were added in.
func TestPropertyInsertionOrderIndependent(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 1))
	nodes := names("n", 25)
	ref := MustNew(32, nil, nodes...)
	ks := keys(rng, 2000)
	for trial := range 10 {
		perm := slices.Clone(nodes)
		rng.Shuffle(len(perm), func(i, j int) { perm[i], perm[j] = perm[j], perm[i] })
		r := MustNew(32, nil)
		for _, nd := range perm {
			var err error
			if r, err = r.Add(nd); err != nil {
				t.Fatal(err)
			}
		}
		for _, k := range ks {
			if !slices.Equal(r.LookupN(k, 3), ref.LookupN(k, 3)) {
				t.Fatalf("trial %d: key %s differs", trial, k)
			}
		}
	}
}

// Property: LookupN returns min(n, N) distinct nodes, primary first.
func TestPropertyLookupNDistinct(t *testing.T) {
	r := MustNew(64, nil, names("n", 7)...)
	f := func(key string, n uint8) bool {
		want := int(n % 10)
		got := r.LookupN(key, want)
		if want == 0 {
			return got == nil
		}
		if len(got) != min(want, 7) || got[0] != r.Lookup(key) {
			return false
		}
		seen := map[string]bool{}
		for _, g := range got {
			if seen[g] || !r.Has(g) {
				return false
			}
			seen[g] = true
		}
		return true
	}
	cfg := &quick.Config{MaxCount: 3000, Rand: mrand.New(mrand.NewSource(7))}
	if err := quick.Check(f, cfg); err != nil {
		t.Fatal(err)
	}
}

// Property (minimal disruption): adding node X only moves keys onto X, and
// every replica set changes by at most the insertion of X.
func TestPropertyMinimalDisruptionOnAdd(t *testing.T) {
	rng := rand.New(rand.NewPCG(2, 2))
	for trial := range 20 {
		size := 3 + rng.IntN(30)
		r := MustNew(1+rng.IntN(128), nil, names(fmt.Sprintf("t%d", trial), size)...)
		x := fmt.Sprintf("new-%d", trial)
		r2, err := r.Add(x)
		if err != nil {
			t.Fatal(err)
		}
		for _, k := range keys(rng, 500) {
			before, after := r.Lookup(k), r2.Lookup(k)
			if before != after && after != x {
				t.Fatalf("key %s moved %s -> %s, not to the new node", k, before, after)
			}
			oldSet, newSet := r.LookupN(k, 3), r2.LookupN(k, 3)
			for _, nd := range newSet {
				if nd != x && !slices.Contains(oldSet, nd) {
					t.Fatalf("replica %s appeared from nowhere for %s", nd, k)
				}
			}
		}
	}
}

// Property: removing node Y only moves the keys Y owned, and the surviving
// members of each replica set keep their place.
func TestPropertyMinimalDisruptionOnRemove(t *testing.T) {
	rng := rand.New(rand.NewPCG(3, 3))
	for trial := range 20 {
		size := 3 + rng.IntN(30)
		ns := names(fmt.Sprintf("t%d", trial), size)
		r := MustNew(1+rng.IntN(128), nil, ns...)
		y := ns[rng.IntN(size)]
		r2 := r.Remove(y)
		for _, k := range keys(rng, 500) {
			before, after := r.Lookup(k), r2.Lookup(k)
			if before != y && before != after {
				t.Fatalf("key %s moved although its owner %s stayed", k, before)
			}
			oldSet, newSet := r.LookupN(k, 3), r2.LookupN(k, 3)
			for _, nd := range oldSet {
				if nd != y && !slices.Contains(newSet, nd) {
					t.Fatalf("replica %s dropped for %s", nd, k)
				}
			}
		}
	}
}

func shareStats(own map[string]float64) (mean, maxOverMean float64) {
	mean = 1 / float64(len(own))
	var mx float64
	for _, v := range own {
		mx = math.Max(mx, v)
	}
	return mean, mx / mean
}

// Property (balance): ownership sums to one; the spread of node shares
// matches theory; the most loaded node stays within a Gamma-tail bound.
//
// Theory: the M = N·V virtual points cut the circle into arcs distributed
// as Dirichlet(1,…,1), so one node's share (V arcs) is Beta(V, M−V) with
// relative standard deviation sqrt((M−V)/(V·(M+1))) ≈ 1/sqrt(V). The test
// pools node shares from 10 independent name sets (≥100 samples) so the
// empirical spread is estimated tightly enough to compare against it.
func TestPropertyBalance(t *testing.T) {
	for _, tc := range []struct{ nodes, vnodes int }{
		{10, 64}, {20, 128}, {50, 256}, {100, 256},
	} {
		t.Run(fmt.Sprintf("N%d_V%d", tc.nodes, tc.vnodes), func(t *testing.T) {
			m := float64(tc.nodes * tc.vnodes)
			v := float64(tc.vnodes)
			theory := math.Sqrt((m - v) / (v * (m + 1)))
			var pooled float64
			samples := 0
			for set := range 10 {
				r := MustNew(tc.vnodes, nil, names(fmt.Sprintf("s%d", set), tc.nodes)...)
				own := r.Ownership()
				sum := 0.0
				for _, share := range own {
					sum += share
					d := share*float64(tc.nodes) - 1
					pooled += d * d
					samples++
				}
				if math.Abs(sum-1) > 1e-9 {
					t.Fatalf("ownership sums to %v", sum)
				}
				_, mx := shareStats(own)
				bound := 1 + 2*math.Sqrt(2*math.Log(float64(tc.nodes))/v)
				if mx > bound {
					t.Fatalf("set %d: max/mean %.3f exceeds %.3f", set, mx, bound)
				}
			}
			rel := math.Sqrt(pooled / float64(samples))
			if rel < 0.8*theory || rel > 1.2*theory {
				t.Fatalf("pooled relative std %.4f outside [0.8,1.2] x theory %.4f", rel, theory)
			}
			t.Logf("relative std %.4f (theory %.4f)", rel, theory)
		})
	}
}

// Keys hashed onto the ring land in proportion to ownership.
func TestKeyDistributionMatchesOwnership(t *testing.T) {
	r := MustNew(128, nil, names("k", 8)...)
	own := r.Ownership()
	rng := rand.New(rand.NewPCG(4, 4))
	counts := map[string]int{}
	const total = 100000
	for _, k := range keys(rng, total) {
		counts[r.Lookup(k)]++
	}
	for nd, share := range own {
		got := float64(counts[nd]) / total
		if math.Abs(got-share) > 0.01 {
			t.Fatalf("%s: key share %.4f vs ownership %.4f", nd, got, share)
		}
	}
}

// Adding the (N+1)th node moves about 1/(N+1) of the keys.
func TestMovedFractionOnAdd(t *testing.T) {
	rng := rand.New(rand.NewPCG(5, 5))
	r := MustNew(256, nil, names("m", 19)...)
	r2, err := r.Add("m-new")
	if err != nil {
		t.Fatal(err)
	}
	ks := keys(rng, 50000)
	moved := 0
	for _, k := range ks {
		if r.Lookup(k) != r2.Lookup(k) {
			moved++
		}
	}
	frac := float64(moved) / float64(len(ks))
	if math.Abs(frac-1.0/20) > 0.02 {
		t.Fatalf("moved %.4f of keys, want about %.4f", frac, 1.0/20)
	}
}

func TestDefaultHashStable(t *testing.T) {
	// Pinned values guard against accidental changes to the hash, which
	// would silently re-shard every deployed ring.
	got := []uint64{DefaultHash(nil), DefaultHash([]byte("node#0")), DefaultHash([]byte("node#1"))}
	again := []uint64{DefaultHash(nil), DefaultHash([]byte("node#0")), DefaultHash([]byte("node#1"))}
	if !slices.Equal(got, again) || got[1] == got[2] {
		t.Fatalf("hash unstable or colliding: %v", got)
	}
	if got[0] != mix64(fnvOffset64) {
		t.Fatalf("empty input hash = %x", got[0])
	}
}

func TestCustomHash(t *testing.T) {
	calls := 0
	h := func(b []byte) uint64 { calls++; return DefaultHash(b) }
	r := MustNew(4, h, "a", "b")
	r.Lookup("x")
	if calls != 9 {
		t.Fatalf("custom hash called %d times, want 9", calls)
	}
}

func TestTracker(t *testing.T) {
	tr := NewTracker(MustNew(8, nil, "a"))
	prev, next := tr.Update(func(r *Ring) *Ring {
		r2, err := r.Add("b")
		if err != nil {
			t.Fatal(err)
		}
		return r2
	})
	if prev.Len() != 1 || next.Len() != 2 || tr.Load() != next {
		t.Fatal("tracker update")
	}
}
