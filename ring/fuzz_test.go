package ring

import (
	"fmt"
	"slices"
	"testing"
)

// FuzzLookupN checks LookupN invariants for arbitrary keys and counts.
func FuzzLookupN(f *testing.F) {
	r := MustNew(16, nil, "a", "b", "c", "d", "e")
	f.Add("", 0)
	f.Add("hello", 3)
	f.Add("\x00\xff", 9)
	f.Fuzz(func(t *testing.T, key string, n int) {
		got := r.LookupN(key, n)
		want := min(max(n, 0), r.Len())
		if len(got) != want {
			t.Fatalf("LookupN(%q,%d) returned %d nodes", key, n, len(got))
		}
		if want > 0 && got[0] != r.Lookup(key) {
			t.Fatal("primary mismatch")
		}
		seen := map[string]bool{}
		for _, g := range got {
			if seen[g] {
				t.Fatalf("duplicate %s", g)
			}
			seen[g] = true
		}
	})
}

// FuzzRingOps drives arbitrary add/remove sequences and checks structural
// invariants: points sorted, P = V * N, every point owned by a member.
func FuzzRingOps(f *testing.F) {
	f.Add([]byte{0, 1, 2, 130, 3, 129})
	f.Add([]byte{255, 255, 0, 0})
	f.Fuzz(func(t *testing.T, ops []byte) {
		r := MustNew(5, nil)
		for _, op := range ops {
			name := fmt.Sprintf("n%d", op&0x0f)
			if op&0x80 == 0 {
				var err error
				if r, err = r.Add(name); err != nil {
					t.Fatal(err)
				}
			} else {
				r = r.Remove(name)
			}
		}
		if len(r.points) != r.vnodes*r.Len() {
			t.Fatalf("%d points for %d nodes", len(r.points), r.Len())
		}
		if !slices.IsSortedFunc(r.points, func(a, b point) int {
			switch {
			case pointLess(a, b):
				return -1
			case pointLess(b, a):
				return 1
			}
			return 0
		}) {
			t.Fatal("points not sorted")
		}
		for _, p := range r.points {
			if !r.Has(p.owner) {
				t.Fatalf("orphan point owned by %s", p.owner)
			}
		}
	})
}
