package ring

import "sync/atomic"

const (
	fnvOffset64 = 14695981039346656037
	fnvPrime64  = 1099511628211
)

// DefaultHash is FNV-1a 64 followed by the splitmix64 finaliser. FNV-1a
// alone has weak avalanche on short keys that differ only in their last
// bytes (exactly what "node#0", "node#1", ... look like), which skews
// virtual-node placement; the finaliser mixes every input bit into every
// output bit. The function is stable across processes and platforms, which
// a ring shared by a cluster requires.
func DefaultHash(b []byte) uint64 {
	h := uint64(fnvOffset64)
	for _, c := range b {
		h ^= uint64(c)
		h *= fnvPrime64
	}
	return mix64(h)
}

// mix64 is the splitmix64 / murmur3 fmix64-style finaliser.
func mix64(z uint64) uint64 {
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	return z ^ (z >> 31)
}

// Tracker holds the current Ring behind an atomic pointer so that many
// readers can look keys up while a membership listener swaps in new rings.
type Tracker struct {
	cur atomic.Pointer[Ring]
}

// NewTracker returns a tracker holding r.
func NewTracker(r *Ring) *Tracker {
	t := &Tracker{}
	t.cur.Store(r)
	return t
}

// Load returns the current ring.
func (t *Tracker) Load() *Ring { return t.cur.Load() }

// Update applies f to the current ring and stores the result, retrying on
// concurrent updates. It returns the old and new rings.
func (t *Tracker) Update(f func(*Ring) *Ring) (prev, next *Ring) {
	for {
		prev = t.cur.Load()
		next = f(prev)
		if t.cur.CompareAndSwap(prev, next) {
			return prev, next
		}
	}
}
