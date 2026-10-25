package swim

import (
	"math"
	"sort"
)

// broadcastQueue holds membership updates waiting to be piggybacked.
//
// At most one update per member is queued: a newer assertion about a member
// replaces the older one, because the merge rules make the newer one
// dominate anyway. Packets take the least-transmitted updates first (ties go
// to the most recently queued), which is the "infection-style" dissemination
// of SWIM section 4.1: fresh news spreads first, and every update is retired
// after λ·log(n) transmissions.
type broadcastQueue struct {
	items  []*broadcast
	byName map[string]*broadcast
	nextID uint64
}

type broadcast struct {
	upd       update
	transmits int
	id        uint64
}

func newBroadcastQueue() *broadcastQueue {
	return &broadcastQueue{byName: make(map[string]*broadcast)}
}

// Len returns the number of queued updates.
func (q *broadcastQueue) Len() int { return len(q.items) }

// Queue enqueues u, replacing any pending update about the same member.
func (q *broadcastQueue) Queue(u update) {
	if old, ok := q.byName[u.Name]; ok {
		q.remove(old)
	}
	q.nextID++
	b := &broadcast{upd: u, id: q.nextID}
	q.items = append(q.items, b)
	q.byName[u.Name] = b
}

// Pending returns the queued update about name, if any.
func (q *broadcastQueue) Pending(name string) (update, bool) {
	b, ok := q.byName[name]
	if !ok {
		return update{}, false
	}
	return b.upd, true
}

func (q *broadcastQueue) remove(b *broadcast) {
	for i, it := range q.items {
		if it == b {
			q.items = append(q.items[:i], q.items[i+1:]...)
			break
		}
	}
	delete(q.byName, b.upd.Name)
}

// Take selects updates whose total encoded size fits in budget bytes,
// counting each selection as one transmission and retiring updates that
// reach limit transmissions. skip names a member whose update the caller
// already included (buddy system) and must not be duplicated.
//
// Cost is O(q log q) for q queued updates; q is bounded by cluster size.
func (q *broadcastQueue) Take(budget, limit int, skip string) []update {
	if len(q.items) == 0 || budget <= 0 {
		return nil
	}
	sort.SliceStable(q.items, func(i, j int) bool {
		a, b := q.items[i], q.items[j]
		if a.transmits != b.transmits {
			return a.transmits < b.transmits
		}
		return a.id > b.id
	})
	var out []update
	kept := q.items[:0]
	for _, b := range q.items {
		if b.upd.Name == skip {
			kept = append(kept, b)
			continue
		}
		sz := b.upd.encodedLen()
		if sz > budget {
			kept = append(kept, b)
			continue
		}
		budget -= sz
		out = append(out, b.upd)
		b.transmits++
		if b.transmits >= limit {
			delete(q.byName, b.upd.Name)
			continue
		}
		kept = append(kept, b)
	}
	for i := len(kept); i < len(q.items); i++ {
		q.items[i] = nil
	}
	q.items = kept
	return out
}

// retransmitLimit returns λ·ceil(log10(n+1)), the number of times each
// update is piggybacked in a cluster of n members.
func retransmitLimit(mult, n int) int {
	if n < 1 {
		n = 1
	}
	scale := int(math.Ceil(math.Log10(float64(n + 1))))
	if scale < 1 {
		scale = 1
	}
	return mult * scale
}
