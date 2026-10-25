// Package cache is a small replicated key-value cache layered on SWIM
// membership and the consistent-hash ring.
//
// Every key is stored on the ReplicationFactor nodes that the ring assigns
// to it. Writes and reads are sent to all replicas and complete once
// WriteQuorum / ReadQuorum of them answer; conflicts resolve
// last-writer-wins on a Lamport version. Reads repair stale replicas, and
// when membership changes each node hands keys to their new replicas, so
// the replication factor is restored after a failure.
//
// The cache is deliberately small: it is a demonstration of how a
// membership layer drives data placement, not a durable database (see the
// README's Limitations section).
package cache

import (
	"sort"
	"time"
)

// Version orders writes: a Lamport counter, with the coordinating node's name
// as a deterministic tie-breaker.
type Version struct {
	Counter uint64
	Node    string
}

// Less reports whether v is older than o.
func (v Version) Less(o Version) bool {
	if v.Counter != o.Counter {
		return v.Counter < o.Counter
	}
	return v.Node < o.Node
}

// IsZero reports whether v is the zero version (no write seen).
func (v Version) IsZero() bool { return v.Counter == 0 && v.Node == "" }

// Entry is one stored value, or a tombstone when Deleted is set.
type Entry struct {
	Value   []byte
	Version Version
	Deleted bool
	// Stored is when this replica accepted the entry (used to expire
	// tombstones). It is local metadata and is not replicated.
	Stored time.Time
}

// store is a replica's local table. It is not safe for concurrent use; the
// owning Node serialises access.
type store struct {
	data map[string]Entry
}

func newStore() *store { return &store{data: make(map[string]Entry)} }

// apply stores e if it is newer than what the replica holds. It reports
// whether the entry was accepted.
func (s *store) apply(key string, e Entry) bool {
	cur, ok := s.data[key]
	if ok && !cur.Version.Less(e.Version) {
		return false
	}
	s.data[key] = e
	return true
}

func (s *store) get(key string) (Entry, bool) {
	e, ok := s.data[key]
	return e, ok
}

func (s *store) remove(key string, v Version) {
	if cur, ok := s.data[key]; ok && cur.Version == v {
		delete(s.data, key)
	}
}

// keys returns all keys in sorted order (deterministic iteration).
func (s *store) keys() []string {
	out := make([]string, 0, len(s.data))
	for k := range s.data {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// expireTombstones drops tombstones stored before cutoff and returns how
// many were dropped.
func (s *store) expireTombstones(cutoff time.Time) int {
	n := 0
	for _, k := range s.keys() {
		if e := s.data[k]; e.Deleted && e.Stored.Before(cutoff) {
			delete(s.data, k)
			n++
		}
	}
	return n
}
