// Package ring implements a consistent-hash ring with virtual nodes.
//
// Each physical node is placed on a 64-bit hash circle at V pseudo-random
// points ("virtual nodes"). A key is owned by the first point clockwise from
// the key's hash; LookupN continues clockwise to collect N distinct physical
// nodes for replication. With V virtual nodes per node the standard
// deviation of a node's share of the key space is about 1/sqrt(V) of the mean
// share, and adding or removing one node moves only the keys of the arcs it
// gains or loses (≈1/N of all keys).
//
// A Ring is immutable: Add and Remove return a new Ring, so readers can share
// one without locks (see Tracker for an atomically swappable holder).
package ring

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
)

// HashFunc maps bytes to a point on the circle.
type HashFunc func([]byte) uint64

// Errors returned by ring construction.
var (
	ErrBadVNodes = errors.New("ring: virtual node count must be >= 1")
	ErrEmptyName = errors.New("ring: empty node name")
)

type point struct {
	hash  uint64
	owner string
}

// Ring is an immutable consistent-hash ring.
type Ring struct {
	vnodes int
	hash   HashFunc
	points []point   // sorted by (hash, owner)
	nodes  []string  // sorted physical node names
}

// New returns an empty ring with vnodes points per node and the default
// hash (FNV-1a 64 with a splitmix64 finaliser). A nil hash selects the
// default.
func New(vnodes int, hash HashFunc) (*Ring, error) {
	if vnodes < 1 {
		return nil, ErrBadVNodes
	}
	if hash == nil {
		hash = DefaultHash
	}
	return &Ring{vnodes: vnodes, hash: hash}, nil
}

// MustNew is New for static configuration; it panics on invalid vnodes.
func MustNew(vnodes int, hash HashFunc, nodes ...string) *Ring {
	r, err := New(vnodes, hash)
	if err != nil {
		panic(err)
	}
	r, err = r.Add(nodes...)
	if err != nil {
		panic(err)
	}
	return r
}

// VNodes returns the number of virtual nodes per physical node.
func (r *Ring) VNodes() int { return r.vnodes }

// Len returns the number of physical nodes.
func (r *Ring) Len() int { return len(r.nodes) }

// Nodes returns the physical nodes in sorted order.
func (r *Ring) Nodes() []string {
	out := make([]string, len(r.nodes))
	copy(out, r.nodes)
	return out
}

// Has reports whether node is on the ring.
func (r *Ring) Has(node string) bool {
	i := sort.SearchStrings(r.nodes, node)
	return i < len(r.nodes) && r.nodes[i] == node
}

func pointLess(a, b point) bool {
	if a.hash != b.hash {
		return a.hash < b.hash
	}
	return a.owner < b.owner
}

func (r *Ring) pointsFor(node string) []point {
	pts := make([]point, r.vnodes)
	buf := make([]byte, 0, len(node)+8)
	for i := range r.vnodes {
		buf = append(buf[:0], node...)
		buf = append(buf, '#')
		buf = strconv.AppendInt(buf, int64(i), 10)
		pts[i] = point{hash: r.hash(buf), owner: node}
	}
	sort.Slice(pts, func(a, b int) bool { return pointLess(pts[a], pts[b]) })
	return pts
}

// Add returns a ring that also contains nodes. Nodes already present are
// ignored. Cost: O((P+V·k) + V·k·log(V·k)) for P existing points and k new
// nodes (a sort of the new points plus a linear merge).
func (r *Ring) Add(nodes ...string) (*Ring, error) {
	var fresh []point
	names := append([]string(nil), r.nodes...)
	seen := make(map[string]bool, len(nodes))
	for _, nd := range nodes {
		if nd == "" {
			return nil, ErrEmptyName
		}
		if r.Has(nd) || seen[nd] {
			continue
		}
		seen[nd] = true
		names = append(names, nd)
		fresh = append(fresh, r.pointsFor(nd)...)
	}
	if len(fresh) == 0 {
		return r, nil
	}
	sort.Slice(fresh, func(a, b int) bool { return pointLess(fresh[a], fresh[b]) })
	sort.Strings(names)
	merged := make([]point, 0, len(r.points)+len(fresh))
	i, j := 0, 0
	for i < len(r.points) && j < len(fresh) {
		if pointLess(r.points[i], fresh[j]) {
			merged = append(merged, r.points[i])
			i++
		} else {
			merged = append(merged, fresh[j])
			j++
		}
	}
	merged = append(merged, r.points[i:]...)
	merged = append(merged, fresh[j:]...)
	return &Ring{vnodes: r.vnodes, hash: r.hash, points: merged, nodes: names}, nil
}

// Remove returns a ring without nodes. Absent nodes are ignored. Cost O(P).
func (r *Ring) Remove(nodes ...string) *Ring {
	drop := make(map[string]bool, len(nodes))
	for _, nd := range nodes {
		if r.Has(nd) {
			drop[nd] = true
		}
	}
	if len(drop) == 0 {
		return r
	}
	pts := make([]point, 0, len(r.points))
	for _, p := range r.points {
		if !drop[p.owner] {
			pts = append(pts, p)
		}
	}
	names := make([]string, 0, len(r.nodes)-len(drop))
	for _, nd := range r.nodes {
		if !drop[nd] {
			names = append(names, nd)
		}
	}
	return &Ring{vnodes: r.vnodes, hash: r.hash, points: pts, nodes: names}
}

// search returns the index of the first point at or clockwise after h.
func (r *Ring) search(h uint64) int {
	i := sort.Search(len(r.points), func(i int) bool { return r.points[i].hash >= h })
	if i == len(r.points) {
		return 0
	}
	return i
}

// Lookup returns the owner of key, or "" on an empty ring. O(log P).
func (r *Ring) Lookup(key string) string {
	if len(r.points) == 0 {
		return ""
	}
	return r.points[r.search(r.hash([]byte(key)))].owner
}

// LookupN returns up to n distinct nodes responsible for key, primary first,
// walking clockwise. Cost O(log P + W) where W is the number of points
// walked, which is O(n·V) in the worst case and close to n in practice.
func (r *Ring) LookupN(key string, n int) []string {
	if n <= 0 || len(r.points) == 0 {
		return nil
	}
	if n > len(r.nodes) {
		n = len(r.nodes)
	}
	out := make([]string, 0, n)
	start := r.search(r.hash([]byte(key)))
	for off := 0; off < len(r.points) && len(out) < n; off++ {
		owner := r.points[(start+off)%len(r.points)].owner
		dup := false
		for _, o := range out {
			if o == owner {
				dup = true
				break
			}
		}
		if !dup {
			out = append(out, owner)
		}
	}
	return out
}

// Ownership returns each node's fraction of the hash circle (as primary
// owner). The fractions sum to 1 on a non-empty ring.
func (r *Ring) Ownership() map[string]float64 {
	out := make(map[string]float64, len(r.nodes))
	if len(r.points) == 0 {
		return out
	}
	if len(r.points) == 1 {
		out[r.points[0].owner] = 1
		return out
	}
	const full = float64(1<<63) * 2
	for i, p := range r.points {
		prev := r.points[(i+len(r.points)-1)%len(r.points)].hash
		// Arc (prev, p.hash] belongs to p.owner; uint64 wraparound handles
		// the arc that crosses zero.
		out[p.owner] += float64(p.hash-prev) / full
	}
	return out
}

// String summarises the ring.
func (r *Ring) String() string {
	return fmt.Sprintf("ring(%d nodes, %d vnodes each)", len(r.nodes), r.vnodes)
}
