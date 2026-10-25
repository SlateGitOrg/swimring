package experiment

import (
	"fmt"
	"math"
	"math/rand/v2"

	"github.com/SlateGitOrg/swimring/ring"
)

// BalanceSpec configures the ring balance measurement.
type BalanceSpec struct {
	Nodes  []int
	VNodes []int
	// Sets is the number of independent node-name sets pooled per cell.
	Sets int
	// Keys is the number of random keys used to measure movement on add.
	Keys int
	Seed uint64
}

// BalanceRow is one (nodes, vnodes) cell.
type BalanceRow struct {
	Nodes, VNodes int
	// RelStd is the pooled relative standard deviation of node shares of
	// the hash circle; Theory is its Dirichlet/Beta expectation.
	RelStd, Theory float64
	// MaxOverMean is the worst max-share/mean-share over all sets.
	MaxOverMean float64
	// MovedOnAdd is the fraction of keys whose owner changed when one node
	// was added (averaged over sets).
	MovedOnAdd float64
}

// RunBalance measures load balance and key movement of the ring.
func RunBalance(spec BalanceSpec) ([]BalanceRow, error) {
	if spec.Sets < 1 || spec.Keys < 1 {
		return nil, fmt.Errorf("experiment: invalid balance spec %+v", spec)
	}
	rng := rand.New(rand.NewPCG(spec.Seed, 0xBA1A))
	keys := make([]string, spec.Keys)
	for i := range keys {
		keys[i] = fmt.Sprintf("key:%016x", rng.Uint64())
	}
	var rows []BalanceRow
	for _, n := range spec.Nodes {
		for _, v := range spec.VNodes {
			row := BalanceRow{Nodes: n, VNodes: v}
			m, vf := float64(n*v), float64(v)
			row.Theory = math.Sqrt((m - vf) / (vf * (m + 1)))
			var sq float64
			samples := 0
			for set := range spec.Sets {
				names := make([]string, n)
				for i := range names {
					names[i] = fmt.Sprintf("s%d-node-%04d", set, i)
				}
				r, err := ring.New(v, nil)
				if err != nil {
					return nil, err
				}
				if r, err = r.Add(names...); err != nil {
					return nil, err
				}
				for _, share := range r.Ownership() {
					d := share*float64(n) - 1
					sq += d * d
					samples++
					row.MaxOverMean = math.Max(row.MaxOverMean, share*float64(n))
				}
				grown, err := r.Add(fmt.Sprintf("s%d-extra", set))
				if err != nil {
					return nil, err
				}
				moved := 0
				for _, k := range keys {
					if r.Lookup(k) != grown.Lookup(k) {
						moved++
					}
				}
				row.MovedOnAdd += float64(moved) / float64(len(keys)) / float64(spec.Sets)
			}
			row.RelStd = math.Sqrt(sq / float64(samples))
			rows = append(rows, row)
		}
	}
	return rows, nil
}
