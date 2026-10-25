package experiment

import (
	"fmt"

	"github.com/archit2501/swimring/swim"
)

// FalsePositiveSpec configures the healthy-cluster loss experiment.
type FalsePositiveSpec struct {
	Sizes     []int
	Losses    []float64
	Periods   int
	Seed      uint64
	Lifeguard swim.Lifeguard
}

// FalsePositiveRow reports one (size, loss) cell. No member fails, so every
// failed probe, suspicion and death is a false positive.
type FalsePositiveRow struct {
	Size        int
	Loss        float64
	NodePeriods int
	Probes      int
	Failures    int
	// Measured is Failures/Probes; Expected is ProbeFailureProbability;
	// StdErr is the binomial standard error of Measured.
	Measured, Expected, StdErr float64
	// Suspicions counts suspicions raised by a node's own failed probe.
	Suspicions int
	// Refutations counts members refuting suspicions about themselves.
	Refutations int
	// FalseDeaths counts (observer, member) death declarations.
	FalseDeaths int
	// MembersLost counts distinct healthy members declared dead by anyone.
	MembersLost int
}

// RunFalsePositives runs healthy clusters under independent message loss
// and compares the per-probe failure rate with the paper's model.
func RunFalsePositives(spec FalsePositiveSpec) ([]FalsePositiveRow, error) {
	if spec.Periods < 1 {
		return nil, fmt.Errorf("experiment: periods %d", spec.Periods)
	}
	var rows []FalsePositiveRow
	for _, size := range spec.Sizes {
		for li, loss := range spec.Losses {
			c, err := NewCluster(ClusterSpec{
				Size: size, Seed: mixSeed(spec.Seed, uint64(size), uint64(li)), Link: baseLink(loss),
				Configure: func(cfg *swim.Config) { cfg.Lifeguard = spec.Lifeguard },
			})
			if err != nil {
				return nil, err
			}
			before := make([]swim.Stats, size)
			for i, n := range c.Nodes {
				before[i] = n.Stats()
			}
			c.Sim.RunFor(c.ProbeInterval() * timeMult(spec.Periods))
			k := min(c.Nodes[0].Config().IndirectChecks, size-2)
			row := FalsePositiveRow{
				Size: size, Loss: loss, NodePeriods: size * spec.Periods,
				Expected: ProbeFailureProbability(loss, k),
			}
			for i, n := range c.Nodes {
				st := n.Stats()
				row.Probes += int(st.Probes - before[i].Probes)
				row.Failures += int(st.ProbeFailures - before[i].ProbeFailures)
				row.Suspicions += int(st.SuspicionsRaised - before[i].SuspicionsRaised)
				row.Refutations += int(st.Refutations - before[i].Refutations)
			}
			lost := map[string]bool{}
			for _, o := range c.Events {
				if o.Event.Type == swim.EventLeave {
					row.FalseDeaths++
					lost[o.Event.Member.Name] = true
				}
			}
			row.MembersLost = len(lost)
			if row.Probes > 0 {
				row.Measured = float64(row.Failures) / float64(row.Probes)
			}
			row.StdErr = BinomialStdErr(row.Expected, row.Probes)
			rows = append(rows, row)
		}
	}
	return rows, nil
}
