package experiment

import (
	"fmt"
	"time"

	"github.com/SlateGitOrg/swimring/swim"
)

// Variant names a protocol configuration under test.
type Variant struct {
	Name      string
	Lifeguard swim.Lifeguard
}

// StandardVariants compares plain SWIM, each Lifeguard component alone,
// and full Lifeguard.
func StandardVariants() []Variant {
	return []Variant{
		{Name: "SWIM (Lifeguard off)", Lifeguard: swim.LifeguardOff()},
		{Name: "+ local health (LHM + nack)", Lifeguard: swim.Lifeguard{LocalHealth: true, Nack: true}},
		{Name: "+ dynamic suspicion + buddy", Lifeguard: swim.Lifeguard{DynamicSuspicion: true, Buddy: true}},
		{Name: "Lifeguard (all)", Lifeguard: swim.LifeguardAll()},
	}
}

// LifeguardSpec configures the slow-member experiment: Slow members process
// inbound packets Lag late while their timers stay on time.
type LifeguardSpec struct {
	Size     int
	Slow     int
	Lag      time.Duration
	Loss     float64
	Periods  int
	Runs     int
	Seed     uint64
	Variants []Variant
}

// LifeguardRow aggregates the runs of one variant.
type LifeguardRow struct {
	Variant string
	Runs    int
	// HealthyAccusations: suspicions of healthy members raised by a
	// member's own failed probe (the false positives Lifeguard targets).
	HealthyAccusations int
	// AccusationsBySlow: the subset of HealthyAccusations raised by slow
	// members.
	AccusationsBySlow int
	// HealthyDeaths: (observer, member) death declarations of healthy
	// members. HealthyLost: distinct healthy members declared dead.
	HealthyDeaths int
	HealthyLost   int
	// SlowAccused: suspicions of slow members raised by probes;
	// SlowDeaths: (observer, member) death declarations of slow members.
	SlowAccused int
	SlowDeaths  int
	// MaxHealthSlow is the highest Local Health score a slow member reached.
	MaxHealthSlow int
}

// RunLifeguard measures false positives caused by slow members for each
// variant, over Runs seeded runs.
func RunLifeguard(spec LifeguardSpec) ([]LifeguardRow, error) {
	if spec.Slow >= spec.Size || spec.Periods < 1 || spec.Runs < 1 {
		return nil, fmt.Errorf("experiment: invalid lifeguard spec %+v", spec)
	}
	var rows []LifeguardRow
	for _, v := range spec.Variants {
		row := LifeguardRow{Variant: v.Name, Runs: spec.Runs}
		for run := range spec.Runs {
			if err := lifeguardRun(spec, v, mixSeed(spec.Seed, uint64(run)), &row); err != nil {
				return nil, err
			}
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func lifeguardRun(spec LifeguardSpec, v Variant, seed uint64, row *LifeguardRow) error {
	c, err := NewCluster(ClusterSpec{
		Size: spec.Size, Seed: seed, Link: baseLink(spec.Loss),
		Configure: func(cfg *swim.Config) { cfg.Lifeguard = v.Lifeguard },
	})
	if err != nil {
		return err
	}
	slow := map[string]bool{}
	for i := 1; i <= spec.Slow; i++ {
		c.Hosts[i].SetLag(spec.Lag)
		slow[c.Nodes[i].Name()] = true
	}
	c.Sim.RunFor(c.ProbeInterval() * timeMult(spec.Periods))
	for _, t := range c.Traces {
		if slow[t.Observer] && t.Trace.Health > row.MaxHealthSlow {
			row.MaxHealthSlow = t.Trace.Health
		}
	}
	lost := map[string]bool{}
	for _, o := range c.Events {
		target := o.Event.Member.Name
		raisedHere := o.Event.Source == o.Observer
		switch {
		case o.Event.Type == swim.EventUpdate && o.Event.Member.State == swim.StateSuspect && raisedHere:
			if slow[target] {
				row.SlowAccused++
				continue
			}
			row.HealthyAccusations++
			if slow[o.Observer] {
				row.AccusationsBySlow++
			}
		case o.Event.Type == swim.EventLeave:
			if slow[target] {
				row.SlowDeaths++
				continue
			}
			row.HealthyDeaths++
			lost[target] = true
		}
	}
	row.HealthyLost += len(lost)
	return nil
}

func timeMult(n int) time.Duration { return time.Duration(n) }
