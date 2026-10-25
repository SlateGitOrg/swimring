package experiment

import (
	"fmt"
	"math"
	"time"

	"github.com/archit2501/swimring/sim"
	"github.com/archit2501/swimring/swim"
)

// DetectionSpec configures the crash-detection experiment.
type DetectionSpec struct {
	Sizes     []int
	Losses    []float64
	Trials    int
	Seed      uint64
	Lifeguard swim.Lifeguard
	// Limit caps the virtual time a trial may take after the crash.
	Limit time.Duration
}

// DetectionRow aggregates the trials for one (size, loss) cell. Latencies
// are in protocol periods T measured from the crash.
type DetectionRow struct {
	Size   int
	Loss   float64
	Trials int
	// Detected counts trials in which every survivor declared the victim
	// dead within Limit.
	Detected int
	// FirstPing is the period index (ceil(t/T)) of the first ping any
	// survivor sent to the victim: the quantity the SWIM paper analyses.
	FirstPing Summary
	// PaperPeriods and RoundRobinPeriods are the analytical expectations
	// of FirstPing for random and round-robin target selection.
	PaperPeriods      float64
	RoundRobinPeriods float64
	// FirstSuspect, FirstDead, AllDead are end-to-end latencies.
	FirstSuspect Summary
	FirstDead    Summary
	AllDead      Summary
	// Dissemination is AllDead - FirstDead: how long the death took to
	// reach every survivor by piggybacked gossip.
	Dissemination Summary
	// FalseDeaths counts death declarations of healthy members.
	FalseDeaths int
}

// RunDetection crashes one random member per trial and records when it is
// first pinged, first suspected, first declared dead and dead everywhere.
func RunDetection(spec DetectionSpec) ([]DetectionRow, error) {
	if spec.Trials < 1 {
		return nil, fmt.Errorf("experiment: trials %d", spec.Trials)
	}
	limit := spec.Limit
	if limit == 0 {
		limit = 5 * time.Minute
	}
	var rows []DetectionRow
	for _, size := range spec.Sizes {
		if size < 3 {
			return nil, fmt.Errorf("experiment: detection needs >= 3 nodes, got %d", size)
		}
		for li, loss := range spec.Losses {
			row := DetectionRow{
				Size: size, Loss: loss, Trials: spec.Trials,
				PaperPeriods:      PaperFirstDetectionPeriods(size),
				RoundRobinPeriods: RoundRobinFirstDetectionPeriods(size),
			}
			var ping, suspect, dead, all, dissem []float64
			for trial := range spec.Trials {
				res, err := detectionTrial(spec, size, loss, mixSeed(spec.Seed, uint64(size), uint64(li), uint64(trial)), limit)
				if err != nil {
					return nil, err
				}
				row.FalseDeaths += res.falseDeaths
				if !res.complete {
					continue
				}
				row.Detected++
				ping = append(ping, res.firstPing)
				suspect = append(suspect, res.firstSuspect)
				dead = append(dead, res.firstDead)
				all = append(all, res.allDead)
				dissem = append(dissem, res.allDead-res.firstDead)
			}
			row.FirstPing = Summarize(ping)
			row.FirstSuspect = Summarize(suspect)
			row.FirstDead = Summarize(dead)
			row.AllDead = Summarize(all)
			row.Dissemination = Summarize(dissem)
			rows = append(rows, row)
		}
	}
	return rows, nil
}

type trialResult struct {
	complete                                    bool
	firstPing, firstSuspect, firstDead, allDead float64
	falseDeaths                                 int
}

func detectionTrial(spec DetectionSpec, size int, loss float64, seed uint64, limit time.Duration) (trialResult, error) {
	c, err := NewCluster(ClusterSpec{
		Size: size, Seed: seed, Link: baseLink(loss),
		Configure: func(cfg *swim.Config) { cfg.Lifeguard = spec.Lifeguard },
	})
	if err != nil {
		return trialResult{}, err
	}
	period := c.ProbeInterval()
	rng := c.Sim.Rand()
	// Warm up, then crash at a uniformly random phase of the period.
	c.Sim.RunFor(3*period + time.Duration(rng.Float64()*float64(period)))
	c.ResetObservations()
	victimIdx := rng.IntN(size)
	victim := c.Nodes[victimIdx].Name()
	c.Crash(victimIdx)
	t0 := c.Sim.Elapsed()

	survivorsDead := func() bool {
		for i := range c.Nodes {
			if i == victimIdx {
				continue
			}
			if st, ok := c.StateAt(i, victim); ok && st.Live() {
				return false
			}
		}
		return true
	}
	c.Sim.RunWhile(func() bool { return !survivorsDead() }, limit)

	res := trialResult{complete: survivorsDead()}
	inPeriods := func(d time.Duration) float64 { return float64(d) / float64(period) }
	res.firstPing = math.Inf(1)
	for _, t := range c.Traces {
		if t.Trace.Kind == swim.TraceProbe && t.Trace.Peer == victim {
			res.firstPing = math.Max(1, math.Ceil(inPeriods(t.At-t0)))
			break
		}
	}
	res.firstSuspect, res.firstDead = math.Inf(1), math.Inf(1)
	seenDead := map[string]bool{}
	for _, o := range c.Events {
		at := inPeriods(o.At - t0)
		if o.Event.Member.Name != victim {
			if o.Event.Type == swim.EventLeave {
				res.falseDeaths++
			}
			continue
		}
		switch {
		case o.Event.Type == swim.EventUpdate && o.Event.Member.State == swim.StateSuspect:
			res.firstSuspect = math.Min(res.firstSuspect, at)
		case o.Event.Type == swim.EventLeave && !seenDead[o.Observer]:
			seenDead[o.Observer] = true
			res.firstDead = math.Min(res.firstDead, at)
			res.allDead = math.Max(res.allDead, at)
		}
	}
	if math.IsInf(res.firstPing, 1) || math.IsInf(res.firstDead, 1) {
		res.complete = false
	}
	return res, nil
}

// mixSeed derives a per-trial seed (splitmix64 over the components).
func mixSeed(parts ...uint64) uint64 {
	h := uint64(0x9E3779B97F4A7C15)
	for _, p := range parts {
		h ^= p + 0x9E3779B97F4A7C15 + (h << 6) + (h >> 2)
		h = (h ^ (h >> 30)) * 0xbf58476d1ce4e5b9
		h = (h ^ (h >> 27)) * 0x94d049bb133111eb
		h ^= h >> 31
	}
	return h
}

// baseLink returns DefaultLAN with the given loss.
func baseLink(loss float64) sim.LinkConfig {
	l := DefaultLAN
	l.Loss = loss
	return l
}
