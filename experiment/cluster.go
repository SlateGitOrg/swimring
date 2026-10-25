// Package experiment runs seeded, deterministic simulations of SWIM
// clusters and measures failure-detection latency, false-positive rates,
// dissemination time and the effect of Lifeguard, alongside the analytical
// expectations from the SWIM paper.
//
// Every experiment is a pure function of its spec (including the seed), so
// a result table can be regenerated bit-for-bit.
package experiment

import (
	"errors"
	"fmt"
	"time"

	"github.com/archit2501/swimring/sim"
	"github.com/archit2501/swimring/swim"
)

// ClusterSpec describes a simulated cluster.
type ClusterSpec struct {
	Size int
	Seed uint64
	// Link applies after the bootstrap; the cluster is formed over a
	// lossless link so every trial starts from a converged membership.
	Link sim.LinkConfig
	// Configure, if set, adjusts each node's configuration.
	Configure func(*swim.Config)
	// BootstrapLimit bounds the virtual time allowed for convergence.
	BootstrapLimit time.Duration
}

// DefaultLAN is the base link: 1–4 ms one-way latency.
var DefaultLAN = sim.LinkConfig{Delay: time.Millisecond, Jitter: 3 * time.Millisecond}

// ErrNoConvergence is returned when a cluster fails to bootstrap.
var ErrNoConvergence = errors.New("experiment: cluster did not converge")

// Cluster is a running simulated cluster with an event recorder.
type Cluster struct {
	Sim   *sim.Sim
	Net   *sim.Network
	Nodes []*swim.Node
	Hosts []*sim.Host
	Index map[string]int

	// Recorded observations, in simulation order.
	Events []Observed
	Traces []ObservedTrace
}

// Observed is a membership event seen by one node.
type Observed struct {
	At       time.Duration
	Observer string
	Event    swim.Event
}

// ObservedTrace is a protocol trace record from one node.
type ObservedTrace struct {
	At       time.Duration
	Observer string
	Trace    swim.Trace
}

// NewCluster builds, starts and bootstraps a cluster: every node joins node
// 0 and the call returns once all nodes see all members alive.
func NewCluster(spec ClusterSpec) (*Cluster, error) {
	if spec.Size < 1 {
		return nil, fmt.Errorf("experiment: cluster size %d", spec.Size)
	}
	if err := spec.Link.Validate(); err != nil {
		return nil, err
	}
	limit := spec.BootstrapLimit
	if limit == 0 {
		limit = 5 * time.Minute
	}
	s := sim.New(spec.Seed)
	nw, err := sim.NewNetwork(s, sim.LinkConfig{Delay: spec.Link.Delay, Jitter: spec.Link.Jitter})
	if err != nil {
		return nil, err
	}
	c := &Cluster{Sim: s, Net: nw, Index: make(map[string]int, spec.Size)}
	for i := range spec.Size {
		name := fmt.Sprintf("n%03d", i)
		h, err := nw.AddHost(name)
		if err != nil {
			return nil, err
		}
		cfg := swim.DefaultConfig(name)
		if spec.Configure != nil {
			spec.Configure(&cfg)
		}
		observer := name
		cfg.Events = func(ev swim.Event) {
			c.Events = append(c.Events, Observed{At: s.Elapsed(), Observer: observer, Event: ev})
		}
		n, err := swim.New(cfg, h, h.Clock(), s.NewRand())
		if err != nil {
			return nil, err
		}
		n.SetTrace(func(t swim.Trace) {
			c.Traces = append(c.Traces, ObservedTrace{At: s.Elapsed(), Observer: observer, Trace: t})
		})
		if err := n.Start(); err != nil {
			return nil, err
		}
		c.Index[name] = i
		c.Nodes = append(c.Nodes, n)
		c.Hosts = append(c.Hosts, h)
	}
	for _, n := range c.Nodes[1:] {
		if err := n.Join([]string{c.Nodes[0].Addr()}); err != nil {
			return nil, err
		}
	}
	if !s.RunWhile(func() bool { return !c.Converged() }, limit) {
		return nil, fmt.Errorf("%w: %d nodes after %v", ErrNoConvergence, spec.Size, limit)
	}
	if err := nw.SetDefaultLink(spec.Link); err != nil {
		return nil, err
	}
	c.ResetObservations()
	return c, nil
}

// Converged reports whether every running node sees every other running
// node as Alive and no crashed node as live.
func (c *Cluster) Converged() bool {
	running := 0
	for i := range c.Nodes {
		if !c.Hosts[i].Down() && !c.Nodes[i].Stopped() {
			running++
		}
	}
	for i, n := range c.Nodes {
		if c.Hosts[i].Down() || n.Stopped() {
			continue
		}
		alive := 0
		for _, m := range n.Members() {
			if m.State == swim.StateAlive {
				alive++
			}
		}
		if alive != running || n.NumLive() != running {
			return false
		}
	}
	return true
}

// ResetObservations clears recorded events and traces.
func (c *Cluster) ResetObservations() {
	c.Events = c.Events[:0]
	c.Traces = c.Traces[:0]
}

// Crash stops node i abruptly.
func (c *Cluster) Crash(i int) { c.Hosts[i].Crash() }

// StateAt returns how observer i currently sees member name.
func (c *Cluster) StateAt(i int, name string) (swim.State, bool) {
	m, ok := c.Nodes[i].Member(name)
	return m.State, ok
}

// ProbeInterval returns node 0's configured protocol period.
func (c *Cluster) ProbeInterval() time.Duration { return c.Nodes[0].Config().ProbeInterval }
