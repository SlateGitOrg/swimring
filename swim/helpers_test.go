package swim

import (
	"fmt"
	"testing"
	"time"

	"github.com/archit2501/swimring/sim"
)

// testCluster is a simulated cluster used by the protocol tests.
type testCluster struct {
	t      *testing.T
	sim    *sim.Sim
	net    *sim.Network
	nodes  []*Node
	hosts  []*sim.Host
	events []recordedEvent
}

type recordedEvent struct {
	at       time.Duration
	observer string
	ev       Event
}

func testConfig(name string) Config {
	cfg := DefaultConfig(name)
	cfg.PushPullInterval = 0
	cfg.ReconnectInterval = 0
	return cfg
}

func newTestCluster(t *testing.T, size int, seed uint64, link sim.LinkConfig, mutate func(*Config)) *testCluster {
	t.Helper()
	s := sim.New(seed)
	nw, err := sim.NewNetwork(s, link)
	if err != nil {
		t.Fatal(err)
	}
	c := &testCluster{t: t, sim: s, net: nw}
	for i := range size {
		c.addNode(fmt.Sprintf("n%02d", i), mutate)
	}
	return c
}

func (c *testCluster) addNode(name string, mutate func(*Config)) *Node {
	c.t.Helper()
	h, err := c.net.AddHost(name)
	if err != nil {
		c.t.Fatal(err)
	}
	cfg := testConfig(name)
	if mutate != nil {
		mutate(&cfg)
	}
	cfg.Events = func(ev Event) {
		c.events = append(c.events, recordedEvent{at: c.sim.Elapsed(), observer: name, ev: ev})
	}
	n, err := New(cfg, h, h.Clock(), c.sim.NewRand())
	if err != nil {
		c.t.Fatal(err)
	}
	if err := n.Start(); err != nil {
		c.t.Fatal(err)
	}
	c.nodes = append(c.nodes, n)
	c.hosts = append(c.hosts, h)
	return n
}

// joinAll joins every node to node 0 and waits for full convergence.
func (c *testCluster) joinAll() {
	c.t.Helper()
	for _, n := range c.nodes[1:] {
		if err := n.Join([]string{c.nodes[0].Addr()}); err != nil {
			c.t.Fatal(err)
		}
	}
	if !c.sim.RunWhile(func() bool { return !c.converged(len(c.nodes)) }, 2*time.Minute) {
		c.t.Fatalf("cluster of %d did not converge", len(c.nodes))
	}
}

func (c *testCluster) converged(want int) bool {
	for i, n := range c.nodes {
		if c.hosts[i].Down() || n.Stopped() {
			continue
		}
		if n.NumLive() != want {
			return false
		}
	}
	return true
}

func (c *testCluster) state(observer int, name string) State {
	m, ok := c.nodes[observer].Member(name)
	if !ok {
		return StateDead
	}
	return m.State
}
