package swim

import (
	"crypto/sha256"
	"fmt"
	"testing"
	"time"

	"github.com/archit2501/swimring/sim"
)

var lan = sim.LinkConfig{Delay: 2 * time.Millisecond, Jitter: 3 * time.Millisecond}

func TestClusterConverges(t *testing.T) {
	c := newTestCluster(t, 16, 1, lan, nil)
	c.joinAll()
	for i, n := range c.nodes {
		if got := n.NumLive(); got != 16 {
			t.Fatalf("node %d sees %d live members", i, got)
		}
	}
}

func TestCrashIsDetectedEverywhere(t *testing.T) {
	c := newTestCluster(t, 12, 2, lan, nil)
	c.joinAll()
	c.sim.RunFor(5 * time.Second)
	victim := c.nodes[5].Name()
	c.hosts[5].Crash()
	start := c.sim.Elapsed()
	ok := c.sim.RunWhile(func() bool {
		for i := range c.nodes {
			if i != 5 && c.state(i, victim) != StateDead {
				return true
			}
		}
		return false
	}, 2*time.Minute)
	if !ok {
		t.Fatalf("crash of %s not detected by everyone", victim)
	}
	took := c.sim.Elapsed() - start
	t.Logf("all members declared %s dead after %v", victim, took)
	if took > time.Minute {
		t.Fatalf("detection took %v", took)
	}
	// No healthy node may have been declared dead.
	for _, r := range c.events {
		if r.ev.Type == EventLeave && r.ev.Member.Name != victim {
			t.Fatalf("false death: %s declared %s dead", r.observer, r.ev.Member.Name)
		}
	}
}

func TestFalseSuspicionIsRefuted(t *testing.T) {
	c := newTestCluster(t, 6, 3, lan, nil)
	c.joinAll()
	target := c.nodes[2]
	inc := target.Incarnation()
	// Inject a suspicion about node 2 into node 0 as if gossiped by node 1.
	c.nodes[0].mu.Lock()
	c.nodes[0].suspectNode(target.Name(), inc, c.nodes[1].Name())
	c.nodes[0].unlock()
	if got := c.state(0, target.Name()); got != StateSuspect {
		t.Fatalf("state = %v, want suspect", got)
	}
	c.sim.RunFor(30 * time.Second)
	if target.Incarnation() <= inc {
		t.Fatalf("incarnation not bumped: %d", target.Incarnation())
	}
	for i := range c.nodes {
		if got := c.state(i, target.Name()); got != StateAlive {
			t.Fatalf("node %d sees %s as %v after refutation", i, target.Name(), got)
		}
	}
	if target.Stats().Refutations == 0 {
		t.Fatal("no refutation recorded")
	}
}

func TestGracefulLeave(t *testing.T) {
	c := newTestCluster(t, 8, 4, lan, nil)
	c.joinAll()
	leaver := c.nodes[3]
	if err := leaver.LeaveAndStop(5 * time.Second); err != nil {
		t.Fatal(err)
	}
	c.sim.RunFor(20 * time.Second)
	for i := range c.nodes {
		if i == 3 {
			continue
		}
		if got := c.state(i, leaver.Name()); got != StateLeft {
			t.Fatalf("node %d sees leaver as %v", i, got)
		}
	}
	if !leaver.Stopped() {
		t.Fatal("leaver did not stop")
	}
}

func TestPartitionAndHeal(t *testing.T) {
	c := newTestCluster(t, 10, 5, lan, func(cfg *Config) {
		cfg.ReconnectInterval = 5 * time.Second
		cfg.PushPullInterval = 15 * time.Second
	})
	c.joinAll()
	var left, right []string
	for i, n := range c.nodes {
		if i < 5 {
			left = append(left, n.Addr())
		} else {
			right = append(right, n.Addr())
		}
	}
	c.net.Partition(left, right)
	ok := c.sim.RunWhile(func() bool {
		for _, n := range c.nodes {
			if n.NumLive() != 5 {
				return true
			}
		}
		return false
	}, 3*time.Minute)
	if !ok {
		t.Fatal("partition halves did not each converge to 5 members")
	}
	c.net.Heal()
	if !c.sim.RunWhile(func() bool { return !c.converged(10) }, 3*time.Minute) {
		for i, n := range c.nodes {
			t.Logf("node %d live=%d", i, n.NumLive())
		}
		t.Fatal("cluster did not re-merge after heal")
	}
}

func TestDeterministicReplay(t *testing.T) {
	run := func() string {
		c := newTestCluster(t, 10, 42, sim.LinkConfig{Loss: 0.1, Delay: time.Millisecond, Jitter: 5 * time.Millisecond}, nil)
		c.joinAll()
		c.hosts[7].Crash()
		c.sim.RunFor(40 * time.Second)
		h := sha256.New()
		for _, r := range c.events {
			fmt.Fprintf(h, "%d %s %s %s %d\n", r.at, r.observer, r.ev.Type, r.ev.Member.Name, r.ev.Member.Incarnation)
		}
		fmt.Fprintf(h, "%+v %d", c.net.Stats(), c.sim.Processed())
		return fmt.Sprintf("%x", h.Sum(nil))
	}
	a, b := run(), run()
	if a != b {
		t.Fatalf("replay diverged: %s vs %s", a, b)
	}
}
