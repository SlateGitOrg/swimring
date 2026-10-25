package cache_test

import (
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/archit2501/swimring"
	"github.com/archit2501/swimring/cache"
	"github.com/archit2501/swimring/sim"
	"github.com/archit2501/swimring/swim"
)

type cluster struct {
	t     *testing.T
	sim   *sim.Sim
	net   *sim.Network
	nodes []*swimring.Node
	hosts []*sim.Host
}

func newCluster(t *testing.T, size int, seed uint64) *cluster {
	t.Helper()
	s := sim.New(seed)
	nw, err := sim.NewNetwork(s, sim.LinkConfig{Delay: 2 * time.Millisecond, Jitter: 2 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	c := &cluster{t: t, sim: s, net: nw}
	for i := range size {
		name := fmt.Sprintf("c%02d", i)
		h, err := nw.AddHost(name)
		if err != nil {
			t.Fatal(err)
		}
		scfg := swim.DefaultConfig(name)
		scfg.PushPullInterval = 0
		n, err := swimring.New(swimring.Options{
			SWIM: scfg, Cache: cache.DefaultConfig(),
			Transport: h, Clock: h.Clock(), Rand: s.NewRand(),
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := n.Start(); err != nil {
			t.Fatal(err)
		}
		c.nodes = append(c.nodes, n)
		c.hosts = append(c.hosts, h)
	}
	for _, n := range c.nodes[1:] {
		if err := n.Join(c.nodes[0].Addr()); err != nil {
			t.Fatal(err)
		}
	}
	ok := s.RunWhile(func() bool {
		for _, n := range c.nodes {
			if n.Cache.Ring().Len() != size {
				return true
			}
		}
		return false
	}, time.Minute)
	if !ok {
		t.Fatal("rings did not converge")
	}
	s.RunFor(time.Second)
	return c
}

func (c *cluster) put(i int, key, val string) error {
	c.t.Helper()
	var res error
	done := false
	c.nodes[i].Cache.PutAsync(key, []byte(val), func(err error) { res, done = err, true })
	if !c.sim.RunWhile(func() bool { return !done }, 5*time.Second) {
		c.t.Fatal("put callback never ran")
	}
	return res
}

func (c *cluster) del(i int, key string) error {
	c.t.Helper()
	var res error
	done := false
	c.nodes[i].Cache.DeleteAsync(key, func(err error) { res, done = err, true })
	if !c.sim.RunWhile(func() bool { return !done }, 5*time.Second) {
		c.t.Fatal("delete callback never ran")
	}
	return res
}

func (c *cluster) get(i int, key string) (string, bool, error) {
	c.t.Helper()
	var (
		val   []byte
		found bool
		res   error
		done  bool
	)
	c.nodes[i].Cache.GetAsync(key, func(v []byte, ok bool, err error) { val, found, res, done = v, ok, err, true })
	if !c.sim.RunWhile(func() bool { return !done }, 5*time.Second) {
		c.t.Fatal("get callback never ran")
	}
	return string(val), found, res
}

func (c *cluster) holders(key string) []string {
	var out []string
	for i, n := range c.nodes {
		if c.hosts[i].Down() {
			continue
		}
		if e, ok := n.Cache.LocalEntry(key); ok && !e.Deleted {
			out = append(out, n.Name())
		}
	}
	return out
}

func TestPutGetAcrossCoordinators(t *testing.T) {
	c := newCluster(t, 5, 1)
	for k := range 20 {
		key := fmt.Sprintf("user:%d", k)
		if err := c.put(k%5, key, fmt.Sprintf("v%d", k)); err != nil {
			t.Fatalf("put %s: %v", key, err)
		}
	}
	c.sim.RunFor(time.Second)
	for k := range 20 {
		key := fmt.Sprintf("user:%d", k)
		v, ok, err := c.get((k+2)%5, key)
		if err != nil || !ok || v != fmt.Sprintf("v%d", k) {
			t.Fatalf("get %s = %q %v %v", key, v, ok, err)
		}
		if got := c.holders(key); len(got) != 3 {
			t.Fatalf("%s stored on %v, want 3 replicas", key, got)
		}
		want := c.nodes[0].Cache.Ring().LookupN(key, 3)
		if got := c.holders(key); !slices.Equal(sortCopy(got), sortCopy(want)) {
			t.Fatalf("%s on %v, ring says %v", key, got, want)
		}
	}
	if _, ok, err := c.get(0, "missing"); ok || err != nil {
		t.Fatalf("missing key: ok=%v err=%v", ok, err)
	}
}

func sortCopy(s []string) []string {
	out := slices.Clone(s)
	slices.Sort(out)
	return out
}

func TestLastWriterWinsAndDelete(t *testing.T) {
	c := newCluster(t, 4, 2)
	if err := c.put(0, "k", "one"); err != nil {
		t.Fatal(err)
	}
	if err := c.put(3, "k", "two"); err != nil {
		t.Fatal(err)
	}
	if v, ok, err := c.get(1, "k"); err != nil || !ok || v != "two" {
		t.Fatalf("got %q %v %v", v, ok, err)
	}
	if err := c.del(2, "k"); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := c.get(0, "k"); ok || err != nil {
		t.Fatalf("deleted key visible: ok=%v err=%v", ok, err)
	}
}

func TestReplicationRestoredAfterCrash(t *testing.T) {
	c := newCluster(t, 6, 3)
	keys := make([]string, 30)
	for i := range keys {
		keys[i] = fmt.Sprintf("item-%02d", i)
		if err := c.put(0, keys[i], "x"+keys[i]); err != nil {
			t.Fatal(err)
		}
	}
	victim := 4
	c.hosts[victim].Crash()
	name := c.nodes[victim].Name()
	ok := c.sim.RunWhile(func() bool {
		for i, n := range c.nodes {
			if i != victim && n.Cache.Ring().Has(name) {
				return true
			}
		}
		return false
	}, 2*time.Minute)
	if !ok {
		t.Fatal("crashed node never left the rings")
	}
	c.sim.RunFor(2 * time.Second)
	for _, k := range keys {
		if got := c.holders(k); len(got) != 3 {
			t.Fatalf("%s has %d live replicas after rebalance: %v", k, len(got), got)
		}
		v, found, err := c.get(1, k)
		if err != nil || !found || v != "x"+k {
			t.Fatalf("get %s after crash = %q %v %v", k, v, found, err)
		}
	}
	var handed uint64
	for _, n := range c.nodes {
		handed += n.Cache.Stats().HandoffsSent
	}
	if handed == 0 {
		t.Fatal("no hand-offs recorded")
	}
}

func TestJoinMovesKeysToNewOwner(t *testing.T) {
	c := newCluster(t, 3, 4)
	for i := range 40 {
		if err := c.put(i%3, fmt.Sprintf("k%02d", i), "v"); err != nil {
			t.Fatal(err)
		}
	}
	// With 3 nodes and RF=3 everyone holds everything; add 3 more nodes and
	// check each key ends up on exactly its 3 ring replicas.
	for i := 3; i < 6; i++ {
		name := fmt.Sprintf("c%02d", i)
		h, err := c.net.AddHost(name)
		if err != nil {
			t.Fatal(err)
		}
		scfg := swim.DefaultConfig(name)
		scfg.PushPullInterval = 0
		n, err := swimring.New(swimring.Options{SWIM: scfg, Cache: cache.DefaultConfig(), Transport: h, Clock: h.Clock(), Rand: c.sim.NewRand()})
		if err != nil {
			t.Fatal(err)
		}
		if err := n.Start(); err != nil {
			t.Fatal(err)
		}
		if err := n.Join(c.nodes[0].Addr()); err != nil {
			t.Fatal(err)
		}
		c.nodes = append(c.nodes, n)
		c.hosts = append(c.hosts, h)
	}
	ok := c.sim.RunWhile(func() bool {
		for _, n := range c.nodes {
			if n.Cache.Ring().Len() != 6 {
				return true
			}
		}
		return false
	}, time.Minute)
	if !ok {
		for _, n := range c.nodes {
			t.Logf("%s ring=%v live=%d", n.Name(), n.Cache.Ring().Nodes(), n.Membership.NumLive())
		}
		t.Fatal("ring did not grow to 6")
	}
	// Allow one ownership sweep to drop copies pushed by stale ring views.
	c.sim.RunFor(cache.DefaultConfig().SweepInterval + 2*time.Second)
	for i := range 40 {
		k := fmt.Sprintf("k%02d", i)
		want := sortCopy(c.nodes[0].Cache.Ring().LookupN(k, 3))
		if got := sortCopy(c.holders(k)); !slices.Equal(got, want) {
			t.Fatalf("%s held by %v, want %v", k, got, want)
		}
	}
}

func TestReadRepair(t *testing.T) {
	c := newCluster(t, 3, 5)
	if err := c.put(0, "rr", "fresh"); err != nil {
		t.Fatal(err)
	}
	// Partition one replica away, write a newer value, heal, then read: the
	// read must return the new value and repair the stale replica.
	stale := c.nodes[2]
	c.net.Partition([]string{stale.Addr()})
	if err := c.put(0, "rr", "newer"); err != nil {
		t.Fatalf("quorum write with one replica partitioned: %v", err)
	}
	c.net.Heal()
	if e, _ := stale.Cache.LocalEntry("rr"); string(e.Value) != "fresh" {
		t.Fatalf("setup: stale replica has %q", e.Value)
	}
	v, ok, err := c.get(0, "rr")
	if err != nil || !ok || v != "newer" {
		t.Fatalf("get = %q %v %v", v, ok, err)
	}
	c.sim.RunFor(time.Second)
	if e, _ := stale.Cache.LocalEntry("rr"); string(e.Value) != "newer" {
		t.Fatalf("stale replica not repaired: %q", e.Value)
	}
	if c.nodes[0].Cache.Stats().ReadRepairs == 0 {
		t.Fatal("no read repair counted")
	}
}

func TestQuorumFailure(t *testing.T) {
	c := newCluster(t, 3, 6)
	c.net.Partition([]string{c.nodes[0].Addr()})
	if err := c.put(0, "q", "v"); !errors.Is(err, cache.ErrQuorum) {
		t.Fatalf("err = %v, want ErrQuorum", err)
	}
	if _, _, err := c.get(0, "q"); !errors.Is(err, cache.ErrQuorum) {
		t.Fatalf("get err = %v, want ErrQuorum", err)
	}
	if c.nodes[0].Cache.Stats().QuorumFailures != 2 {
		t.Fatalf("stats = %+v", c.nodes[0].Cache.Stats())
	}
}

func TestValidation(t *testing.T) {
	c := newCluster(t, 1, 7)
	if err := c.put(0, "", "v"); !errors.Is(err, cache.ErrKeyInvalid) {
		t.Fatalf("err = %v", err)
	}
	if err := c.put(0, "k", string(make([]byte, cache.MaxValueLen+1))); !errors.Is(err, cache.ErrValueTooLarge) {
		t.Fatalf("err = %v", err)
	}
	// A single-node cluster degrades the quorum to the one replica.
	if err := c.put(0, "solo", "v"); err != nil {
		t.Fatal(err)
	}
	if v, ok, err := c.get(0, "solo"); v != "v" || !ok || err != nil {
		t.Fatalf("solo get = %q %v %v", v, ok, err)
	}
	c.nodes[0].Stop()
	if err := c.put(0, "k", "v"); !errors.Is(err, cache.ErrStopped) {
		t.Fatalf("err after stop = %v", err)
	}
}

func TestConfigValidate(t *testing.T) {
	good := cache.DefaultConfig()
	if err := good.Validate(); err != nil {
		t.Fatal(err)
	}
	for i, mut := range []func(*cache.Config){
		func(c *cache.Config) { c.ReplicationFactor = 0 },
		func(c *cache.Config) { c.WriteQuorum = 4 },
		func(c *cache.Config) { c.ReadQuorum = 0 },
		func(c *cache.Config) { c.RequestTimeout = 0 },
		func(c *cache.Config) { c.VNodes = 0 },
	} {
		cfg := good
		mut(&cfg)
		if cfg.Validate() == nil {
			t.Fatalf("case %d accepted", i)
		}
	}
}
