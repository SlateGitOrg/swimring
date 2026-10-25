package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/rand/v2"
	"slices"
	"strings"
	"time"

	"github.com/archit2501/swimring"
	"github.com/archit2501/swimring/cache"
	"github.com/archit2501/swimring/clock"
	"github.com/archit2501/swimring/sim"
	"github.com/archit2501/swimring/swim"
	"github.com/archit2501/swimring/transport"
)

func runDemo(args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("demo", flag.ContinueOnError)
	fs.SetOutput(stderr)
	seed := fs.Uint64("seed", 7, "seed for the simulated part")
	skipUDP := fs.Bool("skip-udp", false, "skip the loopback UDP part")
	if err := fs.Parse(args); err != nil {
		return errUsage
	}
	if err := simDemo(stdout, *seed); err != nil {
		return fmt.Errorf("simulated demo: %w", err)
	}
	if *skipUDP {
		return nil
	}
	if err := udpDemo(stdout); err != nil {
		return fmt.Errorf("udp demo: %w", err)
	}
	return nil
}

// simCluster is a simulated swimring cluster used by the demo.
type simCluster struct {
	sim   *sim.Sim
	net   *sim.Network
	nodes []*swimring.Node
	hosts []*sim.Host
	log   []string
}

func newSimCluster(size int, seed uint64) (*simCluster, error) {
	s := sim.New(seed)
	nw, err := sim.NewNetwork(s, sim.LinkConfig{Delay: time.Millisecond, Jitter: 3 * time.Millisecond})
	if err != nil {
		return nil, err
	}
	c := &simCluster{sim: s, net: nw}
	for i := range size {
		name := fmt.Sprintf("n%d", i)
		h, err := nw.AddHost(name)
		if err != nil {
			return nil, err
		}
		cfg := swim.DefaultConfig(name)
		cfg.ReconnectInterval = 5 * time.Second
		observer := name
		cfg.Events = func(ev swim.Event) {
			if ev.Type == swim.EventJoin && ev.Prev == swim.StateAlive {
				return // initial joins are summarised, not listed
			}
			c.log = append(c.log, fmt.Sprintf("  t=%7.3fs %s: %s is %s (inc %d, source %s)",
				s.Elapsed().Seconds(), observer, ev.Member.Name, ev.Member.State, ev.Member.Incarnation, ev.Source))
		}
		n, err := swimring.New(swimring.Options{SWIM: cfg, Cache: cache.DefaultConfig(), Transport: h, Clock: h.Clock(), Rand: s.NewRand()})
		if err != nil {
			return nil, err
		}
		if err := n.Start(); err != nil {
			return nil, err
		}
		c.nodes = append(c.nodes, n)
		c.hosts = append(c.hosts, h)
	}
	for _, n := range c.nodes[1:] {
		if err := n.Join(c.nodes[0].Addr()); err != nil {
			return nil, err
		}
	}
	return c, nil
}

func (c *simCluster) liveCounts() []int {
	out := make([]int, len(c.nodes))
	for i, n := range c.nodes {
		if !c.hosts[i].Down() {
			out[i] = n.Membership.NumLive()
		}
	}
	return out
}

func (c *simCluster) waitAll(want int, limit time.Duration) bool {
	return c.sim.RunWhile(func() bool {
		for i, n := range c.nodes {
			if !c.hosts[i].Down() && n.Membership.NumLive() != want {
				return true
			}
		}
		return false
	}, limit)
}

func (c *simCluster) put(i int, k, v string) error {
	var res error
	done := false
	c.nodes[i].Cache.PutAsync(k, []byte(v), func(err error) { res, done = err, true })
	c.sim.RunWhile(func() bool { return !done }, 5*time.Second)
	if !done {
		return errors.New("put never completed")
	}
	return res
}

func (c *simCluster) get(i int, k string) (string, bool, error) {
	var (
		val      []byte
		ok, done bool
		res      error
	)
	c.nodes[i].Cache.GetAsync(k, func(v []byte, found bool, err error) { val, ok, res, done = v, found, err, true })
	c.sim.RunWhile(func() bool { return !done }, 5*time.Second)
	if !done {
		return "", false, errors.New("get never completed")
	}
	return string(val), ok, res
}

func (c *simCluster) flushLog(w io.Writer, max int) {
	for i, l := range c.log {
		if i == max {
			fmt.Fprintf(w, "  ... %d more events\n", len(c.log)-max)
			break
		}
		fmt.Fprintln(w, l)
	}
	c.log = c.log[:0]
}

func simDemo(w io.Writer, seed uint64) error {
	const size = 8
	fmt.Fprintf(w, "== Part 1: simulated cluster (%d nodes, seed %d, deterministic) ==\n\n", size, seed)
	c, err := newSimCluster(size, seed)
	if err != nil {
		return err
	}
	if !c.waitAll(size, time.Minute) {
		return errors.New("cluster did not converge")
	}
	fmt.Fprintf(w, "[join] all %d members see each other after %.3fs of virtual time\n", size, c.sim.Elapsed().Seconds())
	c.sim.RunFor(2 * time.Second)
	c.log = c.log[:0]

	r := c.nodes[0].Cache.Ring()
	own := r.Ownership()
	var shares []string
	for _, n := range r.Nodes() {
		shares = append(shares, fmt.Sprintf("%s=%.1f%%", n, own[n]*100))
	}
	fmt.Fprintf(w, "[ring] %d vnodes/member, primary ownership: %s\n", r.VNodes(), strings.Join(shares, " "))

	const nkeys = 60
	for i := range nkeys {
		if err := c.put(i%size, fmt.Sprintf("user:%03d", i), fmt.Sprintf("profile-%03d", i)); err != nil {
			return fmt.Errorf("put: %w", err)
		}
	}
	fmt.Fprintf(w, "[cache] wrote %d keys (N=3, W=2, R=2) through rotating coordinators\n", nkeys)
	for _, k := range []string{"user:007", "user:042"} {
		fmt.Fprintf(w, "        %s -> replicas %v\n", k, r.LookupN(k, 3))
	}

	victim := 5
	vname := c.nodes[victim].Name()
	c.hosts[victim].Crash()
	t0 := c.sim.Elapsed()
	fmt.Fprintf(w, "\n[crash] %s crashes at t=%.3fs (no goodbye)\n", vname, t0.Seconds())
	if !c.waitAll(size-1, 2*time.Minute) {
		return errors.New("crash not detected")
	}
	c.flushLog(w, 12)
	fmt.Fprintf(w, "[detect] every survivor removed %s %.3fs after the crash\n", vname, (c.sim.Elapsed() - t0).Seconds())
	c.sim.RunFor(3 * time.Second)
	readable, full := 0, 0
	for i := range nkeys {
		k := fmt.Sprintf("user:%03d", i)
		coord := (i + 1) % size
		if coord == victim {
			coord = 0
		}
		v, ok, err := c.get(coord, k)
		if err == nil && ok && v == fmt.Sprintf("profile-%03d", i) {
			readable++
		}
		holders := 0
		for j, n := range c.nodes {
			if c.hosts[j].Down() {
				continue
			}
			if e, ok := n.Cache.LocalEntry(k); ok && !e.Deleted {
				holders++
			}
		}
		if holders == 3 {
			full++
		}
	}
	var handoffs uint64
	for _, n := range c.nodes {
		handoffs += n.Cache.Stats().HandoffsSent
	}
	fmt.Fprintf(w, "[cache] after hand-off (%d copies sent): %d/%d keys readable, %d/%d back at 3 live replicas\n",
		handoffs, readable, nkeys, full, nkeys)

	var left, right []string
	for i, n := range c.nodes {
		if i == victim {
			continue
		}
		if i < 4 {
			left = append(left, n.Addr())
		} else {
			right = append(right, n.Addr())
		}
	}
	c.net.Partition(left, right)
	tp := c.sim.Elapsed()
	fmt.Fprintf(w, "\n[partition] network split %v | %v\n", left, right)
	c.sim.RunWhile(func() bool {
		lc := c.liveCounts()
		return lc[0] != len(left) || lc[size-1] != len(right)
	}, 3*time.Minute)
	c.flushLog(w, 6)
	lc := c.liveCounts()
	fmt.Fprintf(w, "[partition] after %.1fs each side sees only itself: %s sees %d live, %s sees %d live\n",
		(c.sim.Elapsed() - tp).Seconds(), c.nodes[0].Name(), lc[0], c.nodes[size-1].Name(), lc[size-1])
	c.net.Heal()
	th := c.sim.Elapsed()
	if !c.waitAll(size-1, 3*time.Minute) {
		return errors.New("partition did not heal")
	}
	c.log = c.log[:0]
	fmt.Fprintf(w, "[heal] partition healed; reconnect probes + refutations re-merged all %d survivors in %.1fs\n",
		size-1, (c.sim.Elapsed() - th).Seconds())
	ns := c.net.Stats()
	fmt.Fprintf(w, "[net] %d packets sent, %d delivered, %d dropped by partition, %d sent to the crashed host\n\n",
		ns.Sent, ns.Delivered, ns.DroppedPartition, ns.DroppedDown)
	return nil
}

// udpDemo runs four real nodes over loopback UDP with the wall clock.
func udpDemo(w io.Writer) error {
	const size = 4
	fmt.Fprintf(w, "== Part 2: real UDP on 127.0.0.1 (%d nodes, wall clock, T=200ms) ==\n\n", size)
	var nodes []*swimring.Node
	defer func() {
		for _, n := range nodes {
			_ = n.Close()
		}
	}()
	for i := range size {
		tr, err := transport.ListenUDP("127.0.0.1:0", nil)
		if err != nil {
			return err
		}
		cfg := fastConfig(fmt.Sprintf("udp%d", i))
		n, err := swimring.New(swimring.Options{
			SWIM: cfg, Cache: cache.DefaultConfig(), Transport: tr,
			Clock: clock.NewReal(), Rand: rand.New(rand.NewPCG(uint64(i), uint64(time.Now().UnixNano()))),
		})
		if err != nil {
			_ = tr.Close()
			return err
		}
		nodes = append(nodes, n)
		if err := n.Start(); err != nil {
			return err
		}
		fmt.Fprintf(w, "[udp] %s listening on %s\n", n.Name(), n.Addr())
	}
	start := time.Now()
	for _, n := range nodes[1:] {
		if err := n.Join(nodes[0].Addr()); err != nil {
			return err
		}
	}
	if !waitReal(func() bool {
		for _, n := range nodes {
			if n.Membership.NumLive() != size || n.Cache.Ring().Len() != size {
				return false
			}
		}
		return true
	}, 10*time.Second) {
		return errors.New("udp cluster did not converge")
	}
	fmt.Fprintf(w, "[udp] converged in %v\n", time.Since(start).Round(time.Millisecond))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	keys := []string{"alpha", "bravo", "charlie", "delta", "echo"}
	for i, k := range keys {
		if err := nodes[i%size].Cache.Put(ctx, k, []byte(strings.ToUpper(k))); err != nil {
			return fmt.Errorf("put %s: %w", k, err)
		}
	}
	fmt.Fprintf(w, "[udp] put %d keys; %s holds %d replicas locally\n", len(keys), nodes[0].Name(), len(nodes[0].Cache.LocalKeys()))

	victim := nodes[size-1]
	vname := victim.Name()
	if err := victim.Close(); err != nil {
		return err
	}
	nodes = nodes[:size-1]
	t0 := time.Now()
	fmt.Fprintf(w, "[udp] killed %s (socket closed, no leave message)\n", vname)
	if !waitReal(func() bool {
		for _, n := range nodes {
			if m, ok := n.Membership.Member(vname); !ok || m.State.Live() {
				return false
			}
		}
		return true
	}, 20*time.Second) {
		return errors.New("udp crash not detected")
	}
	fmt.Fprintf(w, "[udp] all survivors declared %s dead after %v (wall clock)\n", vname, time.Since(t0).Round(10*time.Millisecond))
	got := 0
	for _, k := range keys {
		v, ok, err := nodes[0].Cache.Get(ctx, k)
		if err == nil && ok && string(v) == strings.ToUpper(k) {
			got++
		}
	}
	members := nodes[0].Membership.LiveMembers()
	names := make([]string, 0, len(members))
	for _, m := range members {
		names = append(names, m.Name)
	}
	slices.Sort(names)
	fmt.Fprintf(w, "[udp] %d/%d keys still readable; live members: %v\n", got, len(keys), names)
	return nil
}

// fastConfig shortens the protocol period for interactive use.
func fastConfig(name string) swim.Config {
	cfg := swim.DefaultConfig(name)
	cfg.ProbeInterval = 200 * time.Millisecond
	cfg.ProbeTimeout = 80 * time.Millisecond
	cfg.PushPullInterval = 5 * time.Second
	cfg.ReconnectInterval = 2 * time.Second
	cfg.JoinRetryInterval = 300 * time.Millisecond
	return cfg
}

func waitReal(cond func() bool, limit time.Duration) bool {
	deadline := time.Now().Add(limit)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return cond()
}
