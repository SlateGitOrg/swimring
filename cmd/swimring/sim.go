package main

import (
	"flag"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/archit2501/swimring/experiment"
	"github.com/archit2501/swimring/swim"
)

func runSim(args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("sim", flag.ContinueOnError)
	fs.SetOutput(stderr)
	size := fs.Int("n", 16, "cluster size")
	loss := fs.Float64("loss", 0, "independent packet loss probability")
	seed := fs.Uint64("seed", 1, "simulation seed")
	crash := fs.Int("crash", 1, "number of members to crash after warm-up")
	slow := fs.Int("slow", 0, "number of members with slow inbound processing")
	lag := fs.Duration("lag", 2*time.Second, "inbound processing lag of slow members")
	duration := fs.Duration("duration", 60*time.Second, "virtual time to simulate after warm-up")
	lifeguard := fs.Bool("lifeguard", true, "enable all Lifeguard extensions")
	verbose := fs.Bool("v", false, "print every membership event (default: first observation per member and state)")
	if err := fs.Parse(args); err != nil {
		return errUsage
	}
	if *size < 2 || *crash < 0 || *slow < 0 || *crash+*slow >= *size {
		return fmt.Errorf("need n >= 2 and crash+slow < n")
	}
	lg := swim.LifeguardOff()
	if *lifeguard {
		lg = swim.LifeguardAll()
	}
	link := experiment.DefaultLAN
	link.Loss = *loss
	start := time.Now()
	c, err := experiment.NewCluster(experiment.ClusterSpec{
		Size: *size, Seed: *seed, Link: link,
		Configure: func(cfg *swim.Config) { cfg.Lifeguard = lg },
	})
	if err != nil {
		return err
	}
	fmt.Fprintf(stdout, "bootstrapped %d members in %v of virtual time (loss=%.0f%%, lifeguard=%v, seed=%d)\n",
		*size, c.Sim.Elapsed().Round(time.Millisecond), *loss*100, *lifeguard, *seed)
	c.Sim.RunFor(5 * time.Second)
	t0 := c.Sim.Elapsed()
	var crashed []string
	for i := range *crash {
		idx := *size - 1 - i
		c.Crash(idx)
		crashed = append(crashed, c.Nodes[idx].Name())
	}
	var slowNames []string
	for i := range *slow {
		c.Hosts[1+i].SetLag(*lag)
		slowNames = append(slowNames, c.Nodes[1+i].Name())
	}
	if len(crashed) > 0 {
		fmt.Fprintf(stdout, "t=+0.000s crashed %s\n", strings.Join(crashed, ", "))
	}
	if len(slowNames) > 0 {
		fmt.Fprintf(stdout, "t=+0.000s slowed %s (inbound lag %v)\n", strings.Join(slowNames, ", "), *lag)
	}
	c.Sim.RunFor(*duration)

	type key struct {
		member string
		kind   string
	}
	first := map[key]bool{}
	counts := map[string]int{}
	for _, o := range c.Events {
		if o.At < t0 {
			continue
		}
		kind := o.Event.Type.String() + "/" + o.Event.Member.State.String()
		counts[kind]++
		k := key{o.Event.Member.Name, kind}
		if !*verbose && first[k] {
			continue
		}
		first[k] = true
		fmt.Fprintf(stdout, "t=+%.3fs %-6s sees %-6s %-7s inc=%d (source %s)\n",
			(o.At - t0).Seconds(), o.Observer, o.Event.Member.Name, o.Event.Member.State,
			o.Event.Member.Incarnation, o.Event.Source)
	}
	var probes, fails, refutes, sent uint64
	maxLHM := 0
	for i, n := range c.Nodes {
		if c.Hosts[i].Down() {
			continue
		}
		st := n.Stats()
		probes += st.Probes
		fails += st.ProbeFailures
		refutes += st.Refutations
		sent += st.PacketsSent
		maxLHM = max(maxLHM, n.LocalHealth())
	}
	ns := c.Net.Stats()
	fmt.Fprintf(stdout, "\nsummary after %v virtual (%v wall, %d events):\n", *duration, time.Since(start).Round(time.Millisecond), c.Sim.Processed())
	fmt.Fprintf(stdout, "  events: %v\n", counts)
	fmt.Fprintf(stdout, "  probes=%d failed=%d refutations=%d max-LHM-now=%d\n", probes, fails, refutes, maxLHM)
	fmt.Fprintf(stdout, "  network: sent=%d delivered=%d lost=%d to-crashed=%d bytes=%d\n",
		ns.Sent, ns.Delivered, ns.DroppedLoss, ns.DroppedDown, ns.Bytes)
	live := c.Nodes[0].NumLive()
	fmt.Fprintf(stdout, "  %s sees %d live members (expected %d)\n", c.Nodes[0].Name(), live, *size-*crash)
	return nil
}
