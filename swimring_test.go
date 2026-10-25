package swimring_test

import (
	"testing"
	"time"

	"github.com/SlateGitOrg/swimring"
	"github.com/SlateGitOrg/swimring/cache"
	"github.com/SlateGitOrg/swimring/sim"
	"github.com/SlateGitOrg/swimring/swim"
)

func TestNodeLifecycle(t *testing.T) {
	if _, err := swimring.New(swimring.Options{}); err == nil {
		t.Fatal("missing dependencies accepted")
	}
	s := sim.New(1)
	nw, err := sim.NewNetwork(s, sim.LinkConfig{Delay: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	var seen []swim.Event
	mk := func(name string) *swimring.Node {
		h, err := nw.AddHost(name)
		if err != nil {
			t.Fatal(err)
		}
		cfg := swim.DefaultConfig(name)
		cfg.Events = func(ev swim.Event) { seen = append(seen, ev) }
		n, err := swimring.New(swimring.Options{SWIM: cfg, Cache: cache.DefaultConfig(), Transport: h, Clock: h.Clock(), Rand: s.NewRand()})
		if err != nil {
			t.Fatal(err)
		}
		if err := n.Start(); err != nil {
			t.Fatal(err)
		}
		return n
	}
	a, b := mk("a"), mk("b")
	if err := b.Join(a.Addr()); err != nil {
		t.Fatal(err)
	}
	s.RunFor(5 * time.Second)
	if a.Cache.Ring().Len() != 2 || b.Name() != "b" {
		t.Fatalf("ring = %v", a.Cache.Ring())
	}
	if len(seen) == 0 {
		t.Fatal("user event hook not chained")
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := swimring.New(swimring.Options{SWIM: swim.Config{}, Cache: cache.DefaultConfig(), Transport: nw.Host("a"), Clock: s.Clock(), Rand: s.NewRand()}); err == nil {
		t.Fatal("invalid swim config accepted")
	}
	h, _ := nw.AddHost("c")
	if _, err := swimring.New(swimring.Options{SWIM: swim.DefaultConfig("c"), Cache: cache.Config{}, Transport: h, Clock: s.Clock(), Rand: s.NewRand()}); err == nil {
		t.Fatal("invalid cache config accepted")
	}
}
