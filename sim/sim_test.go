package sim

import (
	"math"
	"testing"
	"time"
)

func TestEventOrdering(t *testing.T) {
	s := New(1)
	var order []int
	s.Schedule(2*time.Second, func() { order = append(order, 3) })
	s.Schedule(time.Second, func() { order = append(order, 1) })
	s.Schedule(time.Second, func() { order = append(order, 2) }) // same instant: FIFO
	s.Schedule(-time.Second, func() { order = append(order, 0) })
	ev := s.Schedule(1500*time.Millisecond, func() { order = append(order, 99) })
	if !ev.Stop() || ev.Stop() {
		t.Fatal("Stop semantics")
	}
	s.RunFor(10 * time.Second)
	if len(order) != 4 || order[0] != 0 || order[1] != 1 || order[2] != 2 || order[3] != 3 {
		t.Fatalf("order = %v", order)
	}
	if s.Elapsed() != 10*time.Second || s.Processed() != 4 || s.Pending() != 0 {
		t.Fatalf("elapsed=%v processed=%d pending=%d", s.Elapsed(), s.Processed(), s.Pending())
	}
	if s.Step() {
		t.Fatal("Step on empty queue")
	}
}

func TestRunWhile(t *testing.T) {
	s := New(1)
	n := 0
	var tick func()
	tick = func() { n++; s.Schedule(time.Second, tick) }
	s.Schedule(time.Second, tick)
	if !s.RunWhile(func() bool { return n < 5 }, time.Minute) || s.Elapsed() != 5*time.Second {
		t.Fatalf("n=%d elapsed=%v", n, s.Elapsed())
	}
	if s.RunWhile(func() bool { return true }, 3*time.Second) || s.Elapsed() != 8*time.Second {
		t.Fatalf("limit not honoured: %v", s.Elapsed())
	}
	empty := New(2)
	if empty.RunWhile(func() bool { return true }, time.Second) || empty.Elapsed() != time.Second {
		t.Fatal("empty queue should run to the limit")
	}
	c := New(3)
	ev := c.Schedule(time.Second, func() {})
	ev.Stop()
	c.Schedule(5*time.Second, func() {})
	c.RunUntil(Epoch.Add(2 * time.Second))
	if c.Processed() != 0 || c.Clock().Now() != Epoch.Add(2*time.Second) {
		t.Fatal("RunUntil with cancelled head")
	}
	tm := c.Clock().AfterFunc(time.Second, func() {})
	if !tm.Stop() {
		t.Fatal("clock timer stop")
	}
	c2 := New(4)
	ev2 := c2.Schedule(time.Second, func() {})
	ev2.Stop()
	if c2.RunWhile(func() bool { return true }, 2*time.Second) {
		t.Fatal("cancelled-only queue")
	}
}

func TestRandStreamsAreDeterministic(t *testing.T) {
	a, b := New(42), New(42)
	for range 3 {
		if a.NewRand().Uint64() != b.NewRand().Uint64() {
			t.Fatal("streams differ for equal seeds")
		}
	}
	if New(1).NewRand().Uint64() == New(2).NewRand().Uint64() {
		t.Fatal("different seeds gave equal streams")
	}
	if a.Rand() == nil || a.Now() != Epoch {
		t.Fatal("accessors")
	}
}

func newNet(t *testing.T, link LinkConfig) (*Sim, *Network, *Host, *Host) {
	t.Helper()
	s := New(7)
	nw, err := NewNetwork(s, link)
	if err != nil {
		t.Fatal(err)
	}
	a, err := nw.AddHost("a")
	if err != nil {
		t.Fatal(err)
	}
	b, err := nw.AddHost("b")
	if err != nil {
		t.Fatal(err)
	}
	return s, nw, a, b
}

func TestNetworkDelayAndLoss(t *testing.T) {
	s, nw, a, b := newNet(t, LinkConfig{Loss: 0.25, Delay: 10 * time.Millisecond, Jitter: 5 * time.Millisecond})
	var lat []time.Duration
	sent := map[int]time.Duration{}
	b.SetHandler(func(from string, p []byte) {
		if from != "a" {
			t.Errorf("from = %s", from)
		}
		lat = append(lat, s.Elapsed()-sent[int(p[0])<<8|int(p[1])])
	})
	const n = 4000
	for i := range n {
		sent[i] = s.Elapsed()
		if err := a.Send("b", []byte{byte(i >> 8), byte(i)}); err != nil {
			t.Fatal(err)
		}
		s.RunFor(time.Millisecond)
	}
	s.RunFor(time.Second)
	got := float64(len(lat)) / n
	if math.Abs(got-0.75) > 0.03 {
		t.Fatalf("delivery ratio %.3f, want ~0.75", got)
	}
	for _, d := range lat {
		if d < 10*time.Millisecond || d >= 15*time.Millisecond {
			t.Fatalf("latency %v outside [10ms,15ms)", d)
		}
	}
	st := nw.Stats()
	if st.Sent != n || st.Delivered+st.DroppedLoss != n || b.Received() != st.Delivered {
		t.Fatalf("stats %+v", st)
	}
}

func TestPartitionCrashLagAndLinks(t *testing.T) {
	s, nw, a, b := newNet(t, LinkConfig{Delay: time.Millisecond})
	c, _ := nw.AddHost("c")
	if _, err := nw.AddHost("c"); err == nil {
		t.Fatal("duplicate host accepted")
	}
	if _, err := nw.AddHost(""); err == nil {
		t.Fatal("empty host accepted")
	}
	if nw.Host("a") != a || nw.Sim() != s {
		t.Fatal("lookup")
	}
	var traced []Packet
	nw.SetTrace(func(p Packet) { traced = append(traced, p) })
	recv := map[string]int{}
	for _, h := range []*Host{a, b, c} {
		name := h.LocalAddr()
		h.SetHandler(func(string, []byte) { recv[name]++ })
	}
	nw.Partition([]string{"a"}, []string{"b", "c"})
	_ = a.Send("b", []byte{1})
	_ = b.Send("c", []byte{1})
	_ = a.Send("nobody", []byte{1})
	s.RunFor(time.Second)
	if recv["b"] != 0 || recv["c"] != 1 {
		t.Fatalf("partition: %v", recv)
	}
	nw.Heal()
	if err := nw.SetLink("a", "b", LinkConfig{Loss: 1}); err != nil {
		t.Fatal(err)
	}
	_ = a.Send("b", []byte{1})
	_ = b.Send("a", []byte{1})
	s.RunFor(time.Second)
	if recv["b"] != 0 || recv["a"] != 1 {
		t.Fatalf("per-link loss: %v", recv)
	}

	// Lag delays inbound handling; timer lag delays timers.
	c.SetLag(500 * time.Millisecond)
	c.SetTimerLag(200 * time.Millisecond)
	if c.Lag() != 500*time.Millisecond {
		t.Fatal("Lag accessor")
	}
	start := s.Elapsed()
	var arrived, fired time.Duration
	c.SetHandler(func(string, []byte) { arrived = s.Elapsed() - start })
	c.Clock().AfterFunc(time.Second, func() { fired = s.Elapsed() - start })
	_ = b.Send("c", []byte{1})
	s.RunFor(5 * time.Second)
	if arrived != 501*time.Millisecond || fired != 1200*time.Millisecond {
		t.Fatalf("arrived=%v fired=%v", arrived, fired)
	}
	if c.Clock().Now() != s.Now() {
		t.Fatal("host clock")
	}

	// A crash drops in-flight packets and silences timers.
	_ = b.Send("c", []byte{1})
	firedAfterCrash := false
	c.Clock().AfterFunc(time.Millisecond, func() { firedAfterCrash = true })
	c.Crash()
	_ = c.Send("b", []byte{1})
	s.RunFor(5 * time.Second)
	if !c.Down() || firedAfterCrash || nw.Stats().DroppedDown == 0 {
		t.Fatal("crash semantics")
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	if err := a.Send("b", []byte{1}); err == nil {
		t.Fatal("send after close")
	}
	if len(traced) == 0 || traced[0].Dropped != "partition" {
		t.Fatalf("trace: %+v", traced)
	}
	st := nw.Stats()
	if st.DroppedPartition != 1 || st.DroppedUnknown != 1 {
		t.Fatalf("stats %+v", st)
	}
}

func TestLinkValidation(t *testing.T) {
	s := New(1)
	if _, err := NewNetwork(s, LinkConfig{Loss: 2}); err == nil {
		t.Fatal("loss > 1 accepted")
	}
	nw, _ := NewNetwork(s, LinkConfig{})
	if nw.SetDefaultLink(LinkConfig{Delay: -1}) == nil || nw.SetLink("a", "b", LinkConfig{Loss: -0.1}) == nil {
		t.Fatal("invalid link accepted")
	}
}
