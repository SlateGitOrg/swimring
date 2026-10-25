package swim

import (
	"errors"
	"math/rand/v2"
	"strings"
	"testing"
	"time"

	"github.com/SlateGitOrg/swimring/sim"
)

func TestBroadcastQueueOrderingAndRetirement(t *testing.T) {
	q := newBroadcastQueue()
	q.Queue(update{Name: "a", Incarnation: 1})
	q.Queue(update{Name: "b", Incarnation: 1})
	q.Queue(update{Name: "a", Incarnation: 2}) // replaces the first
	if q.Len() != 2 {
		t.Fatalf("len = %d", q.Len())
	}
	if u, ok := q.Pending("a"); !ok || u.Incarnation != 2 {
		t.Fatalf("pending a = %+v %v", u, ok)
	}
	// Newest first among equally fresh updates.
	got := q.Take(1000, 3, "")
	if len(got) != 2 || got[0].Name != "a" || got[1].Name != "b" {
		t.Fatalf("take = %+v", got)
	}
	q.Queue(update{Name: "c"})
	// The fresh update goes first; both others have been sent once.
	got = q.Take(1000, 3, "")
	if got[0].Name != "c" {
		t.Fatalf("least-transmitted not first: %+v", got)
	}
	q.Take(1000, 3, "")
	// a and b have now been sent 3 times and are retired; c twice.
	if q.Len() != 1 {
		t.Fatalf("len after retirement = %d", q.Len())
	}
	if _, ok := q.Pending("a"); ok {
		t.Fatal("retired update still pending")
	}
}

func TestBroadcastQueueBudgetAndSkip(t *testing.T) {
	q := newBroadcastQueue()
	big := update{Name: strings.Repeat("x", 200)}
	small := update{Name: "s"}
	q.Queue(big)
	q.Queue(small)
	got := q.Take(50, 10, "")
	if len(got) != 1 || got[0].Name != "s" {
		t.Fatalf("budget not respected: %+v", got)
	}
	got = q.Take(1000, 10, "s")
	if len(got) != 1 || got[0].Name != big.Name {
		t.Fatalf("skip not respected: %+v", got)
	}
	if q.Take(0, 10, "") != nil {
		t.Fatal("zero budget returned updates")
	}
}

func TestRetransmitLimit(t *testing.T) {
	for _, tc := range []struct{ n, want int }{{0, 4}, {1, 4}, {9, 4}, {10, 8}, {99, 8}, {100, 12}, {1000, 16}} {
		if got := retransmitLimit(4, tc.n); got != tc.want {
			t.Errorf("n=%d: %d, want %d", tc.n, got, tc.want)
		}
	}
}

func TestSuspicionTimeoutFormula(t *testing.T) {
	minT, maxT := 2*time.Second, 12*time.Second
	if got := suspicionTimeout(0, 0, minT, maxT); got != minT {
		t.Fatalf("k=0: %v", got)
	}
	if got := suspicionTimeout(0, 3, minT, maxT); got != maxT {
		t.Fatalf("no confirmations: %v", got)
	}
	if got := suspicionTimeout(3, 3, minT, maxT); got != minT {
		t.Fatalf("all confirmations: %v", got)
	}
	prev := maxT
	for c := 1; c <= 3; c++ {
		got := suspicionTimeout(c, 3, minT, maxT)
		if got >= prev || got < minT {
			t.Fatalf("c=%d: %v not decreasing towards min", c, got)
		}
		prev = got
	}
	if got := minSuspicionTimeout(4, 1000, time.Second); got != 12*time.Second {
		t.Fatalf("min timeout n=1000: %v", got)
	}
	if got := minSuspicionTimeout(4, 3, time.Second); got != 4*time.Second {
		t.Fatalf("min timeout floors log10 at 1: %v", got)
	}
}

func TestSuspicionConfirmShortensTimer(t *testing.T) {
	s := sim.New(1)
	fired := time.Duration(-1)
	sus := newSuspicion(s.Clock(), "a", 2, 2*time.Second, 10*time.Second, func() { fired = s.Elapsed() })
	s.RunFor(time.Second)
	if sus.Confirm("a") {
		t.Fatal("the originator confirmed its own suspicion")
	}
	if !sus.Confirm("b") || sus.Confirm("b") {
		t.Fatal("confirmation from b should count exactly once")
	}
	if !sus.Confirm("c") || sus.Confirm("d") {
		t.Fatal("confirmations should cap at k")
	}
	if sus.Confirmations() != 2 {
		t.Fatalf("confirmations = %d", sus.Confirmations())
	}
	s.RunFor(time.Minute)
	if fired != 2*time.Second {
		t.Fatalf("fired at %v, want the 2s minimum", fired)
	}
}

func TestAwareness(t *testing.T) {
	a := newAwareness(4)
	if a.Apply(-1) != 0 || a.Apply(10) != 3 || a.Scale(time.Second) != 4*time.Second {
		t.Fatal("awareness clamping/scaling")
	}
	if a.Apply(-2) != 1 || a.Score() != 1 {
		t.Fatal("awareness decrement")
	}
}

func TestConfigValidate(t *testing.T) {
	if err := DefaultConfig("x").Validate(); err != nil {
		t.Fatal(err)
	}
	muts := []func(*Config){
		func(c *Config) { c.Name = "" },
		func(c *Config) { c.Name = strings.Repeat("n", 300) },
		func(c *Config) { c.Addr = strings.Repeat("a", 300) },
		func(c *Config) { c.ProbeInterval = 0 },
		func(c *Config) { c.ProbeTimeout = c.ProbeInterval },
		func(c *Config) { c.IndirectChecks = -1 },
		func(c *Config) { c.RetransmitMult = 0 },
		func(c *Config) { c.SuspicionMult = 0 },
		func(c *Config) { c.SuspicionMaxTimeoutMult = 0 },
		func(c *Config) { c.AwarenessMaxMultiplier = 0 },
		func(c *Config) { c.MaxPacketSize = 100 },
		func(c *Config) { c.PushPullInterval = -1 },
		func(c *Config) { c.DeadRetention = 0 },
		func(c *Config) { c.JoinMaxAttempts = 0 },
	}
	for i, m := range muts {
		c := DefaultConfig("x")
		m(&c)
		if err := c.Validate(); !errors.Is(err, ErrInvalidConfig) {
			t.Errorf("case %d: err = %v", i, err)
		}
	}
}

func TestNewRequiresDependencies(t *testing.T) {
	s := sim.New(1)
	nw, _ := sim.NewNetwork(s, sim.LinkConfig{})
	h, _ := nw.AddHost("a")
	if _, err := New(DefaultConfig("a"), nil, s.Clock(), s.NewRand()); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("nil transport: %v", err)
	}
	if _, err := New(Config{}, h, s.Clock(), s.NewRand()); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("empty config: %v", err)
	}
	n, err := New(DefaultConfig("a"), h, h.Clock(), s.NewRand())
	if err != nil {
		t.Fatal(err)
	}
	if n.Addr() != "a" || n.Name() != "a" {
		t.Fatal("Addr should default to the transport address")
	}
	if err := n.Join([]string{"", "a"}); !errors.Is(err, ErrNoSeeds) {
		t.Fatalf("join self: %v", err)
	}
	if err := n.Start(); err != nil {
		t.Fatal(err)
	}
	if err := n.Start(); !errors.Is(err, ErrAlreadyStarted) {
		t.Fatalf("double start: %v", err)
	}
	n.Stop()
	n.Stop()
	if err := n.Start(); !errors.Is(err, ErrStopped) {
		t.Fatalf("start after stop: %v", err)
	}
	if err := n.Join([]string{"b"}); !errors.Is(err, ErrStopped) {
		t.Fatalf("join after stop: %v", err)
	}
	if err := n.Leave(); !errors.Is(err, ErrStopped) {
		t.Fatalf("leave after stop: %v", err)
	}
	if err := n.LeaveAndStop(time.Second); !errors.Is(err, ErrStopped) {
		t.Fatalf("leave-and-stop after stop: %v", err)
	}
}

// newIsolated returns a started node whose peers are fed by hand.
func newIsolated(t *testing.T, mutate func(*Config)) (*Node, *sim.Sim, *[]Event) {
	t.Helper()
	s := sim.New(9)
	nw, _ := sim.NewNetwork(s, sim.LinkConfig{})
	h, _ := nw.AddHost("me")
	var evs []Event
	cfg := testConfig("me")
	cfg.Events = func(e Event) { evs = append(evs, e) }
	if mutate != nil {
		mutate(&cfg)
	}
	n, err := New(cfg, h, h.Clock(), rand.New(rand.NewPCG(1, 2)))
	if err != nil {
		t.Fatal(err)
	}
	return n, s, &evs
}

func feed(n *Node, ups ...update) {
	p := &packet{Type: msgAck, Seq: 999, Updates: ups}
	b, _ := p.encode()
	n.HandlePacket("peer", b)
}

func TestMergeRules(t *testing.T) {
	n, _, evs := newIsolated(t, nil)
	alive := func(inc uint64) update { return update{Kind: kindAlive, Name: "x", Addr: "x:1", Incarnation: inc} }
	sus := func(inc uint64, from string) update {
		return update{Kind: kindSuspect, Name: "x", Addr: "x:1", Incarnation: inc, From: from}
	}
	state := func() (State, uint64) {
		m, _ := n.Member("x")
		return m.State, m.Incarnation
	}
	expect := func(step string, st State, inc uint64) {
		t.Helper()
		if gs, gi := state(); gs != st || gi != inc {
			t.Fatalf("%s: %v/%d, want %v/%d", step, gs, gi, st, inc)
		}
	}

	feed(n, sus(0, "y"), update{Kind: kindDead, Name: "x", Incarnation: 0}, update{Kind: kindAlive, Name: "noaddr"})
	if _, ok := n.Member("x"); ok {
		t.Fatal("suspect/dead about an unknown member must be ignored")
	}
	if _, ok := n.Member("noaddr"); ok {
		t.Fatal("alive without address must be ignored")
	}
	feed(n, alive(1))
	expect("join", StateAlive, 1)
	feed(n, alive(1))
	expect("same-inc alive is a no-op", StateAlive, 1)
	feed(n, sus(0, "y"))
	expect("older suspect ignored", StateAlive, 1)
	feed(n, sus(1, "y"))
	expect("suspect at same inc overrides alive", StateSuspect, 1)
	feed(n, alive(1))
	expect("alive at same inc does not clear suspicion", StateSuspect, 1)
	feed(n, sus(1, "z"))
	expect("confirmation keeps suspect", StateSuspect, 1)
	feed(n, alive(2))
	expect("refutation", StateAlive, 2)
	feed(n, sus(3, "y"))
	expect("suspect with higher inc", StateSuspect, 3)
	feed(n, update{Kind: kindDead, Name: "x", Addr: "x:1", Incarnation: 2, From: "y"})
	expect("older dead ignored", StateSuspect, 3)
	feed(n, update{Kind: kindDead, Name: "x", Addr: "x:1", Incarnation: 3, From: "y"})
	expect("dead", StateDead, 3)
	feed(n, sus(3, "y"), alive(3))
	expect("tombstone resists same-inc updates", StateDead, 3)
	feed(n, update{Kind: kindLeft, Name: "x", Addr: "x:1", Incarnation: 3})
	expect("left over dead", StateLeft, 3)
	feed(n, update{Kind: kindLeft, Name: "x", Addr: "x:1", Incarnation: 3})
	expect("repeated left", StateLeft, 3)
	feed(n, update{Kind: kindAlive, Name: "x", Addr: "x:2", Incarnation: 4})
	expect("rejoin with higher inc", StateAlive, 4)
	if m, _ := n.Member("x"); m.Addr != "x:2" {
		t.Fatalf("address not updated on rejoin: %s", m.Addr)
	}

	var kinds []string
	for _, e := range *evs {
		kinds = append(kinds, e.Type.String()+"/"+e.Member.State.String())
	}
	want := "join/alive update/suspect update/alive update/suspect leave/dead join/alive"
	if got := strings.Join(kinds, " "); got != want {
		t.Fatalf("events:\n got %s\nwant %s", got, want)
	}
}

func TestSelfClaimsAreRefuted(t *testing.T) {
	n, _, _ := newIsolated(t, nil)
	feed(n, update{Kind: kindSuspect, Name: "me", Incarnation: 0, From: "x"})
	if n.Incarnation() != 1 || n.Stats().Refutations != 1 || n.LocalHealth() != 1 {
		t.Fatalf("inc=%d stats=%+v lhm=%d", n.Incarnation(), n.Stats(), n.LocalHealth())
	}
	feed(n, update{Kind: kindDead, Name: "me", Incarnation: 5, From: "x"})
	if n.Incarnation() != 6 {
		t.Fatalf("dead claim not refuted: inc=%d", n.Incarnation())
	}
	feed(n, update{Kind: kindAlive, Name: "me", Incarnation: 99})
	if n.Incarnation() != 6 {
		t.Fatal("alive about self must be ignored")
	}
	// A stale claim does not bump the incarnation but re-queues our alive.
	n.mu.Lock()
	n.bq = newBroadcastQueue()
	n.mu.Unlock()
	feed(n, update{Kind: kindSuspect, Name: "me", Incarnation: 2, From: "x"})
	if n.Incarnation() != 6 || n.PendingBroadcasts() != 1 {
		t.Fatalf("stale claim: inc=%d pending=%d", n.Incarnation(), n.PendingBroadcasts())
	}
	if err := n.Leave(); err != nil {
		t.Fatal(err)
	}
	feed(n, update{Kind: kindDead, Name: "me", Incarnation: 6, From: "x"})
	if n.Incarnation() != 6 {
		t.Fatal("a leaving node must not refute")
	}
	if m, _ := n.Member("me"); m.State != StateLeft {
		t.Fatalf("self state after leave = %v", m.State)
	}
}

func TestSyncDowngradesDeadToSuspect(t *testing.T) {
	n, _, _ := newIsolated(t, nil)
	feed(n, update{Kind: kindAlive, Name: "x", Addr: "x:1", Incarnation: 0})
	p := &packet{Type: msgSync, Source: "peer", Updates: []update{{Kind: kindDead, Name: "x", Addr: "x:1", From: "peer"}}}
	b, _ := p.encode()
	n.HandlePacket("peer", b)
	if m, _ := n.Member("x"); m.State != StateSuspect {
		t.Fatalf("dead via sync should become suspect, got %v", m.State)
	}
}

func TestSyncChunking(t *testing.T) {
	c := newTestCluster(t, 2, 1, lan, func(cfg *Config) { cfg.MaxPacketSize = minPacketSize })
	n := c.nodes[0]
	for i := range 100 {
		feed(n, update{Kind: kindAlive, Name: strings.Repeat("m", 20) + string(rune('A'+i%26)) + string(rune('a'+i/26)), Addr: "addr:1234"})
	}
	var sizes []int
	c.net.SetTrace(func(p sim.Packet) { sizes = append(sizes, p.Size) })
	n.mu.Lock()
	n.sendSync(c.nodes[1].Addr(), flagWantReply)
	n.unlock()
	if len(sizes) < 3 {
		t.Fatalf("101 members should need several %d-byte chunks, got %d packets", minPacketSize, len(sizes))
	}
	for _, s := range sizes {
		if s > minPacketSize {
			t.Fatalf("chunk of %d bytes exceeds MaxPacketSize", s)
		}
	}
	c.sim.RunFor(time.Second)
	if got := c.nodes[1].NumLive(); got != 102 {
		t.Fatalf("receiver learned %d live members, want 102", got)
	}
}

func TestMalformedAndMisdirectedPackets(t *testing.T) {
	n, _, _ := newIsolated(t, nil)
	n.HandlePacket("x", []byte{1, 2, 3})
	if n.Stats().DecodeErrors != 1 {
		t.Fatal("decode error not counted")
	}
	p := &packet{Type: msgPing, Seq: 1, Target: "someone-else", Source: "x"}
	b, _ := p.encode()
	n.HandlePacket("x", b)
	if n.Stats().Misdirected != 1 || n.Stats().PacketsSent != 0 {
		t.Fatalf("misdirected ping answered: %+v", n.Stats())
	}
	ack := &packet{Type: msgAck, Seq: 12345}
	b, _ = ack.encode()
	n.HandlePacket("x", b)
	nack := &packet{Type: msgNack, Seq: 12345}
	b, _ = nack.encode()
	n.HandlePacket("x", b)
	if n.Stats().AcksReceived != 0 || n.Stats().NacksReceived != 0 {
		t.Fatal("unknown ack/nack counted")
	}
	pr := &packet{Type: msgPingReq, Seq: 1, Target: "t", Source: "x"}
	b, _ = pr.encode()
	n.HandlePacket("x", b)
	if n.Stats().PingsSent != 0 {
		t.Fatal("ping-req without target address relayed")
	}
	n.Stop()
	n.HandlePacket("x", []byte{1})
	if n.Stats().PacketsReceived != 5 {
		t.Fatalf("packets after stop counted: %d", n.Stats().PacketsReceived)
	}
}

// With the nack extension a prober that hears every relay's nack keeps its
// health score, while one that hears nothing (it is the slow one) raises it.
func TestNacksAndLocalHealth(t *testing.T) {
	c := newTestCluster(t, 6, 21, lan, nil)
	c.joinAll()
	c.sim.RunFor(5 * time.Second)
	c.hosts[5].Crash()
	c.sim.RunFor(10 * time.Second)
	var nacksSent, nacksRecv uint64
	for _, n := range c.nodes[:5] {
		st := n.Stats()
		nacksSent += st.NacksSent
		nacksRecv += st.NacksReceived
		if n.LocalHealth() != 0 {
			t.Fatalf("%s raised its health score although relays nacked", n.Name())
		}
	}
	if nacksSent == 0 || nacksRecv == 0 {
		t.Fatalf("no nacks exchanged: sent=%d recv=%d", nacksSent, nacksRecv)
	}

	slow := newTestCluster(t, 6, 22, lan, nil)
	slow.joinAll()
	slow.hosts[0].SetLag(1500 * time.Millisecond)
	slow.sim.RunFor(20 * time.Second)
	if slow.nodes[0].LocalHealth() == 0 {
		t.Fatal("slow node never raised its local health score")
	}
}

func TestTraceHook(t *testing.T) {
	c := newTestCluster(t, 3, 23, lan, nil)
	c.joinAll()
	var kinds = map[TraceKind]int{}
	c.nodes[0].SetTrace(func(tr Trace) { kinds[tr.Kind]++ })
	c.sim.RunFor(10 * time.Second)
	if kinds[TraceProbe] == 0 || kinds[TraceProbeOK] == 0 {
		t.Fatalf("trace kinds: %v", kinds)
	}
	c.nodes[0].SetTrace(nil)
}

func TestTombstonePurge(t *testing.T) {
	c := newTestCluster(t, 4, 24, lan, func(cfg *Config) { cfg.DeadRetention = 10 * time.Second })
	c.joinAll()
	c.hosts[3].Crash()
	c.sim.RunFor(2 * time.Minute)
	for _, n := range c.nodes[:3] {
		if _, ok := n.Member(c.nodes[3].Name()); ok {
			t.Fatalf("%s still remembers the purged member", n.Name())
		}
		if len(n.Members()) != 3 || len(n.LiveMembers()) != 3 {
			t.Fatalf("members: %v", n.Members())
		}
	}
}

func TestJoinRetriesUntilSeedAppears(t *testing.T) {
	s := sim.New(5)
	nw, _ := sim.NewNetwork(s, lan)
	ha, _ := nw.AddHost("a")
	cfg := testConfig("a")
	cfg.JoinRetryInterval = time.Second
	cfg.JoinMaxAttempts = 3
	a, err := New(cfg, ha, ha.Clock(), s.NewRand())
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Start(); err != nil {
		t.Fatal(err)
	}
	if err := a.Join([]string{"b"}); err != nil {
		t.Fatal(err)
	}
	s.RunFor(1500 * time.Millisecond)
	hb, _ := nw.AddHost("b")
	b, err := New(testConfig("b"), hb, hb.Clock(), s.NewRand())
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Start(); err != nil {
		t.Fatal(err)
	}
	s.RunFor(5 * time.Second)
	if a.NumLive() != 2 || b.NumLive() != 2 {
		t.Fatalf("join retry failed: a=%d b=%d", a.NumLive(), b.NumLive())
	}
	if a.Stats().SyncsSent < 2 {
		t.Fatalf("expected a retry, syncs sent = %d", a.Stats().SyncsSent)
	}
}

func TestPushPullRepairsMissedGossip(t *testing.T) {
	c := newTestCluster(t, 5, 25, lan, func(cfg *Config) { cfg.PushPullInterval = 2 * time.Second })
	c.joinAll()
	// A late member that only node 0 knows about, with all gossip about it
	// suppressed: only the periodic full-state exchange can spread it.
	late := c.addNode("late", func(cfg *Config) { cfg.PushPullInterval = 0 })
	n := c.nodes[0]
	n.mu.Lock()
	n.aliveNode(update{Kind: kindAlive, Name: "late", Addr: late.Addr()})
	n.bq = newBroadcastQueue()
	n.unlock()
	late.mu.Lock()
	late.aliveNode(update{Kind: kindAlive, Name: n.Name(), Addr: n.Addr()})
	late.bq = newBroadcastQueue()
	late.unlock()
	c.sim.RunFor(30 * time.Second)
	for _, other := range c.nodes[1:5] {
		if m, ok := other.Member("late"); !ok || m.State != StateAlive {
			t.Fatalf("%s never learned about the late member via push-pull", other.Name())
		}
	}
}
