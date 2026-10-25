package swim

import (
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sort"
	"sync"
	"time"

	"github.com/archit2501/swimring/clock"
	"github.com/archit2501/swimring/transport"
)

// Errors returned by Node methods.
var (
	ErrAlreadyStarted = errors.New("swim: node already started")
	ErrStopped        = errors.New("swim: node stopped")
	ErrNoSeeds        = errors.New("swim: no seed addresses")
)

// TraceKind classifies protocol trace records.
type TraceKind uint8

// Trace kinds.
const (
	// TraceProbe: a probe period started against Peer.
	TraceProbe TraceKind = iota
	// TraceProbeOK: the probe of Peer was acknowledged within its period.
	TraceProbeOK
	// TraceProbeFail: the probe of Peer failed (no ack, direct or indirect).
	TraceProbeFail
	// TraceRefute: the node refuted a suspicion or death claim about itself.
	TraceRefute
)

func (k TraceKind) String() string {
	switch k {
	case TraceProbe:
		return "probe"
	case TraceProbeOK:
		return "probe-ok"
	case TraceProbeFail:
		return "probe-fail"
	case TraceRefute:
		return "refute"
	default:
		return fmt.Sprintf("trace(%d)", uint8(k))
	}
}

// Trace is a protocol-internal observation, for metrics and experiments.
type Trace struct {
	Kind TraceKind
	Peer string
	At   time.Time
	// Health is the node's Local Health Multiplier score at the time.
	Health int
}

// Stats are cumulative protocol counters.
type Stats struct {
	PacketsSent      uint64
	PacketsReceived  uint64
	BytesSent        uint64
	DecodeErrors     uint64
	PingsSent        uint64
	PingReqsSent     uint64
	AcksReceived     uint64
	NacksSent        uint64
	NacksReceived    uint64
	Probes           uint64
	ProbeFailures    uint64
	SuspicionsRaised uint64
	DeathsDeclared   uint64
	Refutations      uint64
	Misdirected      uint64
	SyncsSent        uint64
	SyncsReceived    uint64
}

// Node is one SWIM cluster member.
type Node struct {
	cfg   Config
	tr    transport.Transport
	clk   clock.Clock
	rng   *rand.Rand
	log   *slog.Logger
	trace func(Trace)

	mu         sync.Mutex
	self       *member
	members    map[string]*member
	order      []*member
	probeOrder []*member
	probeIdx   int
	bq         *broadcastQueue
	health     *awareness
	seq        uint64
	acks       map[uint64]*ackHandler
	probe      *probeState
	started    bool
	stopped    bool
	leaving    bool
	timers     map[string]clock.Timer
	joinSeeds  []string
	joinTries  int
	notify     []func()
	stats      Stats
}

// New creates a node that speaks over tr and keeps time with clk. rng drives
// every random choice (probe order, relay selection, timer offsets); pass a
// seeded source for reproducible runs. The node installs itself as tr's
// packet handler. Call Start to begin probing and Join to contact a cluster.
func New(cfg Config, tr transport.Transport, clk clock.Clock, rng *rand.Rand) (*Node, error) {
	if tr == nil || clk == nil || rng == nil {
		return nil, fmt.Errorf("%w: transport, clock and rng are required", ErrInvalidConfig)
	}
	if cfg.Addr == "" {
		cfg.Addr = tr.LocalAddr()
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	n := &Node{
		cfg:     cfg,
		tr:      tr,
		clk:     clk,
		rng:     rng,
		log:     logger.With("node", cfg.Name),
		members: make(map[string]*member),
		bq:      newBroadcastQueue(),
		health:  newAwareness(cfg.AwarenessMaxMultiplier),
		acks:    make(map[uint64]*ackHandler),
		timers:  make(map[string]clock.Timer),
	}
	n.self = &member{name: cfg.Name, addr: cfg.Addr, state: StateAlive, since: clk.Now()}
	n.members[cfg.Name] = n.self
	n.order = append(n.order, n.self)
	tr.SetHandler(n.HandlePacket)
	return n, nil
}

// SetTrace installs a trace hook (nil removes it). Like Config.Events it is
// called without internal locks held.
func (n *Node) SetTrace(f func(Trace)) {
	n.mu.Lock()
	n.trace = f
	n.mu.Unlock()
}

// Name returns the node's member name.
func (n *Node) Name() string { return n.cfg.Name }

// Addr returns the node's advertised transport address.
func (n *Node) Addr() string { return n.cfg.Addr }

// Config returns the node's effective configuration.
func (n *Node) Config() Config { return n.cfg }

// unlock releases the lock and then runs queued notifications (events and
// traces) in order. Every exported entry point ends with unlock, so user
// callbacks never run under the node's lock and may call back into it.
// Notifications are ordered within one entry point; with the real clock,
// callbacks from concurrent entry points can interleave.
func (n *Node) unlock() {
	pending := n.notify
	n.notify = nil
	n.mu.Unlock()
	for _, f := range pending {
		f()
	}
}

func (n *Node) emit(ev Event) {
	if h := n.cfg.Events; h != nil {
		n.notify = append(n.notify, func() { h(ev) })
	}
}

func (n *Node) traceEvent(kind TraceKind, peer string) {
	if h := n.trace; h != nil {
		t := Trace{Kind: kind, Peer: peer, At: n.clk.Now(), Health: n.health.Score()}
		n.notify = append(n.notify, func() { h(t) })
	}
}

// Start begins the probe loop and the anti-entropy timers. The first probe
// is offset by a random fraction of the protocol period so that nodes
// started together do not probe in lockstep.
func (n *Node) Start() error {
	n.mu.Lock()
	defer n.unlock()
	if n.stopped {
		return ErrStopped
	}
	if n.started {
		return ErrAlreadyStarted
	}
	n.started = true
	// Announce ourselves: whoever we ping or answer learns that we are
	// alive, so a join reaches members that the seed's gossip misses.
	n.bq.Queue(n.self.toUpdate())
	n.setTimer("tick", n.jitter(n.cfg.ProbeInterval), n.tick)
	if n.cfg.PushPullInterval > 0 {
		n.setTimer("pushpull", n.cfg.PushPullInterval+n.jitter(n.cfg.PushPullInterval), n.pushPull)
	}
	if n.cfg.ReconnectInterval > 0 {
		n.setTimer("reconnect", n.cfg.ReconnectInterval+n.jitter(n.cfg.ReconnectInterval), n.reconnect)
	}
	n.log.Debug("started", "addr", n.cfg.Addr)
	return nil
}

func (n *Node) jitter(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	return time.Duration(n.rng.Int64N(int64(d)))
}

// setTimer (re)arms the named timer. Must hold mu.
func (n *Node) setTimer(name string, d time.Duration, f func()) {
	if t, ok := n.timers[name]; ok {
		t.Stop()
	}
	n.timers[name] = n.clk.AfterFunc(d, f)
}

// Join contacts the seed addresses with a full-state exchange and retries
// every JoinRetryInterval until some other member is known or
// JoinMaxAttempts is exhausted. It returns immediately; use NumLive or the
// Events hook to observe progress.
func (n *Node) Join(seeds []string) error {
	n.mu.Lock()
	defer n.unlock()
	if n.stopped {
		return ErrStopped
	}
	var valid []string
	for _, s := range seeds {
		if s != "" && s != n.cfg.Addr {
			valid = append(valid, s)
		}
	}
	if len(valid) == 0 {
		return ErrNoSeeds
	}
	n.joinSeeds = valid
	n.joinTries = 0
	n.attemptJoin()
	return nil
}

func (n *Node) attemptJoin() {
	if n.stopped || n.numLive() > 1 {
		return
	}
	if n.joinTries >= n.cfg.JoinMaxAttempts {
		n.log.Warn("join gave up", "attempts", n.joinTries, "seeds", n.joinSeeds)
		return
	}
	n.joinTries++
	for _, s := range n.joinSeeds {
		n.sendSync(s, flagWantReply|flagJoin)
	}
	n.setTimer("join", n.cfg.JoinRetryInterval, func() {
		n.mu.Lock()
		defer n.unlock()
		n.attemptJoin()
	})
}

// Leave announces a graceful departure. The announcement is piggybacked on
// the node's ongoing traffic, so keep the node running for a few protocol
// periods (see LeaveAndStop) before stopping it.
func (n *Node) Leave() error {
	n.mu.Lock()
	defer n.unlock()
	if n.stopped {
		return ErrStopped
	}
	n.leaving = true
	n.self.state = StateLeft
	n.self.since = n.clk.Now()
	n.bq.Queue(n.self.toUpdate())
	return nil
}

// LeaveAndStop calls Leave and schedules Stop after grace.
func (n *Node) LeaveAndStop(grace time.Duration) error {
	if err := n.Leave(); err != nil {
		return err
	}
	n.mu.Lock()
	defer n.unlock()
	n.setTimer("leave", grace, func() { n.Stop() })
	return nil
}

// Stop halts all protocol activity. Inbound packets are ignored afterwards.
// The transport is not closed; its owner closes it.
func (n *Node) Stop() {
	n.mu.Lock()
	defer n.unlock()
	if n.stopped {
		return
	}
	n.stopped = true
	for _, t := range n.timers {
		t.Stop()
	}
	if n.probe != nil && n.probe.timeout != nil {
		n.probe.timeout.Stop()
	}
	n.probe = nil
	for seq, h := range n.acks {
		if h.timer != nil {
			h.timer.Stop()
		}
		delete(n.acks, seq)
	}
	for _, m := range n.members {
		if m.suspicion != nil {
			m.suspicion.Stop()
		}
		if m.purge != nil {
			m.purge.Stop()
		}
	}
	n.log.Debug("stopped")
}

// Stopped reports whether Stop has been called.
func (n *Node) Stopped() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.stopped
}

// Members returns a snapshot of every known member (including tombstones and
// the local node), sorted by name.
func (n *Node) Members() []Member {
	n.mu.Lock()
	defer n.mu.Unlock()
	out := make([]Member, 0, len(n.order))
	for _, m := range n.order {
		out = append(out, m.snapshot())
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// LiveMembers returns the Alive and Suspect members, sorted by name.
func (n *Node) LiveMembers() []Member {
	all := n.Members()
	out := all[:0]
	for _, m := range all {
		if m.State.Live() {
			out = append(out, m)
		}
	}
	return out
}

// Member returns the snapshot of the named member.
func (n *Node) Member(name string) (Member, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	m, ok := n.members[name]
	if !ok {
		return Member{}, false
	}
	return m.snapshot(), true
}

// NumLive returns the number of Alive and Suspect members, including self.
func (n *Node) NumLive() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.numLive()
}

func (n *Node) numLive() int {
	c := 0
	for _, m := range n.order {
		if m.state.Live() {
			c++
		}
	}
	return c
}

// LocalHealth returns the Lifeguard Local Health Multiplier score
// (0 = healthy; timeouts are scaled by score+1).
func (n *Node) LocalHealth() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.health.Score()
}

// Incarnation returns the node's own incarnation number.
func (n *Node) Incarnation() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.self.incarnation
}

// Stats returns a snapshot of the protocol counters.
func (n *Node) Stats() Stats {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.stats
}

// PendingBroadcasts returns the number of updates waiting to be gossiped.
func (n *Node) PendingBroadcasts() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.bq.Len()
}

// HandlePacket processes one inbound packet. It is installed as the
// transport handler by New and is exported for custom transports.
func (n *Node) HandlePacket(from string, b []byte) {
	n.mu.Lock()
	defer n.unlock()
	if n.stopped {
		return
	}
	n.stats.PacketsReceived++
	p, err := decodePacket(b)
	if err != nil {
		n.stats.DecodeErrors++
		n.log.Debug("dropping malformed packet", "from", from, "err", err)
		return
	}
	if p.Type == msgSync {
		n.handleSync(from, p)
		return
	}
	// Merge gossip first so that, for example, a refutation triggered by a
	// piggybacked suspicion rides on the ack we are about to send.
	for _, u := range p.Updates {
		n.applyUpdate(u, false, "")
	}
	switch p.Type {
	case msgPing:
		n.handlePing(from, p)
	case msgPingReq:
		n.handlePingReq(from, p)
	case msgAck:
		n.handleAck(p)
	case msgNack:
		n.handleNack(p)
	}
}

// send piggybacks queued updates onto p and transmits it. prio, if set, is
// included first regardless of the queue (Lifeguard buddy system).
func (n *Node) send(addr string, p *packet, prio *update) {
	budget := n.cfg.MaxPacketSize - p.headerLen() - 3
	skip := ""
	if prio != nil {
		p.Updates = append(p.Updates, *prio)
		budget -= prio.encodedLen()
		skip = prio.Name
	}
	p.Updates = append(p.Updates, n.bq.Take(budget, retransmitLimit(n.cfg.RetransmitMult, n.numLive()), skip)...)
	n.transmit(addr, p)
}

func (n *Node) transmit(addr string, p *packet) {
	buf, err := p.encode()
	if err != nil {
		n.log.Warn("encode failed", "type", p.Type, "err", err)
		return
	}
	if err := n.tr.Send(addr, buf); err != nil {
		n.log.Debug("send failed", "to", addr, "type", p.Type, "err", err)
		return
	}
	n.stats.PacketsSent++
	n.stats.BytesSent += uint64(len(buf))
}

func (n *Node) nextSeq() uint64 {
	n.seq++
	return n.seq
}
