package cache

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/archit2501/swimring/clock"
	"github.com/archit2501/swimring/ring"
	"github.com/archit2501/swimring/swim"
	"github.com/archit2501/swimring/transport"
)

// Limits keep every request inside one datagram.
const (
	MaxKeyLen   = 255
	MaxValueLen = 1024
)

// Errors returned by cache operations.
var (
	ErrQuorum        = errors.New("cache: quorum not reached before timeout")
	ErrKeyInvalid    = errors.New("cache: key empty or longer than MaxKeyLen")
	ErrValueTooLarge = errors.New("cache: value longer than MaxValueLen")
	ErrNoReplicas    = errors.New("cache: ring is empty")
	ErrStopped       = errors.New("cache: node stopped")
)

// Membership is the view of the cluster the cache needs; *swim.Node
// implements it.
type Membership interface {
	Name() string
	LiveMembers() []swim.Member
	Member(name string) (swim.Member, bool)
}

// Config parameterises a cache Node.
type Config struct {
	// ReplicationFactor is the number of replicas per key.
	ReplicationFactor int
	// WriteQuorum and ReadQuorum are the acks needed for success. When
	// fewer replicas exist than the quorum (a cluster smaller than the
	// replication factor), every existing replica must answer instead.
	WriteQuorum int
	ReadQuorum  int
	// RequestTimeout bounds every operation.
	RequestTimeout time.Duration
	// VNodes is the number of virtual nodes per member on the ring.
	VNodes int
	// RebalanceDelay debounces hand-off after membership changes.
	RebalanceDelay time.Duration
	// SweepInterval is the period of the ownership sweep that hands off
	// and drops keys this node holds but no longer replicates (for example
	// copies that arrived from a peer with a stale ring view).
	SweepInterval time.Duration
	// TombstoneTTL is how long deletes are remembered.
	TombstoneTTL time.Duration
	// Logger receives logs; nil discards them.
	Logger *slog.Logger
}

// DefaultConfig returns N=3, W=2, R=2 with 128 virtual nodes.
func DefaultConfig() Config {
	return Config{
		ReplicationFactor: 3,
		WriteQuorum:       2,
		ReadQuorum:        2,
		RequestTimeout:    500 * time.Millisecond,
		VNodes:            128,
		RebalanceDelay:    200 * time.Millisecond,
		SweepInterval:     5 * time.Second,
		TombstoneTTL:      10 * time.Minute,
	}
}

// Validate checks the configuration.
func (c Config) Validate() error {
	switch {
	case c.ReplicationFactor < 1:
		return errors.New("cache: ReplicationFactor must be >= 1")
	case c.WriteQuorum < 1 || c.WriteQuorum > c.ReplicationFactor:
		return errors.New("cache: WriteQuorum must be in [1, ReplicationFactor]")
	case c.ReadQuorum < 1 || c.ReadQuorum > c.ReplicationFactor:
		return errors.New("cache: ReadQuorum must be in [1, ReplicationFactor]")
	case c.RequestTimeout <= 0 || c.RebalanceDelay <= 0 || c.SweepInterval <= 0 || c.TombstoneTTL <= 0:
		return errors.New("cache: durations must be positive")
	case c.VNodes < 1:
		return errors.New("cache: VNodes must be >= 1")
	}
	return nil
}

// Stats are cumulative cache counters.
type Stats struct {
	Puts, Gets, Deletes uint64
	QuorumFailures      uint64
	ReadRepairs         uint64
	HandoffsSent        uint64
	KeysHandedOff       uint64
	TombstonesExpired   uint64
	RequestsServed      uint64
	MalformedMessages   uint64
}

// Node is one cache replica and request coordinator.
type Node struct {
	cfg  Config
	self string
	mem  Membership
	tr   transport.Transport
	clk  clock.Clock
	log  *slog.Logger
	ring *ring.Tracker

	mu        sync.Mutex
	store     *store
	lamport   uint64
	nextID    uint64
	pending   map[uint64]*pendingReq
	settled   *ring.Ring // ring as of the last completed rebalance
	rebalance clock.Timer
	sweep     clock.Timer
	ownership clock.Timer
	stopped   bool
	notify    []func()
	stats     Stats
}

type pendingReq struct {
	replica string
	onResp  func(m *message)
}

// New creates a cache node. tr should be a dedicated channel (see
// transport.Mux) that reaches the same addresses as mem's members. Wire
// HandleMembership into the SWIM node's event hook.
func New(cfg Config, mem Membership, tr transport.Transport, clk clock.Clock) (*Node, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if mem == nil || tr == nil || clk == nil {
		return nil, errors.New("cache: membership, transport and clock are required")
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	r, err := ring.New(cfg.VNodes, nil)
	if err != nil {
		return nil, err
	}
	names := []string{mem.Name()}
	for _, m := range mem.LiveMembers() {
		names = append(names, m.Name)
	}
	if r, err = r.Add(names...); err != nil {
		return nil, err
	}
	c := &Node{
		cfg:     cfg,
		self:    mem.Name(),
		mem:     mem,
		tr:      tr,
		clk:     clk,
		log:     logger.With("node", mem.Name()),
		ring:    ring.NewTracker(r),
		store:   newStore(),
		pending: make(map[uint64]*pendingReq),
		settled: r,
	}
	tr.SetHandler(c.handle)
	c.sweep = clk.AfterFunc(cfg.TombstoneTTL/2, c.sweepTombstones)
	c.ownership = clk.AfterFunc(cfg.SweepInterval, c.sweepOwnership)
	return c, nil
}

// Ring returns the current ring.
func (c *Node) Ring() *ring.Ring { return c.ring.Load() }

// Stats returns a snapshot of the counters.
func (c *Node) Stats() Stats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.stats
}

// LocalKeys returns the keys this replica stores (including tombstones),
// sorted.
func (c *Node) LocalKeys() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.store.keys()
}

// LocalEntry returns this replica's copy of key.
func (c *Node) LocalEntry(key string) (Entry, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.store.get(key)
}

// Stop cancels timers and fails nothing further; in-flight callbacks whose
// replies never arrive are not invoked.
func (c *Node) Stop() {
	c.mu.Lock()
	defer c.unlock()
	c.stopped = true
	if c.rebalance != nil {
		c.rebalance.Stop()
	}
	c.sweep.Stop()
	c.ownership.Stop()
}

func (c *Node) unlock() {
	pending := c.notify
	c.notify = nil
	c.mu.Unlock()
	for _, f := range pending {
		f()
	}
}

// HandleMembership updates the ring from a SWIM event and schedules a
// rebalance. Suspect members stay on the ring; only Dead/Left members are
// removed, so a transient suspicion does not shuffle data.
//
// The event only says which member changed: its current state is re-read
// from the membership layer, so events delivered out of order by concurrent
// goroutines (possible with the real clock) cannot leave the ring stale.
func (c *Node) HandleMembership(ev swim.Event) {
	name := ev.Member.Name
	if name == c.self {
		return
	}
	cur, known := c.mem.Member(name)
	live := known && cur.State.Live()
	prev, next := c.ring.Update(func(r *ring.Ring) *ring.Ring {
		if live {
			nr, err := r.Add(name)
			if err != nil {
				return r
			}
			return nr
		}
		return r.Remove(name)
	})
	if prev == next {
		return
	}
	c.mu.Lock()
	defer c.unlock()
	if c.stopped {
		return
	}
	if c.rebalance != nil {
		c.rebalance.Stop()
	}
	c.rebalance = c.clk.AfterFunc(c.cfg.RebalanceDelay, c.runRebalance)
}

func validate(key string, value []byte) error {
	if key == "" || len(key) > MaxKeyLen {
		return ErrKeyInvalid
	}
	if len(value) > MaxValueLen {
		return ErrValueTooLarge
	}
	return nil
}

func (c *Node) nextVersion() Version {
	c.lamport++
	return Version{Counter: c.lamport, Node: c.self}
}

func (c *Node) observe(v Version) {
	if v.Counter > c.lamport {
		c.lamport = v.Counter
	}
}

func quorum(want, replicas int) int {
	if replicas < want {
		return replicas
	}
	return want
}

// PutAsync writes key=value and calls done once WriteQuorum replicas have
// acknowledged, or with ErrQuorum after RequestTimeout. done runs without
// internal locks held.
func (c *Node) PutAsync(key string, value []byte, done func(error)) {
	if err := validate(key, value); err != nil {
		done(err)
		return
	}
	c.mu.Lock()
	defer c.unlock()
	c.stats.Puts++
	c.write(key, Entry{Value: slices.Clone(value), Version: c.nextVersion()}, done)
}

// DeleteAsync removes key by writing a tombstone with write-quorum
// semantics.
func (c *Node) DeleteAsync(key string, done func(error)) {
	if err := validate(key, nil); err != nil {
		done(err)
		return
	}
	c.mu.Lock()
	defer c.unlock()
	c.stats.Deletes++
	c.write(key, Entry{Deleted: true, Version: c.nextVersion()}, done)
}

// write sends e to every replica of key. Must hold mu.
func (c *Node) write(key string, e Entry, done func(error)) {
	if c.stopped {
		c.later(func() { done(ErrStopped) })
		return
	}
	replicas := c.ring.Load().LookupN(key, c.cfg.ReplicationFactor)
	if len(replicas) == 0 {
		c.later(func() { done(ErrNoReplicas) })
		return
	}
	need := quorum(c.cfg.WriteQuorum, len(replicas))
	acks := 0
	finished := false
	var timer clock.Timer
	var ids []uint64
	finish := func(err error) {
		if finished {
			return
		}
		finished = true
		if timer != nil {
			timer.Stop()
		}
		for _, id := range ids {
			delete(c.pending, id)
		}
		if err != nil {
			c.stats.QuorumFailures++
		}
		c.later(func() { done(err) })
	}
	ack := func() {
		acks++
		if acks >= need {
			finish(nil)
		}
	}
	for _, r := range replicas {
		if r == c.self {
			c.applyLocal(key, e)
			ack()
			continue
		}
		id, ok := c.request(r, &message{Op: opPutReq, Key: key, Entry: e}, func(m *message) {
			if m.OK {
				ack()
			}
		})
		if ok {
			ids = append(ids, id)
		}
	}
	if !finished {
		timer = c.clk.AfterFunc(c.cfg.RequestTimeout, func() {
			c.mu.Lock()
			defer c.unlock()
			finish(ErrQuorum)
		})
	}
}

// GetAsync reads key from ReadQuorum replicas and returns the newest
// version. Once every replica answered (or the timeout fired), replicas
// that returned an older version are repaired in the background.
func (c *Node) GetAsync(key string, done func(value []byte, found bool, err error)) {
	if err := validate(key, nil); err != nil {
		done(nil, false, err)
		return
	}
	c.mu.Lock()
	defer c.unlock()
	c.stats.Gets++
	if c.stopped {
		c.later(func() { done(nil, false, ErrStopped) })
		return
	}
	replicas := c.ring.Load().LookupN(key, c.cfg.ReplicationFactor)
	if len(replicas) == 0 {
		c.later(func() { done(nil, false, ErrNoReplicas) })
		return
	}
	need := quorum(c.cfg.ReadQuorum, len(replicas))
	replies := make(map[string]Entry, len(replicas))
	var newest Entry
	answered, closed := false, false
	var timer clock.Timer
	var ids []uint64

	closeOut := func() {
		if closed {
			return
		}
		closed = true
		if timer != nil {
			timer.Stop()
		}
		for _, id := range ids {
			delete(c.pending, id)
		}
		if !answered {
			answered = true
			c.stats.QuorumFailures++
			c.later(func() { done(nil, false, ErrQuorum) })
			return
		}
		c.repair(key, newest, replicas, replies)
	}
	reply := func(replica string, e Entry, found bool) {
		if !found {
			e = Entry{}
		}
		replies[replica] = e
		if newest.Version.Less(e.Version) {
			newest = e
		}
		if !answered && len(replies) >= need {
			answered = true
			val, ok := slices.Clone(newest.Value), !newest.Version.IsZero() && !newest.Deleted
			c.later(func() { done(val, ok, nil) })
		}
		if len(replies) == len(replicas) {
			closeOut()
		}
	}
	for _, r := range replicas {
		if r == c.self {
			e, ok := c.store.get(key)
			reply(r, e, ok)
			continue
		}
		replica := r
		id, ok := c.request(r, &message{Op: opGetReq, Key: key}, func(m *message) {
			c.observe(m.Entry.Version)
			reply(replica, m.Entry, m.Found)
		})
		if ok {
			ids = append(ids, id)
		}
	}
	if !closed {
		timer = c.clk.AfterFunc(c.cfg.RequestTimeout, func() {
			c.mu.Lock()
			defer c.unlock()
			closeOut()
		})
	}
}

// repair pushes newest to every replica that answered with something older.
func (c *Node) repair(key string, newest Entry, replicas []string, replies map[string]Entry) {
	if newest.Version.IsZero() {
		return
	}
	for _, r := range replicas {
		got, ok := replies[r]
		if !ok || !got.Version.Less(newest.Version) {
			continue
		}
		c.stats.ReadRepairs++
		if r == c.self {
			c.applyLocal(key, newest)
			continue
		}
		c.request(r, &message{Op: opPutReq, Key: key, Entry: newest}, nil)
	}
}

func (c *Node) applyLocal(key string, e Entry) bool {
	c.observe(e.Version)
	e.Stored = c.clk.Now()
	return c.store.apply(key, e)
}

// later queues f to run after the lock is released.
func (c *Node) later(f func()) { c.notify = append(c.notify, f) }

// request sends m to the named replica. onResp (may be nil) runs under the
// lock when the reply arrives. Unanswered requests are forgotten after
// RequestTimeout. It returns the request id and whether it was sent.
func (c *Node) request(replica string, m *message, onResp func(*message)) (uint64, bool) {
	mem, ok := c.mem.Member(replica)
	if !ok || mem.Addr == "" {
		return 0, false
	}
	c.nextID++
	m.ID = c.nextID
	id := m.ID
	if onResp != nil {
		c.pending[id] = &pendingReq{replica: replica, onResp: onResp}
		c.clk.AfterFunc(c.cfg.RequestTimeout, func() {
			c.mu.Lock()
			defer c.unlock()
			delete(c.pending, id)
		})
	}
	if err := c.tr.Send(mem.Addr, m.encode()); err != nil {
		c.log.Debug("send failed", "to", replica, "err", err)
		delete(c.pending, id)
		return 0, false
	}
	return id, true
}

// handle serves requests and routes responses.
func (c *Node) handle(from string, payload []byte) {
	m, err := decodeMessage(payload)
	c.mu.Lock()
	defer c.unlock()
	if err != nil {
		c.stats.MalformedMessages++
		c.log.Debug("malformed cache message", "from", from, "err", err)
		return
	}
	if c.stopped {
		return
	}
	switch m.Op {
	case opPutReq:
		c.stats.RequestsServed++
		c.applyLocal(m.Key, m.Entry)
		c.reply(from, &message{Op: opPutResp, ID: m.ID, Key: m.Key, OK: true})
	case opGetReq:
		c.stats.RequestsServed++
		e, ok := c.store.get(m.Key)
		c.reply(from, &message{Op: opGetResp, ID: m.ID, Key: m.Key, Found: ok, Entry: e})
	case opPutResp, opGetResp:
		p, ok := c.pending[m.ID]
		if !ok {
			return
		}
		delete(c.pending, m.ID)
		p.onResp(m)
	}
}

func (c *Node) reply(to string, m *message) {
	if err := c.tr.Send(to, m.encode()); err != nil {
		c.log.Debug("reply failed", "to", to, "err", err)
	}
}

// runRebalance runs after membership changes settle: it hands every local
// key to replicas that joined its replica set since the last rebalance,
// and hands off then drops keys this node no longer replicates.
func (c *Node) runRebalance() {
	c.mu.Lock()
	defer c.unlock()
	if c.stopped {
		return
	}
	c.handOff(true)
}

// sweepOwnership periodically re-runs the hand-off for keys this node does
// not replicate. Such orphans appear when a peer with a stale ring sends us
// a copy, or when an earlier hand-off was not acknowledged.
func (c *Node) sweepOwnership() {
	c.mu.Lock()
	defer c.unlock()
	if c.stopped {
		return
	}
	c.handOff(false)
	c.ownership = c.clk.AfterFunc(c.cfg.SweepInterval, c.sweepOwnership)
}

// handOff implements both passes. With ringChanged, owned keys are pushed
// to replicas new since the last settled ring; in every pass, keys this
// node no longer replicates are sent to their whole replica set and dropped
// locally once a write quorum of it acknowledged. Cost O(K·(log P + R)) for
// K local keys. Must hold mu.
func (c *Node) handOff(ringChanged bool) {
	cur := c.ring.Load()
	old := c.settled
	if ringChanged {
		c.settled = cur
	}
	rf := c.cfg.ReplicationFactor
	for _, key := range c.store.keys() {
		e, _ := c.store.get(key)
		newSet := cur.LookupN(key, rf)
		owned := slices.Contains(newSet, c.self)
		if owned && !ringChanged {
			continue
		}
		oldSet := old.LookupN(key, rf)
		var targets []string
		for _, r := range newSet {
			if r != c.self && (!owned || !slices.Contains(oldSet, r)) {
				targets = append(targets, r)
			}
		}
		if len(targets) == 0 {
			continue
		}
		c.stats.KeysHandedOff++
		need := quorum(c.cfg.WriteQuorum, len(targets))
		acks := 0
		k, v := key, e.Version
		for _, r := range targets {
			c.stats.HandoffsSent++
			var onResp func(*message)
			if !owned {
				onResp = func(m *message) {
					if !m.OK {
						return
					}
					acks++
					if acks == need {
						c.store.remove(k, v)
					}
				}
			}
			c.request(r, &message{Op: opPutReq, Key: key, Entry: e}, onResp)
		}
	}
}

func (c *Node) sweepTombstones() {
	c.mu.Lock()
	defer c.unlock()
	if c.stopped {
		return
	}
	c.stats.TombstonesExpired += uint64(c.store.expireTombstones(c.clk.Now().Add(-c.cfg.TombstoneTTL)))
	c.sweep = c.clk.AfterFunc(c.cfg.TombstoneTTL/2, c.sweepTombstones)
}

// Put is the blocking form of PutAsync for real-clock deployments. Do not
// call it from inside a simulation, whose clock only advances when the
// caller runs the simulator.
func (c *Node) Put(ctx context.Context, key string, value []byte) error {
	ch := make(chan error, 1)
	c.PutAsync(key, value, func(err error) { ch <- err })
	return wait(ctx, ch)
}

// Delete is the blocking form of DeleteAsync (same caveat as Put).
func (c *Node) Delete(ctx context.Context, key string) error {
	ch := make(chan error, 1)
	c.DeleteAsync(key, func(err error) { ch <- err })
	return wait(ctx, ch)
}

// Get is the blocking form of GetAsync (same caveat as Put).
func (c *Node) Get(ctx context.Context, key string) ([]byte, bool, error) {
	type result struct {
		v  []byte
		ok bool
		e  error
	}
	ch := make(chan result, 1)
	c.GetAsync(key, func(v []byte, ok bool, err error) { ch <- result{v, ok, err} })
	select {
	case r := <-ch:
		return r.v, r.ok, r.e
	case <-ctx.Done():
		return nil, false, fmt.Errorf("cache get: %w", ctx.Err())
	}
}

func wait(ctx context.Context, ch <-chan error) error {
	select {
	case err := <-ch:
		return err
	case <-ctx.Done():
		return fmt.Errorf("cache: %w", ctx.Err())
	}
}
