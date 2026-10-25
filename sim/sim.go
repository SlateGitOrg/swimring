// Package sim is a deterministic discrete-event simulator: a virtual clock,
// an event queue and a simulated lossy network with partitions, delay,
// crashed hosts and slow hosts.
//
// Everything runs on the caller's goroutine. Given the same seed and the same
// sequence of API calls, a simulation replays bit-for-bit, which is what makes
// the protocol experiments in this repository reproducible.
package sim

import (
	"container/heap"
	"math/rand/v2"
	"time"

	"github.com/archit2501/swimring/clock"
)

// Epoch is the virtual time at which every simulation starts. A fixed epoch
// keeps traces identical across runs.
var Epoch = time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)

// Sim owns the virtual clock and the event queue.
type Sim struct {
	now       time.Time
	queue     eventQueue
	seq       uint64
	seed      uint64
	streams   uint64
	processed uint64
	rng       *rand.Rand
}

// New creates a simulator whose randomness is derived from seed.
func New(seed uint64) *Sim {
	return &Sim{
		now:  Epoch,
		seed: seed,
		rng:  rand.New(rand.NewPCG(seed, 0x5EED)),
	}
}

// Now returns the current virtual time.
func (s *Sim) Now() time.Time { return s.now }

// Elapsed returns the virtual time since Epoch.
func (s *Sim) Elapsed() time.Duration { return s.now.Sub(Epoch) }

// Processed returns the number of events executed so far.
func (s *Sim) Processed() uint64 { return s.processed }

// Pending returns the number of queued events, including cancelled ones that
// have not been popped yet.
func (s *Sim) Pending() int { return len(s.queue) }

// Rand returns the simulator's own random source (used by Network).
func (s *Sim) Rand() *rand.Rand { return s.rng }

// NewRand returns an independent, deterministic random stream. Each call
// yields a different stream; the sequence of streams depends only on the
// seed, so give every simulated node its own stream for reproducibility.
func (s *Sim) NewRand() *rand.Rand {
	s.streams++
	return rand.New(rand.NewPCG(s.seed, 0xC0FFEE+s.streams))
}

// Event is a scheduled callback. It implements clock.Timer.
type Event struct {
	at       time.Time
	seq      uint64
	fn       func()
	index    int
	canceled bool
	fired    bool
}

// Stop cancels the event. It reports whether the event was still pending.
func (e *Event) Stop() bool {
	if e.fired || e.canceled {
		return false
	}
	e.canceled = true
	return true
}

// Schedule runs fn after d of virtual time. Negative durations are treated
// as zero. Events scheduled for the same instant run in scheduling order.
func (s *Sim) Schedule(d time.Duration, fn func()) *Event {
	if d < 0 {
		d = 0
	}
	s.seq++
	ev := &Event{at: s.now.Add(d), seq: s.seq, fn: fn}
	heap.Push(&s.queue, ev)
	return ev
}

// Step executes the next live event. It returns false when the queue is
// empty.
func (s *Sim) Step() bool {
	for len(s.queue) > 0 {
		ev, ok := heap.Pop(&s.queue).(*Event)
		if !ok {
			return false
		}
		if ev.canceled {
			continue
		}
		s.now = ev.at
		ev.fired = true
		s.processed++
		ev.fn()
		return true
	}
	return false
}

// RunUntil executes events up to and including virtual time t, then sets the
// clock to t.
func (s *Sim) RunUntil(t time.Time) {
	for len(s.queue) > 0 {
		next := s.queue[0]
		if next.canceled {
			heap.Pop(&s.queue)
			continue
		}
		if next.at.After(t) {
			break
		}
		s.Step()
	}
	if t.After(s.now) {
		s.now = t
	}
}

// RunFor advances virtual time by d, executing every event due in between.
func (s *Sim) RunFor(d time.Duration) { s.RunUntil(s.now.Add(d)) }

// RunWhile executes events until cond returns false or limit of virtual time
// elapses. cond is evaluated before every event. It reports whether cond
// became false (true) or the limit was hit (false).
func (s *Sim) RunWhile(cond func() bool, limit time.Duration) bool {
	deadline := s.now.Add(limit)
	for cond() {
		if len(s.queue) == 0 {
			s.now = deadline
			return !cond()
		}
		next := s.queue[0]
		if next.canceled {
			heap.Pop(&s.queue)
			continue
		}
		if next.at.After(deadline) {
			s.now = deadline
			return !cond()
		}
		s.Step()
	}
	return true
}

// Clock returns a clock.Clock view of the virtual time.
func (s *Sim) Clock() clock.Clock { return simClock{s} }

type simClock struct{ s *Sim }

func (c simClock) Now() time.Time { return c.s.now }

func (c simClock) AfterFunc(d time.Duration, f func()) clock.Timer {
	return c.s.Schedule(d, f)
}

// eventQueue is a min-heap ordered by (at, seq).
type eventQueue []*Event

func (q eventQueue) Len() int { return len(q) }

func (q eventQueue) Less(i, j int) bool {
	if q[i].at.Equal(q[j].at) {
		return q[i].seq < q[j].seq
	}
	return q[i].at.Before(q[j].at)
}

func (q eventQueue) Swap(i, j int) {
	q[i], q[j] = q[j], q[i]
	q[i].index = i
	q[j].index = j
}

func (q *eventQueue) Push(x any) {
	ev, ok := x.(*Event)
	if !ok {
		return
	}
	ev.index = len(*q)
	*q = append(*q, ev)
}

func (q *eventQueue) Pop() any {
	old := *q
	n := len(old)
	ev := old[n-1]
	old[n-1] = nil
	*q = old[:n-1]
	return ev
}
