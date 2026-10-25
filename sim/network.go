package sim

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/archit2501/swimring/clock"
	"github.com/archit2501/swimring/transport"
)

// LinkConfig describes the behaviour of a directed link.
type LinkConfig struct {
	// Loss is the independent per-packet drop probability in [0, 1].
	Loss float64
	// Delay is the fixed one-way latency.
	Delay time.Duration
	// Jitter adds a uniform random latency in [0, Jitter).
	Jitter time.Duration
}

// Validate reports whether the link parameters are usable.
func (l LinkConfig) Validate() error {
	if l.Loss < 0 || l.Loss > 1 {
		return fmt.Errorf("sim: loss %v outside [0,1]", l.Loss)
	}
	if l.Delay < 0 || l.Jitter < 0 {
		return errors.New("sim: negative delay or jitter")
	}
	return nil
}

// NetStats counts packets by fate.
type NetStats struct {
	Sent             uint64
	Delivered        uint64
	DroppedLoss      uint64
	DroppedPartition uint64
	DroppedDown      uint64
	DroppedUnknown   uint64
	Bytes            uint64
}

// Packet describes one simulated transmission, passed to trace hooks.
type Packet struct {
	At      time.Time
	From    string
	To      string
	Size    int
	Dropped string // "" if the packet was scheduled for delivery
}

// Network is a simulated datagram network attached to a Sim.
type Network struct {
	sim         *Sim
	rng         *rand.Rand
	hosts       map[string]*Host
	defaultLink LinkConfig
	links       map[[2]string]LinkConfig
	group       map[string]int
	stats       NetStats
	trace       func(Packet)
}

// NewNetwork creates a network whose links default to def.
func NewNetwork(s *Sim, def LinkConfig) (*Network, error) {
	if err := def.Validate(); err != nil {
		return nil, err
	}
	return &Network{
		sim:         s,
		rng:         s.NewRand(),
		hosts:       make(map[string]*Host),
		defaultLink: def,
		links:       make(map[[2]string]LinkConfig),
		group:       make(map[string]int),
	}, nil
}

// Sim returns the simulator the network runs on.
func (n *Network) Sim() *Sim { return n.sim }

// Stats returns a snapshot of the packet counters.
func (n *Network) Stats() NetStats { return n.stats }

// SetTrace installs a hook called for every Send. Pass nil to remove it.
func (n *Network) SetTrace(f func(Packet)) { n.trace = f }

// SetDefaultLink changes the default link behaviour for all pairs without an
// explicit override.
func (n *Network) SetDefaultLink(l LinkConfig) error {
	if err := l.Validate(); err != nil {
		return err
	}
	n.defaultLink = l
	return nil
}

// SetLink overrides the directed link from -> to.
func (n *Network) SetLink(from, to string, l LinkConfig) error {
	if err := l.Validate(); err != nil {
		return err
	}
	n.links[[2]string{from, to}] = l
	return nil
}

// Partition splits hosts into isolated groups: packets flow only between
// hosts of the same group. Hosts not listed form one more group together.
func (n *Network) Partition(groups ...[]string) {
	n.group = make(map[string]int)
	for i, g := range groups {
		for _, h := range g {
			n.group[h] = i + 1
		}
	}
}

// Heal removes all partitions.
func (n *Network) Heal() { n.group = make(map[string]int) }

// AddHost attaches a new endpoint with the given address.
func (n *Network) AddHost(addr string) (*Host, error) {
	if addr == "" {
		return nil, errors.New("sim: empty host address")
	}
	if _, ok := n.hosts[addr]; ok {
		return nil, fmt.Errorf("sim: duplicate host %q", addr)
	}
	h := &Host{addr: addr, net: n}
	n.hosts[addr] = h
	return h, nil
}

// Host returns the endpoint for addr, or nil.
func (n *Network) Host(addr string) *Host { return n.hosts[addr] }

func (n *Network) link(from, to string) LinkConfig {
	if l, ok := n.links[[2]string{from, to}]; ok {
		return l
	}
	return n.defaultLink
}

func (n *Network) record(p Packet) {
	if n.trace != nil {
		n.trace(p)
	}
}

func (n *Network) send(src *Host, to string, payload []byte) {
	n.stats.Sent++
	n.stats.Bytes += uint64(len(payload))
	p := Packet{At: n.sim.now, From: src.addr, To: to, Size: len(payload)}
	dst, ok := n.hosts[to]
	switch {
	case !ok:
		n.stats.DroppedUnknown++
		p.Dropped = "unknown"
		n.record(p)
		return
	case n.group[src.addr] != n.group[to]:
		n.stats.DroppedPartition++
		p.Dropped = "partition"
		n.record(p)
		return
	}
	l := n.link(src.addr, to)
	if l.Loss > 0 && n.rng.Float64() < l.Loss {
		n.stats.DroppedLoss++
		p.Dropped = "loss"
		n.record(p)
		return
	}
	d := l.Delay
	if l.Jitter > 0 {
		d += time.Duration(n.rng.Int64N(int64(l.Jitter)))
	}
	n.record(p)
	buf := make([]byte, len(payload))
	copy(buf, payload)
	from := src.addr
	n.sim.Schedule(d, func() { dst.arrive(from, buf) })
}

// Host is one simulated endpoint. It implements transport.Transport and
// provides a per-host clock that honours crashes and processing lag.
type Host struct {
	addr     string
	net      *Network
	handler  transport.Handler
	down     bool
	closed   bool
	lag      time.Duration
	timerLag time.Duration
	received uint64
}

var _ transport.Transport = (*Host)(nil)

// LocalAddr implements transport.Transport.
func (h *Host) LocalAddr() string { return h.addr }

// SetHandler implements transport.Transport.
func (h *Host) SetHandler(f transport.Handler) { h.handler = f }

// Send implements transport.Transport.
func (h *Host) Send(addr string, payload []byte) error {
	if h.closed {
		return transport.ErrClosed
	}
	if h.down {
		// A crashed host cannot transmit; report success like a real NIC
		// that silently goes nowhere.
		return nil
	}
	h.net.send(h, addr, payload)
	return nil
}

// Close implements transport.Transport.
func (h *Host) Close() error {
	h.closed = true
	return nil
}

// Crash stops the host: it no longer sends, receives, or runs timers
// scheduled through its Clock. Crash is permanent for the host's timers.
func (h *Host) Crash() { h.down = true }

// Down reports whether the host has crashed.
func (h *Host) Down() bool { return h.down }

// SetLag makes the host slow to process inbound packets: each packet is
// handed to the handler lag after it arrives (FIFO order is preserved).
// Combined with on-time timers this models a process whose receive path is
// starved, e.g. by CPU contention or GC pauses: its own timeouts fire before
// it gets to read the replies. This is the failure mode Lifeguard targets.
func (h *Host) SetLag(lag time.Duration) { h.lag = max(lag, 0) }

// SetTimerLag delays every timer callback scheduled through the host's
// Clock by lag (a process that is late to wake up).
func (h *Host) SetTimerLag(lag time.Duration) { h.timerLag = max(lag, 0) }

// Lag returns the inbound processing lag.
func (h *Host) Lag() time.Duration { return h.lag }

// Received returns the number of packets handed to the handler.
func (h *Host) Received() uint64 { return h.received }

// Clock returns a clock whose timers fire timerLag late and never fire
// after the host has crashed.
func (h *Host) Clock() clock.Clock { return hostClock{h} }

func (h *Host) arrive(from string, payload []byte) {
	if h.down || h.closed {
		h.net.stats.DroppedDown++
		return
	}
	if h.lag == 0 {
		h.deliver(from, payload)
		return
	}
	h.net.sim.Schedule(h.lag, func() {
		if h.down || h.closed {
			h.net.stats.DroppedDown++
			return
		}
		h.deliver(from, payload)
	})
}

func (h *Host) deliver(from string, payload []byte) {
	h.net.stats.Delivered++
	h.received++
	if h.handler != nil {
		h.handler(from, payload)
	}
}

type hostClock struct{ h *Host }

func (c hostClock) Now() time.Time { return c.h.net.sim.now }

func (c hostClock) AfterFunc(d time.Duration, f func()) clock.Timer {
	h := c.h
	return h.net.sim.Schedule(d+h.timerLag, func() {
		if h.down {
			return
		}
		f()
	})
}
