// Package transport defines the unreliable datagram interface that the SWIM
// protocol and the cache run over, plus a multiplexer that lets several
// protocols share one socket.
//
// A Transport delivers whole packets best-effort: packets may be lost,
// duplicated or reordered, exactly like UDP. Implementations exist for real
// UDP sockets (UDP) and for the deterministic simulator (package sim).
package transport

import (
	"errors"
	"fmt"
	"sync"
)

// Handler receives one inbound packet. from is the sender's transport
// address. The payload slice is owned by the handler.
type Handler func(from string, payload []byte)

// Transport is a best-effort datagram transport.
type Transport interface {
	// LocalAddr is the address peers use to reach this endpoint.
	LocalAddr() string
	// Send transmits payload to addr. A nil error does not imply delivery.
	Send(addr string, payload []byte) error
	// SetHandler installs the inbound packet callback. It must be called
	// before any traffic is expected; packets that arrive with no handler
	// installed are dropped.
	SetHandler(h Handler)
	// Close releases resources. Sends after Close fail with ErrClosed.
	Close() error
}

// ErrClosed is returned by Send on a closed transport.
var ErrClosed = errors.New("transport: closed")

// ErrEmptyPacket is returned when a zero-length packet is sent through a Mux
// channel or received with no channel tag.
var ErrEmptyPacket = errors.New("transport: empty packet")

// Mux multiplexes several logical channels over one Transport by prefixing
// every packet with a one-byte channel tag.
type Mux struct {
	base Transport

	mu       sync.RWMutex
	channels map[byte]*Channel
	unknown  uint64
}

// NewMux wraps base. It installs itself as base's handler.
func NewMux(base Transport) *Mux {
	m := &Mux{base: base, channels: make(map[byte]*Channel)}
	base.SetHandler(m.dispatch)
	return m
}

// Channel returns the logical transport for tag, creating it on first use.
func (m *Mux) Channel(tag byte) *Channel {
	m.mu.Lock()
	defer m.mu.Unlock()
	if c, ok := m.channels[tag]; ok {
		return c
	}
	c := &Channel{mux: m, tag: tag}
	m.channels[tag] = c
	return c
}

// UnknownTagPackets reports how many packets arrived for unregistered channels.
func (m *Mux) UnknownTagPackets() uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.unknown
}

// Close closes the underlying transport.
func (m *Mux) Close() error { return m.base.Close() }

func (m *Mux) dispatch(from string, payload []byte) {
	if len(payload) == 0 {
		return
	}
	m.mu.RLock()
	c, ok := m.channels[payload[0]]
	m.mu.RUnlock()
	if !ok {
		m.mu.Lock()
		m.unknown++
		m.mu.Unlock()
		return
	}
	c.deliver(from, payload[1:])
}

// Channel is one logical transport inside a Mux. It implements Transport.
type Channel struct {
	mux *Mux
	tag byte

	mu      sync.RWMutex
	handler Handler
}

var _ Transport = (*Channel)(nil)

// LocalAddr implements Transport.
func (c *Channel) LocalAddr() string { return c.mux.base.LocalAddr() }

// Send implements Transport by prefixing the channel tag.
func (c *Channel) Send(addr string, payload []byte) error {
	if len(payload) == 0 {
		return ErrEmptyPacket
	}
	buf := make([]byte, 0, len(payload)+1)
	buf = append(buf, c.tag)
	buf = append(buf, payload...)
	if err := c.mux.base.Send(addr, buf); err != nil {
		return fmt.Errorf("channel %d: %w", c.tag, err)
	}
	return nil
}

// SetHandler implements Transport.
func (c *Channel) SetHandler(h Handler) {
	c.mu.Lock()
	c.handler = h
	c.mu.Unlock()
}

// Close detaches the handler. The shared base transport stays open; close it
// through Mux.Close.
func (c *Channel) Close() error {
	c.SetHandler(nil)
	return nil
}

func (c *Channel) deliver(from string, payload []byte) {
	c.mu.RLock()
	h := c.handler
	c.mu.RUnlock()
	if h != nil {
		h(from, payload)
	}
}
