// Package swimring wires the pieces of this repository into one cluster
// node: SWIM membership (package swim) drives a consistent-hash ring
// (package ring) that places keys for a small replicated cache (package
// cache). Membership and cache traffic share one datagram transport through
// a channel multiplexer.
//
// The same Node runs over real UDP (transport.ListenUDP + clock.NewReal) or
// inside the deterministic simulator (sim.Host + its Clock).
package swimring

import (
	"errors"
	"fmt"
	"math/rand/v2"

	"github.com/archit2501/swimring/cache"
	"github.com/archit2501/swimring/clock"
	"github.com/archit2501/swimring/swim"
	"github.com/archit2501/swimring/transport"
)

// Channel tags on the shared transport.
const (
	ChannelSWIM  byte = 1
	ChannelCache byte = 2
)

// Options configures a Node.
type Options struct {
	// SWIM is the membership configuration. Its Events hook, if set, is
	// called after the cache has seen each event.
	SWIM swim.Config
	// Cache is the cache configuration.
	Cache cache.Config
	// Transport is the shared datagram transport. The Node owns it from
	// now on and closes it in Close.
	Transport transport.Transport
	// Clock drives all timers.
	Clock clock.Clock
	// Rand drives protocol randomness; seed it for reproducibility.
	Rand *rand.Rand
}

// Node is a cluster member running membership and the cache.
type Node struct {
	Membership *swim.Node
	Cache      *cache.Node
	mux        *transport.Mux
}

// New assembles a node. Call Start, then Join.
func New(opts Options) (*Node, error) {
	if opts.Transport == nil || opts.Clock == nil || opts.Rand == nil {
		return nil, errors.New("swimring: Transport, Clock and Rand are required")
	}
	mux := transport.NewMux(opts.Transport)
	n := &Node{mux: mux}
	user := opts.SWIM.Events
	scfg := opts.SWIM
	scfg.Events = func(ev swim.Event) {
		if n.Cache != nil {
			n.Cache.HandleMembership(ev)
		}
		if user != nil {
			user(ev)
		}
	}
	sw, err := swim.New(scfg, mux.Channel(ChannelSWIM), opts.Clock, opts.Rand)
	if err != nil {
		return nil, fmt.Errorf("swimring: %w", err)
	}
	c, err := cache.New(opts.Cache, sw, mux.Channel(ChannelCache), opts.Clock)
	if err != nil {
		return nil, fmt.Errorf("swimring: %w", err)
	}
	n.Membership, n.Cache = sw, c
	return n, nil
}

// Name returns the member name.
func (n *Node) Name() string { return n.Membership.Name() }

// Addr returns the advertised address.
func (n *Node) Addr() string { return n.Membership.Addr() }

// Start begins probing.
func (n *Node) Start() error { return n.Membership.Start() }

// Join contacts seed addresses.
func (n *Node) Join(seeds ...string) error { return n.Membership.Join(seeds) }

// Stop halts membership and cache activity without closing the transport.
func (n *Node) Stop() {
	n.Membership.Stop()
	n.Cache.Stop()
}

// Close stops the node and closes the transport.
func (n *Node) Close() error {
	n.Stop()
	if err := n.mux.Close(); err != nil {
		return fmt.Errorf("swimring: close transport: %w", err)
	}
	return nil
}
