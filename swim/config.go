// Package swim implements the SWIM failure detector and membership protocol
// (Das, Gupta, Motivala, DSN 2002) with the Lifeguard extensions (Dadgar,
// Phillips, Currey, 2018).
//
// Each Node probes one peer per protocol period with a direct ping, falls
// back to k indirect ping-req probes through other members, and marks
// unresponsive peers Suspect rather than Dead. Suspicions carry incarnation
// numbers so a falsely suspected member can refute them. All membership
// updates are disseminated infection-style by piggybacking on the probe
// traffic.
//
// Lifeguard adds three mechanisms, each switchable in Config:
//   - Local Health Multiplier (LHM): a node that misses acks or nacks, or has
//     to refute suspicions about itself, assumes it is the slow one and
//     stretches its own probe interval and timeout.
//   - Dynamic suspicion timeout: suspicions start with a long timeout that
//     shrinks logarithmically as independent members confirm them.
//   - Nack + buddy system: ping-req relays send a nack when the target is
//     silent (so the prober can tell its own lag from the target's), and a
//     prober always piggybacks the suspicion onto a ping sent to the
//     suspected member, so it learns about it fast and can refute.
//
// A Node is safe for concurrent use. It owns no goroutines: progress is
// driven entirely by inbound packets and by timers on the injected clock,
// which is what lets the same code run on real UDP or inside package sim.
package swim

import (
	"errors"
	"fmt"
	"log/slog"
	"time"
)

// Lifeguard switches the individual Lifeguard extensions.
type Lifeguard struct {
	// LocalHealth enables the Local Health Multiplier.
	LocalHealth bool
	// DynamicSuspicion enables confirmation-driven suspicion timeouts.
	DynamicSuspicion bool
	// Nack enables nack responses from ping-req relays.
	Nack bool
	// Buddy prioritises piggybacking a suspicion onto pings to the suspect.
	Buddy bool
}

// LifeguardAll enables every Lifeguard extension.
func LifeguardAll() Lifeguard {
	return Lifeguard{LocalHealth: true, DynamicSuspicion: true, Nack: true, Buddy: true}
}

// LifeguardOff disables every Lifeguard extension (plain SWIM with suspicion).
func LifeguardOff() Lifeguard { return Lifeguard{} }

// Config parameterises a Node. Use DefaultConfig and override fields.
type Config struct {
	// Name uniquely identifies the member in the cluster.
	Name string
	// Addr is the transport address peers use to reach this member. If
	// empty, the transport's LocalAddr is used.
	Addr string

	// ProbeInterval is the SWIM protocol period T'.
	ProbeInterval time.Duration
	// ProbeTimeout is how long to wait for a direct ack before indirect
	// probing. It must be less than ProbeInterval.
	ProbeTimeout time.Duration
	// IndirectChecks is k, the number of ping-req relays.
	IndirectChecks int

	// RetransmitMult is λ: each update is piggybacked
	// RetransmitMult*ceil(log10(n+1)) times.
	RetransmitMult int
	// SuspicionMult is α: the minimum suspicion timeout is
	// α*max(1,log10(n))*ProbeInterval.
	SuspicionMult int
	// SuspicionMaxTimeoutMult is β: with dynamic suspicion, a suspicion
	// starts at β times the minimum timeout.
	SuspicionMaxTimeoutMult int
	// AwarenessMaxMultiplier is S, the saturation of the Local Health
	// Multiplier; timeouts are scaled by at most S.
	AwarenessMaxMultiplier int

	// Lifeguard selects the Lifeguard extensions.
	Lifeguard Lifeguard

	// MaxPacketSize caps every encoded packet in bytes.
	MaxPacketSize int

	// PushPullInterval is the period of full-state anti-entropy exchanges
	// with a random live member. Zero disables them.
	PushPullInterval time.Duration
	// ReconnectInterval is the period at which a node tries a full-state
	// exchange with a random member it believes dead, so that clusters split
	// by a partition merge again after it heals. Zero disables it.
	ReconnectInterval time.Duration
	// DeadRetention is how long Dead/Left members are kept as tombstones
	// before being forgotten.
	DeadRetention time.Duration

	// JoinRetryInterval is the delay before re-sending a join to the seeds
	// while the node still knows no other member.
	JoinRetryInterval time.Duration
	// JoinMaxAttempts bounds join retries.
	JoinMaxAttempts int

	// Logger receives debug and warning logs. Nil discards them.
	Logger *slog.Logger
	// Events, if non-nil, is called for every membership change. It is
	// invoked without internal locks held, so it may call back into the
	// node. Events from one packet or timer arrive in the order the changes
	// happened; with the real clock, events from concurrent timers and
	// packets may interleave, so consumers should re-read current state
	// (Node.Member) rather than rely on global ordering.
	Events func(Event)
}

// DefaultConfig returns LAN-oriented defaults matching the values used by
// widely deployed SWIM implementations, with all Lifeguard extensions on.
func DefaultConfig(name string) Config {
	return Config{
		Name:                    name,
		ProbeInterval:           time.Second,
		ProbeTimeout:            500 * time.Millisecond,
		IndirectChecks:          3,
		RetransmitMult:          4,
		SuspicionMult:           4,
		SuspicionMaxTimeoutMult: 6,
		AwarenessMaxMultiplier:  8,
		Lifeguard:               LifeguardAll(),
		MaxPacketSize:           1400,
		PushPullInterval:        30 * time.Second,
		ReconnectInterval:       10 * time.Second,
		DeadRetention:           5 * time.Minute,
		JoinRetryInterval:       2 * time.Second,
		JoinMaxAttempts:         10,
	}
}

// ErrInvalidConfig wraps every configuration validation failure.
var ErrInvalidConfig = errors.New("swim: invalid config")

// Validate checks the configuration for internal consistency.
func (c Config) Validate() error {
	bad := func(format string, args ...any) error {
		return fmt.Errorf("%w: %s", ErrInvalidConfig, fmt.Sprintf(format, args...))
	}
	switch {
	case c.Name == "":
		return bad("empty Name")
	case len(c.Name) > maxStringLen:
		return bad("Name longer than %d bytes", maxStringLen)
	case len(c.Addr) > maxStringLen:
		return bad("Addr longer than %d bytes", maxStringLen)
	case c.ProbeInterval <= 0:
		return bad("ProbeInterval must be positive")
	case c.ProbeTimeout <= 0 || c.ProbeTimeout >= c.ProbeInterval:
		return bad("ProbeTimeout must be in (0, ProbeInterval)")
	case c.IndirectChecks < 0:
		return bad("IndirectChecks must be >= 0")
	case c.RetransmitMult < 1:
		return bad("RetransmitMult must be >= 1")
	case c.SuspicionMult < 1:
		return bad("SuspicionMult must be >= 1")
	case c.SuspicionMaxTimeoutMult < 1:
		return bad("SuspicionMaxTimeoutMult must be >= 1")
	case c.AwarenessMaxMultiplier < 1:
		return bad("AwarenessMaxMultiplier must be >= 1")
	case c.MaxPacketSize < minPacketSize:
		return bad("MaxPacketSize must be >= %d", minPacketSize)
	case c.PushPullInterval < 0 || c.ReconnectInterval < 0:
		return bad("negative anti-entropy interval")
	case c.DeadRetention <= 0:
		return bad("DeadRetention must be positive")
	case c.JoinRetryInterval <= 0 || c.JoinMaxAttempts < 1:
		return bad("join retry settings must be positive")
	}
	return nil
}
