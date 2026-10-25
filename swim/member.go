package swim

import (
	"fmt"
	"time"

	"github.com/archit2501/swimring/clock"
)

// State is a member's liveness as seen by the local node.
type State uint8

// Member states.
const (
	StateAlive State = iota
	StateSuspect
	StateDead
	StateLeft
)

func (s State) String() string {
	switch s {
	case StateAlive:
		return "alive"
	case StateSuspect:
		return "suspect"
	case StateDead:
		return "dead"
	case StateLeft:
		return "left"
	default:
		return fmt.Sprintf("state(%d)", uint8(s))
	}
}

// Live reports whether the state counts towards cluster membership (Alive
// and Suspect members still own data and are still probed).
func (s State) Live() bool { return s == StateAlive || s == StateSuspect }

// Member is an immutable snapshot of one member.
type Member struct {
	Name        string
	Addr        string
	Incarnation uint64
	State       State
	// Since is when the local node last changed this member's state.
	Since time.Time
}

func (m Member) String() string {
	return fmt.Sprintf("%s@%s[%s inc=%d]", m.Name, m.Addr, m.State, m.Incarnation)
}

// EventType classifies membership events.
type EventType uint8

// Event types.
const (
	// EventJoin: a member became known, or came back after Dead/Left.
	EventJoin EventType = iota
	// EventUpdate: a live member changed state (Alive<->Suspect) or
	// incarnation.
	EventUpdate
	// EventLeave: a member was declared Dead or announced it Left.
	EventLeave
)

func (t EventType) String() string {
	switch t {
	case EventJoin:
		return "join"
	case EventUpdate:
		return "update"
	case EventLeave:
		return "leave"
	default:
		return fmt.Sprintf("event(%d)", uint8(t))
	}
}

// Event is delivered to Config.Events.
type Event struct {
	Type   EventType
	Member Member
	// Prev is the member's state before the change (zero value on first
	// join).
	Prev State
	// Source is the member that originated the assertion (the suspecter for
	// suspicions, the declarer for deaths, the member itself otherwise).
	Source string
}

// member is the node-internal mutable record.
type member struct {
	name        string
	addr        string
	incarnation uint64
	state       State
	since       time.Time
	suspicion   *suspicion
	// suspectFrom remembers who first raised the current suspicion, so the
	// buddy system can re-gossip an accurate record.
	suspectFrom string
	purge       clock.Timer
}

func (m *member) snapshot() Member {
	return Member{Name: m.name, Addr: m.addr, Incarnation: m.incarnation, State: m.state, Since: m.since}
}

func (m *member) toUpdate() update {
	u := update{Name: m.name, Addr: m.addr, Incarnation: m.incarnation}
	switch m.state {
	case StateAlive:
		u.Kind = kindAlive
	case StateSuspect:
		u.Kind = kindSuspect
		u.From = m.suspectFrom
	case StateDead:
		u.Kind = kindDead
	case StateLeft:
		u.Kind = kindLeft
	}
	return u
}
