package swim

import "time"

// This file holds the membership merge rules (SWIM section 4.2) and the
// bookkeeping that follows a state change. All functions require n.mu.
//
// Precedence for an update about member M with incarnation i, against the
// local record (state s, incarnation j):
//
//	Alive(i)   overrides any record with j < i
//	Suspect(i) overrides Alive(j) with j <= i, Suspect(j) with j < i
//	Dead(i)    overrides Alive/Suspect with j <= i
//	Left(i)    overrides Alive/Suspect/Dead with j <= i
//
// Dead and Left records are tombstones: only a strictly higher incarnation
// (a restarted or refuting member) brings M back.

// applyUpdate merges one update. viaSync marks updates that came from a
// full-state exchange, where Dead records are downgraded to Suspect so that
// the accused member gets a chance to refute (this is what lets two halves
// of a healed partition merge without either side trusting the other's
// verdicts). source is the sync sender, used as the suspecter in that case.
func (n *Node) applyUpdate(u update, viaSync bool, source string) {
	if u.Name == n.cfg.Name {
		n.applySelfUpdate(u)
		return
	}
	switch u.Kind {
	case kindAlive:
		n.aliveNode(u)
	case kindSuspect:
		n.suspectNode(u.Name, u.Incarnation, u.From)
	case kindDead:
		if viaSync {
			n.suspectNode(u.Name, u.Incarnation, source)
			return
		}
		n.deadNode(u.Name, u.Incarnation, u.From)
	case kindLeft:
		n.leftNode(u.Name, u.Incarnation)
	}
}

// applySelfUpdate handles assertions about the local node: any suspicion or
// death claim at our current incarnation or later is refuted by bumping the
// incarnation past it and gossiping Alive.
func (n *Node) applySelfUpdate(u update) {
	if u.Kind == kindAlive || n.leaving {
		return
	}
	if u.Incarnation < n.self.incarnation {
		// The claimant holds a stale view that our earlier refutation never
		// reached (its gossip budget ran out). Re-announce the current
		// incarnation; with the buddy system this rides on the very ack
		// that answers the claimant's ping.
		if _, queued := n.bq.Pending(n.cfg.Name); !queued {
			n.bq.Queue(n.self.toUpdate())
		}
		return
	}
	n.self.incarnation = u.Incarnation + 1
	n.bq.Queue(n.self.toUpdate())
	n.stats.Refutations++
	if n.cfg.Lifeguard.LocalHealth {
		n.health.Apply(1)
	}
	n.traceEvent(TraceRefute, u.From)
	n.log.Debug("refuted", "claim", u.Kind, "by", u.From, "incarnation", n.self.incarnation)
}

func (n *Node) aliveNode(u update) {
	m, ok := n.members[u.Name]
	if !ok {
		if u.Addr == "" {
			return
		}
		m = &member{name: u.Name, addr: u.Addr, incarnation: u.Incarnation, state: StateAlive, since: n.clk.Now()}
		n.members[u.Name] = m
		n.order = append(n.order, m)
		n.insertProbeTarget(m)
		n.bq.Queue(u)
		n.emit(Event{Type: EventJoin, Member: m.snapshot(), Source: u.Name})
		n.log.Debug("member joined", "member", u.Name, "incarnation", u.Incarnation)
		return
	}
	if u.Incarnation <= m.incarnation {
		return
	}
	prev := m.state
	n.clearSuspicion(m)
	if m.purge != nil {
		m.purge.Stop()
		m.purge = nil
	}
	m.incarnation = u.Incarnation
	if u.Addr != "" {
		m.addr = u.Addr
	}
	m.state = StateAlive
	m.since = n.clk.Now()
	n.bq.Queue(u)
	if prev.Live() {
		n.emit(Event{Type: EventUpdate, Member: m.snapshot(), Prev: prev, Source: u.Name})
		return
	}
	n.insertProbeTarget(m)
	n.emit(Event{Type: EventJoin, Member: m.snapshot(), Prev: prev, Source: u.Name})
	n.log.Debug("member rejoined", "member", u.Name, "incarnation", u.Incarnation)
}

func (n *Node) suspectNode(name string, inc uint64, from string) {
	m, ok := n.members[name]
	if !ok || inc < m.incarnation || !m.state.Live() {
		return
	}
	if m.state == StateSuspect && inc == m.incarnation {
		// Another member independently suspects M: a Lifeguard
		// confirmation. New confirmations are re-gossiped so others can
		// shorten their timeouts too.
		if m.suspicion != nil && m.suspicion.Confirm(from) {
			n.bq.Queue(update{Kind: kindSuspect, Name: name, Addr: m.addr, Incarnation: inc, From: from})
		}
		return
	}
	prev := m.state
	n.clearSuspicion(m)
	m.incarnation = inc
	m.state = StateSuspect
	m.since = n.clk.Now()
	m.suspectFrom = from

	live := n.numLive()
	k := 0
	if n.cfg.Lifeguard.DynamicSuspicion {
		k = n.cfg.SuspicionMult - 2
		if live-2 < k {
			k = 0
		}
	}
	minT := minSuspicionTimeout(n.cfg.SuspicionMult, live, n.cfg.ProbeInterval)
	maxT := minT
	if n.cfg.Lifeguard.DynamicSuspicion {
		maxT = minT * time.Duration(n.cfg.SuspicionMaxTimeoutMult)
	}
	m.suspicion = newSuspicion(n.clk, from, k, minT, maxT, func() { n.suspicionExpired(name, inc) })

	if from == n.cfg.Name {
		n.stats.SuspicionsRaised++
	}
	n.bq.Queue(update{Kind: kindSuspect, Name: name, Addr: m.addr, Incarnation: inc, From: from})
	n.emit(Event{Type: EventUpdate, Member: m.snapshot(), Prev: prev, Source: from})
	n.log.Debug("member suspected", "member", name, "by", from, "incarnation", inc)
}

// suspicionExpired runs on the clock when a suspicion times out without a
// refutation.
func (n *Node) suspicionExpired(name string, inc uint64) {
	n.mu.Lock()
	defer n.unlock()
	if n.stopped {
		return
	}
	m, ok := n.members[name]
	if !ok || m.state != StateSuspect || m.incarnation != inc {
		return
	}
	n.stats.DeathsDeclared++
	n.deadNode(name, inc, n.cfg.Name)
}

func (n *Node) deadNode(name string, inc uint64, from string) {
	m, ok := n.members[name]
	if !ok || inc < m.incarnation || !m.state.Live() {
		return
	}
	prev := m.state
	n.clearSuspicion(m)
	m.incarnation = inc
	m.state = StateDead
	m.since = n.clk.Now()
	n.schedulePurge(m)
	n.bq.Queue(update{Kind: kindDead, Name: name, Addr: m.addr, Incarnation: inc, From: from})
	n.emit(Event{Type: EventLeave, Member: m.snapshot(), Prev: prev, Source: from})
	n.log.Debug("member declared dead", "member", name, "by", from, "incarnation", inc)
}

func (n *Node) leftNode(name string, inc uint64) {
	m, ok := n.members[name]
	if !ok || inc < m.incarnation || m.state == StateLeft {
		return
	}
	prev := m.state
	n.clearSuspicion(m)
	m.incarnation = inc
	m.state = StateLeft
	m.since = n.clk.Now()
	n.schedulePurge(m)
	n.bq.Queue(update{Kind: kindLeft, Name: name, Addr: m.addr, Incarnation: inc})
	if prev.Live() {
		n.emit(Event{Type: EventLeave, Member: m.snapshot(), Prev: prev, Source: name})
	}
	n.log.Debug("member left", "member", name, "incarnation", inc)
}

func (n *Node) clearSuspicion(m *member) {
	if m.suspicion != nil {
		m.suspicion.Stop()
		m.suspicion = nil
	}
	m.suspectFrom = ""
}

// schedulePurge forgets a tombstone after DeadRetention unless the member
// comes back first.
func (n *Node) schedulePurge(m *member) {
	if m.purge != nil {
		m.purge.Stop()
	}
	inc := m.incarnation
	m.purge = n.clk.AfterFunc(n.cfg.DeadRetention, func() {
		n.mu.Lock()
		defer n.unlock()
		cur, ok := n.members[m.name]
		if n.stopped || !ok || cur != m || m.state.Live() || m.incarnation != inc {
			return
		}
		n.removeMember(m)
	})
}

func (n *Node) removeMember(m *member) {
	delete(n.members, m.name)
	n.order = removeFrom(n.order, m)
	for i, x := range n.probeOrder {
		if x == m {
			n.probeOrder = append(n.probeOrder[:i], n.probeOrder[i+1:]...)
			if i < n.probeIdx {
				// Keep the cursor on the same next target.
				n.probeIdx--
			}
			break
		}
	}
}

func removeFrom(list []*member, m *member) []*member {
	for i, x := range list {
		if x == m {
			return append(list[:i], list[i+1:]...)
		}
	}
	return list
}

// insertProbeTarget places a new or returning member at a uniformly random
// position among the not-yet-probed slots of the current round, as SWIM
// section 4.3 prescribes, so it is probed within the current round.
func (n *Node) insertProbeTarget(m *member) {
	for _, x := range n.probeOrder[n.probeIdx:] {
		if x == m {
			return
		}
	}
	rest := len(n.probeOrder) - n.probeIdx
	pos := n.probeIdx + n.rng.IntN(rest+1)
	n.probeOrder = append(n.probeOrder, nil)
	copy(n.probeOrder[pos+1:], n.probeOrder[pos:])
	n.probeOrder[pos] = m
}
