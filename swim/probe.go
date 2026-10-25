package swim

import "github.com/archit2501/swimring/clock"

// probeState is the in-flight probe of the current protocol period.
type probeState struct {
	target        *member
	seq           uint64
	acked         bool
	nacks         int
	expectedNacks int
	timeout       clock.Timer
}

// ackHandler waits for the ack (or nacks) carrying one sequence number.
type ackHandler struct {
	onAck  func()
	onNack func()
	// timer expires the handler; nil for the prober's own handler, which
	// is retired at the end of the protocol period instead.
	timer clock.Timer
}

// tick runs once per (health-scaled) protocol period: it closes the
// previous period's probe and starts the next one.
func (n *Node) tick() {
	n.mu.Lock()
	defer n.unlock()
	if n.stopped {
		return
	}
	n.finishProbe()
	n.startProbe()
	interval := n.cfg.ProbeInterval
	if n.cfg.Lifeguard.LocalHealth {
		interval = n.health.Scale(interval)
	}
	n.setTimer("tick", interval, n.tick)
}

// finishProbe evaluates the probe started in the previous period. A probe
// that was never acknowledged, directly or through a relay, turns into a
// suspicion. Lifeguard: success lowers the local health score; failure
// raises it by one, or, when nacks were requested, by the number of relays
// that failed to nack (silent relays suggest we are the slow one).
func (n *Node) finishProbe() {
	p := n.probe
	if p == nil {
		return
	}
	n.probe = nil
	if p.timeout != nil {
		p.timeout.Stop()
	}
	delete(n.acks, p.seq)
	if p.acked {
		if n.cfg.Lifeguard.LocalHealth {
			n.health.Apply(-1)
		}
		n.traceEvent(TraceProbeOK, p.target.name)
		return
	}
	n.stats.ProbeFailures++
	if n.cfg.Lifeguard.LocalHealth {
		delta := 1
		if p.expectedNacks > 0 {
			delta = p.expectedNacks - p.nacks
		}
		n.health.Apply(delta)
	}
	n.traceEvent(TraceProbeFail, p.target.name)
	t := p.target
	if cur, ok := n.members[t.name]; ok && cur == t && t.state.Live() {
		n.suspectNode(t.name, t.incarnation, n.cfg.Name)
	}
}

func (n *Node) startProbe() {
	target := n.nextProbeTarget()
	if target == nil {
		return
	}
	p := &probeState{target: target, seq: n.nextSeq()}
	n.probe = p
	n.acks[p.seq] = &ackHandler{
		onAck:  func() { p.acked = true },
		onNack: func() { p.nacks++ },
	}
	n.stats.Probes++
	n.stats.PingsSent++
	n.traceEvent(TraceProbe, target.name)

	var prio *update
	if n.cfg.Lifeguard.Buddy && target.state == StateSuspect {
		u := target.toUpdate()
		prio = &u
	}
	n.send(target.addr, &packet{Type: msgPing, Seq: p.seq, Target: target.name, Source: n.cfg.Name}, prio)

	timeout := n.cfg.ProbeTimeout
	if n.cfg.Lifeguard.LocalHealth {
		timeout = n.health.Scale(timeout)
	}
	p.timeout = n.clk.AfterFunc(timeout, func() { n.indirectProbe(p) })
}

// indirectProbe asks k random live members to ping the target on our
// behalf (SWIM section 3.1).
func (n *Node) indirectProbe(p *probeState) {
	n.mu.Lock()
	defer n.unlock()
	if n.stopped || n.probe != p || p.acked {
		return
	}
	relays := n.randomMembers(n.cfg.IndirectChecks, p.target)
	var flags byte
	if n.cfg.Lifeguard.Nack {
		flags |= flagNack
		p.expectedNacks = len(relays)
	}
	for _, r := range relays {
		n.stats.PingReqsSent++
		n.send(r.addr, &packet{
			Type:       msgPingReq,
			Seq:        p.seq,
			Target:     p.target.name,
			TargetAddr: p.target.addr,
			Source:     n.cfg.Name,
			Flags:      flags,
		}, nil)
	}
}

// nextProbeTarget walks a shuffled round-robin list of live members, which
// gives SWIM's time-bounded completeness: every live member is probed by
// every other member within one round of n-1 periods. The list is
// reshuffled at the end of each round.
func (n *Node) nextProbeTarget() *member {
	for range len(n.order) + 1 {
		if n.probeIdx >= len(n.probeOrder) {
			n.reshuffle()
			if len(n.probeOrder) == 0 {
				return nil
			}
		}
		m := n.probeOrder[n.probeIdx]
		n.probeIdx++
		if cur, ok := n.members[m.name]; ok && cur == m && m != n.self && m.state.Live() {
			return m
		}
	}
	return nil
}

func (n *Node) reshuffle() {
	n.probeOrder = n.probeOrder[:0]
	for _, m := range n.order {
		if m != n.self && m.state.Live() {
			n.probeOrder = append(n.probeOrder, m)
		}
	}
	n.rng.Shuffle(len(n.probeOrder), func(i, j int) {
		n.probeOrder[i], n.probeOrder[j] = n.probeOrder[j], n.probeOrder[i]
	})
	n.probeIdx = 0
}

// randomMembers returns up to k distinct Alive members other than self and
// exclude, chosen uniformly (partial Fisher-Yates, O(n)).
func (n *Node) randomMembers(k int, exclude *member) []*member {
	if k <= 0 {
		return nil
	}
	cands := make([]*member, 0, len(n.order))
	for _, m := range n.order {
		if m != n.self && m != exclude && m.state == StateAlive {
			cands = append(cands, m)
		}
	}
	if k > len(cands) {
		k = len(cands)
	}
	for i := range k {
		j := i + n.rng.IntN(len(cands)-i)
		cands[i], cands[j] = cands[j], cands[i]
	}
	return cands[:k]
}

func (n *Node) handlePing(from string, p *packet) {
	if p.Target != "" && p.Target != n.cfg.Name {
		// A ping meant for a previous owner of this address.
		n.stats.Misdirected++
		return
	}
	n.send(from, &packet{Type: msgAck, Seq: p.Seq}, nil)
}

// handlePingReq relays a probe for another member. With the nack flag the
// relay reports back at 80% of the probe timeout if the target stayed
// silent, so the requester can distinguish "target is down" from "I am too
// slow to hear anybody".
func (n *Node) handlePingReq(from string, p *packet) {
	if p.TargetAddr == "" {
		return
	}
	origin, originSeq := from, p.Seq
	seq := n.nextSeq()
	h := &ackHandler{}
	var nackTimer clock.Timer
	h.onAck = func() {
		if nackTimer != nil {
			nackTimer.Stop()
		}
		n.send(origin, &packet{Type: msgAck, Seq: originSeq}, nil)
	}
	h.timer = n.clk.AfterFunc(n.cfg.ProbeInterval, func() {
		n.mu.Lock()
		defer n.unlock()
		if n.acks[seq] == h {
			delete(n.acks, seq)
		}
	})
	n.acks[seq] = h
	n.stats.PingsSent++
	n.send(p.TargetAddr, &packet{Type: msgPing, Seq: seq, Target: p.Target, Source: n.cfg.Name}, nil)
	if p.Flags&flagNack != 0 {
		nackTimer = n.clk.AfterFunc(n.cfg.ProbeTimeout*8/10, func() {
			n.mu.Lock()
			defer n.unlock()
			if n.stopped || n.acks[seq] != h {
				return
			}
			n.stats.NacksSent++
			n.send(origin, &packet{Type: msgNack, Seq: originSeq}, nil)
		})
	}
}

func (n *Node) handleAck(p *packet) {
	h, ok := n.acks[p.Seq]
	if !ok {
		return
	}
	n.stats.AcksReceived++
	delete(n.acks, p.Seq)
	if h.timer != nil {
		h.timer.Stop()
	}
	h.onAck()
}

func (n *Node) handleNack(p *packet) {
	h, ok := n.acks[p.Seq]
	if !ok || h.onNack == nil {
		return
	}
	n.stats.NacksReceived++
	h.onNack()
}
