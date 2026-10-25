package swim

// Full-state synchronisation ("push-pull" anti-entropy). SWIM proper only
// gossips deltas; a full exchange is used for joining, for periodic repair
// of anything gossip missed, and for re-merging clusters after a partition.
// The table is split into as many packets as MaxPacketSize requires; every
// chunk is self-contained and merged independently, so a lost chunk only
// delays convergence until the next exchange.

func (n *Node) sendSync(addr string, flags byte) {
	n.stats.SyncsSent++
	base := packet{Type: msgSync, Source: n.cfg.Name}
	budget := n.cfg.MaxPacketSize - base.headerLen() - 3
	var chunk []update
	used := 0
	first := true
	flush := func() {
		p := base
		p.Flags = flags
		if !first {
			p.Flags &^= flagWantReply
		}
		p.Updates = chunk
		n.transmit(addr, &p)
		first = false
		chunk = nil
		used = 0
	}
	for _, m := range n.order {
		u := m.toUpdate()
		sz := u.encodedLen()
		if used+sz > budget && len(chunk) > 0 {
			flush()
		}
		chunk = append(chunk, u)
		used += sz
	}
	flush()
}

func (n *Node) handleSync(from string, p *packet) {
	n.stats.SyncsReceived++
	for _, u := range p.Updates {
		n.applyUpdate(u, true, p.Source)
	}
	if p.Flags&flagWantReply != 0 {
		n.sendSync(from, 0)
	}
}

// pushPull exchanges full state with one random live member.
func (n *Node) pushPull() {
	n.mu.Lock()
	defer n.unlock()
	if n.stopped {
		return
	}
	if peers := n.randomMembers(1, nil); len(peers) == 1 {
		n.sendSync(peers[0].addr, flagWantReply)
	}
	n.setTimer("pushpull", n.cfg.PushPullInterval, n.pushPull)
}

// reconnect exchanges full state with one random member we believe dead. If
// it is in fact alive on the far side of a healed partition, it will refute
// and both sides re-merge.
func (n *Node) reconnect() {
	n.mu.Lock()
	defer n.unlock()
	if n.stopped {
		return
	}
	var dead []*member
	for _, m := range n.order {
		if m.state == StateDead {
			dead = append(dead, m)
		}
	}
	if len(dead) > 0 {
		n.sendSync(dead[n.rng.IntN(len(dead))].addr, flagWantReply)
	}
	n.setTimer("reconnect", n.cfg.ReconnectInterval, n.reconnect)
}
