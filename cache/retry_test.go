package cache

import (
	"errors"
	"testing"
	"time"

	"github.com/archit2501/swimring/sim"
	"github.com/archit2501/swimring/swim"
	"github.com/archit2501/swimring/transport"
)

// staticMembers is a two-member view: the coordinator and one peer.
type staticMembers struct{ self, peer swim.Member }

func (s staticMembers) Name() string               { return s.self.Name }
func (s staticMembers) LiveMembers() []swim.Member { return []swim.Member{s.peer} }
func (s staticMembers) Member(name string) (swim.Member, bool) {
	switch name {
	case s.self.Name:
		return s.self, true
	case s.peer.Name:
		return s.peer, true
	}
	return swim.Member{}, false
}

// rejectingPeer answers every put as a replica that already holds a version
// one past the one it was sent, for the first `rejects` puts, and accepts
// the rest. It records the versions it was sent.
type rejectingPeer struct {
	host    *sim.Host
	rejects int
	sent    []Version
}

func (p *rejectingPeer) handle(from string, b []byte) {
	m, err := decodeMessage(b)
	if err != nil || m.Op != opPutReq {
		return
	}
	p.sent = append(p.sent, m.Entry.Version)
	resp := &message{Op: opPutResp, ID: m.ID, Key: m.Key, OK: true, Held: m.Entry.Version}
	if len(p.sent) <= p.rejects {
		resp.OK, resp.Held = false, Version{Counter: m.Entry.Version.Counter + 1, Node: "zz"}
	}
	_ = p.host.Send(from, resp.encode())
}

func newRetryRig(t *testing.T, rejects int) (*sim.Sim, *Node, *rejectingPeer) {
	t.Helper()
	s := sim.New(1)
	nw, err := sim.NewNetwork(s, sim.LinkConfig{Delay: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	ha, _ := nw.AddHost("a")
	hb, _ := nw.AddHost("b")
	peer := &rejectingPeer{host: hb, rejects: rejects}
	hb.SetHandler(peer.handle)
	cfg := DefaultConfig()
	cfg.ReplicationFactor, cfg.WriteQuorum, cfg.ReadQuorum = 2, 2, 1
	mem := staticMembers{
		self: swim.Member{Name: "a", Addr: "a", State: swim.StateAlive},
		peer: swim.Member{Name: "b", Addr: "b", State: swim.StateAlive},
	}
	n, err := New(cfg, mem, ha, ha.Clock())
	if err != nil {
		t.Fatal(err)
	}
	return s, n, peer
}

func runPut(t *testing.T, s *sim.Sim, n *Node, key, val string) error {
	t.Helper()
	var res error
	done := false
	n.PutAsync(key, []byte(val), func(err error) { res, done = err, true })
	if !s.RunWhile(func() bool { return !done }, 5*time.Second) {
		t.Fatal("put callback never ran")
	}
	return res
}

func TestWriteRetriesPastRejectingReplica(t *testing.T) {
	s, n, peer := newRetryRig(t, 1)
	if err := runPut(t, s, n, "k", "v"); err != nil {
		t.Fatalf("put: %v", err)
	}
	if len(peer.sent) != 2 {
		t.Fatalf("peer saw %d attempts, want 2", len(peer.sent))
	}
	if first, second := peer.sent[0], peer.sent[1]; second.Counter <= first.Counter+1 {
		t.Fatalf("retry version %v does not pass the held version (%d, zz)", second, first.Counter+1)
	}
	e, _ := n.LocalEntry("k")
	if e.Version != peer.sent[1] || string(e.Value) != "v" {
		t.Fatalf("local copy %v %q, want the retried version %v", e.Version, e.Value, peer.sent[1])
	}
	if st := n.Stats(); st.WriteRetries != 1 || st.WriteConflicts != 0 || st.QuorumFailures != 0 {
		t.Fatalf("stats = %+v", st)
	}
}

func TestWriteGivesUpWithErrConflict(t *testing.T) {
	s, n, peer := newRetryRig(t, 1<<30)
	if err := runPut(t, s, n, "k", "v"); !errors.Is(err, ErrConflict) {
		t.Fatalf("err = %v, want ErrConflict", err)
	}
	if len(peer.sent) != maxWriteAttempts {
		t.Fatalf("peer saw %d attempts, want %d", len(peer.sent), maxWriteAttempts)
	}
	if st := n.Stats(); st.WriteRetries != maxWriteAttempts-1 || st.WriteConflicts != 1 || st.QuorumFailures != 0 {
		t.Fatalf("stats = %+v", st)
	}
}

// captureTransport records outbound packets instead of sending them.
type captureTransport struct {
	transport.Transport
	sink func([]byte)
}

func (c captureTransport) Send(_ string, b []byte) error {
	c.sink(b)
	return nil
}

func TestReplicaRejectsOlderPutWithHeldVersion(t *testing.T) {
	_, n, _ := newRetryRig(t, 0)
	var got *message
	n.tr = captureTransport{Transport: n.tr, sink: func(b []byte) { got, _ = decodeMessage(b) }}
	newer := Version{Counter: 50, Node: "x"}
	put := func(v Version, val string) {
		got = nil
		n.handle("b", (&message{Op: opPutReq, ID: 9, Key: "k", Entry: Entry{Value: []byte(val), Version: v}}).encode())
	}
	put(newer, "new")
	if got == nil || !got.OK || got.Held != newer {
		t.Fatalf("accepted put: reply %+v", got)
	}
	put(newer, "new") // duplicate datagram: same version is still an ack
	if got == nil || !got.OK || got.Held != newer {
		t.Fatalf("duplicate put: reply %+v", got)
	}
	put(Version{Counter: 3, Node: "y"}, "old")
	if got == nil || got.Op != opPutResp || got.OK || got.Held != newer {
		t.Fatalf("older put: reply %+v, want OK=false holding %v", got, newer)
	}
	if e, _ := n.LocalEntry("k"); string(e.Value) != "new" {
		t.Fatalf("older put overwrote the replica: %q", e.Value)
	}
	// The replica's own clock moved past what it holds, so a write it
	// coordinates next is newer than either.
	n.mu.Lock()
	next := n.nextVersion()
	n.mu.Unlock()
	if !newer.Less(next) {
		t.Fatalf("next version %v not past held %v", next, newer)
	}
}
