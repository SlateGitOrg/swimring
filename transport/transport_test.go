package transport

import (
	"errors"
	"sync"
	"testing"
	"time"
)

// loopback is an in-memory Transport pair for mux tests.
type loopback struct {
	addr    string
	peer    *loopback
	handler Handler
	closed  bool
	failing bool
}

func (l *loopback) LocalAddr() string    { return l.addr }
func (l *loopback) SetHandler(h Handler) { l.handler = h }
func (l *loopback) Close() error         { l.closed = true; return nil }
func (l *loopback) Send(_ string, b []byte) error {
	if l.failing {
		return errors.New("boom")
	}
	if l.peer.handler != nil {
		l.peer.handler(l.addr, append([]byte(nil), b...))
	}
	return nil
}

func TestMuxRoutesByChannel(t *testing.T) {
	a, b := &loopback{addr: "a"}, &loopback{addr: "b"}
	a.peer, b.peer = b, a
	ma, mb := NewMux(a), NewMux(b)
	var got1, got2 []string
	mb.Channel(1).SetHandler(func(from string, p []byte) { got1 = append(got1, from+":"+string(p)) })
	mb.Channel(2).SetHandler(func(from string, p []byte) { got2 = append(got2, from+":"+string(p)) })
	if ma.Channel(1) != ma.Channel(1) {
		t.Fatal("Channel must return the same instance")
	}
	if err := ma.Channel(1).Send("b", []byte("one")); err != nil {
		t.Fatal(err)
	}
	if err := ma.Channel(2).Send("b", []byte("two")); err != nil {
		t.Fatal(err)
	}
	if err := ma.Channel(3).Send("b", []byte("lost")); err != nil {
		t.Fatal(err)
	}
	if len(got1) != 1 || got1[0] != "a:one" || len(got2) != 1 || got2[0] != "a:two" {
		t.Fatalf("routing: %v %v", got1, got2)
	}
	if mb.UnknownTagPackets() != 1 {
		t.Fatalf("unknown = %d", mb.UnknownTagPackets())
	}
	if err := ma.Channel(1).Send("b", nil); !errors.Is(err, ErrEmptyPacket) {
		t.Fatalf("empty send: %v", err)
	}
	b.handler("a", nil) // empty packets are ignored
	if ma.Channel(1).LocalAddr() != "a" {
		t.Fatal("LocalAddr")
	}
	if err := mb.Channel(1).Close(); err != nil {
		t.Fatal(err)
	}
	if err := ma.Channel(1).Send("b", []byte("x")); err != nil || len(got1) != 1 {
		t.Fatal("closed channel still delivered")
	}
	a.failing = true
	if err := ma.Channel(1).Send("b", []byte("x")); err == nil {
		t.Fatal("base error not propagated")
	}
	if err := ma.Close(); err != nil || !a.closed {
		t.Fatal("Mux.Close must close the base transport")
	}
}

func TestUDPRoundTrip(t *testing.T) {
	a, err := ListenUDP("127.0.0.1:0", nil)
	if err != nil {
		t.Skipf("loopback UDP unavailable: %v", err)
	}
	b, err := ListenUDP("127.0.0.1:0", nil)
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	got := make(chan string, 1)
	b.SetHandler(func(from string, p []byte) {
		mu.Lock()
		defer mu.Unlock()
		if from == a.LocalAddr() {
			got <- string(p)
		}
	})
	if err := a.Send(b.LocalAddr(), []byte("hello")); err != nil {
		t.Fatal(err)
	}
	select {
	case s := <-got:
		if s != "hello" {
			t.Fatalf("got %q", s)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("datagram not received over loopback")
	}
	if err := a.Send("not an address", []byte("x")); err == nil {
		t.Fatal("bad address accepted")
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	if err := a.Close(); err != nil {
		t.Fatal("second close should be a no-op")
	}
	if err := a.Send(b.LocalAddr(), []byte("x")); !errors.Is(err, ErrClosed) {
		t.Fatalf("send after close: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := ListenUDP("256.0.0.1:0", nil); err == nil {
		t.Fatal("invalid bind accepted")
	}
}
