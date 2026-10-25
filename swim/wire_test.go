package swim

import (
	"errors"
	"reflect"
	"strings"
	"testing"
)

func samplePackets() []*packet {
	ups := []update{
		{Kind: kindAlive, Name: "a", Addr: "10.0.0.1:7946", Incarnation: 3},
		{Kind: kindSuspect, Name: "b", Addr: "10.0.0.2:7946", Incarnation: 1 << 40, From: "a"},
		{Kind: kindDead, Name: "c", Addr: "", Incarnation: 0, From: "b"},
		{Kind: kindLeft, Name: "d", Addr: "x", Incarnation: 9},
	}
	return []*packet{
		{Type: msgPing, Seq: 1, Target: "b", Source: "a", Updates: ups},
		{Type: msgPingReq, Seq: 300, Target: "c", TargetAddr: "10.0.0.3:1", Source: "a", Flags: flagNack},
		{Type: msgAck, Seq: 1<<63 + 5, Updates: ups[:1]},
		{Type: msgNack, Seq: 0},
		{Type: msgSync, Flags: flagJoin | flagWantReply, Source: "z", Updates: ups},
	}
}

func TestWireRoundTrip(t *testing.T) {
	for _, p := range samplePackets() {
		b, err := p.encode()
		if err != nil {
			t.Fatalf("%v: %v", p.Type, err)
		}
		got, err := decodePacket(b)
		if err != nil {
			t.Fatalf("%v: decode: %v", p.Type, err)
		}
		if !reflect.DeepEqual(got, p) {
			t.Fatalf("%v round trip:\n got %+v\nwant %+v", p.Type, got, p)
		}
		// headerLen + count + updates must equal the encoded size exactly,
		// because the piggyback budget relies on it.
		size := p.headerLen() + uvarintLen(uint64(len(p.Updates)))
		for _, u := range p.Updates {
			size += u.encodedLen()
		}
		if size != len(b) {
			t.Fatalf("%v: predicted %d bytes, encoded %d", p.Type, size, len(b))
		}
	}
}

func TestWireRejectsMalformed(t *testing.T) {
	good, err := samplePackets()[0].encode()
	if err != nil {
		t.Fatal(err)
	}
	cases := map[string]struct {
		b    []byte
		want error
	}{
		"empty":         {nil, ErrShortPacket},
		"version":       {[]byte{9, 1}, ErrBadVersion},
		"no type":       {[]byte{wireVersion}, ErrShortPacket},
		"type":          {[]byte{wireVersion, 99}, ErrBadType},
		"truncated":     {good[:len(good)-1], ErrShortPacket},
		"trailing":      {append(append([]byte{}, good...), 0), ErrTrailingData},
		"huge count":    {[]byte{wireVersion, byte(msgAck), 1, 0xff, 0xff, 0x03}, ErrBadUpdate},
		"bad kind":      {[]byte{wireVersion, byte(msgAck), 1, 1, 7, 1, 'a', 0, 0, 0}, ErrBadUpdate},
		"empty name":    {[]byte{wireVersion, byte(msgAck), 1, 1, 0, 0, 0, 0, 0}, ErrBadUpdate},
		"short string":  {[]byte{wireVersion, byte(msgPing), 1, 5, 'a'}, ErrShortPacket},
		"bad varint":    {[]byte{wireVersion, byte(msgAck), 0xff}, ErrShortPacket},
		"sync no flags": {[]byte{wireVersion, byte(msgSync)}, ErrShortPacket},
	}
	for name, tc := range cases {
		if _, err := decodePacket(tc.b); !errors.Is(err, tc.want) {
			t.Errorf("%s: err = %v, want %v", name, err, tc.want)
		}
	}
}

func TestWireRejectsOversizedStrings(t *testing.T) {
	long := strings.Repeat("x", maxStringLen+1)
	for _, p := range []*packet{
		{Type: msgPing, Target: long},
		{Type: msgPingReq, TargetAddr: long},
		{Type: msgSync, Source: long},
		{Type: msgAck, Updates: []update{{Name: long}}},
		{Type: msgAck, Updates: []update{{Name: "a", From: long}}},
		{Type: msgAck, Updates: []update{{Name: "a", Kind: 9}}},
		{Type: 77},
	} {
		if _, err := p.encode(); err == nil {
			t.Errorf("%v encoded invalid packet", p.Type)
		}
	}
}

func TestEnumStrings(t *testing.T) {
	for _, s := range []string{
		msgPing.String(), msgPingReq.String(), msgAck.String(), msgNack.String(), msgSync.String(), msgType(42).String(),
		kindAlive.String(), kindSuspect.String(), kindDead.String(), kindLeft.String(), updateKind(9).String(),
		StateAlive.String(), StateSuspect.String(), StateDead.String(), StateLeft.String(), State(9).String(),
		EventJoin.String(), EventUpdate.String(), EventLeave.String(), EventType(9).String(),
		TraceProbe.String(), TraceProbeOK.String(), TraceProbeFail.String(), TraceRefute.String(), TraceKind(9).String(),
	} {
		if s == "" {
			t.Fatal("empty enum string")
		}
	}
	m := Member{Name: "a", Addr: "b", State: StateSuspect, Incarnation: 2}
	if m.String() != "a@b[suspect inc=2]" {
		t.Fatal(m.String())
	}
}

// FuzzDecodePacket asserts the decoder never panics and that anything it
// accepts re-encodes to the same bytes.
func FuzzDecodePacket(f *testing.F) {
	for _, p := range samplePackets() {
		b, err := p.encode()
		if err != nil {
			f.Fatal(err)
		}
		f.Add(b)
	}
	f.Add([]byte{})
	f.Add([]byte{wireVersion, byte(msgAck), 0x80})
	f.Fuzz(func(t *testing.T, b []byte) {
		p, err := decodePacket(b)
		if err != nil {
			return
		}
		again, err := p.encode()
		if err != nil {
			t.Fatalf("decoded packet does not re-encode: %v", err)
		}
		q, err := decodePacket(again)
		if err != nil || !reflect.DeepEqual(p, q) {
			t.Fatalf("re-encode not stable: %v", err)
		}
	})
}
