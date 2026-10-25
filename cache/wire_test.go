package cache

import (
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestMessageRoundTrip(t *testing.T) {
	e := Entry{Value: []byte("v"), Version: Version{Counter: 1 << 50, Node: "n1"}}
	for _, m := range []*message{
		{Op: opPutReq, ID: 1, Key: "k", Entry: e},
		{Op: opPutReq, ID: 2, Key: "k", Entry: Entry{Deleted: true, Version: Version{Counter: 3, Node: "x"}, Value: []byte{}}},
		{Op: opGetReq, ID: 3, Key: "k"},
		{Op: opPutResp, ID: 4, Key: "k", OK: true},
		{Op: opGetResp, ID: 5, Key: "k", Found: true, Entry: e},
		{Op: opGetResp, ID: 6, Key: "k"},
	} {
		got, err := decodeMessage(m.encode())
		if err != nil {
			t.Fatalf("op %d: %v", m.Op, err)
		}
		if m.Entry.Value == nil && got.Entry.Value != nil && len(got.Entry.Value) == 0 {
			got.Entry.Value = nil
		}
		if !reflect.DeepEqual(got, m) {
			t.Fatalf("round trip:\n got %+v\nwant %+v", got, m)
		}
	}
}

func TestMessageRejectsMalformed(t *testing.T) {
	good := (&message{Op: opPutReq, ID: 1, Key: "k", Entry: Entry{Value: []byte("v")}}).encode()
	for name, b := range map[string][]byte{
		"empty":     nil,
		"version":   {9, 1, 0, 0},
		"op":        {cacheWireVersion, 77, 0, 0},
		"truncated": good[:len(good)-1],
		"trailing":  append(append([]byte{}, good...), 1),
		"long key":  {cacheWireVersion, byte(opGetReq), 0, 50, 'a'},
	} {
		if _, err := decodeMessage(b); !errors.Is(err, ErrMalformed) {
			t.Errorf("%s: err = %v", name, err)
		}
	}
}

func FuzzDecodeMessage(f *testing.F) {
	f.Add((&message{Op: opGetResp, ID: 9, Key: "key", Found: true, Entry: Entry{Value: []byte("x")}}).encode())
	f.Add([]byte{cacheWireVersion, byte(opPutReq)})
	f.Fuzz(func(t *testing.T, b []byte) {
		m, err := decodeMessage(b)
		if err != nil {
			return
		}
		if _, err := decodeMessage(m.encode()); err != nil {
			t.Fatalf("accepted message does not re-encode: %v", err)
		}
	})
}

func TestStoreLWWAndTombstones(t *testing.T) {
	s := newStore()
	v1, v2 := Version{1, "a"}, Version{1, "b"}
	if !v1.Less(v2) || v2.Less(v1) || !(Version{}).IsZero() {
		t.Fatal("version order")
	}
	now := time.Unix(100, 0)
	if !s.apply("k", Entry{Value: []byte("1"), Version: v2, Stored: now}) {
		t.Fatal("first write rejected")
	}
	if s.apply("k", Entry{Value: []byte("0"), Version: v1}) {
		t.Fatal("older write accepted")
	}
	if s.apply("k", Entry{Value: []byte("1"), Version: v2}) {
		t.Fatal("equal version re-applied")
	}
	s.apply("d", Entry{Deleted: true, Version: v1, Stored: now})
	s.remove("k", v1) // wrong version: kept
	if _, ok := s.get("k"); !ok {
		t.Fatal("remove with stale version deleted the entry")
	}
	if n := s.expireTombstones(now.Add(time.Second)); n != 1 {
		t.Fatalf("expired %d", n)
	}
	if got := s.keys(); len(got) != 1 || got[0] != "k" {
		t.Fatalf("keys = %v", got)
	}
}

func BenchmarkMessageEncodeDecode(b *testing.B) {
	m := &message{Op: opPutReq, ID: 77, Key: "user:12345", Entry: Entry{Value: make([]byte, 256), Version: Version{42, "node-7"}}}
	for b.Loop() {
		if _, err := decodeMessage(m.encode()); err != nil {
			b.Fatal(err)
		}
	}
}
