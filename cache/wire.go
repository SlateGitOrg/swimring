package cache

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// Cache RPC wire format (varints are unsigned LEB128; str/bytes are a
// varint length followed by the bytes):
//
//	msg   := version:u8 op:u8 id:varint key:str body
//	body  := putReq:  entry
//	       | getReq:  (empty)
//	       | putResp: ok:u8 counter:varint node:str
//	       | getResp: found:u8 [entry]
//	entry := flags:u8 value:bytes counter:varint node:str
//
// A putResp carries ok=1 when the replica now holds the version it was sent
// (accepted, or already held), and in every case the version the replica
// holds afterwards; ok=0 means it holds a newer one and rejected the write.
//
// Version 2 added the held version to putResp (version 1 always answered
// ok=1, even when the replica had rejected the write).

const cacheWireVersion byte = 2

type op byte

const (
	opPutReq op = iota + 1
	opGetReq
	opPutResp
	opGetResp
)

const flagDeleted byte = 1

// Wire errors.
var (
	ErrMalformed = errors.New("cache: malformed message")
)

type message struct {
	Op    op
	ID    uint64
	Key   string
	Entry Entry
	Found bool
	OK    bool
	// Held is the replica's version after a put (putResp only).
	Held Version
}

func appendBytes(buf, b []byte) []byte {
	buf = binary.AppendUvarint(buf, uint64(len(b)))
	return append(buf, b...)
}

func appendEntry(buf []byte, e Entry) []byte {
	var flags byte
	if e.Deleted {
		flags |= flagDeleted
	}
	buf = append(buf, flags)
	buf = appendBytes(buf, e.Value)
	buf = binary.AppendUvarint(buf, e.Version.Counter)
	return appendBytes(buf, []byte(e.Version.Node))
}

func (m *message) encode() []byte {
	buf := make([]byte, 0, 32+len(m.Key)+len(m.Entry.Value))
	buf = append(buf, cacheWireVersion, byte(m.Op))
	buf = binary.AppendUvarint(buf, m.ID)
	buf = appendBytes(buf, []byte(m.Key))
	switch m.Op {
	case opPutReq:
		buf = appendEntry(buf, m.Entry)
	case opPutResp:
		buf = append(buf, boolByte(m.OK))
		buf = binary.AppendUvarint(buf, m.Held.Counter)
		buf = appendBytes(buf, []byte(m.Held.Node))
	case opGetResp:
		buf = append(buf, boolByte(m.Found))
		if m.Found {
			buf = appendEntry(buf, m.Entry)
		}
	case opGetReq:
	}
	return buf
}

func boolByte(b bool) byte {
	if b {
		return 1
	}
	return 0
}

type decoder struct {
	b   []byte
	off int
	err error
}

func (d *decoder) byte() byte {
	if d.err != nil {
		return 0
	}
	if d.off >= len(d.b) {
		d.err = ErrMalformed
		return 0
	}
	v := d.b[d.off]
	d.off++
	return v
}

func (d *decoder) uvarint() uint64 {
	if d.err != nil {
		return 0
	}
	v, n := binary.Uvarint(d.b[d.off:])
	if n <= 0 {
		d.err = ErrMalformed
		return 0
	}
	d.off += n
	return v
}

func (d *decoder) bytes() []byte {
	l := d.uvarint()
	if d.err != nil {
		return nil
	}
	if l > uint64(len(d.b)-d.off) {
		d.err = ErrMalformed
		return nil
	}
	out := make([]byte, l)
	copy(out, d.b[d.off:d.off+int(l)])
	d.off += int(l)
	return out
}

func (d *decoder) entry() Entry {
	var e Entry
	flags := d.byte()
	e.Deleted = flags&flagDeleted != 0
	e.Value = d.bytes()
	e.Version.Counter = d.uvarint()
	e.Version.Node = string(d.bytes())
	return e
}

func decodeMessage(b []byte) (*message, error) {
	d := &decoder{b: b}
	if v := d.byte(); d.err == nil && v != cacheWireVersion {
		return nil, fmt.Errorf("%w: version %d", ErrMalformed, v)
	}
	m := &message{Op: op(d.byte())}
	m.ID = d.uvarint()
	m.Key = string(d.bytes())
	switch m.Op {
	case opPutReq:
		m.Entry = d.entry()
	case opGetReq:
	case opPutResp:
		m.OK = d.byte() == 1
		m.Held.Counter = d.uvarint()
		m.Held.Node = string(d.bytes())
	case opGetResp:
		m.Found = d.byte() == 1
		if m.Found {
			m.Entry = d.entry()
		}
	default:
		if d.err == nil {
			return nil, fmt.Errorf("%w: op %d", ErrMalformed, m.Op)
		}
	}
	if d.err != nil {
		return nil, d.err
	}
	if d.off != len(b) {
		return nil, fmt.Errorf("%w: trailing bytes", ErrMalformed)
	}
	return m, nil
}
