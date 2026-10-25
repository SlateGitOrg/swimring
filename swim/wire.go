package swim

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// Wire format (all integers are unsigned LEB128 varints, strings are a
// one-byte length followed by bytes):
//
//	packet  := version:u8 type:u8 header updates
//	header  := ping:    seq target source
//	         | pingreq: seq target targetAddr source flags:u8
//	         | ack:     seq
//	         | nack:    seq
//	         | sync:    flags:u8 source
//	updates := count:varint update*
//	update  := kind:u8 name addr incarnation:varint from
//
// Every packet type can carry piggybacked updates; a sync packet's updates
// are a chunk of the sender's full membership table.

const (
	wireVersion   byte = 1
	maxStringLen       = 255
	minPacketSize      = 512
	// minUpdateLen is the smallest possible encoded update: kind, three
	// empty strings and a one-byte incarnation.
	minUpdateLen = 5
)

type msgType byte

const (
	msgPing msgType = iota + 1
	msgPingReq
	msgAck
	msgNack
	msgSync
)

func (t msgType) String() string {
	switch t {
	case msgPing:
		return "ping"
	case msgPingReq:
		return "ping-req"
	case msgAck:
		return "ack"
	case msgNack:
		return "nack"
	case msgSync:
		return "sync"
	default:
		return fmt.Sprintf("msg(%d)", byte(t))
	}
}

// Packet flags.
const (
	flagNack      byte = 1 << 0 // ping-req: relay should nack on timeout
	flagWantReply byte = 1 << 1 // sync: receiver should answer with its state
	flagJoin      byte = 1 << 2 // sync: sender is joining
)

// updateKind is the membership assertion carried by an update.
type updateKind byte

const (
	kindAlive updateKind = iota
	kindSuspect
	kindDead
	kindLeft
)

func (k updateKind) String() string {
	switch k {
	case kindAlive:
		return "alive"
	case kindSuspect:
		return "suspect"
	case kindDead:
		return "dead"
	case kindLeft:
		return "left"
	default:
		return fmt.Sprintf("kind(%d)", byte(k))
	}
}

// update is a single gossiped membership assertion.
type update struct {
	Kind        updateKind
	Name        string
	Addr        string
	Incarnation uint64
	// From is the member that originated a suspect or dead assertion.
	From string
}

func (u update) encodedLen() int {
	return 1 + 1 + len(u.Name) + 1 + len(u.Addr) + uvarintLen(u.Incarnation) + 1 + len(u.From)
}

// packet is the decoded form of any protocol message.
type packet struct {
	Type       msgType
	Seq        uint64
	Target     string
	TargetAddr string
	Source     string
	Flags      byte
	Updates    []update
}

// Decoding errors.
var (
	ErrShortPacket  = errors.New("swim: truncated packet")
	ErrBadVersion   = errors.New("swim: unsupported wire version")
	ErrBadType      = errors.New("swim: unknown message type")
	ErrBadUpdate    = errors.New("swim: malformed update")
	ErrTrailingData = errors.New("swim: trailing bytes after packet")
)

// headerLen returns the encoded size of the packet without its updates and
// without the update count.
func (p *packet) headerLen() int {
	n := 2
	switch p.Type {
	case msgPing:
		n += uvarintLen(p.Seq) + 1 + len(p.Target) + 1 + len(p.Source)
	case msgPingReq:
		n += uvarintLen(p.Seq) + 1 + len(p.Target) + 1 + len(p.TargetAddr) + 1 + len(p.Source) + 1
	case msgAck, msgNack:
		n += uvarintLen(p.Seq)
	case msgSync:
		n += 1 + 1 + len(p.Source)
	}
	return n
}

// encode serialises p. Strings longer than maxStringLen are rejected.
func (p *packet) encode() ([]byte, error) {
	size := p.headerLen() + uvarintLen(uint64(len(p.Updates)))
	for _, u := range p.Updates {
		size += u.encodedLen()
	}
	buf := make([]byte, 0, size)
	buf = append(buf, wireVersion, byte(p.Type))
	var err error
	switch p.Type {
	case msgPing:
		buf = binary.AppendUvarint(buf, p.Seq)
		if buf, err = appendString(buf, p.Target); err != nil {
			return nil, err
		}
		if buf, err = appendString(buf, p.Source); err != nil {
			return nil, err
		}
	case msgPingReq:
		buf = binary.AppendUvarint(buf, p.Seq)
		for _, s := range []string{p.Target, p.TargetAddr, p.Source} {
			if buf, err = appendString(buf, s); err != nil {
				return nil, err
			}
		}
		buf = append(buf, p.Flags)
	case msgAck, msgNack:
		buf = binary.AppendUvarint(buf, p.Seq)
	case msgSync:
		buf = append(buf, p.Flags)
		if buf, err = appendString(buf, p.Source); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("%w: %d", ErrBadType, byte(p.Type))
	}
	buf = binary.AppendUvarint(buf, uint64(len(p.Updates)))
	for _, u := range p.Updates {
		if u.Kind > kindLeft {
			return nil, fmt.Errorf("%w: kind %d", ErrBadUpdate, byte(u.Kind))
		}
		buf = append(buf, byte(u.Kind))
		for _, s := range []string{u.Name, u.Addr} {
			if buf, err = appendString(buf, s); err != nil {
				return nil, err
			}
		}
		buf = binary.AppendUvarint(buf, u.Incarnation)
		if buf, err = appendString(buf, u.From); err != nil {
			return nil, err
		}
	}
	return buf, nil
}

// decodePacket parses b. It never panics on malformed input.
func decodePacket(b []byte) (*packet, error) {
	r := reader{b: b}
	ver, err := r.byte()
	if err != nil {
		return nil, err
	}
	if ver != wireVersion {
		return nil, fmt.Errorf("%w: %d", ErrBadVersion, ver)
	}
	t, err := r.byte()
	if err != nil {
		return nil, err
	}
	p := &packet{Type: msgType(t)}
	switch p.Type {
	case msgPing:
		if p.Seq, err = r.uvarint(); err != nil {
			return nil, err
		}
		if p.Target, err = r.string(); err != nil {
			return nil, err
		}
		if p.Source, err = r.string(); err != nil {
			return nil, err
		}
	case msgPingReq:
		if p.Seq, err = r.uvarint(); err != nil {
			return nil, err
		}
		if p.Target, err = r.string(); err != nil {
			return nil, err
		}
		if p.TargetAddr, err = r.string(); err != nil {
			return nil, err
		}
		if p.Source, err = r.string(); err != nil {
			return nil, err
		}
		if p.Flags, err = r.byte(); err != nil {
			return nil, err
		}
	case msgAck, msgNack:
		if p.Seq, err = r.uvarint(); err != nil {
			return nil, err
		}
	case msgSync:
		if p.Flags, err = r.byte(); err != nil {
			return nil, err
		}
		if p.Source, err = r.string(); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("%w: %d", ErrBadType, t)
	}
	count, err := r.uvarint()
	if err != nil {
		return nil, err
	}
	if count > uint64(r.remaining()/minUpdateLen) {
		return nil, fmt.Errorf("%w: count %d exceeds packet size", ErrBadUpdate, count)
	}
	if count > 0 {
		p.Updates = make([]update, 0, count)
	}
	for range count {
		var u update
		k, err := r.byte()
		if err != nil {
			return nil, err
		}
		if updateKind(k) > kindLeft {
			return nil, fmt.Errorf("%w: kind %d", ErrBadUpdate, k)
		}
		u.Kind = updateKind(k)
		if u.Name, err = r.string(); err != nil {
			return nil, err
		}
		if u.Name == "" {
			return nil, fmt.Errorf("%w: empty member name", ErrBadUpdate)
		}
		if u.Addr, err = r.string(); err != nil {
			return nil, err
		}
		if u.Incarnation, err = r.uvarint(); err != nil {
			return nil, err
		}
		if u.From, err = r.string(); err != nil {
			return nil, err
		}
		p.Updates = append(p.Updates, u)
	}
	if r.remaining() != 0 {
		return nil, ErrTrailingData
	}
	return p, nil
}

func appendString(buf []byte, s string) ([]byte, error) {
	if len(s) > maxStringLen {
		return nil, fmt.Errorf("swim: string of %d bytes exceeds %d", len(s), maxStringLen)
	}
	buf = append(buf, byte(len(s)))
	return append(buf, s...), nil
}

func uvarintLen(v uint64) int {
	n := 1
	for v >= 0x80 {
		v >>= 7
		n++
	}
	return n
}

type reader struct {
	b   []byte
	off int
}

func (r *reader) remaining() int { return len(r.b) - r.off }

func (r *reader) byte() (byte, error) {
	if r.off >= len(r.b) {
		return 0, ErrShortPacket
	}
	v := r.b[r.off]
	r.off++
	return v, nil
}

func (r *reader) uvarint() (uint64, error) {
	v, n := binary.Uvarint(r.b[r.off:])
	if n <= 0 {
		return 0, ErrShortPacket
	}
	r.off += n
	return v, nil
}

func (r *reader) string() (string, error) {
	l, err := r.byte()
	if err != nil {
		return "", err
	}
	if r.remaining() < int(l) {
		return "", ErrShortPacket
	}
	s := string(r.b[r.off : r.off+int(l)])
	r.off += int(l)
	return s, nil
}
