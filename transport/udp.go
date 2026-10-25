package transport

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
)

// MaxUDPPayload is the largest datagram the UDP transport will read. SWIM
// packets are kept well below the usual 1500-byte Ethernet MTU, but the read
// buffer is sized for the largest legal IPv4 UDP payload so oversized peers
// are truncated rather than misparsed.
const MaxUDPPayload = 65507

// UDP is a Transport over a real UDP socket.
type UDP struct {
	conn   *net.UDPConn
	logger *slog.Logger

	mu      sync.RWMutex
	handler Handler
	closed  bool

	addrMu    sync.Mutex
	addrCache map[string]*net.UDPAddr

	wg sync.WaitGroup
}

var _ Transport = (*UDP)(nil)

// ListenUDP binds a UDP socket on bind (for example "127.0.0.1:0") and starts
// the read loop. A nil logger discards logs.
func ListenUDP(bind string, logger *slog.Logger) (*UDP, error) {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	laddr, err := net.ResolveUDPAddr("udp", bind)
	if err != nil {
		return nil, fmt.Errorf("resolve %q: %w", bind, err)
	}
	conn, err := net.ListenUDP("udp", laddr)
	if err != nil {
		return nil, fmt.Errorf("listen %q: %w", bind, err)
	}
	u := &UDP{conn: conn, logger: logger, addrCache: make(map[string]*net.UDPAddr)}
	u.wg.Add(1)
	go u.readLoop()
	return u, nil
}

// LocalAddr implements Transport; it returns the bound host:port.
func (u *UDP) LocalAddr() string { return u.conn.LocalAddr().String() }

// SetHandler implements Transport.
func (u *UDP) SetHandler(h Handler) {
	u.mu.Lock()
	u.handler = h
	u.mu.Unlock()
}

// Send implements Transport.
func (u *UDP) Send(addr string, payload []byte) error {
	u.mu.RLock()
	closed := u.closed
	u.mu.RUnlock()
	if closed {
		return ErrClosed
	}
	raddr, err := u.resolve(addr)
	if err != nil {
		return err
	}
	if _, err := u.conn.WriteToUDP(payload, raddr); err != nil {
		return fmt.Errorf("udp send to %s: %w", addr, err)
	}
	return nil
}

// Close implements Transport. It stops the read loop and waits for it.
func (u *UDP) Close() error {
	u.mu.Lock()
	if u.closed {
		u.mu.Unlock()
		return nil
	}
	u.closed = true
	u.mu.Unlock()
	err := u.conn.Close()
	u.wg.Wait()
	if err != nil {
		return fmt.Errorf("udp close: %w", err)
	}
	return nil
}

func (u *UDP) resolve(addr string) (*net.UDPAddr, error) {
	u.addrMu.Lock()
	defer u.addrMu.Unlock()
	if a, ok := u.addrCache[addr]; ok {
		return a, nil
	}
	a, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return nil, fmt.Errorf("resolve %q: %w", addr, err)
	}
	u.addrCache[addr] = a
	return a, nil
}

func (u *UDP) readLoop() {
	defer u.wg.Done()
	buf := make([]byte, MaxUDPPayload)
	for {
		n, from, err := u.conn.ReadFromUDP(buf)
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}
			u.logger.Warn("udp read failed", "err", err)
			continue
		}
		u.mu.RLock()
		h := u.handler
		u.mu.RUnlock()
		if h == nil || n == 0 {
			continue
		}
		pkt := make([]byte, n)
		copy(pkt, buf[:n])
		h(from.String(), pkt)
	}
}
