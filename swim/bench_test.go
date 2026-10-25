package swim

import (
	"fmt"
	"testing"
	"time"

	"github.com/SlateGitOrg/swimring/sim"
)

func benchPacket() *packet {
	ups := make([]update, 0, 8)
	for i := range 8 {
		ups = append(ups, update{Kind: kindSuspect, Name: fmt.Sprintf("node-%03d", i), Addr: "10.0.0.1:7946", Incarnation: uint64(i), From: "node-999"})
	}
	return &packet{Type: msgPing, Seq: 12345, Target: "node-001", Source: "node-000", Updates: ups}
}

func BenchmarkEncodePing8Updates(b *testing.B) {
	p := benchPacket()
	for b.Loop() {
		if _, err := p.encode(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecodePing8Updates(b *testing.B) {
	buf, err := benchPacket().encode()
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(len(buf)))
	for b.Loop() {
		if _, err := decodePacket(buf); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBroadcastTake(b *testing.B) {
	for _, q := range []int{16, 128, 1024} {
		b.Run(fmt.Sprintf("queue%d", q), func(b *testing.B) {
			bq := newBroadcastQueue()
			for i := range q {
				bq.Queue(update{Name: fmt.Sprintf("n%04d", i), Addr: "10.0.0.1:7946"})
			}
			for b.Loop() {
				bq.Take(1300, 1<<30, "")
			}
		})
	}
}

// BenchmarkSimulatedProtocolPeriod measures the wall-clock cost of one
// protocol period of a whole simulated cluster (every member probes once).
func BenchmarkSimulatedProtocolPeriod(b *testing.B) {
	for _, size := range []int{32, 128} {
		b.Run(fmt.Sprintf("n%d", size), func(b *testing.B) {
			s := sim.New(1)
			nw, _ := sim.NewNetwork(s, sim.LinkConfig{Delay: time.Millisecond, Jitter: 3 * time.Millisecond})
			nodes := make([]*Node, size)
			for i := range nodes {
				h, _ := nw.AddHost(fmt.Sprintf("n%03d", i))
				n, err := New(testConfig(h.LocalAddr()), h, h.Clock(), s.NewRand())
				if err != nil {
					b.Fatal(err)
				}
				if err := n.Start(); err != nil {
					b.Fatal(err)
				}
				nodes[i] = n
			}
			for _, n := range nodes[1:] {
				if err := n.Join([]string{nodes[0].Addr()}); err != nil {
					b.Fatal(err)
				}
			}
			s.RunWhile(func() bool { return nodes[size-1].NumLive() != size }, 5*time.Minute)
			s.RunFor(30 * time.Second)
			b.ResetTimer()
			for b.Loop() {
				s.RunFor(time.Second)
			}
		})
	}
}
