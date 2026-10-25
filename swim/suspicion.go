package swim

import (
	"math"
	"time"

	"github.com/archit2501/swimring/clock"
)

// suspicion tracks one outstanding suspicion and its Lifeguard dynamic
// timeout.
//
// The timeout starts at max and decays towards min as independent members
// confirm the suspicion:
//
//	timeout(c) = max(min, max - (max-min) * log(c+1)/log(k+1))
//
// where c is the number of confirmations so far and k the number expected.
// With k < 1 (dynamic suspicion disabled, or a tiny cluster) the timeout is
// fixed at min, which is plain SWIM's suspicion mechanism.
type suspicion struct {
	clk        clock.Clock
	start      time.Time
	min, max   time.Duration
	k          int
	confirms   int
	confirmers map[string]struct{}
	timer      clock.Timer
	onTimeout  func()
}

func newSuspicion(clk clock.Clock, from string, k int, minT, maxT time.Duration, onTimeout func()) *suspicion {
	s := &suspicion{
		clk:        clk,
		start:      clk.Now(),
		min:        minT,
		max:        maxT,
		k:          k,
		confirmers: map[string]struct{}{from: {}},
		onTimeout:  onTimeout,
	}
	timeout := maxT
	if k < 1 {
		timeout = minT
	}
	s.timer = clk.AfterFunc(timeout, onTimeout)
	return s
}

// suspicionTimeout is the decay formula above.
func suspicionTimeout(confirms, k int, minT, maxT time.Duration) time.Duration {
	if k < 1 {
		return minT
	}
	frac := math.Log(float64(confirms)+1) / math.Log(float64(k)+1)
	raw := float64(maxT) - frac*float64(maxT-minT)
	t := time.Duration(math.Floor(raw))
	if t < minT {
		t = minT
	}
	return t
}

// Confirm records an independent confirmation from member from. It reports
// whether the confirmation was new (and therefore worth re-gossiping).
func (s *suspicion) Confirm(from string) bool {
	if s.confirms >= s.k {
		return false
	}
	if _, dup := s.confirmers[from]; dup {
		return false
	}
	s.confirmers[from] = struct{}{}
	s.confirms++
	timeout := suspicionTimeout(s.confirms, s.k, s.min, s.max)
	remaining := timeout - s.clk.Now().Sub(s.start)
	if s.timer.Stop() {
		if remaining < 0 {
			remaining = 0
		}
		s.timer = s.clk.AfterFunc(remaining, s.onTimeout)
	}
	return true
}

// Confirmations returns how many independent confirmations arrived.
func (s *suspicion) Confirmations() int { return s.confirms }

// Stop cancels the timeout.
func (s *suspicion) Stop() { s.timer.Stop() }

// minSuspicionTimeout is α·max(1, log10(n))·interval (SWIM section 4.2
// recommends a timeout proportional to log n protocol periods).
func minSuspicionTimeout(mult, n int, interval time.Duration) time.Duration {
	scale := math.Max(1, math.Log10(math.Max(1, float64(n))))
	return time.Duration(float64(mult) * scale * float64(interval))
}
