package swim

import "time"

// awareness is Lifeguard's Local Health Multiplier: a saturating counter in
// [0, max-1]. Probe intervals and timeouts are multiplied by score+1, so a
// node that keeps observing evidence of its own slowness (missed acks and
// nacks, having to refute suspicions about itself) backs off instead of
// accusing healthy peers.
type awareness struct {
	max   int
	score int
}

func newAwareness(max int) *awareness { return &awareness{max: max} }

// Apply adds delta, clamping to [0, max-1]. It returns the new score.
func (a *awareness) Apply(delta int) int {
	a.score += delta
	if a.score < 0 {
		a.score = 0
	}
	if a.score > a.max-1 {
		a.score = a.max - 1
	}
	return a.score
}

// Score returns the current multiplier minus one.
func (a *awareness) Score() int { return a.score }

// Scale multiplies d by score+1.
func (a *awareness) Scale(d time.Duration) time.Duration {
	return d * time.Duration(a.score+1)
}
