package experiment

import (
	"math"
	"sort"
)

// Analytical expectations from the SWIM paper's model (Das, Gupta, Motivala,
// DSN 2002, section 3.1), assuming independent message loss with
// probability p and no other failures (q_f = 1).

// PaperFirstDetectionPeriods is the expected number of protocol periods
// until some member first pings a failed member, when every one of the n-1
// survivors picks a uniformly random target each period:
//
//	E = 1 / (1 - (1 - 1/(n-1))^(n-1))  →  e/(e-1) ≈ 1.582 as n → ∞
func PaperFirstDetectionPeriods(n int) float64 {
	if n < 2 {
		return math.Inf(1)
	}
	m := float64(n - 1)
	return 1 / (1 - math.Pow(1-1/m, m))
}

// RoundRobinFirstDetectionPeriods is the same expectation for this
// implementation's shuffled round-robin target selection, where each of the
// n-1 survivors pings the failed member exactly once per round of n-1
// periods at an independent uniform offset. Counting the period in which
// the first such ping falls:
//
//	E = Σ_{j=0}^{n-2} (1 - j/(n-1))^(n-1)  →  e/(e-1) as n → ∞
func RoundRobinFirstDetectionPeriods(n int) float64 {
	if n < 2 {
		return math.Inf(1)
	}
	m := float64(n - 1)
	sum := 0.0
	for j := 0; j < n-1; j++ {
		sum += math.Pow(1-float64(j)/m, m)
	}
	return sum
}

// AsymptoticFirstDetectionPeriods is e/(e-1).
func AsymptoticFirstDetectionPeriods() float64 { return math.E / (math.E - 1) }

// ProbeFailureProbability is the probability that one probe of a live
// member fails under independent loss p with k ping-req relays: the direct
// ping/ack exchange (2 messages) loses a message, and so does every one of
// the k indirect paths (4 messages each: ping-req, ping, ack, ack).
//
//	P = (1 - (1-p)^2) · (1 - (1-p)^4)^k
func ProbeFailureProbability(p float64, k int) float64 {
	ok := 1 - p
	return (1 - ok*ok) * math.Pow(1-math.Pow(ok, 4), float64(k))
}

// Summary describes a sample.
type Summary struct {
	N    int
	Mean float64
	P50  float64
	P95  float64
	Max  float64
}

// Summarize computes order statistics over xs (which it sorts in place).
func Summarize(xs []float64) Summary {
	if len(xs) == 0 {
		return Summary{}
	}
	sort.Float64s(xs)
	sum := 0.0
	for _, x := range xs {
		sum += x
	}
	return Summary{
		N:    len(xs),
		Mean: sum / float64(len(xs)),
		P50:  quantile(xs, 0.50),
		P95:  quantile(xs, 0.95),
		Max:  xs[len(xs)-1],
	}
}

// quantile uses the nearest-rank method on sorted xs.
func quantile(sorted []float64, q float64) float64 {
	idx := int(math.Ceil(q*float64(len(sorted)))) - 1
	idx = max(0, min(idx, len(sorted)-1))
	return sorted[idx]
}

// BinomialStdErr is the standard error of a proportion estimate.
func BinomialStdErr(p float64, n int) float64 {
	if n == 0 {
		return math.Inf(1)
	}
	return math.Sqrt(p * (1 - p) / float64(n))
}
