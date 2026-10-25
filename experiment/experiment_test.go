package experiment

import (
	"bytes"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/SlateGitOrg/swimring/swim"
)

func TestAnalyticFormulas(t *testing.T) {
	asym := AsymptoticFirstDetectionPeriods()
	if math.Abs(asym-1.5820) > 1e-3 {
		t.Fatalf("e/(e-1) = %v", asym)
	}
	// Both finite-n expectations increase towards e/(e-1).
	prevP, prevR := 0.0, 0.0
	for _, n := range []int{3, 8, 32, 128, 4096} {
		p, r := PaperFirstDetectionPeriods(n), RoundRobinFirstDetectionPeriods(n)
		if p < prevP || r < prevR || p > asym+1e-9 || r > asym+1e-9 {
			t.Fatalf("n=%d: paper %v rr %v not monotone towards %v", n, p, r, asym)
		}
		prevP, prevR = p, r
	}
	if math.Abs(PaperFirstDetectionPeriods(4096)-asym) > 1e-3 || math.Abs(RoundRobinFirstDetectionPeriods(4096)-asym) > 1e-3 {
		t.Fatal("finite-n formulas do not converge to e/(e-1)")
	}
	if !math.IsInf(PaperFirstDetectionPeriods(1), 1) || !math.IsInf(RoundRobinFirstDetectionPeriods(1), 1) {
		t.Fatal("n=1 should be infinite")
	}
	// Loss model edge cases.
	if ProbeFailureProbability(0, 3) != 0 || ProbeFailureProbability(1, 3) != 1 {
		t.Fatal("loss model edges")
	}
	if got := ProbeFailureProbability(0.1, 0); math.Abs(got-0.19) > 1e-12 {
		t.Fatalf("k=0: %v", got)
	}
}

func TestSummarize(t *testing.T) {
	s := Summarize([]float64{5, 1, 4, 2, 3})
	if s.N != 5 || s.Mean != 3 || s.P50 != 3 || s.Max != 5 || s.P95 != 5 {
		t.Fatalf("%+v", s)
	}
	if Summarize(nil).N != 0 {
		t.Fatal("empty")
	}
	if !math.IsInf(BinomialStdErr(0.5, 0), 1) {
		t.Fatal("stderr n=0")
	}
}

// The measured number of periods until a crashed member is first pinged
// must agree with the SWIM paper's e/(e-1) analysis.
func TestDetectionMatchesPaper(t *testing.T) {
	rows, err := RunDetection(DetectionSpec{Sizes: []int{24}, Losses: []float64{0}, Trials: 60, Seed: 11, Lifeguard: swim.LifeguardAll()})
	if err != nil {
		t.Fatal(err)
	}
	r := rows[0]
	if r.Detected != r.Trials {
		t.Fatalf("only %d/%d crashes detected everywhere", r.Detected, r.Trials)
	}
	if r.FalseDeaths != 0 {
		t.Fatalf("%d false deaths on a lossless network", r.FalseDeaths)
	}
	// Geometric-like with mean ~1.55 and sd ~0.8: 60 trials give a standard
	// error of ~0.1, so ±0.35 is a >3 SE band.
	if math.Abs(r.FirstPing.Mean-r.RoundRobinPeriods) > 0.35 {
		t.Fatalf("first-ping mean %.2f vs expected %.2f", r.FirstPing.Mean, r.RoundRobinPeriods)
	}
	if !(r.FirstPing.Mean <= r.FirstSuspect.Mean+1 && r.FirstSuspect.Mean < r.FirstDead.Mean && r.FirstDead.Mean <= r.AllDead.Mean) {
		t.Fatalf("latency ordering violated: %+v", r)
	}
}

func TestDetectionValidation(t *testing.T) {
	if _, err := RunDetection(DetectionSpec{Sizes: []int{8}, Losses: []float64{0}, Trials: 0}); err == nil {
		t.Fatal("trials=0 accepted")
	}
	if _, err := RunDetection(DetectionSpec{Sizes: []int{2}, Losses: []float64{0}, Trials: 1}); err == nil {
		t.Fatal("size 2 accepted")
	}
	if _, err := NewCluster(ClusterSpec{Size: 0}); err == nil {
		t.Fatal("size 0 accepted")
	}
}

// Under independent loss the per-probe failure rate must match
// (1-(1-p)^2)(1-(1-p)^4)^k within a few standard errors.
func TestFalsePositiveRateMatchesModel(t *testing.T) {
	rows, err := RunFalsePositives(FalsePositiveSpec{Sizes: []int{16}, Losses: []float64{0.2}, Periods: 300, Seed: 12, Lifeguard: swim.LifeguardAll()})
	if err != nil {
		t.Fatal(err)
	}
	r := rows[0]
	if r.Probes < 1000 {
		t.Fatalf("only %d probes", r.Probes)
	}
	z := (r.Measured - r.Expected) / r.StdErr
	if math.Abs(z) > 4 {
		t.Fatalf("measured %.4f vs model %.4f (z=%.1f)", r.Measured, r.Expected, z)
	}
	if r.Suspicions == 0 || r.Refutations == 0 {
		t.Fatalf("expected suspicions and refutations at 20%% loss: %+v", r)
	}
	if _, err := RunFalsePositives(FalsePositiveSpec{Sizes: []int{4}, Losses: []float64{0}, Periods: 0}); err == nil {
		t.Fatal("periods=0 accepted")
	}
}

// Lifeguard's local health multiplier must stop slow members from accusing
// healthy ones, and dynamic suspicion must prevent healthy members from
// being declared dead.
func TestLifeguardReducesFalsePositives(t *testing.T) {
	rows, err := RunLifeguard(LifeguardSpec{
		Size: 16, Slow: 2, Lag: 2 * time.Second, Periods: 200, Runs: 1, Seed: 13,
		Variants: []Variant{
			{Name: "off", Lifeguard: swim.LifeguardOff()},
			{Name: "all", Lifeguard: swim.LifeguardAll()},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	off, all := rows[0], rows[1]
	if off.HealthyAccusations < 10*max(all.HealthyAccusations, 1) {
		t.Fatalf("accusations: off=%d all=%d, want >=10x reduction", off.HealthyAccusations, all.HealthyAccusations)
	}
	if all.HealthyDeaths != 0 {
		t.Fatalf("Lifeguard declared %d healthy members dead", all.HealthyDeaths)
	}
	if all.MaxHealthSlow == 0 || off.MaxHealthSlow != 0 {
		t.Fatalf("LHM: off=%d all=%d", off.MaxHealthSlow, all.MaxHealthSlow)
	}
	if _, err := RunLifeguard(LifeguardSpec{Size: 4, Slow: 4, Periods: 1, Runs: 1}); err == nil {
		t.Fatal("invalid spec accepted")
	}
}

func TestBalance(t *testing.T) {
	rows, err := RunBalance(BalanceSpec{Nodes: []int{10}, VNodes: []int{1, 64}, Sets: 5, Keys: 5000, Seed: 1})
	if err != nil {
		t.Fatal(err)
	}
	if rows[1].RelStd >= rows[0].RelStd {
		t.Fatalf("more vnodes did not improve balance: %+v", rows)
	}
	for _, r := range rows {
		if r.RelStd < 0.6*r.Theory || r.RelStd > 1.4*r.Theory {
			t.Fatalf("rel std %.3f vs theory %.3f", r.RelStd, r.Theory)
		}
	}
	if _, err := RunBalance(BalanceSpec{Sets: 0}); err == nil {
		t.Fatal("invalid spec accepted")
	}
}

func TestReports(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteDetectionTable(&buf, []DetectionRow{{Size: 8, Trials: 1, Detected: 1}}); err != nil {
		t.Fatal(err)
	}
	if err := WriteFalsePositiveTable(&buf, []FalsePositiveRow{{Size: 8, StdErr: 0.1}}); err != nil {
		t.Fatal(err)
	}
	if err := WriteLifeguardTable(&buf, []LifeguardRow{{Variant: "x"}}); err != nil {
		t.Fatal(err)
	}
	if err := WriteBalanceTable(&buf, []BalanceRow{{Nodes: 1, VNodes: 1}}); err != nil {
		t.Fatal(err)
	}
	if got := strings.Count(buf.String(), "|\n|---"); got != 4 {
		t.Fatalf("expected 4 tables, got %d", got)
	}
}

func TestMixSeedDistinct(t *testing.T) {
	seen := map[uint64]bool{}
	for a := range uint64(20) {
		for b := range uint64(20) {
			s := mixSeed(1, a, b)
			if seen[s] {
				t.Fatalf("collision at %d,%d", a, b)
			}
			seen[s] = true
		}
	}
}
