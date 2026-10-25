package experiment

import (
	"fmt"
	"io"
	"math"
	"strings"
)

// Markdown renderers for experiment results. Every table states its units;
// latencies are in protocol periods T.

// WriteDetectionTable renders detection rows.
func WriteDetectionTable(w io.Writer, rows []DetectionRow) error {
	var b strings.Builder
	b.WriteString("| n | loss | detected | first ping (mean) | paper E[·] random | E[·] round-robin | first suspect | first dead | dead everywhere (mean / p95) | dissemination (mean) | dissem / ln n | false deaths |\n")
	b.WriteString("|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n")
	for _, r := range rows {
		fmt.Fprintf(&b, "| %d | %.0f%% | %d/%d | %.2f | %.2f | %.2f | %.2f | %.2f | %.2f / %.2f | %.2f | %.2f | %d |\n",
			r.Size, r.Loss*100, r.Detected, r.Trials,
			r.FirstPing.Mean, r.PaperPeriods, r.RoundRobinPeriods,
			r.FirstSuspect.Mean, r.FirstDead.Mean, r.AllDead.Mean, r.AllDead.P95,
			r.Dissemination.Mean, r.Dissemination.Mean/math.Log(float64(r.Size)), r.FalseDeaths)
	}
	_, err := io.WriteString(w, b.String())
	return err
}

// WriteFalsePositiveTable renders false-positive rows.
func WriteFalsePositiveTable(w io.Writer, rows []FalsePositiveRow) error {
	var b strings.Builder
	b.WriteString("| n | loss | probes | failed | measured P(fail) | model P(fail) | (meas−model)/SE | suspicions | refutations | false death declarations | members lost |\n")
	b.WriteString("|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n")
	for _, r := range rows {
		z := 0.0
		if r.StdErr > 0 {
			z = (r.Measured - r.Expected) / r.StdErr
		}
		fmt.Fprintf(&b, "| %d | %.0f%% | %d | %d | %.2e | %.2e | %+.1f | %d | %d | %d | %d |\n",
			r.Size, r.Loss*100, r.Probes, r.Failures, r.Measured, r.Expected, z,
			r.Suspicions, r.Refutations, r.FalseDeaths, r.MembersLost)
	}
	_, err := io.WriteString(w, b.String())
	return err
}

// WriteLifeguardTable renders Lifeguard rows.
func WriteLifeguardTable(w io.Writer, rows []LifeguardRow) error {
	var b strings.Builder
	b.WriteString("| variant | runs | accusations of healthy members | …raised by slow members | healthy death declarations | healthy members lost | slow members accused | slow death declarations | max LHM on slow |\n")
	b.WriteString("|---|---:|---:|---:|---:|---:|---:|---:|---:|\n")
	for _, r := range rows {
		fmt.Fprintf(&b, "| %s | %d | %d | %d | %d | %d | %d | %d | %d |\n",
			r.Variant, r.Runs, r.HealthyAccusations, r.AccusationsBySlow,
			r.HealthyDeaths, r.HealthyLost, r.SlowAccused, r.SlowDeaths, r.MaxHealthSlow)
	}
	_, err := io.WriteString(w, b.String())
	return err
}

// WriteBalanceTable renders ring balance rows.
func WriteBalanceTable(w io.Writer, rows []BalanceRow) error {
	var b strings.Builder
	b.WriteString("| nodes | vnodes | rel. std of share (measured) | theory sqrt((M−V)/(V(M+1))) | max/mean share | keys moved on add | ideal 1/(N+1) |\n")
	b.WriteString("|---:|---:|---:|---:|---:|---:|---:|\n")
	for _, r := range rows {
		fmt.Fprintf(&b, "| %d | %d | %.3f | %.3f | %.3f | %.4f | %.4f |\n",
			r.Nodes, r.VNodes, r.RelStd, r.Theory, r.MaxOverMean, r.MovedOnAdd, 1/float64(r.Nodes+1))
	}
	_, err := io.WriteString(w, b.String())
	return err
}
