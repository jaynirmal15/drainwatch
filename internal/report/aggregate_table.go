package report

import (
	"fmt"
	"io"
	"sort"
	"strings"
	"text/tabwriter"
)

// statValue renders "median (min-max)" or "not-measured".
func statValue(s *Stat) string {
	if s == nil || s.Median == nil {
		return "not-measured"
	}
	if *s.Min == *s.Max {
		return fmt.Sprintf("%d", *s.Median)
	}
	return fmt.Sprintf("%d (%d-%d)", *s.Median, *s.Min, *s.Max)
}

// RenderArmAggregate writes the plain-text summary of one arm.
//
// Same-clock and cross-clock statistics are rendered in separate blocks and are
// never combined, because they do not have the same precision and a reader
// comparing them needs to know which is which.
func RenderArmAggregate(w io.Writer, a *ArmAggregate) {
	fmt.Fprintf(w, "ARM %s  behavior=%s  trigger=%s  grace=%ds  repeats=%d\n",
		a.Arm, a.DrainBehavior, a.Trigger, a.GracePeriodSeconds, a.Trials)
	fmt.Fprintf(w, "  %s  |  %s  |  kube-proxy %s  |  %d flows tcp / %d udp\n",
		a.Environment.KubernetesVersion, a.Environment.CNI, a.Environment.KubeProxyMode, a.TCPFlows, a.UDPFlows)
	fmt.Fprintln(w, strings.Repeat("-", 78))

	// Disagreements come first. They are the reason not to trust the medians
	// below, so they are never printed after them.
	if a.HasDisagreements {
		fmt.Fprintf(w, "\n  !! %d DISAGREEMENT(S) BETWEEN REPEATS - the statistics below average over repeats\n", len(a.Disagreements))
		fmt.Fprintln(w, "  !! that were not the same experiment. Resolve these before quoting any number.")
		for _, d := range a.Disagreements {
			fmt.Fprintf(w, "  !!   %s\n", d)
		}
		fmt.Fprintln(w)
	} else {
		noun := "repeats agree"
		if a.Trials == 1 {
			noun = "repeat (nothing to disagree with)"
		}
		fmt.Fprintf(w, "  all %d %s on outcome counts, exit code, configuration and environment\n", a.Trials, noun)
	}

	// Mechanism variation is a result, not a fault, so it is reported plainly
	// rather than as a warning - but still before the statistics, because it
	// qualifies what "10/10 severed" means.
	if a.HasMechanismVariation {
		fmt.Fprintf(w, "\n  NOTE: the repeats agree on outcomes but reached them by different routes.\n")
		fmt.Fprintf(w, "  NOTE: the intervals below remain comparable; the severance mechanism is itself a result.\n")
		for _, m := range a.MechanismVariations {
			fmt.Fprintf(w, "  NOTE:   %s\n", m)
		}
	}
	fmt.Fprintln(w)

	fmt.Fprintln(w, "  PER-REPEAT OUTCOMES")
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "    TRIAL\tTCP (d/s/rt/sw/nm)\tUDP (d/s/rt/sw/nm)\tEXIT\tTCP MECHANISM\tUDP MECHANISM")
	for _, r := range a.Repeats {
		fmt.Fprintf(tw, "    %s\t%d/%d/%d/%d/%d\t%d/%d/%d/%d/%d\t%s\t%s\t%s\n",
			r.TrialID,
			r.TCP.Drained, r.TCP.Severed, r.TCP.ReadTimeout, r.TCP.SurvivedWindow, r.TCP.NotMeasured,
			r.UDP.Drained, r.UDP.Severed, r.UDP.ReadTimeout, r.UDP.SurvivedWindow, r.UDP.NotMeasured,
			exitCodeString(r.ContainerExitCode),
			strings.Join(r.TCPMechanisms, ", "), strings.Join(r.UDPMechanisms, ", "))
	}
	tw.Flush()
	fmt.Fprintln(w, "    legend: d=drained s=severed rt=read-timeout sw=survived-window nm=not-measured")

	renderStatBlock(w, a, ClockSameProcess,
		"SAME-CLOCK INTERVALS (exact relative to the trigger; k8s endpoints are receipt times)")
	renderStatBlock(w, a, ClockCrossHost,
		"CROSS-CLOCK INTERVALS (probe wall clock vs orchestrator wall clock)")

	fmt.Fprintln(w, "\n  CONTAINER EXIT CODES")
	keys := make([]string, 0, len(a.ExitCodes))
	for k := range a.ExitCodes {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Fprintf(w, "    %-14s %d of %d repeats\n", k, a.ExitCodes[k], a.Trials)
	}
}

func renderStatBlock(w io.Writer, a *ArmAggregate, basis ClockBasis, heading string) {
	var block []Stat
	for _, s := range a.Stats {
		if s.ClockBasis == basis {
			block = append(block, s)
		}
	}
	if len(block) == 0 {
		return
	}
	fmt.Fprintf(w, "\n  %s\n", heading)
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "    METRIC\tMEDIAN (MIN-MAX)\tN\tNOT-MEASURED")
	for _, s := range block {
		fmt.Fprintf(tw, "    %s\t%s\t%d\t%d\n", s.Metric, statValue(&s), s.Observed, s.NotMeasured)
	}
	tw.Flush()
	if basis == ClockCrossHost {
		for _, line := range wrap("caveat: "+CrossClockCaveat, 72) {
			fmt.Fprintf(w, "    %s\n", line)
		}
	}
}

// RenderMatrix writes the cross-arm comparison: one row per arm, medians with
// their ranges. Same-clock and cross-clock columns are labelled so the two are
// not read as equally precise.
func RenderMatrix(w io.Writer, arms []*ArmAggregate) {
	fmt.Fprintln(w, "MATRIX  (values are median (min-max) in ms across repeats)")
	fmt.Fprintln(w, strings.Repeat("=", 78))

	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "  ARM\tBEHAVIOR\tTRIGGER\tN\tTCP\tUDP\tFIRST TCP END*\tLAST TCP END*\tENDPOINT GONE*\tCONTAINER EXIT*\tEXIT CODE")
	for _, a := range arms {
		tcp, udp := outcomeShorthand(a, "tcp"), outcomeShorthand(a, "udp")
		fmt.Fprintf(tw, "  %s\t%s\t%s\t%d\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			a.Arm, a.DrainBehavior, a.Trigger, a.Trials, tcp, udp,
			statValue(a.Stat("trigger_to_first_tcp_terminal_ms")),
			statValue(a.Stat("trigger_to_last_tcp_terminal_ms")),
			statValue(a.Stat("trigger_to_endpoint_removed_ms")),
			statValue(a.Stat("trigger_to_container_terminated_ms")),
			exitCodeTally(a))
	}
	tw.Flush()
	fmt.Fprintln(w, "  * same-clock: exact relative to the trigger")

	fmt.Fprintln(w, "\n  CROSS-CLOCK COLUMNS (probe wall clock vs orchestrator wall clock; tens of ms, not sub-ms)")
	tw = tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "  ARM\tTRIGGER->SIGTERM\tSIGTERM->READY:FALSE\tSIGTERM->ENDPOINT GONE")
	for _, a := range arms {
		fmt.Fprintf(tw, "  %s\t%s\t%s\t%s\n", a.Arm,
			statValue(a.Stat("trigger_to_sigterm_ms")),
			statValue(a.Stat("sigterm_to_ready_false_ms")),
			statValue(a.Stat("sigterm_to_endpoint_removed_ms")))
	}
	tw.Flush()

	var flagged []*ArmAggregate
	for _, a := range arms {
		if a.HasDisagreements {
			flagged = append(flagged, a)
		}
	}
	if len(flagged) == 0 {
		fmt.Fprintln(w, "\n  every arm's repeats agree internally")
		return
	}
	fmt.Fprintf(w, "\n  !! %d ARM(S) HAVE REPEATS THAT DISAGREE\n", len(flagged))
	for _, a := range flagged {
		for _, d := range a.Disagreements {
			fmt.Fprintf(w, "  !!   arm %s: %s\n", a.Arm, d)
		}
	}
}

// outcomeShorthand describes an arm's outcome for one protocol, e.g. "10/10
// severed". When the repeats disagree it says so rather than picking one.
func outcomeShorthand(a *ArmAggregate, proto string) string {
	type key struct {
		outcome string
		n       int
	}
	seen := map[key]int{}
	for _, r := range a.Repeats {
		p := r.TCP
		if proto == "udp" {
			p = r.UDP
		}
		switch {
		case p.Drained == p.Total && p.Total > 0:
			seen[key{"drained", p.Total}]++
		case p.Severed == p.Total && p.Total > 0:
			seen[key{"severed", p.Total}]++
		case p.SurvivedWindow == p.Total && p.Total > 0:
			seen[key{"survived", p.Total}]++
		case p.ReadTimeout == p.Total && p.Total > 0:
			seen[key{"read-timeout", p.Total}]++
		default:
			seen[key{"mixed", p.Total}]++
		}
	}
	if len(seen) == 1 {
		for k := range seen {
			return fmt.Sprintf("%d/%d %s", k.n, k.n, k.outcome)
		}
	}
	parts := make([]string, 0, len(seen))
	for k, n := range seen {
		parts = append(parts, fmt.Sprintf("%dx %d/%d %s", n, k.n, k.n, k.outcome))
	}
	sort.Strings(parts)
	return "MIXED: " + strings.Join(parts, " + ")
}

func exitCodeTally(a *ArmAggregate) string {
	if len(a.ExitCodes) == 1 {
		for k := range a.ExitCodes {
			return k
		}
	}
	keys := make([]string, 0, len(a.ExitCodes))
	for k := range a.ExitCodes {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%sx%d", k, a.ExitCodes[k]))
	}
	return strings.Join(parts, ",")
}
