package report

import (
	"fmt"
	"io"
	"sort"
	"strings"
	"text/tabwriter"
)

// ms renders an optional millisecond value. A missing observation renders as
// "not-measured", never as 0.
func ms(v *int64) string {
	if v == nil {
		return "not-measured"
	}
	return fmt.Sprintf("%d ms", *v)
}

func seq(v *int64) string {
	if v == nil {
		return "not-measured"
	}
	return fmt.Sprintf("%d", *v)
}

func dash(s string) string {
	if strings.TrimSpace(s) == "" {
		return "-"
	}
	return s
}

// RenderTable writes the plain-text human view of a report. No colour, no
// escape sequences, no terminal width detection: the output is intended to be
// pasted into an issue or a commit message unchanged.
func RenderTable(w io.Writer, r *Report) {
	fmt.Fprintf(w, "drainwatch %s (%s)  trial %s\n", r.DrainwatchVersion, r.GitCommit, r.Trial.ID)
	fmt.Fprintln(w, strings.Repeat("=", 78))

	renderEnvironment(w, r.Environment)
	fmt.Fprintln(w)
	renderConfig(w, r.Trial.Config)
	fmt.Fprintln(w)
	renderTimeline(w, r.Trial.Timeline)
	fmt.Fprintln(w)
	renderFlows(w, r.Trial.Flows)
	fmt.Fprintln(w)
	renderSummary(w, r.Trial.Summary)
	fmt.Fprintln(w)
	renderNotes(w, r.Trial)
}

func renderEnvironment(w io.Writer, e Environment) {
	fmt.Fprintln(w, "ENVIRONMENT")
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintf(tw, "  kubernetes\t%s\n", dash(e.KubernetesVersion))
	fmt.Fprintf(tw, "  nodes\t%d\n", e.NodeCount)
	for _, n := range e.Nodes {
		fmt.Fprintf(tw, "    %s\t%s / %s / %s\n", n.Name, dash(n.ContainerRuntime), dash(n.KubeletVersion), dash(n.OSImage))
	}
	fmt.Fprintf(tw, "  kube-proxy mode\t%s\n", dash(e.KubeProxyMode))
	fmt.Fprintf(tw, "  cni\t%s\n", dash(e.CNI))
	fmt.Fprintf(tw, "  host\t%s/%s\n", e.OS, e.Arch)
	fmt.Fprintf(tw, "  started\t%s\n", dash(e.WallClockStart))
	tw.Flush()
	for _, warn := range e.Warnings {
		fmt.Fprintf(w, "  WARNING: %s\n", warn)
	}
}

func renderConfig(w io.Writer, c Config) {
	fmt.Fprintln(w, "CONFIG")
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintf(tw, "  flows\t%d tcp, %d udp\n", c.TCPFlows, c.UDPFlows)
	fmt.Fprintf(tw, "  drain behavior\t%s (DRAIN_MAX_SECONDS=%d)\n", dash(c.DrainBehavior), c.DrainMaxSeconds)
	fmt.Fprintf(tw, "  trigger\t%s\n", dash(c.Trigger))
	fmt.Fprintf(tw, "  grace period\t%d s\n", c.GracePeriodSeconds)
	fmt.Fprintf(tw, "  settle / observe\t%d s / %d s\n", c.SettleSeconds, c.ObserveTimeoutSeconds)
	fmt.Fprintf(tw, "  target\t%s tcp:%d udp:%d (ns %s)\n", dash(c.TargetHost), c.TCPPort, c.UDPPort, dash(c.Namespace))
	tw.Flush()
}

func renderTimeline(w io.Writer, evs []Event) {
	fmt.Fprintln(w, "TIMELINE (t_ms relative to trigger_issued)")
	if len(evs) == 0 {
		fmt.Fprintln(w, "  not-measured: no timeline events were recorded")
		return
	}
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "  T_MS\tSOURCE\tEVENT\tDETAIL")
	for _, e := range evs {
		mark := ""
		if e.Approximate {
			mark = " ~"
		}
		detail := e.Detail
		if e.FlowID != "" {
			detail = strings.TrimSpace(e.FlowID + " " + detail)
		}
		fmt.Fprintf(tw, "  %d%s\t%s\t%s\t%s\n", e.TMs, mark, e.Source, e.Event, dash(detail))
	}
	tw.Flush()
	fmt.Fprintln(w, "  (~ marks a cross-host approximate timestamp; see clock_note)")
}

// RenderFlowTable writes just the per-flow table. `drainwatch client` uses it
// for manual runs, which have flows but no cluster timeline.
func RenderFlowTable(w io.Writer, flows []Flow) { renderFlows(w, flows) }

// RenderSummaryTable writes just the outcome tally and derived intervals.
func RenderSummaryTable(w io.Writer, s Summary) { renderSummary(w, s) }

func renderFlows(w io.Writer, flows []Flow) {
	fmt.Fprintln(w, "FLOWS")
	if len(flows) == 0 {
		fmt.Fprintln(w, "  not-measured: no flow records were produced")
		return
	}
	sorted := make([]Flow, len(flows))
	copy(sorted, flows)
	sort.SliceStable(sorted, func(i, j int) bool { return sorted[i].ID < sorted[j].ID })

	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "  ID\tPROTO\tOUTCOME\tT_TERMINAL\tLAST_SEQ\tDETAIL")
	for _, f := range sorted {
		last := f.LastHeartbeatSeq
		if f.Proto == "udp" {
			last = f.LastAnsweredSeq
		}
		fmt.Fprintf(tw, "  %s\t%s\t%s\t%s\t%s\t%s\n", f.ID, f.Proto, f.Outcome, ms(f.TTerminalMs), seq(last), dash(f.Detail))
	}
	tw.Flush()
}

func renderSummary(w io.Writer, s Summary) {
	fmt.Fprintln(w, "SUMMARY")
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "  PROTO\tTOTAL\tDRAINED\tSEVERED\tREAD_TIMEOUT\tSURVIVED_WINDOW\tNOT_MEASURED")
	fmt.Fprintf(tw, "  tcp\t%d\t%d\t%d\t%d\t%d\t%d\n", s.TCP.Total, s.TCP.Drained, s.TCP.Severed, s.TCP.ReadTimeout, s.TCP.SurvivedWindow, s.TCP.NotMeasured)
	fmt.Fprintf(tw, "  udp\t%d\t%d\t%d\t%d\t%d\t%d\n", s.UDP.Total, s.UDP.Drained, s.UDP.Severed, s.UDP.ReadTimeout, s.UDP.SurvivedWindow, s.UDP.NotMeasured)
	tw.Flush()
	fmt.Fprintln(w)
	tw = tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintf(tw, "  trigger -> sigterm\t%s\n", ms(s.TriggerToSigtermMs))
	fmt.Fprintf(tw, "  sigterm -> endpoint ready:false\t%s\n", ms(s.SigtermToReadyFalseMs))
	fmt.Fprintf(tw, "  trigger -> endpoint removed\t%s\n", ms(s.TriggerToEndpointRemovedMs))
	fmt.Fprintf(tw, "  trigger -> container terminated\t%s\n", ms(s.TriggerToContainerTerminated))
	fmt.Fprintf(tw, "  sigterm -> last flow terminal\t%s\n", ms(s.SigtermToLastFlowTerminalMs))
	tw.Flush()
	for _, n := range s.Notes {
		fmt.Fprintf(w, "  NOTE: %s\n", n)
	}
}

func renderNotes(w io.Writer, t Trial) {
	fmt.Fprintln(w, "CLOCKS")
	for _, line := range wrap(t.ClockNote, 74) {
		fmt.Fprintf(w, "  %s\n", line)
	}
	if t.ProbeClockOffsetNote != "" {
		for _, line := range wrap(t.ProbeClockOffsetNote, 74) {
			fmt.Fprintf(w, "  %s\n", line)
		}
	}
}

// wrap breaks s into lines of at most width runes at word boundaries.
func wrap(s string, width int) []string {
	words := strings.Fields(s)
	if len(words) == 0 {
		return nil
	}
	var out []string
	cur := words[0]
	for _, wd := range words[1:] {
		if len(cur)+1+len(wd) > width {
			out = append(out, cur)
			cur = wd
			continue
		}
		cur += " " + wd
	}
	return append(out, cur)
}

// RenderRunSummary writes the cross-trial table shown at the end of a
// multi-trial run.
func RenderRunSummary(w io.Writer, rs *RunSummary) {
	fmt.Fprintf(w, "drainwatch %s (%s)  %d trial(s)\n", rs.DrainwatchVersion, rs.GitCommit, len(rs.Trials))
	fmt.Fprintln(w, strings.Repeat("=", 78))
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "  TRIAL\tBEHAVIOR\tTRIGGER\tTCP(d/s/rt/sw)\tUDP(d/s/rt/sw)\tTRIG->TERM\tSIGTERM->READY_FALSE\tSIGTERM->LAST_FLOW")
	for _, t := range rs.Trials {
		fmt.Fprintf(tw, "  %s\t%s\t%s\t%d/%d/%d/%d\t%d/%d/%d/%d\t%s\t%s\t%s\n",
			t.TrialID, dash(t.DrainBehavior), dash(t.Trigger),
			t.TCP.Drained, t.TCP.Severed, t.TCP.ReadTimeout, t.TCP.SurvivedWindow,
			t.UDP.Drained, t.UDP.Severed, t.UDP.ReadTimeout, t.UDP.SurvivedWindow,
			ms(t.TriggerToSigtermMs), ms(t.SigtermToReadyFalseMs), ms(t.SigtermToLastFlowTerminalMs))
	}
	tw.Flush()
	fmt.Fprintln(w, "  legend: d=drained-clean-close s=severed rt=read-timeout sw=survived-observation-window")
}
