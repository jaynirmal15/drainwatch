package report

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// ClockBasis records which clocks an interval was measured against. It exists
// so that the aggregator can refuse to combine intervals of different bases
// into one statistic: a spread over exact intervals and a spread over intervals
// carrying unmeasured host-to-host skew do not mean the same thing, and
// averaging them would produce a number with no defined precision.
type ClockBasis string

const (
	// ClockSameProcess: both endpoints were stamped by the orchestrator's own
	// monotonic clock. The interval is exact relative to the trigger. (For k8s
	// endpoints this is the orchestrator's receipt time, which is an upper
	// bound on when the change occurred, but it is still one clock.)
	ClockSameProcess ClockBasis = "same-clock"
	// ClockCrossHost: one endpoint came from the probe's wall clock and the
	// other from the orchestrator's. The interval carries unmeasured skew.
	ClockCrossHost ClockBasis = "cross-clock"
)

// CrossClockCaveat is attached to every cross-clock statistic.
const CrossClockCaveat = "one endpoint is the probe's wall clock and the other the orchestrator's, with no synchronisation between them; " +
	"read these as accurate to tens of milliseconds, and read a small negative value as 'within skew of simultaneous' rather than as a reversal of order"

// Stat is one metric aggregated across the repeats of an arm.
//
// Min, Median and Max are pointers: an arm in which no repeat produced an
// observation reports null, not zero.
type Stat struct {
	Metric      string     `json:"metric"`
	Description string     `json:"description"`
	ClockBasis  ClockBasis `json:"clock_basis"`
	Unit        string     `json:"unit"`
	// Observed is how many repeats contributed a value; NotMeasured is how many
	// did not. Observed+NotMeasured always equals the arm's trial count.
	Observed    int     `json:"observed_repeats"`
	NotMeasured int     `json:"not_measured_repeats"`
	Min         *int64  `json:"min"`
	Median      *int64  `json:"median"`
	Max         *int64  `json:"max"`
	Values      []int64 `json:"values"`
	Caveat      string  `json:"caveat,omitempty"`
}

// RepeatRow is the per-repeat outcome record, kept in full so that a
// disagreement can be traced to the repeat that caused it.
type RepeatRow struct {
	TrialID           string       `json:"trial_id"`
	ReportPath        string       `json:"report_path"`
	TCP               ProtoSummary `json:"tcp"`
	UDP               ProtoSummary `json:"udp"`
	TCPMechanisms     []string     `json:"tcp_mechanisms"`
	UDPMechanisms     []string     `json:"udp_mechanisms"`
	ContainerExitCode *int         `json:"container_exit_code"`
}

// ArmAggregate is the aggregate of one arm.
type ArmAggregate struct {
	Arm                string      `json:"arm"`
	Dir                string      `json:"dir"`
	Trials             int         `json:"trials"`
	DrainBehavior      string      `json:"drain_behavior"`
	Trigger            string      `json:"trigger"`
	GracePeriodSeconds int         `json:"grace_period_seconds"`
	TCPFlows           int         `json:"tcp_flows"`
	UDPFlows           int         `json:"udp_flows"`
	Environment        Environment `json:"environment"`
	// Build is the drainwatch build that produced these reports. It is recorded
	// per arm and compared across repeats: reports from different binaries are
	// not repeats of one experiment.
	Build   BuildID     `json:"build"`
	Stats   []Stat      `json:"stats"`
	Repeats []RepeatRow `json:"repeats"`
	// ExitCodes tallies observed container exit codes. "not-measured" is a key
	// like any other, so an unobserved exit is visible rather than absent.
	ExitCodes map[string]int `json:"container_exit_codes"`
	// Disagreements names every way the repeats of this arm are not the same
	// experiment: differing outcome counts, exit codes, configuration or
	// environment. Any entry here invalidates the arm's statistics, so it is
	// surfaced above them and never buried.
	Disagreements    []string `json:"disagreements"`
	HasDisagreements bool     `json:"has_disagreements"`

	// MechanismVariations records repeats that reached the same outcomes by
	// different routes - for example UDP flows severed by re-homing in one
	// repeat and by silence in another. This does NOT invalidate the arm: the
	// outcome counts agree and the intervals remain comparable. It is reported
	// because the variation is itself a result, not because it is a fault.
	MechanismVariations   []string `json:"mechanism_variations"`
	HasMechanismVariation bool     `json:"has_mechanism_variation"`
}

// BuildID names the binary that produced a report.
type BuildID struct {
	Version string `json:"drainwatch_version"`
	Commit  string `json:"git_commit"`
}

func (b BuildID) String() string { return b.Version + " (" + b.Commit + ")" }

// Dirty reports whether the build came from a tree with uncommitted changes,
// which means its commit does not fully identify the code that ran.
func (b BuildID) Dirty() bool { return strings.HasSuffix(b.Commit, "-dirty") }

// metricSpec describes one aggregated metric and how to pull it from a report.
type metricSpec struct {
	name    string
	desc    string
	basis   ClockBasis
	extract func(*Report) *int64
}

// flowTerminalExtremum returns the earliest or latest observed terminal time
// among flows of one protocol, or nil when none of them terminated.
func flowTerminalExtremum(r *Report, proto string, latest bool) *int64 {
	var out *int64
	for _, f := range r.Trial.Flows {
		if f.Proto != proto || f.TTerminalMs == nil {
			continue
		}
		v := *f.TTerminalMs
		if out == nil || (latest && v > *out) || (!latest && v < *out) {
			cp := v
			out = &cp
		}
	}
	return out
}

// sigtermToEvent computes the interval from the probe's SIGTERM to a k8s event.
// Both endpoints must exist; otherwise the answer is not-measured.
func sigtermToEvent(r *Report, event string) *int64 {
	sig := FirstEvent(r.Trial.Timeline, SourceProbe, EventSigtermReceived)
	ev := FirstEvent(r.Trial.Timeline, SourceK8s, event)
	if sig == nil || ev == nil {
		return nil
	}
	return Ptr(ev.TMs - sig.TMs)
}

// metricSpecs is the closed set of aggregated metrics.
var metricSpecs = []metricSpec{
	{
		name: "trigger_to_first_tcp_terminal_ms", basis: ClockSameProcess,
		desc:    "trigger -> the first TCP flow to end",
		extract: func(r *Report) *int64 { return flowTerminalExtremum(r, "tcp", false) },
	},
	{
		name: "trigger_to_last_tcp_terminal_ms", basis: ClockSameProcess,
		desc:    "trigger -> the last TCP flow to end",
		extract: func(r *Report) *int64 { return flowTerminalExtremum(r, "tcp", true) },
	},
	{
		name: "trigger_to_first_udp_terminal_ms", basis: ClockSameProcess,
		desc:    "trigger -> the first UDP flow to end",
		extract: func(r *Report) *int64 { return flowTerminalExtremum(r, "udp", false) },
	},
	{
		name: "trigger_to_last_udp_terminal_ms", basis: ClockSameProcess,
		desc:    "trigger -> the last UDP flow to end",
		extract: func(r *Report) *int64 { return flowTerminalExtremum(r, "udp", true) },
	},
	{
		name: "trigger_to_endpoint_removed_ms", basis: ClockSameProcess,
		desc:    "trigger -> the endpoint left the EndpointSlice",
		extract: func(r *Report) *int64 { return r.Trial.Summary.TriggerToEndpointRemovedMs },
	},
	{
		name: "trigger_to_container_terminated_ms", basis: ClockSameProcess,
		desc:    "trigger -> the container terminated",
		extract: func(r *Report) *int64 { return r.Trial.Summary.TriggerToContainerTerminated },
	},
	{
		name: "tcp_dead_before_endpoint_removed_ms", basis: ClockSameProcess,
		desc: "how long the Service still advertised an endpoint after the last TCP flow had already died (negative means the endpoint went first)",
		extract: func(r *Report) *int64 {
			last := flowTerminalExtremum(r, "tcp", true)
			removed := r.Trial.Summary.TriggerToEndpointRemovedMs
			if last == nil || removed == nil {
				return nil
			}
			return Ptr(*removed - *last)
		},
	},
	{
		name: "udp_first_terminal_minus_container_exit_ms", basis: ClockSameProcess,
		desc: "when the first UDP flow stopped being served, relative to the container actually exiting (negative means the flow was already gone while the process was still running)",
		extract: func(r *Report) *int64 {
			first := flowTerminalExtremum(r, "udp", false)
			exit := r.Trial.Summary.TriggerToContainerTerminated
			if first == nil || exit == nil {
				return nil
			}
			return Ptr(*first - *exit)
		},
	},
	{
		name: "udp_last_terminal_minus_container_exit_ms", basis: ClockSameProcess,
		desc: "when the last UDP flow stopped being served, relative to the container actually exiting",
		extract: func(r *Report) *int64 {
			last := flowTerminalExtremum(r, "udp", true)
			exit := r.Trial.Summary.TriggerToContainerTerminated
			if last == nil || exit == nil {
				return nil
			}
			return Ptr(*last - *exit)
		},
	},
	{
		name: "trigger_to_sigterm_ms", basis: ClockCrossHost,
		desc:    "trigger -> SIGTERM delivered to the probe",
		extract: func(r *Report) *int64 { return r.Trial.Summary.TriggerToSigtermMs },
	},
	{
		name: "sigterm_to_ready_false_ms", basis: ClockCrossHost,
		desc:    "SIGTERM -> the endpoint reported ready:false",
		extract: func(r *Report) *int64 { return r.Trial.Summary.SigtermToReadyFalseMs },
	},
	{
		name: "sigterm_to_endpoint_removed_ms", basis: ClockCrossHost,
		desc:    "SIGTERM -> the endpoint left the EndpointSlice",
		extract: func(r *Report) *int64 { return sigtermToEvent(r, EventEndpointSliceEndpointGone) },
	},
}

var (
	instanceDetailRe = regexp.MustCompile(`^answered by probe instance \S+, was \S+$`)
	readTimeoutRe    = regexp.MustCompile(`^no bytes for [^ ]+ \(flow timeout\)$`)
	udpSilenceRe     = regexp.MustCompile(`^udp-silence-\d+-datagrams$`)
	exitCodeRe       = regexp.MustCompile(`exitCode=(-?\d+)`)
)

// NormalizeMechanism collapses a flow's detail string to the mechanism it
// describes, dropping the parts that legitimately differ between repeats (pod
// names, exact durations). Without this, comparing repeats would report a
// disagreement every time a pod got a different random suffix.
func NormalizeMechanism(detail string) string {
	d := strings.TrimSpace(detail)
	switch {
	case d == "":
		return "not-measured"
	case instanceDetailRe.MatchString(d):
		return "rehomed onto a different probe instance"
	case readTimeoutRe.MatchString(d):
		return "read timeout (no bytes for the flow timeout)"
	case udpSilenceRe.MatchString(d):
		return "udp silence"
	}
	return d
}

// mechanisms returns the distinct normalized mechanisms for one protocol.
func mechanisms(r *Report, proto string) []string {
	seen := map[string]bool{}
	for _, f := range r.Trial.Flows {
		if f.Proto != proto {
			continue
		}
		seen[NormalizeMechanism(f.Detail)] = true
	}
	out := make([]string, 0, len(seen))
	for k := range seen {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// containerExitCode reads the exit code out of the container_terminated event,
// or returns nil when the container's termination was never observed.
func containerExitCode(r *Report) *int {
	ev := FirstEvent(r.Trial.Timeline, SourceK8s, EventContainerTerminated)
	if ev == nil {
		return nil
	}
	m := exitCodeRe.FindStringSubmatch(ev.Detail)
	if len(m) != 2 {
		return nil
	}
	n, err := strconv.Atoi(m[1])
	if err != nil {
		return nil
	}
	return &n
}

// median returns the median of a sorted slice. For an even count it averages
// the two central values, which can yield a number that was not itself
// observed; Values is always carried alongside so the raw observations remain
// visible.
func median(sorted []int64) int64 {
	n := len(sorted)
	if n%2 == 1 {
		return sorted[n/2]
	}
	a, b := sorted[n/2-1], sorted[n/2]
	return (a + b) / 2
}

// LoadArm reads every trial report in an arm directory and aggregates them.
//
// It refuses rather than guesses: a directory with no trial reports is an
// error, and a report that does not match the schema aborts the whole load.
func LoadArm(dir string) (*ArmAggregate, error) {
	matches, err := filepath.Glob(filepath.Join(dir, "trial-*", "report.json"))
	if err != nil {
		return nil, fmt.Errorf("cannot search %s for trial reports: %w", dir, err)
	}
	sort.Strings(matches)
	if len(matches) == 0 {
		return nil, fmt.Errorf("no trial reports found under %s (invariant: an arm directory contains trial-NNN/report.json for each repeat; check the path)", dir)
	}

	agg := &ArmAggregate{
		Arm:       strings.TrimPrefix(filepath.Base(dir), "arm-"),
		Dir:       dir,
		ExitCodes: map[string]int{},
	}

	reports := make([]*Report, 0, len(matches))
	for _, path := range matches {
		r, err := ReadReport(path)
		if err != nil {
			return nil, err
		}
		reports = append(reports, r)

		row := RepeatRow{
			TrialID:           r.Trial.ID,
			ReportPath:        path,
			TCP:               r.Trial.Summary.TCP,
			UDP:               r.Trial.Summary.UDP,
			TCPMechanisms:     mechanisms(r, "tcp"),
			UDPMechanisms:     mechanisms(r, "udp"),
			ContainerExitCode: containerExitCode(r),
		}
		agg.Repeats = append(agg.Repeats, row)
		if row.ContainerExitCode == nil {
			agg.ExitCodes["not-measured"]++
		} else {
			agg.ExitCodes[strconv.Itoa(*row.ContainerExitCode)]++
		}
	}

	first := reports[0]
	agg.Trials = len(reports)
	agg.DrainBehavior = first.Trial.Config.DrainBehavior
	agg.Trigger = first.Trial.Config.Trigger
	agg.GracePeriodSeconds = first.Trial.Config.GracePeriodSeconds
	agg.TCPFlows = first.Trial.Config.TCPFlows
	agg.UDPFlows = first.Trial.Config.UDPFlows
	agg.Environment = first.Environment
	agg.Build = BuildID{Version: first.DrainwatchVersion, Commit: first.GitCommit}

	agg.Stats = buildStats(reports)
	agg.Disagreements, agg.MechanismVariations = findDisagreements(reports, agg.Repeats)
	agg.HasDisagreements = len(agg.Disagreements) > 0
	agg.HasMechanismVariation = len(agg.MechanismVariations) > 0
	return agg, nil
}

func buildStats(reports []*Report) []Stat {
	stats := make([]Stat, 0, len(metricSpecs))
	for _, spec := range metricSpecs {
		st := Stat{
			Metric: spec.name, Description: spec.desc,
			ClockBasis: spec.basis, Unit: "ms",
		}
		if spec.basis == ClockCrossHost {
			st.Caveat = CrossClockCaveat
		}
		var vals []int64
		for _, r := range reports {
			v := spec.extract(r)
			if v == nil {
				st.NotMeasured++
				continue
			}
			vals = append(vals, *v)
		}
		st.Observed = len(vals)
		st.Values = append([]int64(nil), vals...)
		if len(vals) > 0 {
			sorted := append([]int64(nil), vals...)
			sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
			st.Min = Ptr(sorted[0])
			st.Max = Ptr(sorted[len(sorted)-1])
			st.Median = Ptr(median(sorted))
		}
		stats = append(stats, st)
	}
	return stats
}

// findDisagreements separates two different things.
//
// A disagreement means the repeats were not the same experiment: different
// outcome counts, exit codes, configuration or environment. Any of those makes
// the arm's medians meaningless, so they are returned first and rendered above
// the statistics.
//
// A mechanism variation means the repeats reached the same outcomes by
// different routes. That is a result about the system, not a fault in the run,
// and it must not be presented as one - but it must not be silently dropped
// either, because "10/10 severed" hides whether they were severed the same way.
func findDisagreements(reports []*Report, rows []RepeatRow) (disagreements, mechanisms []string) {
	if len(reports) == 0 {
		return nil, nil
	}
	base, baseRow := reports[0], rows[0]

	protoKey := func(p ProtoSummary) string {
		return fmt.Sprintf("total=%d drained=%d severed=%d read_timeout=%d survived=%d not_measured=%d",
			p.Total, p.Drained, p.Severed, p.ReadTimeout, p.SurvivedWindow, p.NotMeasured)
	}

	for i := 1; i < len(reports); i++ {
		r, row := reports[i], rows[i]

		if got, want := protoKey(row.TCP), protoKey(baseRow.TCP); got != want {
			disagreements = append(disagreements, fmt.Sprintf("%s TCP outcomes differ from %s: [%s] vs [%s]", row.TrialID, baseRow.TrialID, got, want))
		}
		if got, want := protoKey(row.UDP), protoKey(baseRow.UDP); got != want {
			disagreements = append(disagreements, fmt.Sprintf("%s UDP outcomes differ from %s: [%s] vs [%s]", row.TrialID, baseRow.TrialID, got, want))
		}
		if !sameExitCode(row.ContainerExitCode, baseRow.ContainerExitCode) {
			disagreements = append(disagreements, fmt.Sprintf("%s container exit code differs from %s: %s vs %s",
				row.TrialID, baseRow.TrialID, exitCodeString(row.ContainerExitCode), exitCodeString(baseRow.ContainerExitCode)))
		}
		if r.Trial.Config.DrainBehavior != base.Trial.Config.DrainBehavior || r.Trial.Config.Trigger != base.Trial.Config.Trigger {
			disagreements = append(disagreements, fmt.Sprintf("%s ran a different configuration from %s: behavior/trigger %s/%s vs %s/%s (these repeats are not the same experiment)",
				row.TrialID, baseRow.TrialID, r.Trial.Config.DrainBehavior, r.Trial.Config.Trigger,
				base.Trial.Config.DrainBehavior, base.Trial.Config.Trigger))
		}
		if r.DrainwatchVersion != base.DrainwatchVersion || r.GitCommit != base.GitCommit {
			disagreements = append(disagreements, fmt.Sprintf("%s was produced by a different drainwatch build from %s: %s vs %s (reports from different binaries are not repeats of one experiment)",
				row.TrialID, baseRow.TrialID,
				BuildID{r.DrainwatchVersion, r.GitCommit}, BuildID{base.DrainwatchVersion, base.GitCommit}))
		}
		if r.Environment.KubernetesVersion != base.Environment.KubernetesVersion ||
			r.Environment.KubeProxyMode != base.Environment.KubeProxyMode ||
			r.Environment.NodeCount != base.Environment.NodeCount {
			disagreements = append(disagreements, fmt.Sprintf("%s ran against a different environment from %s: k8s %s/%s, kube-proxy %s/%s, nodes %d/%d",
				row.TrialID, baseRow.TrialID,
				r.Environment.KubernetesVersion, base.Environment.KubernetesVersion,
				r.Environment.KubeProxyMode, base.Environment.KubeProxyMode,
				r.Environment.NodeCount, base.Environment.NodeCount))
		}

		if got, want := strings.Join(row.TCPMechanisms, " + "), strings.Join(baseRow.TCPMechanisms, " + "); got != want {
			mechanisms = append(mechanisms, fmt.Sprintf("%s TCP severance route differs from %s: %q vs %q", row.TrialID, baseRow.TrialID, got, want))
		}
		if got, want := strings.Join(row.UDPMechanisms, " + "), strings.Join(baseRow.UDPMechanisms, " + "); got != want {
			mechanisms = append(mechanisms, fmt.Sprintf("%s UDP severance route differs from %s: %q vs %q", row.TrialID, baseRow.TrialID, got, want))
		}
	}
	return disagreements, mechanisms
}

func sameExitCode(a, b *int) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

func exitCodeString(v *int) string {
	if v == nil {
		return "not-measured"
	}
	return strconv.Itoa(*v)
}

// LoadArms aggregates several arm directories, in the order given.
func LoadArms(dirs []string) ([]*ArmAggregate, error) {
	out := make([]*ArmAggregate, 0, len(dirs))
	for _, d := range dirs {
		info, err := os.Stat(d)
		if err != nil {
			return nil, fmt.Errorf("cannot read arm directory %s: %w", d, err)
		}
		if !info.IsDir() {
			return nil, fmt.Errorf("%s is not a directory (invariant: aggregate takes arm directories, each containing trial-NNN/report.json)", d)
		}
		agg, err := LoadArm(d)
		if err != nil {
			return nil, err
		}
		out = append(out, agg)
	}
	return out, nil
}

// Stat looks up an aggregated metric by name.
func (a *ArmAggregate) Stat(metric string) *Stat {
	for i := range a.Stats {
		if a.Stats[i].Metric == metric {
			return &a.Stats[i]
		}
	}
	return nil
}
