package report

import (
	"bytes"
	"path/filepath"
	"strings"
	"testing"
)

// armFixture builds a synthetic arm directory on disk. Each trial is a real
// report written through WriteJSON, so the tests exercise the same load path
// the CLI uses rather than a hand-built struct.
type trialFixture struct {
	behavior      string
	trigger       string
	tcpOutcome    Outcome
	tcpDetail     string
	udpOutcome    Outcome
	udpDetail     string
	tcpTerminals  []int64 // one per tcp flow; nil entries are impossible here
	udpTerminal   *int64
	sigtermMs     *int64
	readyFalseMs  *int64
	endpointGone  *int64
	containerMs   *int64
	exitCode      string // "" means no container_terminated event at all
	k8sVersion    string
	kubeProxyMode string
	version       string
	commit        string
}

func writeArm(t *testing.T, dir string, fixtures []trialFixture) {
	t.Helper()
	for i, f := range fixtures {
		var timeline []Event
		timeline = append(timeline, Event{TMs: 0, Source: SourceOrchestrator, Event: EventTriggerIssued})
		if f.sigtermMs != nil {
			timeline = append(timeline, Event{TMs: *f.sigtermMs, Source: SourceProbe, Event: EventSigtermReceived, Approximate: true})
		}
		if f.readyFalseMs != nil {
			timeline = append(timeline, Event{TMs: *f.readyFalseMs, Source: SourceK8s, Event: EventEndpointSliceReadyFalse})
		}
		if f.endpointGone != nil {
			timeline = append(timeline, Event{TMs: *f.endpointGone, Source: SourceK8s, Event: EventEndpointSliceEndpointGone})
		}
		if f.containerMs != nil && f.exitCode != "" {
			timeline = append(timeline, Event{TMs: *f.containerMs, Source: SourceK8s, Event: EventContainerTerminated,
				Detail: "probe exitCode=" + f.exitCode + " reason=Completed signal=0"})
		}

		var flows []Flow
		for j, term := range f.tcpTerminals {
			fl := Flow{ID: "tcp-000" + string(rune('1'+j)), Proto: "tcp", Outcome: f.tcpOutcome, Detail: f.tcpDetail}
			v := term
			fl.TTerminalMs = &v
			flows = append(flows, fl)
		}
		udp := Flow{ID: "udp-0001", Proto: "udp", Outcome: f.udpOutcome, Detail: f.udpDetail}
		if f.udpTerminal != nil {
			v := *f.udpTerminal
			udp.TTerminalMs = &v
		}
		flows = append(flows, udp)

		summary := BuildSummary(timeline, flows)
		if f.endpointGone != nil {
			summary.TriggerToEndpointRemovedMs = Ptr(*f.endpointGone)
		}
		if f.containerMs != nil && f.exitCode != "" {
			summary.TriggerToContainerTerminated = Ptr(*f.containerMs)
		}

		k8s, kp := f.k8sVersion, f.kubeProxyMode
		if k8s == "" {
			k8s = "v1.34.0"
		}
		if kp == "" {
			kp = "iptables"
		}

		ver, commit := f.version, f.commit
		if ver == "" {
			ver = "0.1.0"
		}
		if commit == "" {
			commit = "abc1234"
		}
		r := Report{
			DrainwatchVersion: ver, GitCommit: commit,
			Environment: Environment{
				KubernetesVersion: k8s, NodeCount: 2, KubeProxyMode: kp,
				CNI: "kindnet", OS: "linux", Arch: "amd64", Warnings: []string{},
			},
			Trial: Trial{
				ID: trialID(i + 1),
				Config: Config{
					DrainBehavior: f.behavior, Trigger: f.trigger,
					TCPFlows: len(f.tcpTerminals), UDPFlows: 1, GracePeriodSeconds: 30,
				},
				Timeline: timeline, Flows: flows, Summary: summary,
				ClockNote: ClockNoteText,
			},
		}
		if err := WriteJSON(filepath.Join(dir, trialID(i+1), "report.json"), r); err != nil {
			t.Fatalf("WriteJSON: %v", err)
		}
	}
}

func trialID(n int) string {
	return "trial-00" + string(rune('0'+n))
}

func baseFixture() trialFixture {
	return trialFixture{
		behavior: "exit-now", trigger: "delete",
		tcpOutcome: OutcomeSevered, tcpDetail: "econnreset",
		udpOutcome: OutcomeSevered, udpDetail: "answered by probe instance p-aaa, was p-bbb",
		tcpTerminals: []int64{46, 48, 50},
		udpTerminal:  Ptr(int64(2400)),
		sigtermMs:    Ptr(int64(45)),
		readyFalseMs: Ptr(int64(29)),
		endpointGone: Ptr(int64(376)),
		containerMs:  Ptr(int64(340)),
		exitCode:     "0",
	}
}

func TestLoadArmAggregatesAcrossRepeats(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "arm-A")

	// Five repeats whose last-TCP-terminal values are deliberately unsorted, so
	// the median cannot come out right by accident.
	fixtures := make([]trialFixture, 5)
	lasts := []int64{70, 50, 90, 60, 80}
	for i := range fixtures {
		f := baseFixture()
		f.tcpTerminals = []int64{lasts[i] - 20, lasts[i]}
		fixtures[i] = f
	}
	writeArm(t, dir, fixtures)

	a, err := LoadArm(dir)
	if err != nil {
		t.Fatalf("LoadArm: %v", err)
	}
	if a.Arm != "A" {
		t.Errorf("arm = %q, want A (taken from the directory name)", a.Arm)
	}
	if a.Trials != 5 {
		t.Fatalf("trials = %d, want 5", a.Trials)
	}
	if a.DrainBehavior != "exit-now" || a.Trigger != "delete" {
		t.Errorf("config = %s/%s", a.DrainBehavior, a.Trigger)
	}

	last := a.Stat("trigger_to_last_tcp_terminal_ms")
	if last == nil {
		t.Fatal("missing trigger_to_last_tcp_terminal_ms")
	}
	if *last.Min != 50 || *last.Median != 70 || *last.Max != 90 {
		t.Errorf("last tcp terminal min/median/max = %d/%d/%d, want 50/70/90", *last.Min, *last.Median, *last.Max)
	}
	if last.Observed != 5 || last.NotMeasured != 0 {
		t.Errorf("observed/not-measured = %d/%d, want 5/0", last.Observed, last.NotMeasured)
	}
	if last.ClockBasis != ClockSameProcess {
		t.Errorf("clock basis = %q, want %q", last.ClockBasis, ClockSameProcess)
	}
	if last.Caveat != "" {
		t.Error("a same-clock metric must not carry the cross-clock caveat")
	}

	first := a.Stat("trigger_to_first_tcp_terminal_ms")
	if *first.Min != 30 || *first.Max != 70 {
		t.Errorf("first tcp terminal min/max = %d/%d, want 30/70", *first.Min, *first.Max)
	}

	if a.HasDisagreements {
		t.Errorf("identical repeats must not disagree, got %v", a.Disagreements)
	}
	if a.ExitCodes["0"] != 5 {
		t.Errorf("exit codes = %v, want five zeros", a.ExitCodes)
	}
}

// TestCrossClockStatsAreSeparatedAndCaveated is the core requirement: the two
// clock bases must never end up in one statistic.
func TestCrossClockStatsAreSeparatedAndCaveated(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "arm-B")
	fixtures := make([]trialFixture, 3)
	for i := range fixtures {
		fixtures[i] = baseFixture()
	}
	writeArm(t, dir, fixtures)

	a, err := LoadArm(dir)
	if err != nil {
		t.Fatalf("LoadArm: %v", err)
	}

	sameCount, crossCount := 0, 0
	for _, s := range a.Stats {
		switch s.ClockBasis {
		case ClockSameProcess:
			sameCount++
			if s.Caveat != "" {
				t.Errorf("%s is same-clock but carries a caveat", s.Metric)
			}
		case ClockCrossHost:
			crossCount++
			if s.Caveat == "" {
				t.Errorf("%s is cross-clock and must carry the resolution caveat", s.Metric)
			}
		default:
			t.Errorf("%s has no clock basis", s.Metric)
		}
	}
	if sameCount == 0 || crossCount == 0 {
		t.Fatalf("expected both bases to be present, got same=%d cross=%d", sameCount, crossCount)
	}

	// sigterm_to_ready_false is cross-clock and here negative; it must survive
	// as a measured negative rather than being clamped.
	rf := a.Stat("sigterm_to_ready_false_ms")
	if rf.Median == nil || *rf.Median != -16 {
		t.Errorf("sigterm_to_ready_false median = %v, want -16 (29-45)", rf.Median)
	}

	var buf bytes.Buffer
	RenderArmAggregate(&buf, a)
	out := buf.String()
	sameIdx := strings.Index(out, "SAME-CLOCK INTERVALS")
	crossIdx := strings.Index(out, "CROSS-CLOCK INTERVALS")
	if sameIdx < 0 || crossIdx < 0 {
		t.Fatal("both clock blocks must be rendered")
	}
	if crossIdx < sameIdx {
		t.Error("blocks are out of order")
	}
	if !strings.Contains(out, "within skew of simultaneous") {
		t.Error("the cross-clock block must print the caveat")
	}
}

// TestNotMeasuredNeverBecomesZero: a repeat that produced no observation must
// reduce the sample, not contribute a zero.
func TestNotMeasuredNeverBecomesZero(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "arm-C")
	f1 := baseFixture()
	f2 := baseFixture()
	f2.sigtermMs = nil    // no probe SIGTERM line in this repeat
	f2.readyFalseMs = nil // and therefore no interval from it
	f3 := baseFixture()
	writeArm(t, dir, []trialFixture{f1, f2, f3})

	a, err := LoadArm(dir)
	if err != nil {
		t.Fatalf("LoadArm: %v", err)
	}
	s := a.Stat("sigterm_to_ready_false_ms")
	if s.Observed != 2 || s.NotMeasured != 1 {
		t.Errorf("observed/not-measured = %d/%d, want 2/1", s.Observed, s.NotMeasured)
	}
	for _, v := range s.Values {
		if v == 0 {
			t.Error("a missing observation leaked in as a zero")
		}
	}
	if *s.Min != -16 || *s.Max != -16 {
		t.Errorf("min/max = %d/%d, want both -16", *s.Min, *s.Max)
	}
}

// TestAllRepeatsMissingYieldsNullNotZero: when nothing was observed, the stat
// is null.
func TestAllRepeatsMissingYieldsNullNotZero(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "arm-D")
	f := baseFixture()
	f.sigtermMs = nil
	f.readyFalseMs = nil
	writeArm(t, dir, []trialFixture{f, f})

	a, err := LoadArm(dir)
	if err != nil {
		t.Fatalf("LoadArm: %v", err)
	}
	s := a.Stat("sigterm_to_ready_false_ms")
	if s.Min != nil || s.Median != nil || s.Max != nil {
		t.Errorf("an entirely unobserved metric must be null, got min=%v median=%v max=%v", s.Min, s.Median, s.Max)
	}
	if s.Observed != 0 || s.NotMeasured != 2 {
		t.Errorf("observed/not-measured = %d/%d, want 0/2", s.Observed, s.NotMeasured)
	}
	if statValue(s) != "not-measured" {
		t.Errorf("rendered as %q, want not-measured", statValue(s))
	}
}

// TestDisagreementIsFlaggedLoudly: a repeat whose outcomes differ from the rest
// must be named, and must appear before the statistics it undermines.
func TestDisagreementIsFlaggedLoudly(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "arm-E")
	good := baseFixture()
	odd := baseFixture()
	odd.tcpOutcome = OutcomeReadTimeout
	odd.tcpDetail = "no bytes for 10.002s (flow timeout)"
	writeArm(t, dir, []trialFixture{good, odd, good})

	a, err := LoadArm(dir)
	if err != nil {
		t.Fatalf("LoadArm: %v", err)
	}
	if !a.HasDisagreements {
		t.Fatal("a repeat with different outcomes must be flagged")
	}
	joined := strings.Join(a.Disagreements, "\n")
	if !strings.Contains(joined, "trial-002") {
		t.Errorf("the disagreement must name the offending repeat: %s", joined)
	}
	if !strings.Contains(joined, "TCP outcomes differ") {
		t.Errorf("the disagreement must say what differed: %s", joined)
	}
	// The same repeat also used a different mechanism; that belongs in the
	// informational bucket, not duplicated into the invalidating one.
	if strings.Contains(joined, "severance route differs") {
		t.Errorf("mechanism variation must not be counted as a disagreement: %s", joined)
	}

	var buf bytes.Buffer
	RenderArmAggregate(&buf, a)
	out := buf.String()
	if !strings.Contains(out, "DISAGREEMENT") {
		t.Error("the rendered summary must announce disagreements")
	}
	if strings.Index(out, "DISAGREEMENT") > strings.Index(out, "SAME-CLOCK INTERVALS") {
		t.Error("disagreements must be printed before the statistics they undermine")
	}
}

// TestMechanismVariationIsReportedButDoesNotInvalidateTheArm.
//
// Repeats that reach the same outcomes by different routes are a result about
// the system, not a broken run: "10/10 severed" hides whether they were severed
// the same way, so the variation must be reported - but the arm's intervals
// remain comparable, so it must not be presented as a disagreement.
func TestMechanismVariationIsReportedButDoesNotInvalidateTheArm(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "arm-M")
	rehomed := baseFixture()
	silent := baseFixture()
	silent.udpDetail = "udp-silence-6-datagrams" // same outcome, different route
	writeArm(t, dir, []trialFixture{rehomed, silent, rehomed})

	a, err := LoadArm(dir)
	if err != nil {
		t.Fatalf("LoadArm: %v", err)
	}
	if a.HasDisagreements {
		t.Errorf("a mechanism-only difference must not invalidate the arm, got %v", a.Disagreements)
	}
	if !a.HasMechanismVariation {
		t.Fatal("a mechanism-only difference must still be reported")
	}
	joined := strings.Join(a.MechanismVariations, "\n")
	if !strings.Contains(joined, "trial-002") || !strings.Contains(joined, "UDP severance route differs") {
		t.Errorf("the variation must name the repeat and what varied: %s", joined)
	}

	var buf bytes.Buffer
	RenderArmAggregate(&buf, a)
	out := buf.String()
	if strings.Contains(out, "DISAGREEMENT") {
		t.Error("mechanism variation must not be rendered as a disagreement")
	}
	if !strings.Contains(out, "different routes") {
		t.Error("mechanism variation must be rendered")
	}
	if strings.Index(out, "NOTE: the repeats agree on outcomes") > strings.Index(out, "SAME-CLOCK INTERVALS") {
		t.Error("the note must appear before the statistics it qualifies")
	}
}

// TestExitCodeDisagreementIsFlagged covers the ignore-arm case: a repeat where
// the container exited 0 instead of 137 is a different experiment.
func TestExitCodeDisagreementIsFlagged(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "arm-F")
	a1 := baseFixture()
	a1.exitCode = "137"
	a2 := baseFixture()
	a2.exitCode = "0"
	writeArm(t, dir, []trialFixture{a1, a2})

	agg, err := LoadArm(dir)
	if err != nil {
		t.Fatalf("LoadArm: %v", err)
	}
	if !agg.HasDisagreements {
		t.Fatal("differing exit codes must be flagged")
	}
	if !strings.Contains(strings.Join(agg.Disagreements, "\n"), "exit code differs") {
		t.Errorf("got %v", agg.Disagreements)
	}
	if agg.ExitCodes["137"] != 1 || agg.ExitCodes["0"] != 1 {
		t.Errorf("exit code tally = %v, want one each", agg.ExitCodes)
	}
}

// TestEnvironmentDisagreementIsFlagged: repeats run against different clusters
// are not repeats.
func TestEnvironmentDisagreementIsFlagged(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "arm-G")
	a1 := baseFixture()
	a2 := baseFixture()
	a2.kubeProxyMode = "ipvs"
	writeArm(t, dir, []trialFixture{a1, a2})

	agg, err := LoadArm(dir)
	if err != nil {
		t.Fatalf("LoadArm: %v", err)
	}
	if !agg.HasDisagreements {
		t.Fatal("a different kube-proxy mode between repeats must be flagged")
	}
	if !strings.Contains(strings.Join(agg.Disagreements, "\n"), "different environment") {
		t.Errorf("got %v", agg.Disagreements)
	}
}

// TestBuildDriftIsADisagreement.
//
// The repeat-5 matrix recorded arm A from one commit and arms B-E from another,
// because the driver rebuilt per arm and a commit landed mid-run. The code
// difference happened to be confined to post-hoc aggregation, but that is an
// argument, not a guarantee: reports from different binaries are not repeats of
// one experiment, and the aggregator must say so rather than leave it to be
// noticed by hand.
func TestBuildDriftIsADisagreement(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "arm-P")
	a1 := baseFixture()
	a1.version, a1.commit = "0.1.0-4-g70142e0", "70142e0"
	a2 := baseFixture()
	a2.version, a2.commit = "0.1.0-5-gc7b7e94", "c7b7e94"
	writeArm(t, dir, []trialFixture{a1, a2})

	agg, err := LoadArm(dir)
	if err != nil {
		t.Fatalf("LoadArm: %v", err)
	}
	if !agg.HasDisagreements {
		t.Fatal("repeats produced by different builds must be flagged as a disagreement")
	}
	joined := strings.Join(agg.Disagreements, "\n")
	if !strings.Contains(joined, "different drainwatch build") {
		t.Errorf("the disagreement must name the cause: %s", joined)
	}
	if !strings.Contains(joined, "70142e0") || !strings.Contains(joined, "c7b7e94") {
		t.Errorf("the disagreement must name both builds: %s", joined)
	}
}

func TestBuildIDDirtyDetection(t *testing.T) {
	if !(BuildID{"0.1.0", "abc1234-dirty"}).Dirty() {
		t.Error("a -dirty commit must be detected")
	}
	if (BuildID{"0.1.0", "abc1234"}).Dirty() {
		t.Error("a clean commit must not be reported dirty")
	}
}

// TestMatrixAnnouncesBuildDriftAcrossArms: the cross-arm view must not let a
// build difference hide behind per-arm consistency.
func TestMatrixAnnouncesBuildDriftAcrossArms(t *testing.T) {
	base := t.TempDir()
	mk := func(name, version, commit string) *ArmAggregate {
		dir := filepath.Join(base, name)
		f := baseFixture()
		f.version, f.commit = version, commit
		writeArm(t, dir, []trialFixture{f, f})
		a, err := LoadArm(dir)
		if err != nil {
			t.Fatalf("LoadArm %s: %v", name, err)
		}
		return a
	}
	one := mk("arm-A", "0.1.0-4-g70142e0", "70142e0")
	two := mk("arm-B", "0.1.0-5-gc7b7e94", "c7b7e94")

	var buf bytes.Buffer
	RenderMatrix(&buf, []*ArmAggregate{one, two})
	out := buf.String()
	if !strings.Contains(out, "NOT PRODUCED BY ONE BUILD") {
		t.Errorf("the matrix must announce build drift across arms:\n%s", out)
	}

	var clean bytes.Buffer
	RenderMatrix(&clean, []*ArmAggregate{one, mk("arm-C", "0.1.0-4-g70142e0", "70142e0")})
	if !strings.Contains(clean.String(), "all arms produced by one build") {
		t.Error("a matrix from one build must say so")
	}

	var dirty bytes.Buffer
	RenderMatrix(&dirty, []*ArmAggregate{mk("arm-D", "0.1.0-4-g70142e0", "70142e0-dirty")})
	if !strings.Contains(dirty.String(), "uncommitted changes") {
		t.Error("a dirty build must be announced")
	}
}

// TestNormalizeMechanism: pod names and exact durations differ legitimately
// between repeats and must not read as disagreements; genuinely different
// mechanisms must still differ.
func TestNormalizeMechanism(t *testing.T) {
	cases := map[string]string{
		"answered by probe instance drainwatch-probe-abc-1, was drainwatch-probe-abc-2": "rehomed onto a different probe instance",
		"answered by probe instance p-x, was p-y":                                       "rehomed onto a different probe instance",
		"no bytes for 10.001s (flow timeout)":                                           "read timeout (no bytes for the flow timeout)",
		"no bytes for 9.998s (flow timeout)":                                            "read timeout (no bytes for the flow timeout)",
		"udp-silence-6-datagrams":                                                       "udp silence",
		"udp-silence-7-datagrams":                                                       "udp silence",
		"econnreset":                                                                    "econnreset",
		"fin after drain announcement":                                                  "fin after drain announcement",
		"eof-without-drain-announcement":                                                "eof-without-drain-announcement",
		"":                                                                              "not-measured",
	}
	for in, want := range cases {
		if got := NormalizeMechanism(in); got != want {
			t.Errorf("NormalizeMechanism(%q) = %q, want %q", in, got, want)
		}
	}
	if NormalizeMechanism("econnreset") == NormalizeMechanism("econnrefused (icmp port unreachable)") {
		t.Error("genuinely different mechanisms must stay distinct")
	}
}

func TestLoadArmRefusesAnEmptyDirectory(t *testing.T) {
	dir := t.TempDir()
	if _, err := LoadArm(dir); err == nil {
		t.Fatal("expected an error for a directory with no trial reports")
	} else if !strings.Contains(err.Error(), "invariant") {
		t.Errorf("error should name the invariant, got: %v", err)
	}
}

func TestMedian(t *testing.T) {
	cases := []struct {
		in   []int64
		want int64
	}{
		{[]int64{5}, 5},
		{[]int64{1, 2, 3}, 2},
		{[]int64{1, 2, 3, 4}, 2},
		{[]int64{10, 20, 30, 40, 50}, 30},
	}
	for _, c := range cases {
		if got := median(c.in); got != c.want {
			t.Errorf("median(%v) = %d, want %d", c.in, got, c.want)
		}
	}
}

func TestRenderMatrixFlagsMixedArms(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "arm-H")
	good := baseFixture()
	odd := baseFixture()
	odd.tcpOutcome = OutcomeDrainedCleanClose
	odd.tcpDetail = "fin after drain announcement"
	writeArm(t, dir, []trialFixture{good, odd})

	a, err := LoadArm(dir)
	if err != nil {
		t.Fatalf("LoadArm: %v", err)
	}
	var buf bytes.Buffer
	RenderMatrix(&buf, []*ArmAggregate{a})
	out := buf.String()
	if !strings.Contains(out, "MIXED") {
		t.Errorf("an arm whose repeats disagree must render as MIXED, not as a clean tally:\n%s", out)
	}
	if !strings.Contains(out, "ARM(S) HAVE REPEATS THAT DISAGREE") {
		t.Error("the matrix must announce disagreeing arms")
	}
}

// TestUDPRelativeToContainerExitIsPerRepeat guards against the difference-of-
// medians mistake: the statistic must be the median of each repeat's own
// difference, not the difference between two medians.
func TestUDPRelativeToContainerExitIsPerRepeat(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "arm-U")

	// Three repeats whose UDP terminal and container exit both vary, arranged
	// so that the difference of the medians (2400-1000 = 1400) differs from the
	// median of the differences (1000).
	udp := []int64{2000, 2400, 3000}
	exit := []int64{1000, 2000, 1500}
	// per-repeat differences: 1000, 400, 1500 -> median 1000
	var fixtures []trialFixture
	for i := range udp {
		f := baseFixture()
		f.udpTerminal = Ptr(udp[i])
		f.containerMs = Ptr(exit[i])
		fixtures = append(fixtures, f)
	}
	writeArm(t, dir, fixtures)

	a, err := LoadArm(dir)
	if err != nil {
		t.Fatalf("LoadArm: %v", err)
	}
	s := a.Stat("udp_last_terminal_minus_container_exit_ms")
	if s == nil {
		t.Fatal("missing udp_last_terminal_minus_container_exit_ms")
	}
	if *s.Median != 1000 {
		t.Errorf("median = %d, want 1000 (the median of the per-repeat differences, not 1400 which is the difference of the medians)", *s.Median)
	}
	if *s.Min != 400 || *s.Max != 1500 {
		t.Errorf("min/max = %d/%d, want 400/1500", *s.Min, *s.Max)
	}
	if s.ClockBasis != ClockSameProcess {
		t.Errorf("both endpoints are orchestrator-clock, want %q", ClockSameProcess)
	}
}
