package report

import (
	"bytes"
	"strings"
	"testing"
)

// TestTableRendersNotMeasured: the human table must use the same vocabulary as
// the JSON. A missing number is "not-measured", never a blank or a zero.
func TestTableRendersNotMeasured(t *testing.T) {
	r := sampleReport()
	r.Trial.Summary.SigtermToReadyFalseMs = nil
	r.Trial.Flows = append(r.Trial.Flows, Flow{ID: "udp-0009", Proto: "udp", Outcome: OutcomeNotMeasured})

	var buf bytes.Buffer
	RenderTable(&buf, &r)
	out := buf.String()

	if !strings.Contains(out, "sigterm -> endpoint ready:false") {
		t.Fatal("summary section is missing the ready:false row")
	}
	if !strings.Contains(out, "not-measured") {
		t.Error("a missing interval must render as not-measured")
	}
	if strings.Contains(out, "\x1b[") {
		t.Error("table output must contain no ANSI escape sequences")
	}
	for _, want := range []string{"ENVIRONMENT", "CONFIG", "TIMELINE", "FLOWS", "SUMMARY", "CLOCKS"} {
		if !strings.Contains(out, want) {
			t.Errorf("table is missing the %s section", want)
		}
	}
	if !strings.Contains(out, "tcp-0001") || !strings.Contains(out, "udp-0004") {
		t.Error("flow rows are missing")
	}
	// The approximate marker must be visible for probe-sourced events.
	if !strings.Contains(out, "~") {
		t.Error("probe-sourced events must be marked approximate in the table")
	}
}

func TestTableHandlesAnEmptyTrial(t *testing.T) {
	r := Report{DrainwatchVersion: "0.1.0", GitCommit: "unknown",
		Trial: Trial{ID: "trial-001", ClockNote: ClockNoteText}}
	var buf bytes.Buffer
	RenderTable(&buf, &r)
	out := buf.String()
	if !strings.Contains(out, "not-measured: no timeline events were recorded") {
		t.Error("an empty timeline must say so explicitly")
	}
	if !strings.Contains(out, "not-measured: no flow records were produced") {
		t.Error("an empty flow set must say so explicitly")
	}
}

func TestRunSummaryTable(t *testing.T) {
	rs := RunSummary{
		DrainwatchVersion: "0.1.0", GitCommit: "abc1234",
		Trials: []TrialSummary{
			{TrialID: "trial-001", DrainBehavior: "drain", Trigger: "delete",
				TCP: ProtoSummary{Total: 10, Drained: 10}, UDP: ProtoSummary{Total: 10, Severed: 10},
				TriggerToSigtermMs: Ptr(int64(112))},
			{TrialID: "trial-002", DrainBehavior: "exit-now", Trigger: "delete",
				TCP: ProtoSummary{Total: 10, Severed: 10}, UDP: ProtoSummary{Total: 10, Severed: 10}},
		},
	}
	var buf bytes.Buffer
	RenderRunSummary(&buf, &rs)
	out := buf.String()
	if !strings.Contains(out, "trial-001") || !strings.Contains(out, "exit-now") {
		t.Error("run summary is missing trial rows")
	}
	if !strings.Contains(out, "not-measured") {
		t.Error("a trial with no observed SIGTERM interval must render not-measured")
	}
	if !strings.Contains(out, "legend:") {
		t.Error("run summary must explain its column abbreviations")
	}
}

func TestWrapDoesNotLoseWords(t *testing.T) {
	lines := wrap(ClockNoteText, 60)
	joined := strings.Join(lines, " ")
	if joined != strings.Join(strings.Fields(ClockNoteText), " ") {
		t.Error("wrapping changed the text")
	}
	for _, l := range lines {
		if len(l) > 60 && !strings.Contains(l, " ") {
			continue // a single word longer than the width is allowed through
		}
		if len(l) > 60 {
			t.Errorf("line exceeds width: %q", l)
		}
	}
}
