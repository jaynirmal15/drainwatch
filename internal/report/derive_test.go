package report

import (
	"strings"
	"testing"
)

func TestCountFlowsBucketsSumToTotal(t *testing.T) {
	flows := []Flow{
		{ID: "tcp-0001", Proto: "tcp", Outcome: OutcomeDrainedCleanClose},
		{ID: "tcp-0002", Proto: "tcp", Outcome: OutcomeSevered},
		{ID: "tcp-0003", Proto: "tcp", Outcome: OutcomeReadTimeout},
		{ID: "tcp-0004", Proto: "tcp", Outcome: OutcomeSurvivedObservationWindow},
		{ID: "tcp-0005", Proto: "tcp", Outcome: OutcomeNotMeasured},
		{ID: "udp-0001", Proto: "udp", Outcome: OutcomeSevered},
	}
	tcp := CountFlows(flows, "tcp")
	if tcp.Total != 5 {
		t.Fatalf("tcp total = %d, want 5", tcp.Total)
	}
	sum := tcp.Drained + tcp.Severed + tcp.ReadTimeout + tcp.SurvivedWindow + tcp.NotMeasured
	if sum != tcp.Total {
		t.Errorf("buckets sum to %d but total is %d; a flow was dropped", sum, tcp.Total)
	}
	if udp := CountFlows(flows, "udp"); udp.Total != 1 || udp.Severed != 1 {
		t.Errorf("udp = %+v, want total 1 severed 1", udp)
	}
}

// TestCountFlowsCountsUnknownOutcomesRatherThanDropping guards the invariant
// that a flow can never vanish from the tally.
func TestCountFlowsCountsUnknownOutcomesRatherThanDropping(t *testing.T) {
	s := CountFlows([]Flow{{ID: "tcp-0001", Proto: "tcp", Outcome: Outcome("something-new")}}, "tcp")
	if s.Total != 1 || s.NotMeasured != 1 {
		t.Fatalf("got %+v, want total 1 counted as not-measured", s)
	}
}

func TestBuildSummaryComputesIntervalsFromObservedEndpoints(t *testing.T) {
	timeline := []Event{
		{TMs: 0, Source: SourceOrchestrator, Event: EventTriggerIssued},
		{TMs: 112, Source: SourceProbe, Event: EventSigtermReceived},
		{TMs: 340, Source: SourceK8s, Event: EventEndpointSliceReadyFalse},
		{TMs: 8100, Source: SourceK8s, Event: EventEndpointSliceEndpointGone},
		{TMs: 9000, Source: SourceK8s, Event: EventContainerTerminated},
	}
	flows := []Flow{
		{ID: "tcp-0001", Proto: "tcp", Outcome: OutcomeDrainedCleanClose, TTerminalMs: Ptr(int64(4210))},
		{ID: "tcp-0002", Proto: "tcp", Outcome: OutcomeDrainedCleanClose, TTerminalMs: Ptr(int64(4300))},
		{ID: "udp-0001", Proto: "udp", Outcome: OutcomeSevered, TTerminalMs: Ptr(int64(3550))},
	}

	s := BuildSummary(timeline, flows)
	if s.TriggerToSigtermMs == nil || *s.TriggerToSigtermMs != 112 {
		t.Errorf("trigger_to_sigterm_ms = %v, want 112", s.TriggerToSigtermMs)
	}
	if s.SigtermToReadyFalseMs == nil || *s.SigtermToReadyFalseMs != 228 {
		t.Errorf("sigterm_to_ready_false_ms = %v, want 228", s.SigtermToReadyFalseMs)
	}
	if s.SigtermToLastFlowTerminalMs == nil || *s.SigtermToLastFlowTerminalMs != 4300-112 {
		t.Errorf("sigterm_to_last_flow_terminal_ms = %v, want %d", s.SigtermToLastFlowTerminalMs, 4300-112)
	}
	if s.TriggerToEndpointRemovedMs == nil || *s.TriggerToEndpointRemovedMs != 8100 {
		t.Errorf("trigger_to_endpoint_removed_ms = %v, want 8100", s.TriggerToEndpointRemovedMs)
	}
	if s.TriggerToContainerTerminated == nil || *s.TriggerToContainerTerminated != 9000 {
		t.Errorf("trigger_to_container_terminated_ms = %v, want 9000", s.TriggerToContainerTerminated)
	}
	if len(s.Notes) != 0 {
		t.Errorf("expected no notes for a fully observed trial, got %v", s.Notes)
	}
}

// TestBuildSummaryWithoutSigtermRefusesToInvent: if the probe's SIGTERM line
// never arrived, every interval anchored on it must be null and the report must
// say why.
func TestBuildSummaryWithoutSigtermRefusesToInvent(t *testing.T) {
	timeline := []Event{
		{TMs: 0, Source: SourceOrchestrator, Event: EventTriggerIssued},
		{TMs: 340, Source: SourceK8s, Event: EventEndpointSliceReadyFalse},
	}
	flows := []Flow{{ID: "tcp-0001", Proto: "tcp", Outcome: OutcomeSevered, TTerminalMs: Ptr(int64(900))}}

	s := BuildSummary(timeline, flows)
	if s.SigtermToReadyFalseMs != nil {
		t.Errorf("sigterm_to_ready_false_ms = %d, want nil", *s.SigtermToReadyFalseMs)
	}
	if s.SigtermToLastFlowTerminalMs != nil {
		t.Errorf("sigterm_to_last_flow_terminal_ms = %d, want nil", *s.SigtermToLastFlowTerminalMs)
	}
	if s.TriggerToSigtermMs != nil {
		t.Errorf("trigger_to_sigterm_ms = %d, want nil", *s.TriggerToSigtermMs)
	}
	if !hasNoteContaining(s.Notes, "sigterm_received was not observed") {
		t.Errorf("expected a note explaining the missing SIGTERM, got %v", s.Notes)
	}
}

// TestBuildSummaryFlagsUnterminatedFlows: survivors must not silently shrink
// the "last flow terminal" number into looking like everything ended.
func TestBuildSummaryFlagsUnterminatedFlows(t *testing.T) {
	timeline := []Event{
		{TMs: 0, Source: SourceOrchestrator, Event: EventTriggerIssued},
		{TMs: 100, Source: SourceProbe, Event: EventSigtermReceived},
	}
	flows := []Flow{
		{ID: "tcp-0001", Proto: "tcp", Outcome: OutcomeSevered, TTerminalMs: Ptr(int64(900))},
		{ID: "tcp-0002", Proto: "tcp", Outcome: OutcomeSurvivedObservationWindow},
	}
	s := BuildSummary(timeline, flows)
	if !hasNoteContaining(s.Notes, "without an observed terminal event are excluded") {
		t.Errorf("expected a note about excluded flows, got %v", s.Notes)
	}
	if s.SigtermToLastFlowTerminalMs == nil || *s.SigtermToLastFlowTerminalMs != 800 {
		t.Errorf("sigterm_to_last_flow_terminal_ms = %v, want 800", s.SigtermToLastFlowTerminalMs)
	}
}

func hasNoteContaining(notes []string, substr string) bool {
	for _, n := range notes {
		if strings.Contains(n, substr) {
			return true
		}
	}
	return false
}

func TestSortTimelineIsStableAndDeterministic(t *testing.T) {
	in := []Event{
		{TMs: 8100, Source: SourceK8s, Event: EventEndpointSliceEndpointGone},
		{TMs: 0, Source: SourceOrchestrator, Event: EventTriggerIssued},
		{TMs: 340, Source: SourceK8s, Event: "b"},
		{TMs: 340, Source: SourceK8s, Event: "a"},
		{TMs: 340, Source: SourceClient, Event: "z"},
	}
	SortTimeline(in)
	want := []string{EventTriggerIssued, "z", "a", "b", EventEndpointSliceEndpointGone}
	for i, w := range want {
		if in[i].Event != w {
			t.Fatalf("position %d = %q, want %q (order: %v)", i, in[i].Event, w, in)
		}
	}
}

func TestFirstEventPicksTheEarliest(t *testing.T) {
	timeline := []Event{
		{TMs: 900, Source: SourceK8s, Event: EventEndpointSliceReadyFalse},
		{TMs: 340, Source: SourceK8s, Event: EventEndpointSliceReadyFalse},
		{TMs: 12, Source: SourceProbe, Event: EventEndpointSliceReadyFalse},
	}
	got := FirstEvent(timeline, SourceK8s, EventEndpointSliceReadyFalse)
	if got == nil || got.TMs != 340 {
		t.Fatalf("got %+v, want the k8s event at 340", got)
	}
	if FirstEvent(timeline, SourceK8s, "never-happened") != nil {
		t.Error("FirstEvent must return nil for an event that was not observed")
	}
}
