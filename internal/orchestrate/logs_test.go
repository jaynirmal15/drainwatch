package orchestrate

import (
	"strings"
	"testing"
	"time"

	"github.com/jaynirmal15/drainwatch/internal/report"
)

const triggerWallStr = "2026-01-01T12:00:00Z"

func triggerWall(t *testing.T) time.Time {
	t.Helper()
	tw, err := time.Parse(time.RFC3339, triggerWallStr)
	if err != nil {
		t.Fatal(err)
	}
	return tw
}

// TestProbeLogFiltersHeartbeatsOutOfTheTimeline: per-heartbeat lines belong in
// the flow records, not in a 40-line timeline.
func TestProbeLogFiltersHeartbeatsOutOfTheTimeline(t *testing.T) {
	lines := strings.Join([]string{
		`{"wall":"2026-01-01T11:59:50Z","mono_ms":0,"event":"probe_started","drain_behavior":"drain"}`,
		`{"wall":"2026-01-01T11:59:50.1Z","mono_ms":100,"event":"tcp_accept","flow_id":"srv-tcp-0001"}`,
		`{"wall":"2026-01-01T11:59:51Z","mono_ms":1000,"event":"tcp_heartbeat","flow_id":"srv-tcp-0001","seq":0}`,
		`{"wall":"2026-01-01T11:59:51.5Z","mono_ms":1500,"event":"tcp_heartbeat","flow_id":"srv-tcp-0001","seq":1}`,
		`{"wall":"2026-01-01T12:00:00.112Z","mono_ms":10112,"event":"sigterm_received","drain_behavior":"drain","detail":"signal=terminated"}`,
		`{"wall":"2026-01-01T12:00:00.115Z","mono_ms":10115,"event":"readyz_now_503"}`,
		`{"wall":"2026-01-01T12:00:25.2Z","mono_ms":35200,"event":"tcp_flow_closed","flow_id":"srv-tcp-0001","seq":50,"detail":"closed gracefully: drain-deadline"}`,
		`this line is not json at all`,
		`{"wall":"2026-01-01T12:00:25.3Z","mono_ms":35300,"event":"exiting","detail":"exit(0) after drain"}`,
	}, "\n")

	p := NewProbeLog()
	p.consume(strings.NewReader(lines))

	if !p.Saw(report.EventSigtermReceived) {
		t.Fatal("the sigterm line was not collected")
	}

	evs, warnings := p.TimelineEvents(triggerWall(t))
	if len(warnings) != 0 {
		t.Errorf("unexpected warnings: %v", warnings)
	}

	byName := map[string]report.Event{}
	for _, e := range evs {
		if _, dup := byName[e.Event]; dup && e.Event != "tcp_flow_closed" {
			t.Errorf("duplicate timeline event %q", e.Event)
		}
		byName[e.Event] = e
		if !e.Approximate {
			t.Errorf("probe event %q must be marked approximate", e.Event)
		}
		if e.Source != report.SourceProbe {
			t.Errorf("event %q has source %q, want %q", e.Event, e.Source, report.SourceProbe)
		}
	}

	if _, present := byName["tcp_heartbeat"]; present {
		t.Error("per-heartbeat lines must not reach the timeline")
	}
	if _, present := byName["tcp_accept"]; present {
		t.Error("per-accept lines must not reach the timeline")
	}

	sig, ok := byName[report.EventSigtermReceived]
	if !ok {
		t.Fatal("sigterm_received missing from the timeline")
	}
	if sig.TMs != 112 {
		t.Errorf("sigterm t_ms = %d, want 112 (wall clock difference from the trigger)", sig.TMs)
	}

	started, ok := byName["probe_started"]
	if !ok {
		t.Fatal("probe_started missing from the timeline")
	}
	if started.TMs != -10000 {
		t.Errorf("probe_started t_ms = %d, want -10000; events before the trigger keep negative offsets", started.TMs)
	}

	if ws := p.Warnings(); len(ws) != 1 || !strings.Contains(ws[0], "not drainwatch JSON") {
		t.Errorf("the unparsed line must be reported, got %v", ws)
	}
}

// TestProbeLogReportsUnparseableWallClocks: a malformed timestamp becomes a
// warning, never a silently dropped or zero-valued event.
func TestProbeLogReportsUnparseableWallClocks(t *testing.T) {
	p := NewProbeLog()
	p.consume(strings.NewReader(`{"wall":"not-a-timestamp","mono_ms":5,"event":"sigterm_received"}`))

	evs, warnings := p.TimelineEvents(triggerWall(t))
	if len(evs) != 0 {
		t.Errorf("an event with an unparseable clock must not enter the timeline, got %+v", evs)
	}
	if len(warnings) != 1 || !strings.Contains(warnings[0], "unparseable wall clock") {
		t.Errorf("expected a warning naming the bad clock, got %v", warnings)
	}
}

func TestProbeLogHandlesEmptyStream(t *testing.T) {
	p := NewProbeLog()
	p.consume(strings.NewReader(""))
	if p.Saw(report.EventSigtermReceived) {
		t.Error("an empty stream cannot have seen anything")
	}
	evs, _ := p.TimelineEvents(triggerWall(t))
	if len(evs) != 0 {
		t.Errorf("expected no events, got %d", len(evs))
	}
}
