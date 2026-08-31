package flowgen

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jaynirmal15/drainwatch/internal/report"
)

// TestTerminalLatches: once a flow has ended, a second observer reporting a
// different ending must not be able to rewrite it. The reader and the writer
// goroutines race on this in every severed flow.
func TestTerminalLatches(t *testing.T) {
	f := newFlow("tcp", 0)
	f.record(KindConnected, at(0), NoSeq, "")
	f.record(KindHeartbeat, at(500), 0, "")
	f.record(KindEOF, at(600), NoSeq, "")
	f.record(KindReset, at(700), NoSeq, "econnreset")
	f.record(KindHeartbeat, at(800), 1, "")

	evs := f.snapshot()
	if len(evs) != 3 {
		t.Fatalf("recorded %d events, want 3 (everything after the first terminal must be dropped): %+v", len(evs), evs)
	}
	if !f.isTerminal() {
		t.Error("flow should be terminal after EOF")
	}
}

func TestFlowIDsAreSequentialAndZeroPadded(t *testing.T) {
	cfg := Config{
		Host: "127.0.0.1", TCPPort: 7001, UDPPort: 7002,
		TCPFlows: 3, UDPFlows: 2,
		Interval: DefaultInterval, FlowTimeout: DefaultFlowTimeout,
		UDPSilenceDatagrams: DefaultUDPSilenceDatagrams,
	}
	g, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	want := []string{"tcp-0001", "tcp-0002", "tcp-0003", "udp-0001", "udp-0002"}
	got := g.FlowIDs()
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("flow %d = %q, want %q", i, got[i], want[i])
		}
	}
}

// TestFlowToReportLeavesUnobservedFieldsNull is the schema-level statement of
// "the absence of an observation is not measured, never zero".
func TestFlowToReportLeavesUnobservedFieldsNull(t *testing.T) {
	f := newFlow("tcp", 0)
	f.record(KindConnected, at(0), NoSeq, "")
	f.record(KindHeartbeat, at(500), 7, "")
	f.record(KindObservationEnd, at(30000), NoSeq, "")

	rec := f.toReport(at(1000))
	if rec.Outcome != report.OutcomeSurvivedObservationWindow {
		t.Fatalf("outcome = %q, want %q", rec.Outcome, report.OutcomeSurvivedObservationWindow)
	}
	if rec.TTerminalMs != nil {
		t.Errorf("t_terminal_ms = %d, want nil for a flow that never terminated", *rec.TTerminalMs)
	}
	if rec.ConnectedTMs == nil || *rec.ConnectedTMs != -1000 {
		t.Errorf("connected_t_ms = %v, want -1000 (flows are established before the trigger)", rec.ConnectedTMs)
	}
	if rec.LastHeartbeatSeq == nil || *rec.LastHeartbeatSeq != 7 {
		t.Errorf("last_heartbeat_seq = %v, want 7", rec.LastHeartbeatSeq)
	}
	if rec.LastAnsweredSeq != nil {
		t.Errorf("last_answered_seq = %v, want nil on a tcp flow", rec.LastAnsweredSeq)
	}
}

func TestConfigValidateNamesTheInvariant(t *testing.T) {
	base := Config{
		Host: "127.0.0.1", TCPPort: 7001, UDPPort: 7002,
		TCPFlows: 1, UDPFlows: 1,
		Interval: DefaultInterval, FlowTimeout: DefaultFlowTimeout,
		UDPSilenceDatagrams: DefaultUDPSilenceDatagrams,
	}
	if err := base.Validate(); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}

	cases := map[string]func(*Config){
		"empty host":                  func(c *Config) { c.Host = "" },
		"tcp port out of range":       func(c *Config) { c.TCPPort = 0 },
		"udp port out of range":       func(c *Config) { c.UDPPort = 70000 },
		"no flows at all":             func(c *Config) { c.TCPFlows, c.UDPFlows = 0, 0 },
		"flow timeout below interval": func(c *Config) { c.FlowTimeout = c.Interval },
		"zero udp silence threshold":  func(c *Config) { c.UDPSilenceDatagrams = 0 },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			c := base
			mutate(&c)
			if err := c.Validate(); err == nil {
				t.Fatal("expected an error, got nil")
			}
		})
	}
}

// TestRetryDialGivesUpWithAnInformativeError: the retry window must not turn an
// unreachable target into an indefinite hang, and the error must say how hard
// drainwatch tried.
func TestRetryDialGivesUpWithAnInformativeError(t *testing.T) {
	attempts := 0
	_, err := retryDial(context.Background(), 600*time.Millisecond, func() (int, error) {
		attempts++
		return 0, errors.New("connect: connection refused")
	})
	if err == nil {
		t.Fatal("expected retryDial to give up")
	}
	if attempts < 2 {
		t.Errorf("made %d attempt(s), want the window to allow retries", attempts)
	}
	for _, want := range []string{"connection refused", "attempt(s) over"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error is missing %q: %v", want, err)
		}
	}
}

// TestRetryDialSucceedsAfterTransientRefusal models the real case: kube-proxy
// has not yet programmed its rules, so the first dials are refused.
func TestRetryDialSucceedsAfterTransientRefusal(t *testing.T) {
	attempts := 0
	got, err := retryDial(context.Background(), 5*time.Second, func() (string, error) {
		attempts++
		if attempts < 3 {
			return "", errors.New("connect: connection refused")
		}
		return "connected", nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "connected" || attempts != 3 {
		t.Errorf("got %q after %d attempts", got, attempts)
	}
}

func TestRetryDialHonoursCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := retryDial(ctx, time.Minute, func() (int, error) { return 0, errors.New("refused") })
	if err == nil || !strings.Contains(err.Error(), "cancelled") {
		t.Fatalf("got %v, want a cancellation error", err)
	}
}

// TestNoteInstanceDetectsReplacement: the first instance seen is the baseline,
// repeats are silent, and a different instance is reported as a change. This is
// what stops a UDP flow re-homed onto a replacement pod from being recorded as
// an undisturbed one.
func TestNoteInstanceDetectsReplacement(t *testing.T) {
	f := newFlow("udp", 0)

	if changed, _ := f.noteInstance(""); changed {
		t.Error("an empty instance must not count as a change")
	}
	if changed, _ := f.noteInstance("probe-pq4m5"); changed {
		t.Error("the first instance seen is the baseline, not a change")
	}
	if changed, prev := f.noteInstance("probe-pq4m5"); changed || prev != "probe-pq4m5" {
		t.Errorf("the same instance must not be a change (changed=%t prev=%q)", changed, prev)
	}
	if changed, _ := f.noteInstance(""); changed {
		t.Error("an empty instance must never be treated as a change")
	}
	changed, prev := f.noteInstance("probe-9q5nt")
	if !changed || prev != "probe-pq4m5" {
		t.Errorf("a different instance must be reported as a change (changed=%t prev=%q)", changed, prev)
	}
}
