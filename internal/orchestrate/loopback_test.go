package orchestrate

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/jaynirmal15/drainwatch/internal/flowgen"
	"github.com/jaynirmal15/drainwatch/internal/probe"
	"github.com/jaynirmal15/drainwatch/internal/report"
)

// These tests wire the real probe to the real flow generator over loopback,
// with no Kubernetes involved. They exist because the outcome classification is
// only meaningful if the probe's three termination behaviours actually produce
// the wire events the classifier expects: a drain must put a "bye" on the wire
// before its FIN, and an abrupt exit must not.
//
// Timings are compressed (100ms heartbeats, a 1s drain window, 3-datagram UDP
// silence) so the suite stays fast; the code paths are the production ones.

type loopbackRig struct {
	srv *probe.Server
	gen *flowgen.Generator
	t0  time.Time
}

func startLoopback(t *testing.T, ctx context.Context, behavior string, drainMaxSeconds int, forceRST bool) *loopbackRig {
	t.Helper()

	cfg := probe.Config{
		TCPPort: 0, UDPPort: 0, ReadyPort: 0,
		Behavior:          behavior,
		DrainMaxSeconds:   drainMaxSeconds,
		ExitNowForceRST:   forceRST,
		HeartbeatInterval: 100 * time.Millisecond,
	}
	srv := probe.NewServer(cfg, probe.NewLogger(io.Discard, time.Now()))
	if err := srv.Listen(); err != nil {
		t.Fatalf("probe listen: %v", err)
	}
	t.Cleanup(srv.Close)
	srv.Serve(ctx)

	gen, err := flowgen.New(flowgen.Config{
		Host:                "127.0.0.1",
		TCPPort:             srv.TCPPort(),
		UDPPort:             srv.UDPPort(),
		TCPFlows:            2,
		UDPFlows:            2,
		Interval:            100 * time.Millisecond,
		GapThreshold:        300 * time.Millisecond,
		FlowTimeout:         2 * time.Second,
		UDPSilenceDatagrams: 3,
		DialTimeout:         2 * time.Second,
	})
	if err != nil {
		t.Fatalf("flowgen.New: %v", err)
	}

	t0 := time.Now()
	if err := gen.Start(ctx); err != nil {
		t.Fatalf("flowgen.Start: %v", err)
	}
	t.Cleanup(func() { gen.CloseObservation(time.Now()) })

	if err := gen.WaitEstablished(ctx, 10*time.Second, ""); err != nil {
		t.Fatalf("flows never established against the loopback probe: %v", err)
	}
	return &loopbackRig{srv: srv, gen: gen, t0: t0}
}

// waitAllTerminal blocks until every flow has ended or the deadline passes.
func waitAllTerminal(t *testing.T, gen *flowgen.Generator, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if gen.AllTerminal() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("not every flow reached a terminal event within %s; terminal so far: %v", within, gen.TerminalFlows())
}

func outcomesByProto(flows []report.Flow, proto string) map[report.Outcome]int {
	m := map[report.Outcome]int{}
	for _, f := range flows {
		if f.Proto == proto {
			m[f.Outcome]++
		}
	}
	return m
}

// TestLoopbackDrainProducesCleanCloses: the drain arm must put an announcement
// on the wire before its FIN, so the client can tell a drain from abandonment.
func TestLoopbackDrainProducesCleanCloses(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rig := startLoopback(t, ctx, probe.BehaviorDrain, 1, true)

	go rig.srv.Terminate(ctx)

	// Readiness must fail as soon as the drain begins; this is what makes the
	// EndpointSlice transition observable in a real cluster.
	waitForReadyz(t, rig.srv.ReadyPort(), http.StatusServiceUnavailable, 3*time.Second)

	// The drain window closes the TCP flows; UDP has no close handshake, so it
	// only ends when the socket goes away with the process.
	waitFlowsTerminal(t, rig.gen, "tcp", 2, 5*time.Second)
	rig.srv.Close()
	waitAllTerminal(t, rig.gen, 5*time.Second)

	rig.gen.CloseObservation(time.Now())
	flows := rig.gen.Records(rig.t0)

	tcp := outcomesByProto(flows, "tcp")
	if tcp[report.OutcomeDrainedCleanClose] != 2 {
		t.Errorf("tcp outcomes = %v, want 2 %s", tcp, report.OutcomeDrainedCleanClose)
	}

	udp := outcomesByProto(flows, "udp")
	if udp[report.OutcomeSevered] != 2 {
		t.Errorf("udp outcomes = %v, want 2 %s (udp has no drain semantics)", udp, report.OutcomeSevered)
	}

	for _, f := range flows {
		if f.Proto == "tcp" && f.LastHeartbeatSeq == nil {
			t.Errorf("%s drained without recording a last heartbeat sequence", f.ID)
		}
		if f.Proto == "udp" && f.LastAnsweredSeq == nil {
			t.Errorf("%s was severed without recording the last answered sequence", f.ID)
		}
		if f.TTerminalMs == nil {
			t.Errorf("%s has a terminal outcome %q but no terminal time", f.ID, f.Outcome)
		}
	}
}

// TestLoopbackExitNowSeversFlows: abandonment must never be recorded as a drain.
func TestLoopbackExitNowSeversFlows(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rig := startLoopback(t, ctx, probe.BehaviorExitNow, 60, true)

	rig.srv.Terminate(ctx)
	rig.srv.Close()

	waitAllTerminal(t, rig.gen, 5*time.Second)
	rig.gen.CloseObservation(time.Now())
	flows := rig.gen.Records(rig.t0)

	tcp := outcomesByProto(flows, "tcp")
	if tcp[report.OutcomeDrainedCleanClose] != 0 {
		t.Errorf("tcp outcomes = %v; an abandoned flow must never be recorded as drained", tcp)
	}
	if tcp[report.OutcomeSevered] != 2 {
		t.Errorf("tcp outcomes = %v, want 2 %s", tcp, report.OutcomeSevered)
	}
	if udp := outcomesByProto(flows, "udp"); udp[report.OutcomeSevered] != 2 {
		t.Errorf("udp outcomes = %v, want 2 %s", udp, report.OutcomeSevered)
	}
}

// TestLoopbackIgnoreKeepsServing: the ignore arm must keep flows alive and keep
// answering readiness with 200, so that what ends the flows in a real cluster is
// SIGKILL and not the application.
func TestLoopbackIgnoreKeepsServing(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rig := startLoopback(t, ctx, probe.BehaviorIgnore, 60, true)

	terminated := make(chan int, 1)
	go func() { terminated <- rig.srv.Terminate(ctx) }()

	time.Sleep(700 * time.Millisecond)

	if dead := rig.gen.TerminalFlows(); len(dead) != 0 {
		t.Errorf("flows ended while the probe was ignoring SIGTERM: %v", dead)
	}
	waitForReadyz(t, rig.srv.ReadyPort(), http.StatusOK, 2*time.Second)

	// Closing the observation window now is what an orchestrator does when the
	// window expires: survivors are recorded as survivors, not as drained.
	closedAt := time.Now()
	rig.gen.CloseObservation(closedAt)
	flows := rig.gen.Records(rig.t0)

	for _, f := range flows {
		if f.Outcome != report.OutcomeSurvivedObservationWindow {
			t.Errorf("%s outcome = %q, want %q", f.ID, f.Outcome, report.OutcomeSurvivedObservationWindow)
		}
		if f.TTerminalMs != nil {
			t.Errorf("%s survived the window but has a terminal time of %d; it must be null", f.ID, *f.TTerminalMs)
		}
	}

	cancel()
	select {
	case <-terminated:
	case <-time.After(3 * time.Second):
		t.Error("Terminate did not return after the context was cancelled")
	}
}

// TestLoopbackSummaryCountsMatchFlows checks the tally the report prints against
// the flow records it was derived from.
func TestLoopbackSummaryCountsMatchFlows(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rig := startLoopback(t, ctx, probe.BehaviorExitNow, 60, true)
	rig.srv.Terminate(ctx)
	rig.srv.Close()
	waitAllTerminal(t, rig.gen, 5*time.Second)

	rig.gen.CloseObservation(time.Now())
	flows := rig.gen.Records(rig.t0)
	timeline := rig.gen.TimelineEvents(rig.t0)
	report.SortTimeline(timeline)
	summary := report.BuildSummary(timeline, flows)

	if summary.TCP.Total != 2 || summary.UDP.Total != 2 {
		t.Fatalf("summary totals = tcp %d udp %d, want 2 and 2", summary.TCP.Total, summary.UDP.Total)
	}
	if len(timeline) != 4 {
		t.Errorf("expected one flow_terminal timeline entry per flow, got %d", len(timeline))
	}
	// No probe or k8s events exist here, so every SIGTERM-anchored interval must
	// be null rather than zero.
	if summary.SigtermToReadyFalseMs != nil || summary.SigtermToLastFlowTerminalMs != nil {
		t.Error("intervals anchored on an unobserved SIGTERM must be null")
	}
}

func waitFlowsTerminal(t *testing.T, gen *flowgen.Generator, proto string, want int, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		n := 0
		for _, id := range gen.TerminalFlows() {
			if len(id) >= 3 && id[:3] == proto {
				n++
			}
		}
		if n >= want {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("fewer than %d %s flows terminated within %s (terminal: %v)", want, proto, within, gen.TerminalFlows())
}

func waitForReadyz(t *testing.T, port int, wantStatus int, within time.Duration) {
	t.Helper()
	url := fmt.Sprintf("http://127.0.0.1:%d/readyz", port)
	client := &http.Client{Timeout: time.Second}
	deadline := time.Now().Add(within)
	last := 0
	for time.Now().Before(deadline) {
		resp, err := client.Get(url)
		if err == nil {
			last = resp.StatusCode
			resp.Body.Close()
			if last == wantStatus {
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("/readyz returned %d, want %d within %s", last, wantStatus, within)
}
