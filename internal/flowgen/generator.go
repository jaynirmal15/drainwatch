package flowgen

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/jaynirmal15/drainwatch/internal/report"
)

// Defaults for the flow generator. They are exported so that the CLI flag
// defaults and the documented behaviour cannot drift apart.
const (
	DefaultTCPFlows            = 10
	DefaultUDPFlows            = 10
	DefaultFlowTimeout         = 10 * time.Second
	DefaultInterval            = 500 * time.Millisecond
	DefaultUDPSilenceDatagrams = 6
	DefaultDialTimeout         = 5 * time.Second
	// DefaultDialRetryWindow is how long Start keeps retrying a flow that will
	// not connect.
	//
	// A connection refused immediately after the EndpointSlice reports a ready
	// endpoint is not a broken harness: the EndpointSlice is control-plane
	// state, and kube-proxy programs the corresponding data-plane rules
	// asynchronously. Until it does, the NodePort rejects. Failing on the first
	// refusal would make every trial a race against rule propagation, so Start
	// retries within this bounded window and only then aborts.
	DefaultDialRetryWindow = 20 * time.Second
	// DefaultGapThreshold is how long a TCP flow may go without a heartbeat
	// before a (non-terminal) gap is recorded. Three missed heartbeats.
	DefaultGapThreshold = 1500 * time.Millisecond
)

// Config configures a Generator. Zero values are rejected rather than silently
// defaulted, so that a report's config block always describes what actually ran.
type Config struct {
	Host                string
	TCPPort             int
	UDPPort             int
	TCPFlows            int
	UDPFlows            int
	Interval            time.Duration
	GapThreshold        time.Duration
	FlowTimeout         time.Duration
	UDPSilenceDatagrams int
	DialTimeout         time.Duration
	// DialRetryWindow bounds how long a flow may keep failing to connect before
	// Start gives up. Zero means DefaultDialRetryWindow.
	DialRetryWindow time.Duration
}

// Validate checks the configuration and returns an error naming the invariant
// that failed.
func (c Config) Validate() error {
	if c.Host == "" {
		return fmt.Errorf("flowgen config invalid: host is empty (invariant: a flow generator needs a target host; check --target-host)")
	}
	if c.TCPPort <= 0 || c.TCPPort > 65535 {
		return fmt.Errorf("flowgen config invalid: tcp port %d out of range (invariant: 1-65535; check --tcp-port)", c.TCPPort)
	}
	if c.UDPPort <= 0 || c.UDPPort > 65535 {
		return fmt.Errorf("flowgen config invalid: udp port %d out of range (invariant: 1-65535; check --udp-port)", c.UDPPort)
	}
	if c.TCPFlows < 0 || c.UDPFlows < 0 {
		return fmt.Errorf("flowgen config invalid: flow counts must not be negative (got tcp=%d udp=%d)", c.TCPFlows, c.UDPFlows)
	}
	if c.TCPFlows == 0 && c.UDPFlows == 0 {
		return fmt.Errorf("flowgen config invalid: zero tcp flows and zero udp flows (invariant: a run must measure at least one flow; check --tcp-flows/--udp-flows)")
	}
	if c.Interval <= 0 {
		return fmt.Errorf("flowgen config invalid: interval must be positive (got %s)", c.Interval)
	}
	if c.FlowTimeout <= c.Interval {
		return fmt.Errorf("flowgen config invalid: flow timeout %s must exceed the heartbeat interval %s (invariant: a single missed heartbeat must not be a timeout; check --flow-timeout)", c.FlowTimeout, c.Interval)
	}
	if c.UDPSilenceDatagrams <= 0 {
		return fmt.Errorf("flowgen config invalid: udp silence threshold must be positive (got %d)", c.UDPSilenceDatagrams)
	}
	return nil
}

// Generator owns every flow in a trial.
type Generator struct {
	cfg   Config
	flows []*flow
	tcp   []*tcpFlow
	udp   []*udpFlow

	wg     sync.WaitGroup
	cancel context.CancelFunc

	mu      sync.Mutex
	started bool
	closed  bool
}

// New builds a Generator with sequential flow IDs. No sockets are opened yet.
func New(cfg Config) (*Generator, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	g := &Generator{cfg: cfg}
	for i := 0; i < cfg.TCPFlows; i++ {
		g.flows = append(g.flows, newFlow("tcp", i))
	}
	for i := 0; i < cfg.UDPFlows; i++ {
		g.flows = append(g.flows, newFlow("udp", i))
	}
	return g, nil
}

// FlowIDs returns every flow ID in creation order.
func (g *Generator) FlowIDs() []string {
	out := make([]string, 0, len(g.flows))
	for _, f := range g.flows {
		out = append(out, f.id)
	}
	return out
}

func (g *Generator) tcpAddr() string {
	return net.JoinHostPort(g.cfg.Host, strconv.Itoa(g.cfg.TCPPort))
}

func (g *Generator) udpAddr() string {
	return net.JoinHostPort(g.cfg.Host, strconv.Itoa(g.cfg.UDPPort))
}

// Start dials every flow and begins driving them. If any flow fails to
// establish, Start tears down the ones that did and returns an error: a partial
// flow set is a broken harness, not a degraded measurement.
func (g *Generator) Start(ctx context.Context) error {
	g.mu.Lock()
	if g.started {
		g.mu.Unlock()
		return fmt.Errorf("flow generator already started (invariant: a generator drives exactly one trial)")
	}
	g.started = true
	g.mu.Unlock()

	runCtx, cancel := context.WithCancel(ctx)
	g.cancel = cancel

	window := g.cfg.DialRetryWindow
	if window <= 0 {
		window = DefaultDialRetryWindow
	}

	i := 0
	for ; i < g.cfg.TCPFlows; i++ {
		flowRef := g.flows[i]
		t, err := retryDial(ctx, window, func() (*tcpFlow, error) {
			return dialTCP(ctx, flowRef, g.tcpAddr(), g.cfg.DialTimeout)
		})
		if err != nil {
			g.teardown()
			return fmt.Errorf("%w (invariant: every requested tcp flow must establish before measurement; check that the probe is Ready and that %s is reachable from this host)", err, g.tcpAddr())
		}
		g.tcp = append(g.tcp, t)
	}
	for j := 0; j < g.cfg.UDPFlows; j++ {
		flowRef := g.flows[i+j]
		u, err := retryDial(ctx, window, func() (*udpFlow, error) {
			return dialUDP(ctx, flowRef, g.udpAddr(), g.cfg.DialTimeout)
		})
		if err != nil {
			g.teardown()
			return fmt.Errorf("%w (invariant: every requested udp flow must open a socket before measurement; check %s)", err, g.udpAddr())
		}
		g.udp = append(g.udp, u)
	}

	for _, t := range g.tcp {
		g.wg.Add(1)
		go t.run(runCtx, &g.wg, g.cfg.Interval, g.cfg.GapThreshold, g.cfg.FlowTimeout)
	}
	for _, u := range g.udp {
		g.wg.Add(1)
		go u.run(runCtx, &g.wg, g.cfg.Interval, g.cfg.UDPSilenceDatagrams)
	}
	return nil
}

// retryDial calls dial until it succeeds or the window expires. The error it
// returns on give-up names the attempt count and the window, so that a genuinely
// unreachable target is distinguishable from a slow one.
func retryDial[T any](ctx context.Context, window time.Duration, dial func() (T, error)) (T, error) {
	deadline := time.Now().Add(window)
	var zero T
	for attempt := 1; ; attempt++ {
		v, err := dial()
		if err == nil {
			return v, nil
		}
		if ctx.Err() != nil {
			return zero, fmt.Errorf("%w (cancelled after %d attempt(s))", err, attempt)
		}
		if !time.Now().Before(deadline) {
			return zero, fmt.Errorf("%w (still failing after %d attempt(s) over %s)", err, attempt, window)
		}
		select {
		case <-ctx.Done():
			return zero, fmt.Errorf("%w (cancelled after %d attempt(s))", err, attempt)
		case <-time.After(250 * time.Millisecond):
		}
	}
}

func (g *Generator) teardown() {
	if g.cancel != nil {
		g.cancel()
	}
	for _, t := range g.tcp {
		_ = t.conn.Close()
	}
	for _, u := range g.udp {
		_ = u.conn.Close()
	}
}

// WaitEstablished blocks until every flow of the given protocol has carried at
// least one heartbeat or ack, or until timeout. An empty proto means all flows.
// The returned error names the flows that never carried data.
func (g *Generator) WaitEstablished(ctx context.Context, timeout time.Duration, proto string) error {
	deadline := time.Now().Add(timeout)
	poll := time.NewTicker(50 * time.Millisecond)
	defer poll.Stop()
	for {
		missing := g.notEstablished(proto)
		if len(missing) == 0 {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("%s flows never carried data within %s: %v (invariant: every requested flow must be receiving probe traffic before the trigger; check the probe logs and that the NodePort mappings for tcp:%d and udp:%d reach the pod from this host)",
				protoLabel(proto), timeout, missing, g.cfg.TCPPort, g.cfg.UDPPort)
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("cancelled while waiting for %s flows to establish: %w", protoLabel(proto), ctx.Err())
		case <-poll.C:
		}
	}
}

func protoLabel(proto string) string {
	if proto == "" {
		return "all"
	}
	return proto
}

func (g *Generator) notEstablished(proto string) []string {
	var missing []string
	for _, f := range g.flows {
		if proto != "" && f.proto != proto {
			continue
		}
		if !f.hasData() {
			missing = append(missing, f.id)
		}
	}
	return missing
}

// TerminalFlows returns the IDs of flows that have already ended.
func (g *Generator) TerminalFlows() []string {
	var out []string
	for _, f := range g.flows {
		if f.isTerminal() {
			out = append(out, f.id)
		}
	}
	return out
}

// AllTerminal reports whether every flow has ended.
func (g *Generator) AllTerminal() bool {
	for _, f := range g.flows {
		if !f.isTerminal() {
			return false
		}
	}
	return true
}

// CloseObservation ends the observation window: every flow still alive is
// recorded as survived-observation-window at instant at, and all sockets are
// closed. It is safe to call more than once.
func (g *Generator) CloseObservation(at time.Time) {
	g.mu.Lock()
	if g.closed {
		g.mu.Unlock()
		return
	}
	g.closed = true
	g.mu.Unlock()

	for _, f := range g.flows {
		if !f.isTerminal() {
			f.record(KindObservationEnd, at, NoSeq, "flow was still alive when the observation window closed")
		}
	}
	g.teardown()
	g.wg.Wait()
}

// Records renders every flow relative to t0.
func (g *Generator) Records(t0 time.Time) []report.Flow {
	out := make([]report.Flow, 0, len(g.flows))
	for _, f := range g.flows {
		out = append(out, f.toReport(t0))
	}
	return out
}

// TimelineEvents renders one client-sourced timeline entry per flow that
// reached a terminal event, relative to t0.
func (g *Generator) TimelineEvents(t0 time.Time) []report.Event {
	var out []report.Event
	for _, f := range g.flows {
		rec := f.toReport(t0)
		if rec.TTerminalMs == nil {
			continue
		}
		out = append(out, report.Event{
			TMs:    *rec.TTerminalMs,
			Source: report.SourceClient,
			Event:  report.EventFlowTerminal,
			FlowID: rec.ID,
			Detail: string(rec.Outcome) + ": " + rec.Detail,
		})
	}
	return out
}
