// Package probe implements the in-cluster test workload: the thing whose
// termination drainwatch measures.
//
// The probe's job is to be a completely legible server. It serves long-lived
// TCP flows and a UDP echo, and it emits a structured event for everything that
// happens to it, so that the orchestrator can reconstruct the termination from
// evidence rather than inference.
package probe

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/jaynirmal15/drainwatch/internal/flowgen"
	"github.com/jaynirmal15/drainwatch/internal/report"
)

// Probe event names. These strings appear verbatim in the report timeline.
const (
	EventStarted          = "probe_started"
	EventListening        = "listening"
	EventSigtermReceived  = report.EventSigtermReceived
	EventAccept           = "tcp_accept"
	EventHeartbeat        = "tcp_heartbeat"
	EventFlowClosed       = "tcp_flow_closed"
	EventFlowOpenAtExit   = "tcp_flow_open_at_exit"
	EventResetForced      = "tcp_flow_reset_forced"
	EventDrainStarted     = "drain_started"
	EventDrainComplete    = "drain_complete"
	EventDrainDeadline    = "drain_deadline_reached"
	EventStoppedAccepting = "stopped_accepting"
	EventReadyzNotReady   = "readyz_now_503"
	EventSigtermIgnored   = "sigterm_ignored"
	EventUDPFlowSeen      = "udp_flow_first_seen"
	EventUDPStats         = "udp_stats"
	EventExiting          = "exiting"
)

// Drain behaviours, selected by the DRAIN_BEHAVIOR environment variable.
const (
	// BehaviorDrain: on SIGTERM stop accepting, fail readiness, keep serving
	// established flows, exit when they have all closed or when
	// DRAIN_MAX_SECONDS expires — announcing the close on every flow that is
	// still open at the deadline.
	BehaviorDrain = "drain"
	// BehaviorExitNow: on SIGTERM abandon every established flow and exit(0).
	BehaviorExitNow = "exit-now"
	// BehaviorIgnore: log SIGTERM and keep serving until SIGKILL.
	BehaviorIgnore = "ignore"
)

// DefaultDrainMaxSeconds is the drain window when DRAIN_MAX_SECONDS is unset.
const DefaultDrainMaxSeconds = 60

// Config is the probe's resolved configuration.
type Config struct {
	TCPPort         int
	UDPPort         int
	ReadyPort       int
	Behavior        string
	DrainMaxSeconds int
	// ExitNowForceRST controls how BehaviorExitNow abandons its connections.
	//
	// When true (the default), the probe sets SO_LINGER=0 on every established
	// connection and closes it before exiting, which makes the peer observe a
	// deterministic RST. This is a deliberate model of an application that dies
	// mid-flow; it is recorded in the log as tcp_flow_reset_forced so the
	// report shows what the harness did rather than implying the kernel did it
	// unprompted.
	//
	// When false, the probe simply calls exit(0) and lets the kernel close the
	// sockets however it will — usually FIN, sometimes RST depending on whether
	// unread client data is sitting in the receive buffer. That is more
	// faithful to a bare crash but is not deterministic, which is why it is not
	// the default.
	ExitNowForceRST bool
	// HeartbeatInterval is the TCP heartbeat period.
	HeartbeatInterval time.Duration
	// Instance identifies this probe process on the wire. It is the pod name
	// (POD_NAME, set by the downward API) or the hostname, so that a client can
	// tell when its flow has been re-homed onto a replacement pod rather than
	// assuming a reply means its original peer is still alive.
	Instance string
}

// ConfigFromEnv resolves the probe configuration from the environment, applying
// documented defaults. It returns an error naming the offending variable rather
// than falling back silently, because a probe running a different behaviour
// from the one the report claims would contaminate every result.
func ConfigFromEnv(getenv func(string) string) (Config, error) {
	c := Config{
		TCPPort:           7001,
		UDPPort:           7002,
		ReadyPort:         7003,
		Behavior:          BehaviorDrain,
		DrainMaxSeconds:   DefaultDrainMaxSeconds,
		ExitNowForceRST:   true,
		HeartbeatInterval: flowgen.DefaultInterval,
	}

	if v := strings.TrimSpace(getenv("POD_NAME")); v != "" {
		c.Instance = v
	} else if h, err := os.Hostname(); err == nil && h != "" {
		c.Instance = h
	} else {
		// An unidentifiable probe would make re-homing undetectable, so say so
		// rather than shipping an empty instance that silently disables the
		// check.
		return c, fmt.Errorf("cannot determine this probe's instance identity (invariant: the probe must identify its process on the wire so clients can detect being re-homed onto a replacement pod; set POD_NAME via the downward API)")
	}

	if v := strings.TrimSpace(getenv("DRAIN_BEHAVIOR")); v != "" {
		switch v {
		case BehaviorDrain, BehaviorExitNow, BehaviorIgnore:
			c.Behavior = v
		default:
			return c, fmt.Errorf("DRAIN_BEHAVIOR=%q is not a known behaviour (invariant: DRAIN_BEHAVIOR must be one of %q, %q, %q)", v, BehaviorDrain, BehaviorExitNow, BehaviorIgnore)
		}
	}
	if v := strings.TrimSpace(getenv("DRAIN_MAX_SECONDS")); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			return c, fmt.Errorf("DRAIN_MAX_SECONDS=%q is not a non-negative integer (invariant: the drain window must be a whole number of seconds)", v)
		}
		c.DrainMaxSeconds = n
	}
	if v := strings.TrimSpace(getenv("EXIT_NOW_FORCE_RST")); v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			return c, fmt.Errorf("EXIT_NOW_FORCE_RST=%q is not a boolean (invariant: must parse as a Go bool, e.g. true/false)", v)
		}
		c.ExitNowForceRST = b
	}
	for _, p := range []struct {
		name string
		dst  *int
	}{
		{"PROBE_TCP_PORT", &c.TCPPort},
		{"PROBE_UDP_PORT", &c.UDPPort},
		{"PROBE_READY_PORT", &c.ReadyPort},
	} {
		v := strings.TrimSpace(getenv(p.name))
		if v == "" {
			continue
		}
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 || n > 65535 {
			return c, fmt.Errorf("%s=%q is not a valid port (invariant: 1-65535)", p.name, v)
		}
		*p.dst = n
	}
	return c, nil
}

// srvConn is one accepted TCP connection.
type srvConn struct {
	id   string
	conn *net.TCPConn
	log  *Logger

	seq    atomic.Int64
	closed atomic.Bool
	// selfClosed marks a connection the probe itself closed. The reader
	// goroutine then stays quiet about the resulting net.ErrClosed, so that a
	// probe-initiated close produces one log line describing what the probe
	// did, not two describing the same event from both ends.
	selfClosed atomic.Bool
	done       chan struct{}
}

// Server is the probe process.
type Server struct {
	cfg Config
	log *Logger

	mu       sync.Mutex
	conns    map[string]*srvConn
	nextID   int
	draining atomic.Bool
	ready    atomic.Bool

	tcpLn      *net.TCPListener
	udpConn    *net.UDPConn
	readyLn    net.Listener
	readySrv   *http.Server
	udpFlows   sync.Map // remote addr string -> flow id
	udpSeen    atomic.Int64
	udpReplies atomic.Int64
	udpNextID  atomic.Int64

	allClosed chan struct{}
	closeOnce sync.Once
}

// NewServer builds a probe server. Nothing is bound until Run.
func NewServer(cfg Config, log *Logger) *Server {
	s := &Server{cfg: cfg, log: log, conns: map[string]*srvConn{}, allClosed: make(chan struct{})}
	s.ready.Store(true)
	return s
}

// Run binds the listeners and serves until the configured termination path is
// taken. It returns the process exit code.
//
// Run is Listen + Serve + wait-for-signal + Terminate. Those four steps are
// exported separately so that the drain behaviours can be exercised over
// loopback in a test, without a signal and without a cluster.
func (s *Server) Run(ctx context.Context) int {
	s.log.Event(report.ProbeEvent{
		Event:         EventStarted,
		DrainBehavior: s.cfg.Behavior,
		Detail: fmt.Sprintf("instance=%s tcp=%d udp=%d readyz=%d drain_max_seconds=%d exit_now_force_rst=%t",
			s.cfg.Instance, s.cfg.TCPPort, s.cfg.UDPPort, s.cfg.ReadyPort, s.cfg.DrainMaxSeconds, s.cfg.ExitNowForceRST),
	})

	if err := s.Listen(); err != nil {
		s.log.Simple("fatal", err.Error())
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer s.Close()

	s.Serve(ctx)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)

	select {
	case sg := <-sig:
		// This is the single most important line the probe writes: it is how
		// SIGTERM delivery time enters the timeline. It is emitted before any
		// other work so that the timestamp reflects delivery, not handling.
		s.log.Event(report.ProbeEvent{
			Event:         EventSigtermReceived,
			DrainBehavior: s.cfg.Behavior,
			Detail:        "signal=" + sg.String(),
		})
		return s.Terminate(ctx)
	case <-ctx.Done():
		s.log.Simple(EventExiting, "context cancelled before any signal was delivered")
		return 0
	}
}

// Serve starts the accept, UDP and stats loops. Listen must have succeeded.
func (s *Server) Serve(ctx context.Context) {
	go s.acceptLoop()
	go s.udpLoop()
	go s.udpStatsLoop(ctx)
}

// TCPPort reports the port the TCP listener actually bound, which differs from
// the configured port when the config asked for port 0.
func (s *Server) TCPPort() int {
	if s.tcpLn == nil {
		return 0
	}
	return s.tcpLn.Addr().(*net.TCPAddr).Port
}

// UDPPort reports the bound UDP port.
func (s *Server) UDPPort() int {
	if s.udpConn == nil {
		return 0
	}
	return s.udpConn.LocalAddr().(*net.UDPAddr).Port
}

// ReadyPort reports the bound readiness port.
func (s *Server) ReadyPort() int {
	if s.readyLn == nil {
		return 0
	}
	return s.readyLn.Addr().(*net.TCPAddr).Port
}

// Close shuts down every listener. It does not touch established connections;
// how those end is the behaviour under test.
func (s *Server) Close() { s.shutdownListeners() }

// Listen binds the TCP, UDP and readiness listeners.
func (s *Server) Listen() error {
	tcpAddr := &net.TCPAddr{Port: s.cfg.TCPPort}
	ln, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		return fmt.Errorf("probe cannot listen on tcp :%d: %w (invariant: the probe must own its TCP port; check for a port collision in the pod)", s.cfg.TCPPort, err)
	}
	s.tcpLn = ln

	uc, err := net.ListenUDP("udp", &net.UDPAddr{Port: s.cfg.UDPPort})
	if err != nil {
		ln.Close()
		return fmt.Errorf("probe cannot listen on udp :%d: %w (invariant: the probe must own its UDP port; check for a port collision in the pod)", s.cfg.UDPPort, err)
	}
	s.udpConn = uc

	mux := http.NewServeMux()
	mux.HandleFunc("/readyz", s.handleReadyz)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		// Liveness is deliberately unconditional: the probe must never be
		// restarted by the kubelet in the middle of a measured drain.
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "ok\n")
	})
	s.readySrv = &http.Server{Addr: fmt.Sprintf(":%d", s.cfg.ReadyPort), Handler: mux}
	readyLn, err := net.Listen("tcp", s.readySrv.Addr)
	if err == nil {
		s.readyLn = readyLn
	}
	if err != nil {
		ln.Close()
		uc.Close()
		return fmt.Errorf("probe cannot listen on tcp :%d for readiness: %w (invariant: the readiness endpoint must bind, otherwise EndpointSlice transitions are unobservable)", s.cfg.ReadyPort, err)
	}
	go func() {
		if err := s.readySrv.Serve(readyLn); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.log.Simple("readyz_server_error", err.Error())
		}
	}()

	s.log.Simple(EventListening, fmt.Sprintf("tcp=:%d udp=:%d readyz=:%d", s.TCPPort(), s.UDPPort(), s.ReadyPort()))
	return nil
}

func (s *Server) handleReadyz(w http.ResponseWriter, _ *http.Request) {
	if s.ready.Load() {
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "ready\n")
		return
	}
	w.WriteHeader(http.StatusServiceUnavailable)
	_, _ = io.WriteString(w, "draining\n")
}

func (s *Server) acceptLoop() {
	for {
		c, err := s.tcpLn.AcceptTCP()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}
			s.log.Simple("tcp_accept_error", err.Error())
			return
		}
		if s.draining.Load() {
			// Draining means no new work. Refuse loudly rather than accepting
			// and immediately closing, which would look like a severed flow.
			s.log.Simple("tcp_accept_refused_draining", c.RemoteAddr().String())
			_ = c.Close()
			continue
		}
		s.register(c)
	}
}

func (s *Server) register(c *net.TCPConn) {
	s.mu.Lock()
	s.nextID++
	sc := &srvConn{id: fmt.Sprintf("srv-tcp-%04d", s.nextID), conn: c, log: s.log, done: make(chan struct{})}
	s.conns[sc.id] = sc
	s.mu.Unlock()

	s.log.Flow(EventAccept, sc.id, nil, c.RemoteAddr().String()+" -> "+c.LocalAddr().String())
	go s.serveConn(sc)
}

// serveConn runs one accepted connection: a heartbeat writer and a reader that
// exists to observe how the client's side of the flow ends.
func (s *Server) serveConn(sc *srvConn) {
	defer close(sc.done)

	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		tick := time.NewTicker(s.cfg.HeartbeatInterval)
		defer tick.Stop()
		for range tick.C {
			if sc.closed.Load() {
				return
			}
			n := sc.seq.Load()
			line := fmt.Sprintf("%s%d %d %s\n", flowgen.PrefixHeartbeat, n, time.Now().UnixNano(), s.cfg.Instance)
			_ = sc.conn.SetWriteDeadline(time.Now().Add(s.cfg.HeartbeatInterval * 4))
			if _, err := io.WriteString(sc.conn, line); err != nil {
				s.log.Event(report.ProbeEvent{
					Event: EventFlowClosed, FlowID: sc.id, Seq: &n,
					Detail: "write failed", Errno: errnoString(err),
				})
				sc.closed.Store(true)
				return
			}
			s.log.Flow(EventHeartbeat, sc.id, &n, "")
			sc.seq.Store(n + 1)
		}
	}()

	br := bufio.NewReader(sc.conn)
	for {
		line, err := br.ReadString('\n')
		_ = line // client pings carry no information the probe needs
		if err == nil {
			continue
		}
		last := sc.seq.Load()
		switch {
		case err == io.EOF:
			s.log.Event(report.ProbeEvent{Event: EventFlowClosed, FlowID: sc.id, Seq: &last, Detail: "fin received from client"})
		case errors.Is(err, net.ErrClosed):
			if !sc.selfClosed.Load() {
				s.log.Event(report.ProbeEvent{Event: EventFlowClosed, FlowID: sc.id, Seq: &last, Detail: "closed locally by probe"})
			}
		default:
			s.log.Event(report.ProbeEvent{Event: EventFlowClosed, FlowID: sc.id, Seq: &last, Detail: "read failed", Errno: errnoString(err)})
		}
		sc.closed.Store(true)
		<-writerDone
		s.unregister(sc.id)
		return
	}
}

func (s *Server) unregister(id string) {
	s.mu.Lock()
	delete(s.conns, id)
	n := len(s.conns)
	s.mu.Unlock()
	if n == 0 && s.draining.Load() {
		s.closeOnce.Do(func() { close(s.allClosed) })
	}
}

func (s *Server) liveConns() []*srvConn {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*srvConn, 0, len(s.conns))
	for _, c := range s.conns {
		out = append(out, c)
	}
	return out
}

// udpLoop echoes every datagram, preserving the client's sequence number.
func (s *Server) udpLoop() {
	buf := make([]byte, 2048)
	for {
		n, addr, err := s.udpConn.ReadFromUDP(buf)
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}
			s.log.Simple("udp_read_error", err.Error())
			continue
		}
		key := addr.String()
		if _, seen := s.udpFlows.Load(key); !seen {
			id := fmt.Sprintf("srv-udp-%04d", s.udpNextID.Add(1))
			s.udpFlows.Store(key, id)
			s.udpSeen.Add(1)
			s.log.Flow(EventUDPFlowSeen, id, nil, key)
		}
		line := strings.TrimRight(string(buf[:n]), "\r\n")
		seq := strings.TrimPrefix(line, flowgen.PrefixPing)
		if _, err := s.udpConn.WriteToUDP([]byte(flowgen.PrefixAck+seq+" "+s.cfg.Instance+"\n"), addr); err != nil {
			s.log.Simple("udp_write_error", err.Error())
			continue
		}
		s.udpReplies.Add(1)
	}
}

// udpStatsLoop emits a periodic count rather than a line per datagram: the
// per-datagram record lives on the client side, which is where severance is
// judged.
func (s *Server) udpStatsLoop(ctx context.Context) {
	tick := time.NewTicker(5 * time.Second)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			s.log.Simple(EventUDPStats, fmt.Sprintf("flows=%d replies=%d", s.udpSeen.Load(), s.udpReplies.Load()))
		}
	}
}

// Terminate implements the three drain behaviours and returns the process exit
// code. Under BehaviorIgnore it blocks until ctx is cancelled, which in
// production means it never returns and the kubelet sends SIGKILL.
func (s *Server) Terminate(ctx context.Context) int {
	switch s.cfg.Behavior {
	case BehaviorExitNow:
		return s.terminateExitNow()
	case BehaviorIgnore:
		return s.terminateIgnore(ctx)
	default:
		return s.terminateDrain()
	}
}

func (s *Server) terminateExitNow() int {
	live := s.liveConns()
	if s.cfg.ExitNowForceRST {
		for _, sc := range live {
			// SO_LINGER with a zero timeout makes close() emit RST instead of
			// FIN. This is the harness deliberately modelling abandonment; the
			// event below records that drainwatch caused it.
			if err := sc.conn.SetLinger(0); err != nil {
				s.log.Flow(EventResetForced, sc.id, nil, "SetLinger(0) failed: "+err.Error())
			}
			sc.closed.Store(true)
			sc.selfClosed.Store(true)
			_ = sc.conn.Close()
			s.log.Flow(EventResetForced, sc.id, nil, "so_linger=0 close issued by probe (exit-now)")
		}
	} else {
		for _, sc := range live {
			s.log.Flow(EventFlowOpenAtExit, sc.id, nil, "abandoned without close (exit-now, EXIT_NOW_FORCE_RST=false)")
		}
	}
	s.log.Event(report.ProbeEvent{Event: EventExiting, DrainBehavior: s.cfg.Behavior, Detail: fmt.Sprintf("exit(0) immediately on SIGTERM with %d established flows", len(live))})
	return 0
}

func (s *Server) terminateIgnore(ctx context.Context) int {
	// Readiness stays 200 under ignore, per the documented behaviour: the point
	// of this arm is to observe what the cluster does when the application
	// never cooperates.
	s.log.Event(report.ProbeEvent{
		Event:         EventSigtermIgnored,
		DrainBehavior: s.cfg.Behavior,
		Detail:        "continuing to serve; readiness remains 200; waiting for SIGKILL at the grace-period boundary",
	})
	<-ctx.Done()
	s.log.Simple(EventExiting, "context cancelled")
	return 0
}

func (s *Server) terminateDrain() int {
	s.draining.Store(true)
	s.ready.Store(false)
	s.log.Event(report.ProbeEvent{Event: EventReadyzNotReady, DrainBehavior: s.cfg.Behavior, Detail: "/readyz now returns 503"})

	if s.tcpLn != nil {
		_ = s.tcpLn.Close()
		s.log.Simple(EventStoppedAccepting, "tcp listener closed; established flows continue")
	}

	live := s.liveConns()
	s.log.Event(report.ProbeEvent{
		Event:         EventDrainStarted,
		DrainBehavior: s.cfg.Behavior,
		Detail:        fmt.Sprintf("established_flows=%d drain_max_seconds=%d", len(live), s.cfg.DrainMaxSeconds),
	})

	if len(live) == 0 {
		s.closeOnce.Do(func() { close(s.allClosed) })
	}

	deadline := time.NewTimer(time.Duration(s.cfg.DrainMaxSeconds) * time.Second)
	defer deadline.Stop()

	select {
	case <-s.allClosed:
		s.log.Event(report.ProbeEvent{Event: EventDrainComplete, DrainBehavior: s.cfg.Behavior, Detail: "all established flows closed by the client before the drain deadline"})
	case <-deadline.C:
		remaining := s.liveConns()
		s.log.Event(report.ProbeEvent{
			Event:         EventDrainDeadline,
			DrainBehavior: s.cfg.Behavior,
			Detail:        fmt.Sprintf("drain window of %ds expired with %d flows still open; closing them gracefully", s.cfg.DrainMaxSeconds, len(remaining)),
		})
		for _, sc := range remaining {
			s.closeGracefully(sc, flowgen.ReasonDrainDeadline)
		}
	}

	s.log.Event(report.ProbeEvent{Event: EventExiting, DrainBehavior: s.cfg.Behavior, Detail: "exit(0) after drain"})
	return 0
}

// closeGracefully announces the close on the wire and then sends FIN. The
// announcement is what distinguishes a drain from a severance on the client
// side; without it, a FIN mid-heartbeat is indistinguishable from abandonment.
func (s *Server) closeGracefully(sc *srvConn, reason string) {
	n := sc.seq.Load()
	sc.closed.Store(true)
	sc.selfClosed.Store(true)
	_ = sc.conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
	line := fmt.Sprintf("%s%d %s\n", flowgen.PrefixBye, n, reason)
	if _, err := io.WriteString(sc.conn, line); err != nil {
		s.log.Event(report.ProbeEvent{Event: EventFlowClosed, FlowID: sc.id, Seq: &n, Detail: "drain announcement failed", Errno: errnoString(err)})
	}
	// CloseWrite sends FIN while leaving the read side open long enough for the
	// client to observe the announcement in order.
	_ = sc.conn.CloseWrite()
	_ = sc.conn.SetLinger(-1)
	_ = sc.conn.Close()
	s.log.Event(report.ProbeEvent{Event: EventFlowClosed, FlowID: sc.id, Seq: &n, Detail: "closed gracefully: " + reason})
}

func (s *Server) shutdownListeners() {
	if s.tcpLn != nil {
		_ = s.tcpLn.Close()
	}
	if s.udpConn != nil {
		_ = s.udpConn.Close()
	}
	if s.readySrv != nil {
		shutCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = s.readySrv.Shutdown(shutCtx)
	}
}

// errnoString extracts a syscall errno name when there is one, so the report
// records the mechanism rather than a formatted sentence.
func errnoString(err error) string {
	var errno syscall.Errno
	if errors.As(err, &errno) {
		return errno.Error()
	}
	if err == nil {
		return ""
	}
	return err.Error()
}
