package flowgen

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// tcpFlow is one long-lived TCP connection against the probe.
type tcpFlow struct {
	*flow
	conn net.Conn
}

// dialTCP establishes the connection. Establishment failures are returned to
// the caller rather than recorded as an outcome: a flow that never connected is
// a preflight failure, not a measurement.
func dialTCP(ctx context.Context, f *flow, addr string, timeout time.Duration) (*tcpFlow, error) {
	d := net.Dialer{Timeout: timeout}
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("flow %s could not connect to %s: %w", f.id, addr, err)
	}
	if tc, ok := conn.(*net.TCPConn); ok {
		// Keepalive is disabled deliberately: drainwatch measures what the
		// application and the cluster do to the flow, and a kernel keepalive
		// probe would be an extra variable in that measurement.
		_ = tc.SetKeepAlive(false)
	}
	f.record(KindConnected, time.Now(), NoSeq, conn.LocalAddr().String()+" -> "+conn.RemoteAddr().String())
	return &tcpFlow{flow: f, conn: conn}, nil
}

// run drives one TCP flow until it reaches a terminal event or ctx is done.
// gapThreshold is how long a silence must last before it is recorded as a gap;
// flowTimeout is how long a silence must last before the flow is declared
// read-timeout.
func (t *tcpFlow) run(ctx context.Context, wg *sync.WaitGroup, pingInterval, gapThreshold, flowTimeout time.Duration) {
	defer wg.Done()

	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		t.writeLoop(ctx, pingInterval)
	}()

	t.readLoop(ctx, gapThreshold, flowTimeout)

	// Closing the socket unblocks the writer. The writer's resulting
	// net.ErrClosed is mapped to a non-peer event and, because the reader has
	// already latched a terminal, dropped.
	_ = t.conn.Close()
	<-writerDone
}

// writeLoop sends "ping <seq>" at a fixed interval. Its purpose is to keep the
// flow bidirectional so that an abrupt server exit is observable as a write
// error rather than as indefinite silence.
func (t *tcpFlow) writeLoop(ctx context.Context, interval time.Duration) {
	tick := time.NewTicker(interval)
	defer tick.Stop()
	var seq int64
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			if t.isTerminal() {
				return
			}
			_ = t.conn.SetWriteDeadline(time.Now().Add(interval * 4))
			_, err := io.WriteString(t.conn, PrefixPing+strconv.FormatInt(seq, 10)+"\n")
			if err != nil {
				kind, detail := classifyWriteError(err)
				t.record(kind, time.Now(), NoSeq, detail)
				return
			}
			seq++
		}
	}
}

// readLoop consumes heartbeats until a terminal event.
func (t *tcpFlow) readLoop(ctx context.Context, gapThreshold, flowTimeout time.Duration) {
	br := bufio.NewReader(t.conn)
	lastData := time.Now()
	gapOpen := false

	for {
		if ctx.Err() != nil {
			// The generator is shutting the observation window; the generator
			// itself records observation_end for every still-live flow.
			return
		}
		_ = t.conn.SetReadDeadline(time.Now().Add(gapThreshold))
		line, err := br.ReadString('\n')
		now := time.Now()

		if line != "" {
			lastData = now
			if gapOpen {
				gapOpen = false
			}
			t.consumeLine(strings.TrimRight(line, "\r\n"), now)
		}
		if err == nil {
			continue
		}

		if isTimeout(err) {
			if now.Sub(lastData) >= flowTimeout {
				t.record(KindReadTimeout, now, NoSeq,
					fmt.Sprintf("no bytes for %s (flow timeout)", now.Sub(lastData).Round(time.Millisecond)))
				return
			}
			if !gapOpen {
				gapOpen = true
				t.record(KindGap, now, NoSeq,
					fmt.Sprintf("no heartbeat for %s", now.Sub(lastData).Round(time.Millisecond)))
			}
			continue
		}
		if err == io.EOF {
			t.record(KindEOF, now, NoSeq, "")
			return
		}
		kind, detail := classifyReadError(err)
		t.record(kind, now, NoSeq, detail)
		return
	}
}

// consumeLine parses one protocol line. Unrecognised lines are recorded as gaps
// with the raw text rather than dropped, so a protocol mismatch shows up in the
// report instead of looking like silence.
func (t *tcpFlow) consumeLine(line string, now time.Time) {
	switch {
	case strings.HasPrefix(line, PrefixHeartbeat):
		fields := strings.Fields(strings.TrimPrefix(line, PrefixHeartbeat))
		s := NoSeq
		if len(fields) > 0 {
			if v, err := strconv.ParseInt(fields[0], 10, 64); err == nil {
				s = v
			}
		}
		if len(fields) > 2 {
			if changed, previous := t.noteInstance(fields[2]); changed {
				t.record(KindRehomed, now, s,
					fmt.Sprintf("answered by probe instance %s, was %s", fields[2], previous))
				return
			}
		}
		t.record(KindHeartbeat, now, s, "")
	case strings.HasPrefix(line, PrefixBye):
		fields := strings.Fields(strings.TrimPrefix(line, PrefixBye))
		s := NoSeq
		reason := ""
		if len(fields) > 0 {
			if v, err := strconv.ParseInt(fields[0], 10, 64); err == nil {
				s = v
			}
		}
		if len(fields) > 1 {
			reason = fields[1]
		}
		t.record(KindDrainAnnounced, now, s, reason)
	case line == "":
		// keepalive newline; ignore
	default:
		t.record(KindGap, now, NoSeq, "unrecognised protocol line: "+truncate(line, 64))
	}
}

// isTimeout reports whether err is a socket deadline expiry rather than a real
// transport error. A deadline expiry means "nothing arrived"; every other error
// means "something happened to the flow", and the two must not be conflated.
func isTimeout(err error) bool {
	if errors.Is(err, os.ErrDeadlineExceeded) {
		return true
	}
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
