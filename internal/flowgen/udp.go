package flowgen

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// udpFlow is one connected UDP socket against the probe.
//
// The socket is "connected" (net.Dial rather than net.ListenPacket) for two
// reasons: the kernel then delivers ICMP port-unreachable to us as
// ECONNREFUSED, which is a real observation about the peer; and each flow gets
// a distinct source port, which is what makes it a distinguishable flow through
// conntrack.
type udpFlow struct {
	*flow
	conn net.Conn

	// lastAnswered is the highest sequence number the probe has acknowledged,
	// or -1 if it has never answered. Written by the reader, read by the
	// sender, hence atomic.
	lastAnswered atomic.Int64
}

func dialUDP(ctx context.Context, f *flow, addr string, timeout time.Duration) (*udpFlow, error) {
	d := net.Dialer{Timeout: timeout}
	conn, err := d.DialContext(ctx, "udp", addr)
	if err != nil {
		return nil, fmt.Errorf("flow %s could not create a udp socket toward %s: %w", f.id, addr, err)
	}
	u := &udpFlow{flow: f, conn: conn}
	u.lastAnswered.Store(-1)
	f.record(KindConnected, time.Now(), NoSeq, conn.LocalAddr().String()+" -> "+conn.RemoteAddr().String())
	return u, nil
}

// run sends a datagram every interval and reads replies until the flow is
// declared severed or ctx is done.
//
// silenceDatagrams is the number of consecutive unanswered datagrams that
// constitutes severance. With the default interval of 500ms and 6 datagrams,
// severance is declared 3 seconds after the last answered datagram, and the
// flow record carries the sequence number of that last answered datagram.
func (u *udpFlow) run(ctx context.Context, wg *sync.WaitGroup, interval time.Duration, silenceDatagrams int) {
	defer wg.Done()

	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		u.readLoop(ctx, interval)
	}()

	u.sendLoop(ctx, interval, silenceDatagrams)

	_ = u.conn.Close()
	<-readerDone
}

func (u *udpFlow) sendLoop(ctx context.Context, interval time.Duration, silenceDatagrams int) {
	tick := time.NewTicker(interval)
	defer tick.Stop()
	var seq int64
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			if u.isTerminal() {
				return
			}
			now := time.Now()

			// Evaluate silence before sending: at this instant, datagrams
			// seq-silenceDatagrams .. seq-1 have all had at least one full
			// interval to be answered.
			answered := u.lastAnswered.Load()
			unanswered := seq - answered - 1
			if unanswered >= int64(silenceDatagrams) {
				u.record(KindSilence, now, NoSeq,
					fmt.Sprintf("udp-silence-%d-datagrams", unanswered))
				return
			}

			_ = u.conn.SetWriteDeadline(now.Add(interval * 4))
			if _, err := io.WriteString(u.conn, PrefixPing+strconv.FormatInt(seq, 10)+"\n"); err != nil {
				kind, detail := classifyWriteError(err)
				u.record(kind, time.Now(), NoSeq, detail)
				return
			}
			seq++
		}
	}
}

func (u *udpFlow) readLoop(ctx context.Context, interval time.Duration) {
	br := bufio.NewReader(u.conn)
	for {
		if ctx.Err() != nil {
			return
		}
		// The read deadline here is a poll interval, not a measurement: silence
		// is judged by the send loop, which knows how many datagrams are
		// outstanding. A deadline expiry on this socket is therefore not an
		// event.
		_ = u.conn.SetReadDeadline(time.Now().Add(interval))
		line, err := br.ReadString('\n')
		now := time.Now()
		if line != "" {
			u.consumeLine(strings.TrimRight(line, "\r\n"), now)
		}
		if err == nil {
			continue
		}
		if isTimeout(err) {
			continue
		}
		if err == io.EOF {
			// A connected UDP socket does not produce a meaningful EOF; treat
			// it as a read error rather than as a clean close, because UDP has
			// no close.
			u.record(KindReset, now, NoSeq, "unexpected eof on udp socket")
			return
		}
		kind, detail := classifyReadError(err)
		u.record(kind, now, NoSeq, detail)
		return
	}
}

func (u *udpFlow) consumeLine(line string, now time.Time) {
	if !strings.HasPrefix(line, PrefixAck) {
		if line != "" {
			u.record(KindGap, now, NoSeq, "unrecognised protocol line: "+truncate(line, 64))
		}
		return
	}
	fields := strings.Fields(strings.TrimPrefix(line, PrefixAck))
	s := NoSeq
	if len(fields) > 0 {
		if v, err := strconv.ParseInt(fields[0], 10, 64); err == nil {
			s = v
		}
	}
	u.record(KindReply, now, s, "")
	if s != NoSeq {
		// Datagrams can be reordered; keep the highest answered sequence.
		for {
			cur := u.lastAnswered.Load()
			if s <= cur || u.lastAnswered.CompareAndSwap(cur, s) {
				break
			}
		}
	}
}
