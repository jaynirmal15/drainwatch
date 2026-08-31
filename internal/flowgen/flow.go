package flowgen

import (
	"errors"
	"fmt"
	"net"
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/jaynirmal15/drainwatch/internal/report"
)

// flow accumulates observations for one connection. Flow IDs are assigned
// sequentially by the generator (tcp-0001, tcp-0002, ...); there is no
// randomness anywhere in the measurement path.
type flow struct {
	id    string
	proto string

	mu       sync.Mutex
	events   []Event
	terminal bool
	// instance is the probe process that has been answering this flow. The
	// first non-empty value seen becomes the baseline; a later different value
	// means the flow was re-homed onto a replacement pod.
	instance string
}

// noteInstance records the answering probe instance and reports whether it
// changed. A change is terminal: the flow to the pod under test has ended.
func (f *flow) noteInstance(id string) (changed bool, previous string) {
	if id == "" {
		return false, ""
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.instance == "" {
		f.instance = id
		return false, ""
	}
	if f.instance == id {
		return false, f.instance
	}
	return true, f.instance
}

func newFlow(proto string, index int) *flow {
	return &flow{id: fmt.Sprintf("%s-%04d", proto, index+1), proto: proto}
}

// record appends an observation. The first terminal event latches: later
// terminal events are dropped so that a flow's outcome cannot be rewritten by a
// subsequent observer (for example the writer goroutine reporting EPIPE just
// after the reader already saw the FIN).
func (f *flow) record(kind EventKind, at time.Time, seq int64, detail string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.terminal {
		return
	}
	if kind.terminal() {
		f.terminal = true
	}
	f.events = append(f.events, Event{Kind: kind, At: at, Seq: seq, Detail: detail})
}

func (f *flow) isTerminal() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.terminal
}

func (f *flow) snapshot() []Event {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]Event, len(f.events))
	copy(out, f.events)
	return out
}

// hasData reports whether the flow has received at least one heartbeat or ack.
// Preflight uses this: a connected socket that has never carried data is not an
// established flow.
func (f *flow) hasData() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, e := range f.events {
		if e.Kind == KindHeartbeat || e.Kind == KindReply {
			return true
		}
	}
	return false
}

// classifyReadError maps a read error onto an event kind, preserving the errno
// in the detail so the report records the mechanism, not just the category.
func classifyReadError(err error) (EventKind, string) {
	switch {
	case errors.Is(err, os.ErrDeadlineExceeded):
		return KindReadTimeout, "read deadline exceeded"
	case errors.Is(err, syscall.ECONNRESET):
		return KindReset, "econnreset"
	case errors.Is(err, syscall.ECONNREFUSED):
		return KindReset, "econnrefused (icmp port unreachable)"
	case errors.Is(err, syscall.ENETUNREACH):
		return KindReset, "enetunreach"
	case errors.Is(err, syscall.EHOSTUNREACH):
		return KindReset, "ehostunreach"
	case errors.Is(err, net.ErrClosed):
		// The local side closed the socket; that is the harness shutting down,
		// not an observation about the peer.
		return KindObservationEnd, "local socket closed by drainwatch"
	}
	return KindReset, fmt.Sprintf("read error: %v", err)
}

// classifyWriteError does the same for the send path.
func classifyWriteError(err error) (EventKind, string) {
	switch {
	case errors.Is(err, syscall.EPIPE):
		return KindWriteError, "epipe"
	case errors.Is(err, syscall.ECONNRESET):
		return KindWriteError, "econnreset on write"
	case errors.Is(err, syscall.ECONNREFUSED):
		return KindWriteError, "econnrefused on write (icmp port unreachable)"
	case errors.Is(err, os.ErrDeadlineExceeded):
		return KindWriteError, "write deadline exceeded"
	case errors.Is(err, net.ErrClosed):
		return KindObservationEnd, "local socket closed by drainwatch"
	}
	return KindWriteError, fmt.Sprintf("write error: %v", err)
}

// relMs converts an absolute instant to milliseconds relative to t0.
func relMs(t, t0 time.Time) int64 { return t.Sub(t0).Milliseconds() }

// toReport renders the flow as a report record, relative to t0.
func (f *flow) toReport(t0 time.Time) report.Flow {
	res := Classify(f.proto, f.snapshot())
	rec := report.Flow{
		ID:      f.id,
		Proto:   f.proto,
		Outcome: res.Outcome,
		Detail:  res.Detail,
	}
	if res.Connected != nil {
		rec.ConnectedTMs = report.Ptr(relMs(*res.Connected, t0))
	}
	if res.Terminal != nil {
		rec.TTerminalMs = report.Ptr(relMs(*res.Terminal, t0))
	}
	if res.LastData != nil {
		rec.LastDataTMs = report.Ptr(relMs(*res.LastData, t0))
	}
	if res.LastSeq != nil {
		if f.proto == "tcp" {
			rec.LastHeartbeatSeq = report.Ptr(*res.LastSeq)
		} else {
			rec.LastAnsweredSeq = report.Ptr(*res.LastSeq)
		}
	}
	return rec
}
