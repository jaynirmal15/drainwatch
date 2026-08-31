package flowgen

import (
	"fmt"
	"time"

	"github.com/jaynirmal15/drainwatch/internal/report"
)

// EventKind enumerates everything a flow observer can witness. Terminal kinds
// are the ones that end a flow; the rest are progress observations.
type EventKind string

const (
	KindConnected      EventKind = "connected"
	KindHeartbeat      EventKind = "heartbeat"       // tcp: probe heartbeat received
	KindReply          EventKind = "reply"           // udp: ack received
	KindDrainAnnounced EventKind = "drain_announced" // tcp: "bye" line received
	KindGap            EventKind = "gap"             // non-terminal: expected data did not arrive on time

	// Terminal kinds.
	KindEOF            EventKind = "eof"          // clean FIN from the peer
	KindReset          EventKind = "reset"        // ECONNRESET on read
	KindReadTimeout    EventKind = "read_timeout" // no bytes for the whole flow timeout, no error
	KindWriteError     EventKind = "write_error"  // EPIPE, ECONNRESET or ECONNREFUSED on write
	KindSilence        EventKind = "silence"      // udp: N consecutive unanswered datagrams
	KindRehomed        EventKind = "rehomed"      // the answering probe instance changed
	KindObservationEnd EventKind = "observation_end"
)

// NoSeq marks an event that carries no sequence number.
const NoSeq int64 = -1

// Event is one observation about one flow. At is taken from the client
// process's monotonic clock, so differences between events on the same host are
// exact.
type Event struct {
	Kind   EventKind
	At     time.Time
	Seq    int64
	Detail string
}

// Result is the classification of a flow's event history.
type Result struct {
	Outcome   report.Outcome
	Connected *time.Time
	Terminal  *time.Time
	LastSeq   *int64
	LastData  *time.Time
	Detail    string
}

// terminalKinds is the closed set of kinds that end a flow.
func (k EventKind) terminal() bool {
	switch k {
	case KindEOF, KindReset, KindReadTimeout, KindWriteError, KindSilence, KindRehomed, KindObservationEnd:
		return true
	}
	return false
}

// Classify maps a flow's event history to exactly one report.Outcome.
//
// It is a pure function of the event slice: no clocks, no I/O, no randomness.
// The live TCP and UDP observers do nothing but append events; every judgement
// about what an outcome means happens here, which is what makes the outcome
// rules testable against synthetic sequences.
//
// The first terminal event wins. Events after it are ignored, because a flow
// cannot end twice and a later observation cannot revise an earlier terminal.
func Classify(proto string, evs []Event) Result {
	res := Result{Outcome: report.OutcomeNotMeasured}
	if len(evs) == 0 {
		res.Detail = "no observations were recorded for this flow"
		return res
	}

	drainAnnounced := false
	var lastSeq *int64

	for _, ev := range evs {
		at := ev.At

		switch ev.Kind {
		case KindConnected:
			if res.Connected == nil {
				t := at
				res.Connected = &t
			}
			continue

		case KindHeartbeat, KindReply:
			if ev.Seq != NoSeq {
				s := ev.Seq
				lastSeq = &s
			}
			t := at
			res.LastData = &t
			continue

		case KindDrainAnnounced:
			drainAnnounced = true
			t := at
			res.LastData = &t
			continue

		case KindGap:
			// Non-terminal by construction: a gap is evidence about quality of
			// service, not about the end of the flow.
			continue
		}

		if !ev.Kind.terminal() {
			// An unknown kind is a harness bug, not a measurement. Say so
			// rather than silently classifying it.
			res.Outcome = report.OutcomeNotMeasured
			res.Detail = fmt.Sprintf("unknown flow event kind %q", ev.Kind)
			res.LastSeq = lastSeq
			return res
		}

		t := at
		res.LastSeq = lastSeq

		switch ev.Kind {
		case KindEOF:
			res.Terminal = &t
			if drainAnnounced {
				res.Outcome = report.OutcomeDrainedCleanClose
				res.Detail = "fin after drain announcement"
			} else {
				// A FIN with no preceding "bye" means the server ended the flow
				// without completing it. That is not a drain.
				res.Outcome = report.OutcomeSevered
				res.Detail = "eof-without-drain-announcement"
			}
		case KindReset:
			res.Terminal = &t
			res.Outcome = report.OutcomeSevered
			res.Detail = orDefault(ev.Detail, "econnreset")
		case KindWriteError:
			res.Terminal = &t
			res.Outcome = report.OutcomeSevered
			res.Detail = orDefault(ev.Detail, "write-error")
		case KindSilence:
			res.Terminal = &t
			res.Outcome = report.OutcomeSevered
			res.Detail = orDefault(ev.Detail, "udp-silence")
		case KindRehomed:
			// A different probe process answered. The flow to the pod under
			// test ended here; that something else answered afterwards is not
			// survival, it is replacement.
			res.Terminal = &t
			res.Outcome = report.OutcomeSevered
			res.Detail = orDefault(ev.Detail, "rehomed onto a different probe instance")
		case KindReadTimeout:
			res.Terminal = &t
			res.Outcome = report.OutcomeReadTimeout
			res.Detail = orDefault(ev.Detail, "no bytes received within the flow timeout")
		case KindObservationEnd:
			// The flow outlived the observation window. There is no terminal
			// time, because nothing terminated: t_terminal_ms stays null.
			res.Terminal = nil
			res.Outcome = report.OutcomeSurvivedObservationWindow
			res.Detail = orDefault(ev.Detail, "flow was still alive when the observation window closed")
		}
		return res
	}

	// Observations exist but none of them ended the flow and the observation
	// window was never closed. That is a harness gap, not a drained flow.
	res.LastSeq = lastSeq
	res.Outcome = report.OutcomeNotMeasured
	res.Detail = "flow was still open and the observation window was never closed"
	return res
}

func orDefault(s, def string) string {
	if s == "" {
		return def
	}
	return s
}
