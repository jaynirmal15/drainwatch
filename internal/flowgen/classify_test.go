package flowgen

import (
	"testing"
	"time"

	"github.com/jaynirmal15/drainwatch/internal/report"
)

// base is a fixed instant. There is no clock and no RNG in these tests, because
// there is none in the classification path either.
var base = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

func at(ms int) time.Time { return base.Add(time.Duration(ms) * time.Millisecond) }

func ev(kind EventKind, ms int, seq int64, detail string) Event {
	return Event{Kind: kind, At: at(ms), Seq: seq, Detail: detail}
}

func TestClassifyOutcomes(t *testing.T) {
	tests := []struct {
		name        string
		proto       string
		events      []Event
		wantOutcome report.Outcome
		wantTermMs  int // -1 means "no terminal time"
		wantSeq     int64
		wantNoSeq   bool
		wantDetail  string
	}{
		{
			name:  "tcp drained: bye then fin",
			proto: "tcp",
			events: []Event{
				ev(KindConnected, 0, NoSeq, ""),
				ev(KindHeartbeat, 500, 0, ""),
				ev(KindHeartbeat, 1000, 1, ""),
				ev(KindDrainAnnounced, 1200, 1, ReasonDrainDeadline),
				ev(KindEOF, 1210, NoSeq, ""),
			},
			wantOutcome: report.OutcomeDrainedCleanClose,
			wantTermMs:  1210,
			wantSeq:     1,
			wantDetail:  "fin after drain announcement",
		},
		{
			name:  "tcp severed: fin with no drain announcement is not a drain",
			proto: "tcp",
			events: []Event{
				ev(KindConnected, 0, NoSeq, ""),
				ev(KindHeartbeat, 500, 0, ""),
				ev(KindEOF, 700, NoSeq, ""),
			},
			wantOutcome: report.OutcomeSevered,
			wantTermMs:  700,
			wantSeq:     0,
			wantDetail:  "eof-without-drain-announcement",
		},
		{
			name:  "tcp severed: rst",
			proto: "tcp",
			events: []Event{
				ev(KindConnected, 0, NoSeq, ""),
				ev(KindHeartbeat, 500, 0, ""),
				ev(KindHeartbeat, 1000, 1, ""),
				ev(KindReset, 1100, NoSeq, "econnreset"),
			},
			wantOutcome: report.OutcomeSevered,
			wantTermMs:  1100,
			wantSeq:     1,
			wantDetail:  "econnreset",
		},
		{
			name:  "tcp severed: write error",
			proto: "tcp",
			events: []Event{
				ev(KindConnected, 0, NoSeq, ""),
				ev(KindHeartbeat, 500, 0, ""),
				ev(KindWriteError, 900, NoSeq, "epipe"),
			},
			wantOutcome: report.OutcomeSevered,
			wantTermMs:  900,
			wantSeq:     0,
			wantDetail:  "epipe",
		},
		{
			name:  "tcp read-timeout: gaps do not terminate, the timeout does",
			proto: "tcp",
			events: []Event{
				ev(KindConnected, 0, NoSeq, ""),
				ev(KindHeartbeat, 500, 0, ""),
				ev(KindGap, 2000, NoSeq, "no heartbeat for 1.5s"),
				ev(KindReadTimeout, 10500, NoSeq, "no bytes for 10s (flow timeout)"),
			},
			wantOutcome: report.OutcomeReadTimeout,
			wantTermMs:  10500,
			wantSeq:     0,
		},
		{
			name:  "tcp survived: still alive when the window closed, no terminal time",
			proto: "tcp",
			events: []Event{
				ev(KindConnected, 0, NoSeq, ""),
				ev(KindHeartbeat, 500, 0, ""),
				ev(KindHeartbeat, 60000, 119, ""),
				ev(KindObservationEnd, 60500, NoSeq, ""),
			},
			wantOutcome: report.OutcomeSurvivedObservationWindow,
			wantTermMs:  -1,
			wantSeq:     119,
		},
		{
			name:  "tcp survived: a drain announcement without a fin is not a drain",
			proto: "tcp",
			events: []Event{
				ev(KindConnected, 0, NoSeq, ""),
				ev(KindHeartbeat, 500, 0, ""),
				ev(KindDrainAnnounced, 600, 0, ReasonDrainDeadline),
				ev(KindObservationEnd, 30000, NoSeq, ""),
			},
			wantOutcome: report.OutcomeSurvivedObservationWindow,
			wantTermMs:  -1,
			wantSeq:     0,
		},
		{
			name:  "udp severed: silence records the last answered sequence",
			proto: "udp",
			events: []Event{
				ev(KindConnected, 0, NoSeq, ""),
				ev(KindReply, 500, 0, ""),
				ev(KindReply, 1000, 1, ""),
				ev(KindReply, 9000, 17, ""),
				ev(KindSilence, 12500, NoSeq, "udp-silence-6-datagrams"),
			},
			wantOutcome: report.OutcomeSevered,
			wantTermMs:  12500,
			wantSeq:     17,
			wantDetail:  "udp-silence-6-datagrams",
		},
		{
			name:  "udp severed: icmp port unreachable on write",
			proto: "udp",
			events: []Event{
				ev(KindConnected, 0, NoSeq, ""),
				ev(KindReply, 500, 0, ""),
				ev(KindWriteError, 1500, NoSeq, "econnrefused on write (icmp port unreachable)"),
			},
			wantOutcome: report.OutcomeSevered,
			wantTermMs:  1500,
			wantSeq:     0,
			wantDetail:  "econnrefused on write (icmp port unreachable)",
		},
		{
			name:  "udp survived",
			proto: "udp",
			events: []Event{
				ev(KindConnected, 0, NoSeq, ""),
				ev(KindReply, 500, 0, ""),
				ev(KindObservationEnd, 30000, NoSeq, ""),
			},
			wantOutcome: report.OutcomeSurvivedObservationWindow,
			wantTermMs:  -1,
			wantSeq:     0,
		},
		{
			name:        "no observations at all is not-measured, never zero",
			proto:       "tcp",
			events:      nil,
			wantOutcome: report.OutcomeNotMeasured,
			wantTermMs:  -1,
			wantNoSeq:   true,
		},
		{
			name:  "connected but never terminated and the window never closed is not-measured",
			proto: "tcp",
			events: []Event{
				ev(KindConnected, 0, NoSeq, ""),
				ev(KindHeartbeat, 500, 0, ""),
			},
			wantOutcome: report.OutcomeNotMeasured,
			wantTermMs:  -1,
			wantSeq:     0,
		},
		{
			name:  "udp severed: re-homed onto a replacement pod is not survival",
			proto: "udp",
			events: []Event{
				ev(KindConnected, 0, NoSeq, ""),
				ev(KindReply, 500, 0, ""),
				ev(KindReply, 25000, 49, ""),
				ev(KindRehomed, 25500, 50, "answered by probe instance probe-9q5nt, was probe-pq4m5"),
				ev(KindReply, 26000, 51, ""),
			},
			wantOutcome: report.OutcomeSevered,
			wantTermMs:  25500,
			wantSeq:     49,
			wantDetail:  "answered by probe instance probe-9q5nt, was probe-pq4m5",
		},
		{
			name:  "the first terminal wins: a later reset cannot rewrite a drain",
			proto: "tcp",
			events: []Event{
				ev(KindConnected, 0, NoSeq, ""),
				ev(KindHeartbeat, 500, 0, ""),
				ev(KindDrainAnnounced, 600, 0, ReasonDrainComplete),
				ev(KindEOF, 610, NoSeq, ""),
				ev(KindReset, 700, NoSeq, "econnreset"),
			},
			wantOutcome: report.OutcomeDrainedCleanClose,
			wantTermMs:  610,
			wantSeq:     0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := Classify(tc.proto, tc.events)

			if got.Outcome != tc.wantOutcome {
				t.Errorf("outcome = %q, want %q", got.Outcome, tc.wantOutcome)
			}
			if !got.Outcome.Valid() {
				t.Errorf("outcome %q is not a member of the closed enum", got.Outcome)
			}

			if tc.wantTermMs < 0 {
				if got.Terminal != nil {
					t.Errorf("terminal = %v, want nil (an unobserved terminal must stay null, not become zero)", got.Terminal)
				}
			} else {
				if got.Terminal == nil {
					t.Fatalf("terminal = nil, want %dms after base", tc.wantTermMs)
				}
				if diff := got.Terminal.Sub(base).Milliseconds(); diff != int64(tc.wantTermMs) {
					t.Errorf("terminal = base+%dms, want base+%dms", diff, tc.wantTermMs)
				}
			}

			if tc.wantNoSeq {
				if got.LastSeq != nil {
					t.Errorf("lastSeq = %d, want nil", *got.LastSeq)
				}
			} else {
				if got.LastSeq == nil {
					t.Fatalf("lastSeq = nil, want %d", tc.wantSeq)
				}
				if *got.LastSeq != tc.wantSeq {
					t.Errorf("lastSeq = %d, want %d", *got.LastSeq, tc.wantSeq)
				}
			}

			if tc.wantDetail != "" && got.Detail != tc.wantDetail {
				t.Errorf("detail = %q, want %q", got.Detail, tc.wantDetail)
			}
		})
	}
}

// TestClassifyIsDeterministic asserts the property the design rests on: the
// same event sequence always yields the same record.
func TestClassifyIsDeterministic(t *testing.T) {
	events := []Event{
		ev(KindConnected, 0, NoSeq, ""),
		ev(KindHeartbeat, 500, 0, ""),
		ev(KindDrainAnnounced, 900, 0, ReasonDrainDeadline),
		ev(KindEOF, 910, NoSeq, ""),
	}
	first := Classify("tcp", events)
	for i := 0; i < 50; i++ {
		got := Classify("tcp", events)
		if got.Outcome != first.Outcome || !got.Terminal.Equal(*first.Terminal) || *got.LastSeq != *first.LastSeq {
			t.Fatalf("classification changed on iteration %d: %+v vs %+v", i, got, first)
		}
	}
}

// TestClassifyRejectsUnknownKind: an event kind the classifier does not know is
// a harness bug, and must surface as not-measured rather than being silently
// treated as progress.
func TestClassifyRejectsUnknownKind(t *testing.T) {
	got := Classify("tcp", []Event{
		ev(KindConnected, 0, NoSeq, ""),
		ev(EventKind("teleported"), 100, NoSeq, ""),
	})
	if got.Outcome != report.OutcomeNotMeasured {
		t.Fatalf("outcome = %q, want %q", got.Outcome, report.OutcomeNotMeasured)
	}
	if got.Detail == "" {
		t.Error("an unknown event kind must be explained in the detail field")
	}
}
