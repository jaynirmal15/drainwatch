package probe

import (
	"encoding/json"
	"io"
	"sync"
	"time"

	"github.com/jaynirmal15/drainwatch/internal/report"
)

// Logger writes structured probe events, one JSON object per line, to stdout.
//
// Every line carries both a wall clock (for cross-host alignment with the
// orchestrator, which is approximate) and a monotonic offset from probe start
// (for exact intra-probe deltas). The orchestrator reads these lines out of the
// pod log stream; they are the only way SIGTERM delivery time enters the
// timeline.
type Logger struct {
	mu    sync.Mutex
	w     io.Writer
	enc   *json.Encoder
	start time.Time
}

// NewLogger returns a Logger whose monotonic offsets are measured from start.
func NewLogger(w io.Writer, start time.Time) *Logger {
	enc := json.NewEncoder(w)
	return &Logger{w: w, enc: enc, start: start}
}

// Event emits one line. It never returns an error: a probe that cannot write
// its log has already lost the measurement, and the orchestrator will report
// the missing events as not-measured rather than the probe crashing mid-trial.
func (l *Logger) Event(e report.ProbeEvent) {
	now := time.Now()
	e.Wall = now.Format(time.RFC3339Nano)
	e.MonoMs = now.Sub(l.start).Milliseconds()
	l.mu.Lock()
	defer l.mu.Unlock()
	_ = l.enc.Encode(e)
	if f, ok := l.w.(interface{ Sync() error }); ok {
		// Flush to the container runtime immediately. Buffered SIGTERM
		// timestamps that arrive after the process exits are worthless.
		_ = f.Sync()
	}
}

// Simple emits an event with only a name and optional detail.
func (l *Logger) Simple(event, detail string) {
	l.Event(report.ProbeEvent{Event: event, Detail: detail})
}

// Flow emits an event attached to a server-side flow.
func (l *Logger) Flow(event, flowID string, seq *int64, detail string) {
	l.Event(report.ProbeEvent{Event: event, FlowID: flowID, Seq: seq, Detail: detail})
}
