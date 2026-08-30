package orchestrate

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/jaynirmal15/drainwatch/internal/report"
)

// probeEventsInTimeline is the allowlist of probe events that reach the report
// timeline. Per-heartbeat lines are excluded because the per-flow record on the
// client side already carries the sequence numbers, and a timeline with 800
// heartbeat entries is not a timeline.
var probeEventsInTimeline = map[string]bool{
	report.EventSigtermReceived: true,
	"probe_started":             true,
	"listening":                 true,
	"drain_started":             true,
	"drain_complete":            true,
	"drain_deadline_reached":    true,
	"stopped_accepting":         true,
	"readyz_now_503":            true,
	"sigterm_ignored":           true,
	"tcp_flow_reset_forced":     true,
	"tcp_flow_open_at_exit":     true,
	"tcp_flow_closed":           true,
	"exiting":                   true,
	"fatal":                     true,
}

// ProbeLog collects the probe's structured stdout.
type ProbeLog struct {
	mu       sync.Mutex
	events   []report.ProbeEvent
	warnings []string
	// unparsed counts stdout lines that were not drainwatch events. A non-zero
	// count is reported rather than ignored, because it means the log stream
	// contained something the harness did not understand.
	unparsed int
	closed   bool
}

// NewProbeLog returns an empty collector.
func NewProbeLog() *ProbeLog { return &ProbeLog{} }

func (p *ProbeLog) add(e report.ProbeEvent) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.events = append(p.events, e)
}

func (p *ProbeLog) warn(format string, args ...any) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.warnings = append(p.warnings, fmt.Sprintf(format, args...))
}

// Warnings returns everything that went wrong while collecting probe output.
func (p *ProbeLog) Warnings() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := append([]string(nil), p.warnings...)
	if p.unparsed > 0 {
		out = append(out, fmt.Sprintf("%d probe stdout line(s) were not drainwatch JSON events and were ignored", p.unparsed))
	}
	return out
}

// Saw reports whether an event with this name arrived from the probe.
func (p *ProbeLog) Saw(event string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, e := range p.events {
		if e.Event == event {
			return true
		}
	}
	return false
}

// Events returns a copy of everything collected.
func (p *ProbeLog) Events() []report.ProbeEvent {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]report.ProbeEvent(nil), p.events...)
}

// Stream follows the probe container's stdout until the stream ends, the
// container exits, or ctx is cancelled. It is started before the trigger so
// that the SIGTERM line cannot be missed: once the pod object is gone, its logs
// are gone with it.
func (p *ProbeLog) Stream(ctx context.Context, cs kubernetes.Interface, namespace, podName string, since time.Time) {
	opts := &corev1.PodLogOptions{
		Container: ProbeContainerName,
		Follow:    true,
	}
	if !since.IsZero() {
		p.warn("log stream opened with a since-time of %s", since.Format(time.RFC3339))
	}
	rc, err := cs.CoreV1().Pods(namespace).GetLogs(podName, opts).Stream(ctx)
	if err != nil {
		p.warn("could not open the probe log stream for %s/%s: %v; probe-sourced timeline events will be not-measured unless the --previous fallback succeeds", namespace, podName, err)
		return
	}
	defer rc.Close()
	p.consume(rc)
}

// StreamPrevious is the fallback: fetch the terminated container's log in one
// shot. It only works while the pod object still exists, which is why it is a
// fallback and not the primary path.
func (p *ProbeLog) StreamPrevious(ctx context.Context, cs kubernetes.Interface, namespace, podName string) {
	opts := &corev1.PodLogOptions{Container: ProbeContainerName, Previous: true}
	rc, err := cs.CoreV1().Pods(namespace).GetLogs(podName, opts).Stream(ctx)
	if err != nil {
		p.warn("the --previous log fallback for %s/%s also failed: %v", namespace, podName, err)
		return
	}
	defer rc.Close()
	p.consume(rc)
}

func (p *ProbeLog) consume(r io.Reader) {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		var ev report.ProbeEvent
		if err := json.Unmarshal([]byte(line), &ev); err != nil || ev.Event == "" {
			p.mu.Lock()
			p.unparsed++
			p.mu.Unlock()
			continue
		}
		p.add(ev)
	}
	if err := sc.Err(); err != nil {
		p.warn("probe log stream ended with an error: %v", err)
	}
}

// TimelineEvents converts collected probe events into timeline entries.
//
// Probe timestamps live on a different host's wall clock. They are mapped onto
// the orchestrator's timebase by subtracting the orchestrator's wall clock at
// the trigger, and every resulting entry is marked Approximate so that no
// reader mistakes it for a monotonic measurement.
func (p *ProbeLog) TimelineEvents(triggerWall time.Time) ([]report.Event, []string) {
	events := p.Events()
	var out []report.Event
	var warnings []string
	for _, e := range events {
		if !probeEventsInTimeline[e.Event] {
			continue
		}
		wall, err := e.WallTime()
		if err != nil {
			warnings = append(warnings, err.Error())
			continue
		}
		detail := e.Detail
		if e.Errno != "" {
			detail = strings.TrimSpace(detail + " errno=" + e.Errno)
		}
		out = append(out, report.Event{
			TMs:         wall.Sub(triggerWall).Milliseconds(),
			Source:      report.SourceProbe,
			Event:       e.Event,
			FlowID:      e.FlowID,
			Detail:      detail,
			Approximate: true,
		})
	}
	return out, warnings
}
