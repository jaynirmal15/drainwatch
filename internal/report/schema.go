// Package report defines the drainwatch trial record: the on-disk JSON schema,
// the closed outcome enum, and the plain-text table renderer.
//
// Two rules govern everything in this package:
//
//  1. The absence of an observation is "not measured", never zero. Any quantity
//     that may be unobservable is a pointer and serialises as JSON null.
//  2. Cross-source clock alignment is stated, not assumed. Every trial carries a
//     clock_note describing the precision of its own t_ms values.
package report

import (
	"fmt"
	"time"
)

// Version and GitCommit are overwritten at build time via -ldflags -X.
// See the Makefile. They are deliberately not "0.0.0"/"" so that a binary built
// without ldflags is identifiable in a report rather than silently anonymous.
var (
	Version   = "0.1.0"
	GitCommit = "unknown"
)

// Source identifies which observer produced a timeline event. The precision of
// an event's t_ms depends on its source; see Trial.ClockNote.
const (
	SourceOrchestrator = "orchestrator"
	SourceProbe        = "probe"
	SourceK8s          = "k8s"
	SourceClient       = "client"
)

// Timeline event names emitted by the orchestrator, the k8s watches and the
// probe. Probe event names arrive over stdout and are copied through verbatim;
// the constants here are the ones drainwatch itself reasons about.
const (
	EventTriggerIssued             = "trigger_issued"
	EventSigtermReceived           = "sigterm_received"
	EventEndpointSliceReadyFalse   = "endpointslice_ready_false"
	EventEndpointSliceEndpointGone = "endpointslice_endpoint_removed"
	EventPodDeletionTimestampSet   = "pod_deletion_timestamp_set"
	EventContainerTerminated       = "container_terminated"
	EventPodObjectGone             = "pod_object_deleted"
	EventFlowTerminal              = "flow_terminal"
	EventObservationWindowClosed   = "observation_window_closed"
	EventPreflightPassed           = "preflight_passed"
	EventSettleComplete            = "settle_complete"
	EventProbeLogStreamOpened      = "probe_log_stream_opened"
	EventProbeLogStreamClosed      = "probe_log_stream_closed"
)

// Outcome is a closed enum. Every flow in every report carries exactly one of
// these values; there is no "other" and no empty string.
type Outcome string

const (
	// OutcomeDrainedCleanClose: the server announced the close before sending
	// FIN, and the client observed the announcement followed by a clean EOF.
	// This is the only outcome that licenses the word "drained".
	OutcomeDrainedCleanClose Outcome = "drained-clean-close"

	// OutcomeSevered: the flow was terminated without the server completing it.
	// Concretely, one of: TCP RST (ECONNRESET on read or write), an unannounced
	// FIN arriving mid-heartbeat, a write error such as EPIPE, ICMP port
	// unreachable surfacing as ECONNREFUSED, or UDP silence for the configured
	// number of consecutive unanswered datagrams. The mechanism is recorded in
	// the flow's detail field; this enum value does not distinguish them.
	OutcomeSevered Outcome = "severed"

	// OutcomeReadTimeout: no bytes arrived within the flow timeout, but the
	// socket produced no error either. The flow is neither drained nor provably
	// severed; it simply went quiet.
	OutcomeReadTimeout Outcome = "read-timeout"

	// OutcomeSurvivedObservationWindow: the flow was still alive when the
	// observation window closed. It is NOT a drained flow and must never be
	// counted as one; it is a flow whose fate drainwatch did not observe.
	OutcomeSurvivedObservationWindow Outcome = "survived-observation-window"

	// OutcomeNotMeasured: no usable observation exists for this flow.
	OutcomeNotMeasured Outcome = "not-measured"
)

// Outcomes returns the enum in a stable order, for rendering and validation.
func Outcomes() []Outcome {
	return []Outcome{
		OutcomeDrainedCleanClose,
		OutcomeSevered,
		OutcomeReadTimeout,
		OutcomeSurvivedObservationWindow,
		OutcomeNotMeasured,
	}
}

// Valid reports whether o is a member of the closed enum.
func (o Outcome) Valid() bool {
	for _, k := range Outcomes() {
		if k == o {
			return true
		}
	}
	return false
}

// Report is the top-level content of report.json: one trial, plus the build and
// environment context needed to interpret it.
type Report struct {
	DrainwatchVersion string      `json:"drainwatch_version"`
	GitCommit         string      `json:"git_commit"`
	Environment       Environment `json:"environment"`
	Trial             Trial       `json:"trial"`
}

// Environment is captured before anything else happens in a run. A run that
// cannot capture its environment refuses to start; fields that are genuinely
// unreadable are recorded as "unknown" with an accompanying warning, never
// guessed.
type Environment struct {
	KubernetesVersion string     `json:"kubernetes_version"`
	NodeCount         int        `json:"node_count"`
	Nodes             []NodeInfo `json:"nodes"`
	KubeProxyMode     string     `json:"kube_proxy_mode"`
	CNI               string     `json:"cni"`
	DrainwatchVersion string     `json:"drainwatch_version"`
	GitCommit         string     `json:"git_commit"`
	OS                string     `json:"os"`
	Arch              string     `json:"arch"`
	WallClockStart    string     `json:"wall_clock_start"`
	// Warnings records every field that could not be determined and why. An
	// empty slice means the environment was fully observed.
	Warnings []string `json:"warnings"`
}

// NodeInfo is the per-node slice of the environment record.
type NodeInfo struct {
	Name             string `json:"name"`
	ContainerRuntime string `json:"container_runtime"`
	KubeletVersion   string `json:"kubelet_version"`
	OSImage          string `json:"os_image"`
	Architecture     string `json:"architecture"`
}

// Config is the exact configuration the trial ran with, as resolved (defaults
// already applied), so a report is interpretable without the command line.
type Config struct {
	TCPFlows              int     `json:"tcp_flows"`
	UDPFlows              int     `json:"udp_flows"`
	GracePeriodSeconds    int     `json:"grace_period_seconds"`
	DrainBehavior         string  `json:"drain_behavior"`
	DrainMaxSeconds       int     `json:"drain_max_seconds"`
	Trigger               string  `json:"trigger"`
	Workload              string  `json:"workload"`
	Namespace             string  `json:"namespace"`
	TargetHost            string  `json:"target_host"`
	TCPPort               int     `json:"tcp_port"`
	UDPPort               int     `json:"udp_port"`
	SettleSeconds         int     `json:"settle_seconds"`
	ObserveTimeoutSeconds int     `json:"observe_timeout_seconds"`
	FlowTimeoutSeconds    float64 `json:"flow_timeout_seconds"`
	UDPSilenceDatagrams   int     `json:"udp_silence_datagrams"`
}

// Trial is one measured termination.
type Trial struct {
	ID       string  `json:"id"`
	Config   Config  `json:"config"`
	Timeline []Event `json:"timeline"`
	Flows    []Flow  `json:"flows"`
	Summary  Summary `json:"summary"`
	// ClockNote states, in prose, how precise the t_ms values in this trial are
	// per source. It exists so the report never implies sub-millisecond
	// cross-host accuracy it does not have.
	ClockNote string `json:"clock_note"`
	// ProbeClockOffsetNote records how probe-sourced timestamps were mapped onto
	// the orchestrator's timebase.
	ProbeClockOffsetNote string `json:"probe_clock_offset_note"`
}

// Event is one timeline entry. TMs is milliseconds relative to trigger_issued
// and may be negative for events observed before the trigger.
type Event struct {
	TMs    int64  `json:"t_ms"`
	Source string `json:"source"`
	Event  string `json:"event"`
	FlowID string `json:"flow_id,omitempty"`
	Detail string `json:"detail,omitempty"`
	// Approximate is set on events whose t_ms was derived by differencing wall
	// clocks across hosts (probe events), rather than from a single monotonic
	// clock.
	Approximate bool `json:"approximate,omitempty"`
}

// Flow is the per-flow outcome record. Every pointer field is null when the
// corresponding observation does not exist.
type Flow struct {
	ID    string `json:"id"`
	Proto string `json:"proto"`
	// ConnectedTMs is milliseconds relative to trigger_issued; normally
	// negative, because flows are established during preflight.
	ConnectedTMs *int64  `json:"connected_t_ms"`
	Outcome      Outcome `json:"outcome"`
	// TTerminalMs is null when no terminal event was observed, including for
	// survived-observation-window.
	TTerminalMs      *int64 `json:"t_terminal_ms"`
	LastHeartbeatSeq *int64 `json:"last_heartbeat_seq,omitempty"`
	LastAnsweredSeq  *int64 `json:"last_answered_seq,omitempty"`
	// LastDataTMs is when the last byte arrived on this flow, relative to
	// trigger_issued.
	LastDataTMs *int64 `json:"last_data_t_ms"`
	// Detail names the concrete mechanism behind Outcome (e.g. "econnreset",
	// "eof-without-drain-announcement", "udp-silence-6-datagrams").
	Detail string `json:"detail,omitempty"`
}

// ProtoSummary counts flows of one protocol by outcome. All five enum members
// are always present so that a zero is a measured zero.
type ProtoSummary struct {
	Total          int `json:"total"`
	Drained        int `json:"drained"`
	Severed        int `json:"severed"`
	ReadTimeout    int `json:"read_timeout"`
	SurvivedWindow int `json:"survived_window"`
	NotMeasured    int `json:"not_measured"`
}

// Summary is derived wholly from Timeline and Flows. Every derived duration is
// a pointer: if either endpoint was not observed, the answer is null, not zero.
type Summary struct {
	TCP                          ProtoSummary `json:"tcp"`
	UDP                          ProtoSummary `json:"udp"`
	SigtermToReadyFalseMs        *int64       `json:"sigterm_to_ready_false_ms"`
	SigtermToLastFlowTerminalMs  *int64       `json:"sigterm_to_last_flow_terminal_ms"`
	TriggerToSigtermMs           *int64       `json:"trigger_to_sigterm_ms"`
	TriggerToEndpointRemovedMs   *int64       `json:"trigger_to_endpoint_removed_ms"`
	TriggerToContainerTerminated *int64       `json:"trigger_to_container_terminated_ms"`
	// Notes carries anything that qualifies the numbers above.
	Notes []string `json:"notes"`
}

// ProbeEvent is one structured stdout line from the in-cluster probe. It is
// defined here because both the probe (writer) and the orchestrator (reader)
// depend on this shape.
type ProbeEvent struct {
	// Wall is the probe's wall clock, RFC3339Nano. Used only for cross-host
	// alignment, which is approximate by construction.
	Wall string `json:"wall"`
	// MonoMs is milliseconds since probe start on the probe's monotonic clock.
	// Deltas between two probe events are exact; deltas against another host
	// are not.
	MonoMs        int64  `json:"mono_ms"`
	Event         string `json:"event"`
	FlowID        string `json:"flow_id,omitempty"`
	Seq           *int64 `json:"seq,omitempty"`
	Detail        string `json:"detail,omitempty"`
	Errno         string `json:"errno,omitempty"`
	DrainBehavior string `json:"drain_behavior,omitempty"`
}

// WallTime parses the probe's wall-clock stamp.
func (p ProbeEvent) WallTime() (time.Time, error) {
	t, err := time.Parse(time.RFC3339Nano, p.Wall)
	if err != nil {
		return time.Time{}, fmt.Errorf("probe event %q has unparseable wall clock %q: %w", p.Event, p.Wall, err)
	}
	return t, nil
}

// TrialSummary is one line of summary.json, produced when a run executes one or
// more trials into a single output directory.
type TrialSummary struct {
	TrialID                     string       `json:"trial_id"`
	ReportPath                  string       `json:"report_path"`
	DrainBehavior               string       `json:"drain_behavior"`
	Trigger                     string       `json:"trigger"`
	TCP                         ProtoSummary `json:"tcp"`
	UDP                         ProtoSummary `json:"udp"`
	TriggerToSigtermMs          *int64       `json:"trigger_to_sigterm_ms"`
	SigtermToReadyFalseMs       *int64       `json:"sigterm_to_ready_false_ms"`
	SigtermToLastFlowTerminalMs *int64       `json:"sigterm_to_last_flow_terminal_ms"`
}

// RunSummary is the content of summary.json.
type RunSummary struct {
	DrainwatchVersion string         `json:"drainwatch_version"`
	GitCommit         string         `json:"git_commit"`
	Environment       Environment    `json:"environment"`
	Trials            []TrialSummary `json:"trials"`
}

// Ptr is a helper for building the pointer-valued fields above.
func Ptr[T any](v T) *T { return &v }
