package orchestrate

import (
	"errors"
	"fmt"
	"time"

	"github.com/jaynirmal15/drainwatch/internal/flowgen"
	"github.com/jaynirmal15/drainwatch/internal/probe"
)

// ErrAttachNotImplemented is returned for --workload attach. main maps it to
// exit code 2.
var ErrAttachNotImplemented = errors.New("--workload attach: not implemented in v0.1")

// Workload modes.
const (
	WorkloadDeploy = "deploy"
	WorkloadAttach = "attach"
)

// Trigger modes.
const (
	TriggerDelete = "delete"
	TriggerEvict  = "evict"
	TriggerScale  = "scale"
)

// Options is the resolved configuration for `drainwatch run`.
type Options struct {
	Kubeconfig string
	Context    string

	Namespace    string
	ManifestPath string
	Image        string
	Workload     string

	Trigger         string
	DrainBehavior   string
	GracePeriod     int
	DrainMaxSeconds int
	ExitNowForceRST bool

	TCPFlows   int
	UDPFlows   int
	TargetHost string
	TCPPort    int
	UDPPort    int

	SettleSeconds           int
	ObserveTimeoutSeconds   int
	ReadyTimeoutSeconds     int
	PostTerminalGraceSecond int
	FlowTimeout             time.Duration
	Interval                time.Duration
	UDPSilenceDatagrams     int

	Repeat int
	OutDir string
}

// Normalize applies the documented defaults for values left at zero and then
// validates the result. Every derived default is written back into Options so
// that the config block of a report states what actually ran, not what was
// typed.
func (o *Options) Normalize() error {
	if o.Workload == "" {
		o.Workload = WorkloadDeploy
	}
	switch o.Workload {
	case WorkloadDeploy:
	case WorkloadAttach:
		return ErrAttachNotImplemented
	default:
		return fmt.Errorf("--workload %q is not recognised (invariant: --workload must be %q or %q)", o.Workload, WorkloadDeploy, WorkloadAttach)
	}

	switch o.Trigger {
	case TriggerDelete, TriggerEvict, TriggerScale:
	default:
		return fmt.Errorf("--trigger %q is not recognised (invariant: --trigger must be one of %q, %q, %q)", o.Trigger, TriggerDelete, TriggerEvict, TriggerScale)
	}

	switch o.DrainBehavior {
	case probe.BehaviorDrain, probe.BehaviorExitNow, probe.BehaviorIgnore:
	default:
		return fmt.Errorf("--drain-behavior %q is not recognised (invariant: must be one of %q, %q, %q)", o.DrainBehavior, probe.BehaviorDrain, probe.BehaviorExitNow, probe.BehaviorIgnore)
	}

	if o.GracePeriod <= 0 {
		return fmt.Errorf("--grace-period must be positive, got %d (invariant: terminationGracePeriodSeconds must leave room for a drain to be observable)", o.GracePeriod)
	}
	if o.Repeat <= 0 {
		return fmt.Errorf("--repeat must be at least 1, got %d", o.Repeat)
	}
	if o.SettleSeconds < 0 {
		return fmt.Errorf("--settle-seconds must not be negative, got %d", o.SettleSeconds)
	}

	if o.ObserveTimeoutSeconds == 0 {
		// Documented default: grace period plus 30 seconds, which is long
		// enough for the SIGKILL boundary plus the tail of endpoint removal.
		o.ObserveTimeoutSeconds = o.GracePeriod + 30
	}
	if o.ObserveTimeoutSeconds <= 0 {
		return fmt.Errorf("--observe-timeout must be positive, got %d", o.ObserveTimeoutSeconds)
	}
	if o.DrainMaxSeconds == 0 {
		// Auto: finish the drain inside the grace period, so that the drain arm
		// measures a drain rather than a SIGKILL. Five seconds of headroom.
		o.DrainMaxSeconds = o.GracePeriod - 5
		if o.DrainMaxSeconds < 1 {
			o.DrainMaxSeconds = 1
		}
	}
	if o.DrainMaxSeconds < 0 {
		return fmt.Errorf("--drain-max-seconds must not be negative, got %d", o.DrainMaxSeconds)
	}
	if o.ReadyTimeoutSeconds <= 0 {
		o.ReadyTimeoutSeconds = 120
	}
	if o.PostTerminalGraceSecond <= 0 {
		o.PostTerminalGraceSecond = 5
	}
	if o.Interval <= 0 {
		o.Interval = flowgen.DefaultInterval
	}
	if o.FlowTimeout <= 0 {
		o.FlowTimeout = flowgen.DefaultFlowTimeout
	}
	if o.UDPSilenceDatagrams <= 0 {
		o.UDPSilenceDatagrams = flowgen.DefaultUDPSilenceDatagrams
	}
	if o.Namespace == "" {
		return fmt.Errorf("--namespace must not be empty")
	}
	if o.OutDir == "" {
		return fmt.Errorf("--out must not be empty")
	}
	if o.ManifestPath == "" {
		return fmt.Errorf("--manifest must not be empty")
	}

	if o.DrainBehavior == probe.BehaviorDrain && o.DrainMaxSeconds >= o.GracePeriod {
		return fmt.Errorf("--drain-max-seconds (%d) is not shorter than --grace-period (%d) (invariant: a drain trial must be able to finish draining before SIGKILL, otherwise the arm measures a kill and not a drain; lower --drain-max-seconds or raise --grace-period)", o.DrainMaxSeconds, o.GracePeriod)
	}
	return nil
}

// flowConfig maps run options onto the flow generator.
func (o *Options) flowConfig() flowgen.Config {
	return flowgen.Config{
		Host:                o.TargetHost,
		TCPPort:             o.TCPPort,
		UDPPort:             o.UDPPort,
		TCPFlows:            o.TCPFlows,
		UDPFlows:            o.UDPFlows,
		Interval:            o.Interval,
		GapThreshold:        flowgen.DefaultGapThreshold,
		FlowTimeout:         o.FlowTimeout,
		UDPSilenceDatagrams: o.UDPSilenceDatagrams,
		DialTimeout:         flowgen.DefaultDialTimeout,
		DialRetryWindow:     flowgen.DefaultDialRetryWindow,
	}
}
