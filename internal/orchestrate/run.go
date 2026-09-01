package orchestrate

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"sync"
	"time"

	autoscalingv1 "k8s.io/api/autoscaling/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/jaynirmal15/drainwatch/internal/envrecord"
	"github.com/jaynirmal15/drainwatch/internal/flowgen"
	"github.com/jaynirmal15/drainwatch/internal/preflight"
	"github.com/jaynirmal15/drainwatch/internal/report"
)

// Orchestrator-sourced timeline event names beyond those in the report package.
const (
	eventTriggerAPIReturned = "trigger_api_returned"
	eventAllFlowsTerminal   = "all_flows_terminal"
	eventLogStreamEnded     = "probe_log_stream_ended"
)

// Run executes the whole lifecycle: capture the environment, then run
// o.Repeat trials into o.OutDir, writing one report per trial plus a summary.
func Run(ctx context.Context, out io.Writer, o Options) error {
	if err := o.Normalize(); err != nil {
		return err
	}

	cs, _, err := NewClient(o.Kubeconfig, o.Context)
	if err != nil {
		return err
	}

	// The environment record comes before anything else. A run that cannot say
	// what it ran against is not allowed to produce numbers.
	env, err := envrecord.Capture(ctx, cs, time.Now())
	if err != nil {
		return err
	}
	envrecord.PrintWarnings(env)

	manifest, err := loadManifest(o.ManifestPath)
	if err != nil {
		return err
	}

	summary := &report.RunSummary{
		DrainwatchVersion: report.Version,
		GitCommit:         report.GitCommit,
		Environment:       env,
	}

	for i := 1; i <= o.Repeat; i++ {
		trialID := fmt.Sprintf("trial-%03d", i)
		fmt.Fprintf(out, "\n=== %s (%d of %d): behavior=%s trigger=%s grace=%ds ===\n",
			trialID, i, o.Repeat, o.DrainBehavior, o.Trigger, o.GracePeriod)

		rep, err := runTrial(ctx, out, cs, manifest, env, o, trialID)
		if err != nil {
			return fmt.Errorf("%s aborted: %w", trialID, err)
		}

		trialDir := filepath.Join(o.OutDir, trialID)
		reportPath := filepath.Join(trialDir, "report.json")
		if err := report.WriteJSON(reportPath, rep); err != nil {
			return err
		}
		if o.Repeat == 1 {
			// Convenience for the single-trial case documented in the README.
			if err := report.WriteJSON(filepath.Join(o.OutDir, "report.json"), rep); err != nil {
				return err
			}
		}

		fmt.Fprintln(out)
		report.RenderTable(out, rep)
		fmt.Fprintf(out, "\nreport written to %s\n", reportPath)

		summary.Trials = append(summary.Trials, report.TrialSummary{
			TrialID:                     rep.Trial.ID,
			ReportPath:                  reportPath,
			DrainBehavior:               rep.Trial.Config.DrainBehavior,
			Trigger:                     rep.Trial.Config.Trigger,
			TCP:                         rep.Trial.Summary.TCP,
			UDP:                         rep.Trial.Summary.UDP,
			TriggerToSigtermMs:          rep.Trial.Summary.TriggerToSigtermMs,
			SigtermToReadyFalseMs:       rep.Trial.Summary.SigtermToReadyFalseMs,
			SigtermToLastFlowTerminalMs: rep.Trial.Summary.SigtermToLastFlowTerminalMs,
		})
	}

	summaryPath := filepath.Join(o.OutDir, "summary.json")
	if err := report.WriteJSON(summaryPath, summary); err != nil {
		return err
	}
	if o.Repeat > 1 {
		fmt.Fprintln(out)
		report.RenderRunSummary(out, summary)
	}
	fmt.Fprintf(out, "summary written to %s\n", summaryPath)
	return nil
}

// trialState carries the mutable pieces of one trial.
type trialState struct {
	mu     sync.Mutex
	events []kEvent
}

func (t *trialState) record(event, detail string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.events = append(t.events, kEvent{At: time.Now(), Event: event, Detail: detail})
}

func (t *trialState) recordAt(at time.Time, event, detail string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.events = append(t.events, kEvent{At: at, Event: event, Detail: detail})
}

func (t *trialState) timeline(t0 time.Time) []report.Event {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]report.Event, 0, len(t.events))
	for _, e := range t.events {
		out = append(out, report.Event{
			TMs:    e.At.Sub(t0).Milliseconds(),
			Source: report.SourceOrchestrator,
			Event:  e.Event,
			Detail: e.Detail,
		})
	}
	return out
}

// runTrial performs one measured termination end to end.
func runTrial(ctx context.Context, out io.Writer, cs kubernetes.Interface, manifest *manifestObjects, env report.Environment, o Options, trialID string) (*report.Report, error) {
	state := &trialState{}

	dep, err := applyManifest(ctx, cs, manifest, deployOptions{
		Namespace:       o.Namespace,
		Image:           o.Image,
		GracePeriod:     o.GracePeriod,
		DrainBehavior:   o.DrainBehavior,
		DrainMaxSeconds: o.DrainMaxSeconds,
		ExitNowForceRST: o.ExitNowForceRST,
	})
	if err != nil {
		return nil, err
	}
	state.record("workload_applied", fmt.Sprintf("deployment=%s/%s image=%s grace=%ds", dep.Namespace, dep.Name, dep.Spec.Template.Spec.Containers[0].Image, o.GracePeriod))

	watcher := NewWatcher()
	if err := watcher.Start(ctx, cs, o.Namespace, manifest.Service.Name); err != nil {
		return nil, err
	}
	defer watcher.Stop()

	gen, err := flowgen.New(o.flowConfig())
	if err != nil {
		return nil, err
	}
	defer gen.CloseObservation(time.Now())

	if err := runPreflight(ctx, out, watcher, gen, o); err != nil {
		return nil, err
	}
	state.record(report.EventPreflightPassed, fmt.Sprintf("%d tcp flows and %d udp flows established; pod and EndpointSlice watches delivering", o.TCPFlows, o.UDPFlows))

	podName := watcher.PodName()
	if podName == "" {
		return nil, fmt.Errorf("the pod watch delivered no pod name after preflight passed (invariant: the probe pod must be identifiable before the trigger)")
	}

	// The probe log stream is opened before the trigger, not at it: once the
	// pod object is deleted its logs are unreachable, so a stream opened after
	// the trigger can lose the SIGTERM line entirely.
	probeLog := NewProbeLog()
	logDone := make(chan struct{})
	logCtx, cancelLog := context.WithCancel(ctx)
	defer cancelLog()
	go func() {
		defer close(logDone)
		probeLog.Stream(logCtx, cs, o.Namespace, podName, time.Time{})
	}()
	state.record(report.EventProbeLogStreamOpened, "following container "+ProbeContainerName+" of pod "+podName)

	if err := holdSteady(ctx, gen, o.SettleSeconds); err != nil {
		return nil, err
	}
	state.record(report.EventSettleComplete, fmt.Sprintf("held %d tcp and %d udp flows for %ds with no flow terminating", o.TCPFlows, o.UDPFlows, o.SettleSeconds))

	// t0. Everything in the report is relative to this instant.
	t0 := time.Now()
	state.recordAt(t0, report.EventTriggerIssued, fmt.Sprintf("trigger=%s target=%s/%s", o.Trigger, o.Namespace, podName))
	fmt.Fprintf(out, "trigger: %s %s/%s at %s\n", o.Trigger, o.Namespace, podName, t0.Format(time.RFC3339Nano))

	triggerErr := issueTrigger(ctx, cs, o, podName, dep.Name)
	state.record(eventTriggerAPIReturned, triggerAPIDetail(triggerErr))
	if triggerErr != nil {
		return nil, fmt.Errorf("the %s trigger failed: %w (invariant: the termination must actually be requested; check RBAC for the pods/eviction or deployments/scale subresource)", o.Trigger, triggerErr)
	}

	observe(ctx, out, state, gen, t0, o)

	closedAt := time.Now()
	gen.CloseObservation(closedAt)
	state.recordAt(closedAt, report.EventObservationWindowClosed,
		fmt.Sprintf("window=%ds; flows still alive at this instant are recorded as %s", o.ObserveTimeoutSeconds, report.OutcomeSurvivedObservationWindow))

	// Give the log stream a bounded moment to deliver the tail of the probe's
	// output after the container exits.
	select {
	case <-logDone:
	case <-time.After(10 * time.Second):
		probeLog.warn("the probe log stream had not ended 10s after the observation window closed; probe events after this point are not-measured")
	}
	cancelLog()
	state.record(eventLogStreamEnded, fmt.Sprintf("collected %d probe events", len(probeLog.Events())))

	if !probeLog.Saw(report.EventSigtermReceived) {
		// Documented fallback. It can only work while the pod object still
		// exists, which is why the follow stream above is the primary path.
		probeLog.warn("no %s event arrived on the follow stream; attempting the --previous fallback", report.EventSigtermReceived)
		probeLog.StreamPrevious(ctx, cs, o.Namespace, podName)
	}

	return assemble(state, watcher, probeLog, gen, env, o, trialID, t0), nil
}

func triggerAPIDetail(err error) string {
	if err != nil {
		return "api call returned an error: " + err.Error()
	}
	return "api call returned success"
}

// runPreflight verifies every precondition. The first failure aborts the trial.
func runPreflight(ctx context.Context, out io.Writer, watcher *Watcher, gen *flowgen.Generator, o Options) error {
	readyTimeout := time.Duration(o.ReadyTimeoutSeconds) * time.Second
	establishTimeout := 30 * time.Second

	checks := []preflight.Check{
		{
			Name:      "probe-pod-ready",
			Invariant: "exactly one probe pod exists and its Ready condition is true before any flow is opened",
			Hint:      "kubectl -n " + o.Namespace + " describe pod -l " + LabelApp + "=" + LabelAppValue + " (image pull, readiness probe on :7003/readyz, scheduling)",
			Fn: func(ctx context.Context) error {
				return preflight.WaitFor(ctx, readyTimeout, 250*time.Millisecond, "probe pod readiness",
					func(context.Context) (bool, string, error) {
						ok, why := watcher.PodReady()
						return ok, why, nil
					})
			},
		},
		{
			Name:      "k8s-watches-delivering",
			Invariant: "the pod and EndpointSlice watches have synced and each has delivered at least one object",
			Hint:      "RBAC for watching pods and discovery.k8s.io endpointslices in namespace " + o.Namespace,
			Fn: func(ctx context.Context) error {
				return preflight.WaitFor(ctx, 30*time.Second, 250*time.Millisecond, "kubernetes watches delivering events",
					func(context.Context) (bool, string, error) {
						ok, why := watcher.Delivering()
						return ok, why, nil
					})
			},
		},
		{
			Name:      "service-endpoint-stable",
			Invariant: "the endpoint for THIS trial's probe pod has been continuously ready long enough for kube-proxy to have programmed it",
			Hint:      "kubectl -n " + o.Namespace + " get endpointslices -l " + ServiceNameLabel + "=drainwatch-probe -o yaml",
			Fn: func(ctx context.Context) error {
				stable := time.Duration(o.EndpointStableMs) * time.Millisecond
				return preflight.WaitFor(ctx, 60*time.Second, 250*time.Millisecond,
					fmt.Sprintf("the endpoint for the probe pod ready continuously for %s", stable),
					func(context.Context) (bool, string, error) {
						ok, why := watcher.ReadyEndpointStableFor(watcher.PodName(), stable)
						return ok, why, nil
					})
			},
		},
		{
			Name:      "flows-dialed",
			Invariant: "every requested TCP connection and UDP socket opens against the target",
			Hint:      fmt.Sprintf("that %s tcp:%d and udp:%d reach the probe (for kind, the extraPortMappings in deploy/kind/kind-config.yaml and the NodePorts in deploy/manifests/probe.yaml)", o.TargetHost, o.TCPPort, o.UDPPort),
			Fn: func(ctx context.Context) error {
				return gen.Start(ctx)
			},
		},
		{
			Name:      "tcp-flows-established",
			Invariant: "every TCP flow has received at least one heartbeat from the probe",
			Hint:      "the probe log for tcp_accept and tcp_heartbeat events, and that the NodePort maps to container port 7001",
			Fn: func(ctx context.Context) error {
				return gen.WaitEstablished(ctx, establishTimeout, "tcp")
			},
		},
		{
			Name:      "udp-flows-replying",
			Invariant: "every UDP flow has received at least one ack from the probe",
			Hint:      "that the UDP NodePort mapping exists and maps to container port 7002; UDP port mappings are a common kind-config omission",
			Fn: func(ctx context.Context) error {
				return gen.WaitEstablished(ctx, establishTimeout, "udp")
			},
		},
	}

	_, err := preflight.Run(ctx, out, checks)
	return err
}

// holdSteady keeps the flows running for the settle period and aborts if any
// flow ends before the trigger. A flow that dies during steady state means the
// harness, the network path or the probe is unhealthy, and any measurement
// taken afterwards would be contaminated.
func holdSteady(ctx context.Context, gen *flowgen.Generator, seconds int) error {
	deadline := time.Now().Add(time.Duration(seconds) * time.Second)
	for {
		if dead := gen.TerminalFlows(); len(dead) > 0 {
			return fmt.Errorf("flows ended during steady state before the trigger was issued: %v (invariant: every flow must survive the settle period untouched; a flow dying here means the probe, the node port path or the network is unhealthy, and the trial would be measuring that instead of the termination)", dead)
		}
		if !time.Now().Before(deadline) {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("cancelled during the settle period: %w", ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// issueTrigger performs the requested termination.
func issueTrigger(ctx context.Context, cs kubernetes.Interface, o Options, podName, deploymentName string) error {
	switch o.Trigger {
	case TriggerDelete:
		// No grace override: the pod spec's terminationGracePeriodSeconds is
		// what the trial is configured to measure.
		return cs.CoreV1().Pods(o.Namespace).Delete(ctx, podName, metav1.DeleteOptions{})
	case TriggerEvict:
		return cs.PolicyV1().Evictions(o.Namespace).Evict(ctx, &policyv1.Eviction{
			ObjectMeta: metav1.ObjectMeta{Name: podName, Namespace: o.Namespace},
		})
	case TriggerScale:
		zero := int32(0)
		_, err := cs.AppsV1().Deployments(o.Namespace).UpdateScale(ctx, deploymentName, &autoscalingv1.Scale{
			ObjectMeta: metav1.ObjectMeta{Name: deploymentName, Namespace: o.Namespace},
			Spec:       autoscalingv1.ScaleSpec{Replicas: zero},
		}, metav1.UpdateOptions{})
		return err
	}
	return fmt.Errorf("unreachable: unvalidated trigger %q", o.Trigger)
}

// observe waits until every flow has a terminal event or the observation window
// expires. When all flows end early it keeps watching for a short grace period,
// because endpoint removal and container termination often land after the last
// flow dies and are part of the same story.
func observe(ctx context.Context, out io.Writer, state *trialState, gen *flowgen.Generator, t0 time.Time, o Options) {
	observeDeadline := t0.Add(time.Duration(o.ObserveTimeoutSeconds) * time.Second)
	postGrace := time.Duration(o.PostTerminalGraceSecond) * time.Second

	var allTerminalAt time.Time
	for {
		if gen.AllTerminal() {
			if allTerminalAt.IsZero() {
				allTerminalAt = time.Now()
				state.recordAt(allTerminalAt, eventAllFlowsTerminal,
					fmt.Sprintf("all %d flows reached a terminal event; continuing to watch for %s to catch trailing cluster events", o.TCPFlows+o.UDPFlows, postGrace))
				fmt.Fprintf(out, "all flows terminal at t+%dms\n", allTerminalAt.Sub(t0).Milliseconds())
			}
			if time.Since(allTerminalAt) >= postGrace {
				return
			}
		}
		if !time.Now().Before(observeDeadline) {
			fmt.Fprintf(out, "observation window (%ds) expired with %d of %d flows still alive\n",
				o.ObserveTimeoutSeconds, o.TCPFlows+o.UDPFlows-len(gen.TerminalFlows()), o.TCPFlows+o.UDPFlows)
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// assemble merges the four event sources into one timeline and builds the
// report. Merging happens here, once, so that the precision caveats attached to
// each source are applied in exactly one place.
func assemble(state *trialState, watcher *Watcher, probeLog *ProbeLog, gen *flowgen.Generator, env report.Environment, o Options, trialID string, t0 time.Time) *report.Report {
	timeline := state.timeline(t0)
	timeline = append(timeline, watcher.Events(t0)...)

	probeEvents, probeWarnings := probeLog.TimelineEvents(t0)
	timeline = append(timeline, probeEvents...)
	timeline = append(timeline, gen.TimelineEvents(t0)...)
	report.SortTimeline(timeline)

	flows := gen.Records(t0)
	summary := report.BuildSummary(timeline, flows)
	for _, w := range probeLog.Warnings() {
		summary.Notes = append(summary.Notes, "probe log: "+w)
	}
	for _, w := range probeWarnings {
		summary.Notes = append(summary.Notes, "probe log: "+w)
	}

	return &report.Report{
		DrainwatchVersion: report.Version,
		GitCommit:         report.GitCommit,
		Environment:       env,
		Trial: report.Trial{
			ID: trialID,
			Config: report.Config{
				TCPFlows:              o.TCPFlows,
				UDPFlows:              o.UDPFlows,
				GracePeriodSeconds:    o.GracePeriod,
				DrainBehavior:         o.DrainBehavior,
				DrainMaxSeconds:       o.DrainMaxSeconds,
				Trigger:               o.Trigger,
				Workload:              o.Workload,
				Namespace:             o.Namespace,
				TargetHost:            o.TargetHost,
				TCPPort:               o.TCPPort,
				UDPPort:               o.UDPPort,
				SettleSeconds:         o.SettleSeconds,
				ObserveTimeoutSeconds: o.ObserveTimeoutSeconds,
				FlowTimeoutSeconds:    o.FlowTimeout.Seconds(),
				UDPSilenceDatagrams:   o.UDPSilenceDatagrams,
			},
			Timeline:             timeline,
			Flows:                flows,
			Summary:              summary,
			ClockNote:            report.ClockNoteText,
			ProbeClockOffsetNote: report.ProbeClockOffsetNoteText,
		},
	}
}
