package orchestrate

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"

	"github.com/jaynirmal15/drainwatch/internal/report"
)

// kEvent is one observation from a Kubernetes watch. At is the orchestrator's
// clock at the moment the event was received, which is an upper bound on when
// the change happened in the API server.
type kEvent struct {
	At     time.Time
	Event  string
	Detail string
}

// Watcher runs the pod and EndpointSlice watches for one trial.
//
// Everything here is watch-driven: drainwatch never polls kubectl and never
// re-lists to discover a transition, because the thing being measured is
// exactly how quickly those transitions are published.
type Watcher struct {
	mu     sync.Mutex
	events []kEvent

	podSynced   bool
	sliceSynced bool
	podSeen     int
	sliceSeen   int

	podName string
	// lastPod is the most recent pod object delivered by the watch. Preflight
	// reads readiness from here rather than issuing a fresh GET, so that the
	// check is testing the same event stream the measurement depends on.
	lastPod *corev1.Pod

	// terminatedContainers latches container termination so that repeated
	// status updates do not produce duplicate timeline entries.
	terminatedContainers map[string]bool
	// knownEndpoints is the last observed endpoint set per slice, keyed by
	// slice name then endpoint identity.
	knownEndpoints map[string]map[string]bool
	// readyEndpoints tracks the last observed readiness per endpoint identity.
	// Entries are deleted when an endpoint leaves the slice: a departed
	// endpoint is not a ready one, and leaving it behind would let a previous
	// trial's endpoint satisfy this trial's preflight.
	readyEndpoints map[string]bool
	// readySince records when each endpoint most recently became ready, so
	// preflight can require an endpoint to have been ready for a minimum
	// duration rather than accepting the instant the watch first reports it.
	readySince map[string]time.Time

	stop chan struct{}
	once sync.Once
}

// NewWatcher constructs an unstarted Watcher.
func NewWatcher() *Watcher {
	return &Watcher{
		terminatedContainers: map[string]bool{},
		knownEndpoints:       map[string]map[string]bool{},
		readyEndpoints:       map[string]bool{},
		readySince:           map[string]time.Time{},
		stop:                 make(chan struct{}),
	}
}

func (w *Watcher) record(event, detail string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.events = append(w.events, kEvent{At: time.Now(), Event: event, Detail: detail})
}

// Start begins both informers and blocks until their caches have synced.
func (w *Watcher) Start(ctx context.Context, cs kubernetes.Interface, namespace, serviceName string) error {
	selector := LabelApp + "=" + LabelAppValue

	podLW := &cache.ListWatch{
		ListFunc: func(opts metav1.ListOptions) (runtime.Object, error) {
			opts.LabelSelector = selector
			return cs.CoreV1().Pods(namespace).List(ctx, opts)
		},
		WatchFunc: func(opts metav1.ListOptions) (watch.Interface, error) {
			opts.LabelSelector = selector
			return cs.CoreV1().Pods(namespace).Watch(ctx, opts)
		},
	}
	_, podCtl := cache.NewInformer(podLW, &corev1.Pod{}, 0, cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj any) { w.onPodAdd(obj) },
		UpdateFunc: func(oldObj, newObj any) { w.onPodUpdate(oldObj, newObj) },
		DeleteFunc: func(obj any) { w.onPodDelete(obj) },
	})

	sliceSelector := ServiceNameLabel + "=" + serviceName
	sliceLW := &cache.ListWatch{
		ListFunc: func(opts metav1.ListOptions) (runtime.Object, error) {
			opts.LabelSelector = sliceSelector
			return cs.DiscoveryV1().EndpointSlices(namespace).List(ctx, opts)
		},
		WatchFunc: func(opts metav1.ListOptions) (watch.Interface, error) {
			opts.LabelSelector = sliceSelector
			return cs.DiscoveryV1().EndpointSlices(namespace).Watch(ctx, opts)
		},
	}
	_, sliceCtl := cache.NewInformer(sliceLW, &discoveryv1.EndpointSlice{}, 0, cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj any) { w.onSliceAdd(obj) },
		UpdateFunc: func(oldObj, newObj any) { w.onSliceUpdate(oldObj, newObj) },
		DeleteFunc: func(obj any) { w.onSliceDelete(obj) },
	})

	go podCtl.Run(w.stop)
	go sliceCtl.Run(w.stop)

	if !cache.WaitForCacheSync(w.stop, podCtl.HasSynced) {
		return fmt.Errorf("the pod watch never synced (invariant: drainwatch must be receiving pod events before it triggers a termination; check RBAC for watching pods in %s)", namespace)
	}
	w.mu.Lock()
	w.podSynced = true
	w.mu.Unlock()

	if !cache.WaitForCacheSync(w.stop, sliceCtl.HasSynced) {
		return fmt.Errorf("the EndpointSlice watch never synced (invariant: drainwatch must be receiving EndpointSlice events before it triggers a termination; check RBAC for watching endpointslices in %s)", namespace)
	}
	w.mu.Lock()
	w.sliceSynced = true
	w.mu.Unlock()
	return nil
}

// Stop shuts both informers down.
func (w *Watcher) Stop() { w.once.Do(func() { close(w.stop) }) }

// Delivering reports whether both watches have synced and each has delivered at
// least one object. Preflight requires this: a watch that has synced but seen
// nothing is not evidence that events will arrive.
func (w *Watcher) Delivering() (bool, string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	switch {
	case !w.podSynced:
		return false, "the pod watch has not synced"
	case !w.sliceSynced:
		return false, "the EndpointSlice watch has not synced"
	case w.podSeen == 0:
		return false, "the pod watch has synced but delivered no pod matching " + LabelApp + "=" + LabelAppValue
	case w.sliceSeen == 0:
		return false, "the EndpointSlice watch has synced but delivered no slice for the probe Service"
	}
	return true, ""
}

// PodReady reports whether the probe pod, as last seen by the watch, has a
// Ready condition of true. The second return value explains what was observed
// when it does not.
func (w *Watcher) PodReady() (bool, string) {
	w.mu.Lock()
	pod := w.lastPod
	w.mu.Unlock()
	if pod == nil {
		return false, "the pod watch has not delivered a pod yet"
	}
	if pod.DeletionTimestamp != nil {
		return false, fmt.Sprintf("pod %s already has a deletionTimestamp", pod.Name)
	}
	for _, c := range pod.Status.Conditions {
		if c.Type != corev1.PodReady {
			continue
		}
		if c.Status == corev1.ConditionTrue {
			return true, ""
		}
		return false, fmt.Sprintf("pod %s Ready=%s reason=%q message=%q", pod.Name, c.Status, c.Reason, c.Message)
	}
	return false, fmt.Sprintf("pod %s phase=%s has no Ready condition yet", pod.Name, pod.Status.Phase)
}

// ReadyEndpointCount returns how many endpoints across all watched slices are
// currently ready. It reflects the last watch event, not a fresh API read.
func (w *Watcher) ReadyEndpointCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	n := 0
	for _, ready := range w.readyEndpoints {
		if ready {
			n++
		}
	}
	return n
}

// EndpointIdentityForPod is how the EndpointSlice controller names an endpoint
// backed by a pod.
func EndpointIdentityForPod(podName string) string { return "Pod/" + podName }

// ReadyEndpointStableFor reports whether the endpoint belonging to podName has
// been continuously ready for at least minStable.
//
// Both halves matter, and both were learned the hard way. Scoping to the pod
// stops a previous trial's endpoint - which can still be listed in the slice
// when this trial's watch performs its initial LIST - from satisfying the
// check. Requiring a minimum duration stops flows being dialed in the window
// between the API server publishing the endpoint and kube-proxy programming it
// on the node; flows established in that window can be torn down when
// kube-proxy converges and flushes conntrack, which looks like an unhealthy
// harness rather than the endpoint churn it really is.
func (w *Watcher) ReadyEndpointStableFor(podName string, minStable time.Duration) (bool, string) {
	if podName == "" {
		return false, "the probe pod name is not known yet"
	}
	id := EndpointIdentityForPod(podName)

	w.mu.Lock()
	ready := w.readyEndpoints[id]
	since, hasSince := w.readySince[id]
	others := 0
	for other, r := range w.readyEndpoints {
		if r && other != id {
			others++
		}
	}
	w.mu.Unlock()

	if !ready || !hasSince {
		if others > 0 {
			return false, fmt.Sprintf("no ready endpoint for %s yet (%d ready endpoint(s) belong to other pods and do not count)", podName, others)
		}
		return false, fmt.Sprintf("no ready endpoint for %s yet", podName)
	}
	if stable := time.Since(since); stable < minStable {
		return false, fmt.Sprintf("endpoint for %s has been ready for %s, needs %s", podName, stable.Round(time.Millisecond), minStable)
	}
	return true, ""
}

// PodName returns the name of the probe pod as observed by the watch, or "".
func (w *Watcher) PodName() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.podName
}

func (w *Watcher) onPodAdd(obj any) {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		return
	}
	w.mu.Lock()
	w.podSeen++
	w.podName = pod.Name
	w.lastPod = pod
	w.mu.Unlock()
	w.record("pod_observed", fmt.Sprintf("%s phase=%s", pod.Name, pod.Status.Phase))
	w.inspectPod(nil, pod)
}

func (w *Watcher) onPodUpdate(oldObj, newObj any) {
	oldPod, _ := oldObj.(*corev1.Pod)
	newPod, ok := newObj.(*corev1.Pod)
	if !ok {
		return
	}
	w.mu.Lock()
	w.podName = newPod.Name
	w.lastPod = newPod
	w.mu.Unlock()
	w.inspectPod(oldPod, newPod)
}

func (w *Watcher) onPodDelete(obj any) {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		if tomb, isTomb := obj.(cache.DeletedFinalStateUnknown); isTomb {
			pod, ok = tomb.Obj.(*corev1.Pod)
		}
		if !ok {
			return
		}
	}
	w.record(report.EventPodObjectGone, pod.Name+" removed from the API")
}

// inspectPod turns pod status transitions into timeline events. Each transition
// is latched so that repeated status writes do not duplicate entries.
func (w *Watcher) inspectPod(oldPod, newPod *corev1.Pod) {
	if newPod.DeletionTimestamp != nil && (oldPod == nil || oldPod.DeletionTimestamp == nil) {
		grace := int64(-1)
		if newPod.DeletionGracePeriodSeconds != nil {
			grace = *newPod.DeletionGracePeriodSeconds
		}
		w.record(report.EventPodDeletionTimestampSet,
			fmt.Sprintf("%s deletionGracePeriodSeconds=%d", newPod.Name, grace))
	}

	for _, cs := range newPod.Status.ContainerStatuses {
		if cs.State.Terminated == nil {
			continue
		}
		key := newPod.Name + "/" + cs.Name
		w.mu.Lock()
		already := w.terminatedContainers[key]
		w.terminatedContainers[key] = true
		w.mu.Unlock()
		if already {
			continue
		}
		t := cs.State.Terminated
		// Exit code 137 is 128+SIGKILL: this is how the grace-period boundary
		// shows up in the record when the application never exits on its own.
		w.record(report.EventContainerTerminated,
			fmt.Sprintf("%s exitCode=%d reason=%s signal=%d finishedAt=%s",
				cs.Name, t.ExitCode, t.Reason, t.Signal, t.FinishedAt.Format(time.RFC3339)))
	}

	oldReady := podReady(oldPod)
	newReady := podReady(newPod)
	if oldPod != nil && oldReady && !newReady {
		w.record("pod_ready_false", newPod.Name)
	}
}

func podReady(p *corev1.Pod) bool {
	if p == nil {
		return false
	}
	for _, c := range p.Status.Conditions {
		if c.Type == corev1.PodReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

func (w *Watcher) onSliceAdd(obj any) {
	slice, ok := obj.(*discoveryv1.EndpointSlice)
	if !ok {
		return
	}
	w.mu.Lock()
	w.sliceSeen++
	w.mu.Unlock()
	w.record("endpointslice_observed", fmt.Sprintf("%s endpoints=%d", slice.Name, len(slice.Endpoints)))
	w.diffSlice(nil, slice)
}

func (w *Watcher) onSliceUpdate(oldObj, newObj any) {
	oldSlice, _ := oldObj.(*discoveryv1.EndpointSlice)
	newSlice, ok := newObj.(*discoveryv1.EndpointSlice)
	if !ok {
		return
	}
	w.diffSlice(oldSlice, newSlice)
}

func (w *Watcher) onSliceDelete(obj any) {
	slice, ok := obj.(*discoveryv1.EndpointSlice)
	if !ok {
		if tomb, isTomb := obj.(cache.DeletedFinalStateUnknown); isTomb {
			slice, ok = tomb.Obj.(*discoveryv1.EndpointSlice)
		}
		if !ok {
			return
		}
	}
	w.mu.Lock()
	previous := w.knownEndpoints[slice.Name]
	delete(w.knownEndpoints, slice.Name)
	w.mu.Unlock()
	for id := range previous {
		w.mu.Lock()
		delete(w.readyEndpoints, id)
		delete(w.readySince, id)
		w.mu.Unlock()
		w.record(report.EventEndpointSliceEndpointGone, fmt.Sprintf("%s (slice %s deleted)", id, slice.Name))
	}
}

// endpointIdentity names an endpoint stably across updates: the target pod when
// there is one, otherwise its addresses.
func endpointIdentity(ep discoveryv1.Endpoint) string {
	if ep.TargetRef != nil && ep.TargetRef.Name != "" {
		return ep.TargetRef.Kind + "/" + ep.TargetRef.Name
	}
	addrs := append([]string(nil), ep.Addresses...)
	sort.Strings(addrs)
	if len(addrs) == 0 {
		return "endpoint-without-address"
	}
	return "addr/" + addrs[0]
}

func endpointReady(ep discoveryv1.Endpoint) bool {
	return ep.Conditions.Ready != nil && *ep.Conditions.Ready
}

// diffSlice emits the two transitions that matter for connection draining:
// an endpoint flipping to ready:false (it stops receiving new connections) and
// an endpoint disappearing from the slice entirely.
func (w *Watcher) diffSlice(oldSlice, newSlice *discoveryv1.EndpointSlice) {
	name := newSlice.Name

	current := map[string]bool{}
	for _, ep := range newSlice.Endpoints {
		id := endpointIdentity(ep)
		current[id] = true

		ready := endpointReady(ep)
		w.mu.Lock()
		prevReady, known := w.readyEndpoints[id]
		w.readyEndpoints[id] = ready
		switch {
		case ready && (!known || !prevReady):
			w.readySince[id] = time.Now()
		case !ready:
			delete(w.readySince, id)
		}
		w.mu.Unlock()

		if known && prevReady && !ready {
			detail := fmt.Sprintf("%s in %s", id, name)
			if ep.Conditions.Terminating != nil && *ep.Conditions.Terminating {
				detail += " (terminating=true)"
			}
			if ep.Conditions.Serving != nil {
				detail += fmt.Sprintf(" serving=%t", *ep.Conditions.Serving)
			}
			w.record(report.EventEndpointSliceReadyFalse, detail)
		}
	}

	w.mu.Lock()
	previous := w.knownEndpoints[name]
	w.knownEndpoints[name] = current
	w.mu.Unlock()

	if previous == nil {
		return
	}
	for id := range previous {
		if current[id] {
			continue
		}
		w.mu.Lock()
		delete(w.readyEndpoints, id)
		delete(w.readySince, id)
		w.mu.Unlock()
		w.record(report.EventEndpointSliceEndpointGone, fmt.Sprintf("%s removed from %s", id, name))
	}
}

// Events returns the recorded events as timeline entries relative to t0.
func (w *Watcher) Events(t0 time.Time) []report.Event {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := make([]report.Event, 0, len(w.events))
	for _, e := range w.events {
		out = append(out, report.Event{
			TMs:    e.At.Sub(t0).Milliseconds(),
			Source: report.SourceK8s,
			Event:  e.Event,
			Detail: e.Detail,
		})
	}
	return out
}

// Saw reports whether an event with the given name has been recorded.
func (w *Watcher) Saw(event string) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, e := range w.events {
		if e.Event == event {
			return true
		}
	}
	return false
}
