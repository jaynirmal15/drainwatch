package orchestrate

import (
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// slice builds an EndpointSlice listing the given pods with the given readiness.
func slice(name string, pods map[string]bool) *discoveryv1.EndpointSlice {
	es := &discoveryv1.EndpointSlice{ObjectMeta: metav1.ObjectMeta{Name: name}}
	for pod, ready := range pods {
		r := ready
		es.Endpoints = append(es.Endpoints, discoveryv1.Endpoint{
			Addresses:  []string{"10.244.1.9"},
			Conditions: discoveryv1.EndpointConditions{Ready: &r},
			TargetRef:  &corev1.ObjectReference{Kind: "Pod", Name: pod},
		})
	}
	return es
}

// TestReadyEndpointIsScopedToThisTrialsPod is the regression test for the
// defect that aborted arm E: a previous trial's endpoint, still listed in the
// slice when this trial's watch performs its initial LIST, must not satisfy
// this trial's readiness check.
func TestReadyEndpointIsScopedToThisTrialsPod(t *testing.T) {
	w := NewWatcher()
	// The watch's first observation still carries the previous trial's pod.
	w.diffSlice(nil, slice("es-1", map[string]bool{"probe-OLD": true}))

	ok, why := w.ReadyEndpointStableFor("probe-NEW", 0)
	if ok {
		t.Fatal("a ready endpoint belonging to another pod must not satisfy this trial's check")
	}
	if why == "" {
		t.Error("the check must explain what it is still waiting for")
	}
	if w.ReadyEndpointCount() != 1 {
		t.Errorf("the old endpoint is still genuinely ready, count = %d, want 1", w.ReadyEndpointCount())
	}

	// Once this trial's pod is ready, the check passes.
	w.diffSlice(nil, slice("es-1", map[string]bool{"probe-OLD": true, "probe-NEW": true}))
	if ok, why := w.ReadyEndpointStableFor("probe-NEW", 0); !ok {
		t.Errorf("expected the check to pass for probe-NEW: %s", why)
	}
}

// TestDepartedEndpointsStopCounting: an endpoint removed from the slice is not
// a ready endpoint, and must not linger in the readiness state.
func TestDepartedEndpointsStopCounting(t *testing.T) {
	w := NewWatcher()
	old := slice("es-1", map[string]bool{"probe-OLD": true})
	w.diffSlice(nil, old)
	if w.ReadyEndpointCount() != 1 {
		t.Fatalf("count = %d, want 1", w.ReadyEndpointCount())
	}

	// The old pod's endpoint leaves the slice entirely.
	w.diffSlice(old, slice("es-1", map[string]bool{}))
	if got := w.ReadyEndpointCount(); got != 0 {
		t.Errorf("a departed endpoint must not still count as ready, count = %d", got)
	}
	if ok, _ := w.ReadyEndpointStableFor("probe-OLD", 0); ok {
		t.Error("a departed endpoint must not satisfy the readiness check")
	}
	if !w.Saw("endpointslice_endpoint_removed") {
		t.Error("the removal should still have been recorded on the timeline")
	}
}

// TestWholeSliceDeletionClearsReadiness covers the other removal path.
func TestWholeSliceDeletionClearsReadiness(t *testing.T) {
	w := NewWatcher()
	es := slice("es-1", map[string]bool{"probe-A": true})
	w.diffSlice(nil, es)
	w.onSliceDelete(es)
	if got := w.ReadyEndpointCount(); got != 0 {
		t.Errorf("deleting the slice must clear readiness, count = %d", got)
	}
	if ok, _ := w.ReadyEndpointStableFor("probe-A", 0); ok {
		t.Error("a deleted slice must not satisfy the readiness check")
	}
}

// TestReadinessMustBeStableForTheMinimumDuration: publishing an endpoint and
// programming it on the node are not the same instant, so the check requires
// the endpoint to have been ready for a minimum time.
func TestReadinessMustBeStableForTheMinimumDuration(t *testing.T) {
	w := NewWatcher()
	w.diffSlice(nil, slice("es-1", map[string]bool{"probe-A": true}))

	if ok, why := w.ReadyEndpointStableFor("probe-A", time.Hour); ok {
		t.Error("an endpoint ready for a moment must not satisfy a one-hour stability requirement")
	} else if why == "" {
		t.Error("the check must say how long it has been ready and how long is needed")
	}

	if ok, why := w.ReadyEndpointStableFor("probe-A", 0); !ok {
		t.Errorf("with no stability requirement the check should pass: %s", why)
	}
}

// TestFlappingReadinessRestartsTheStabilityWindow: an endpoint that goes
// not-ready and back must not inherit the earlier readiness onset.
func TestFlappingReadinessRestartsTheStabilityWindow(t *testing.T) {
	w := NewWatcher()
	ready := slice("es-1", map[string]bool{"probe-A": true})
	w.diffSlice(nil, ready)

	w.mu.Lock()
	w.readySince[EndpointIdentityForPod("probe-A")] = time.Now().Add(-time.Hour)
	w.mu.Unlock()
	if ok, _ := w.ReadyEndpointStableFor("probe-A", time.Minute); !ok {
		t.Fatal("an endpoint ready for an hour should satisfy a one-minute requirement")
	}

	// It flaps not-ready, then ready again.
	notReady := slice("es-1", map[string]bool{"probe-A": false})
	w.diffSlice(ready, notReady)
	if ok, _ := w.ReadyEndpointStableFor("probe-A", 0); ok {
		t.Error("a not-ready endpoint must not satisfy the check at all")
	}
	w.diffSlice(notReady, ready)
	if ok, _ := w.ReadyEndpointStableFor("probe-A", time.Minute); ok {
		t.Error("readiness that flapped must restart the stability window, not inherit the old onset")
	}
}

func TestReadyEndpointStableForWithoutAPodName(t *testing.T) {
	w := NewWatcher()
	ok, why := w.ReadyEndpointStableFor("", 0)
	if ok {
		t.Fatal("an empty pod name cannot satisfy the check")
	}
	if why == "" {
		t.Error("the check must explain itself")
	}
}
