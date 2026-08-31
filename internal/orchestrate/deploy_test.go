package orchestrate

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/jaynirmal15/drainwatch/internal/probe"
	"github.com/jaynirmal15/drainwatch/internal/report"
)

const repoManifest = "../../deploy/manifests/probe.yaml"

// TestLoadRepoManifest parses the manifest that ships with the repository. If
// this fails, `drainwatch run` fails on every cluster.
func TestLoadRepoManifest(t *testing.T) {
	m, err := loadManifest(repoManifest)
	if err != nil {
		t.Fatalf("loadManifest: %v", err)
	}
	if m.Deployment.Name != "drainwatch-probe" || m.Service.Name != "drainwatch-probe" {
		t.Errorf("names = %q / %q, want drainwatch-probe", m.Deployment.Name, m.Service.Name)
	}
	if len(m.Deployment.Spec.Template.Spec.Containers) != 1 {
		t.Fatalf("want exactly one container, got %d", len(m.Deployment.Spec.Template.Spec.Containers))
	}
	if got := m.Deployment.Spec.Template.Spec.Containers[0].Name; got != ProbeContainerName {
		t.Errorf("container name = %q, want %q (log streaming addresses it by name)", got, ProbeContainerName)
	}
	if m.Service.Spec.Type != "NodePort" {
		t.Errorf("service type = %q, want NodePort (the host reaches the probe through a node port)", m.Service.Spec.Type)
	}

	var sawTCP, sawUDP bool
	for _, p := range m.Service.Spec.Ports {
		switch p.Protocol {
		case "TCP":
			if p.Port == 7001 {
				sawTCP = true
			}
		case "UDP":
			if p.Port == 7002 {
				sawUDP = true
			}
		}
	}
	if !sawTCP || !sawUDP {
		t.Errorf("service must expose tcp/7001 and udp/7002, got %+v", m.Service.Spec.Ports)
	}
}

func TestLoadManifestRejectsMissingFile(t *testing.T) {
	_, err := loadManifest(filepath.Join(t.TempDir(), "absent.yaml"))
	if err == nil {
		t.Fatal("expected an error for a missing manifest")
	}
	if !strings.Contains(err.Error(), "invariant") {
		t.Errorf("error should name the invariant, got: %v", err)
	}
}

func TestMutateDeploymentMapsFlagsOntoThePodSpec(t *testing.T) {
	m, err := loadManifest(repoManifest)
	if err != nil {
		t.Fatalf("loadManifest: %v", err)
	}
	dep := m.Deployment.DeepCopy()
	opts := deployOptions{
		Namespace:       "drainwatch",
		Image:           "drainwatch-probe:test",
		GracePeriod:     45,
		DrainBehavior:   probe.BehaviorExitNow,
		DrainMaxSeconds: 40,
		ExitNowForceRST: false,
	}
	if err := mutateDeployment(dep, opts); err != nil {
		t.Fatalf("mutateDeployment: %v", err)
	}

	if dep.Spec.Replicas == nil || *dep.Spec.Replicas != 1 {
		t.Errorf("replicas = %v, want 1", dep.Spec.Replicas)
	}
	if dep.Spec.Template.Spec.TerminationGracePeriodSeconds == nil || *dep.Spec.Template.Spec.TerminationGracePeriodSeconds != 45 {
		t.Errorf("terminationGracePeriodSeconds = %v, want 45", dep.Spec.Template.Spec.TerminationGracePeriodSeconds)
	}

	c := dep.Spec.Template.Spec.Containers[0]
	if c.Image != "drainwatch-probe:test" {
		t.Errorf("image = %q, want the --image value", c.Image)
	}
	want := map[string]string{
		"DRAIN_BEHAVIOR":     probe.BehaviorExitNow,
		"DRAIN_MAX_SECONDS":  "40",
		"EXIT_NOW_FORCE_RST": "false",
	}
	got := map[string]string{}
	for _, e := range c.Env {
		got[e.Name] = e.Value
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("env %s = %q, want %q", k, got[k], v)
		}
	}
	// The manifest already declares these variables; mutation must overwrite
	// them rather than append duplicates, which the kubelet resolves last-wins.
	counts := map[string]int{}
	for _, e := range c.Env {
		counts[e.Name]++
	}
	for k, n := range counts {
		if n != 1 {
			t.Errorf("env %s appears %d times, want exactly 1", k, n)
		}
	}
}

func TestMutateDeploymentRejectsAMisnamedContainer(t *testing.T) {
	m, err := loadManifest(repoManifest)
	if err != nil {
		t.Fatalf("loadManifest: %v", err)
	}
	dep := m.Deployment.DeepCopy()
	dep.Spec.Template.Spec.Containers[0].Name = "app"
	if err := mutateDeployment(dep, deployOptions{}); err == nil {
		t.Fatal("expected an error: the log stream addresses the container by name")
	}
}

func TestSplitYAMLDocuments(t *testing.T) {
	docs := splitYAMLDocuments("a: 1\n---\nb: 2\n---\nc: 3\n")
	if len(docs) != 3 {
		t.Fatalf("got %d documents, want 3: %q", len(docs), docs)
	}
	if !strings.Contains(docs[1], "b: 2") {
		t.Errorf("second document = %q", docs[1])
	}
	// A separator-looking line inside a value must not split the document.
	docs = splitYAMLDocuments("a: \"---\"\n")
	if len(docs) != 1 {
		t.Errorf("got %d documents, want 1", len(docs))
	}
}

// TestProbeWorkloadRemnantsReportsBothBlockers: waiting only for pods is not
// enough, because foreground deletion keeps the Deployment object alive until
// its dependents are collected. Creating into that window fails with "object is
// being deleted", which is exactly the race this reports on.
func TestProbeWorkloadRemnantsReportsBothBlockers(t *testing.T) {
	now := metav1.Now()
	terminating := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{
		Name: "drainwatch-probe", Namespace: "drainwatch", DeletionTimestamp: &now,
		Finalizers: []string{"foregroundDeletion"},
	}}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: "drainwatch-probe-abc", Namespace: "drainwatch",
		Labels: map[string]string{LabelApp: LabelAppValue},
	}}

	cs := fake.NewSimpleClientset(terminating, pod)
	got, err := probeWorkloadRemnants(context.Background(), cs, "drainwatch", "drainwatch-probe")
	if err != nil {
		t.Fatalf("probeWorkloadRemnants: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("got %v, want both the terminating deployment and the pod", got)
	}
	if !strings.Contains(got[0], "still being deleted") {
		t.Errorf("a terminating deployment must be reported as a blocker, got %q", got[0])
	}
	if !strings.Contains(got[1], "drainwatch-probe-abc") {
		t.Errorf("the pod must be reported as a blocker, got %q", got[1])
	}

	// A clean namespace blocks nothing.
	got, err = probeWorkloadRemnants(context.Background(), fake.NewSimpleClientset(), "drainwatch", "drainwatch-probe")
	if err != nil {
		t.Fatalf("probeWorkloadRemnants on a clean namespace: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("a clean namespace must report no blockers, got %v", got)
	}
}

// TestDefaultProbeImageTagDoesNotTrackTheBuildVersion.
//
// The image tag used to be "drainwatch-probe:" + report.Version. Once the
// version became `git describe`, it moved with every commit, so the
// orchestrator asked for a tag that only existed if the image had been rebuilt
// at exactly that commit. When it had not, the pod sat in ImagePullBackOff
// trying to pull a local-only tag from Docker Hub and the trial died in
// preflight. This pins the two apart.
func TestDefaultProbeImageTagDoesNotTrackTheBuildVersion(t *testing.T) {
	if DefaultProbeImage != "drainwatch-probe:0.1.0" {
		t.Errorf("DefaultProbeImage = %q, want a stable tag", DefaultProbeImage)
	}
	if strings.Contains(DefaultProbeImage, report.Version) && report.Version != "0.1.0" {
		t.Errorf("DefaultProbeImage %q embeds the build version %q; the tag must not move with the build",
			DefaultProbeImage, report.Version)
	}
	// A describe-style version must never leak into the tag.
	for _, marker := range []string{"-g", "-dirty", "-dev"} {
		if strings.Contains(DefaultProbeImage, marker) {
			t.Errorf("DefaultProbeImage %q contains build-provenance marker %q", DefaultProbeImage, marker)
		}
	}
}

// TestMutateDeploymentUsesTheDefaultImageWhenNoneGiven guards the other half:
// an empty --image must leave the manifest's image alone rather than blanking
// it.
func TestMutateDeploymentUsesTheDefaultImageWhenNoneGiven(t *testing.T) {
	m, err := loadManifest(repoManifest)
	if err != nil {
		t.Fatalf("loadManifest: %v", err)
	}
	dep := m.Deployment.DeepCopy()
	manifestImage := dep.Spec.Template.Spec.Containers[0].Image
	if err := mutateDeployment(dep, deployOptions{}); err != nil {
		t.Fatalf("mutateDeployment: %v", err)
	}
	if got := dep.Spec.Template.Spec.Containers[0].Image; got != manifestImage {
		t.Errorf("image = %q, want the manifest's %q left untouched", got, manifestImage)
	}
	if manifestImage != DefaultProbeImage {
		t.Errorf("the manifest ships %q but the CLI default is %q; they must agree or a plain `kubectl apply` deploys a different image than `drainwatch run`",
			manifestImage, DefaultProbeImage)
	}
}
