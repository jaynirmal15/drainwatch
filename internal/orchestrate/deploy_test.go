package orchestrate

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/jaynirmal15/drainwatch/internal/probe"
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
