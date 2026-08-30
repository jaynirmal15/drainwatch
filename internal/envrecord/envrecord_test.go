package envrecord

import (
	"context"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/version"
	discoveryfake "k8s.io/client-go/discovery/fake"
	"k8s.io/client-go/kubernetes/fake"
)

func node(name, runtime string) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Status: corev1.NodeStatus{NodeInfo: corev1.NodeSystemInfo{
			ContainerRuntimeVersion: runtime,
			KubeletVersion:          "v1.31.0",
			OSImage:                 "Debian GNU/Linux 12 (bookworm)",
			Architecture:            "arm64",
		}},
	}
}

func newFake(objs ...any) *fake.Clientset {
	runtimeObjs := make([]interface{}, 0, len(objs))
	for _, o := range objs {
		runtimeObjs = append(runtimeObjs, o)
	}
	cs := fake.NewSimpleClientset(toRuntimeObjects(runtimeObjs)...)
	cs.Discovery().(*discoveryfake.FakeDiscovery).FakedServerVersion = &version.Info{GitVersion: "v1.31.0"}
	return cs
}

func TestCaptureRecordsTheFullEnvironment(t *testing.T) {
	kubeProxy := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "kube-proxy", Namespace: "kube-system"},
		Data:       map[string]string{KubeProxyConfigMapKey: "kind: KubeProxyConfiguration\nmode: \"iptables\"\n"},
	}
	kindnet := &appsv1.DaemonSet{ObjectMeta: metav1.ObjectMeta{Name: "kindnet", Namespace: "kube-system"}}

	cs := newFake(node("worker", "containerd://1.7.18"), node("control-plane", "containerd://1.7.18"), kubeProxy, kindnet)

	env, err := Capture(context.Background(), cs, time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("Capture: %v", err)
	}
	if env.KubernetesVersion != "v1.31.0" {
		t.Errorf("kubernetes version = %q", env.KubernetesVersion)
	}
	if env.NodeCount != 2 || len(env.Nodes) != 2 {
		t.Errorf("node count = %d, nodes = %d, want 2 and 2", env.NodeCount, len(env.Nodes))
	}
	if env.Nodes[0].Name != "control-plane" {
		t.Errorf("nodes should be sorted by name for a stable report, got %q first", env.Nodes[0].Name)
	}
	if env.Nodes[0].ContainerRuntime != "containerd://1.7.18" {
		t.Errorf("container runtime = %q", env.Nodes[0].ContainerRuntime)
	}
	if env.KubeProxyMode != "iptables" {
		t.Errorf("kube-proxy mode = %q, want iptables", env.KubeProxyMode)
	}
	if !strings.Contains(env.CNI, "kindnet") {
		t.Errorf("cni = %q, want it to mention kindnet", env.CNI)
	}
	if len(env.Warnings) != 0 {
		t.Errorf("a fully readable environment should produce no warnings, got %v", env.Warnings)
	}
	if env.WallClockStart != "2026-01-01T12:00:00Z" {
		t.Errorf("wall clock start = %q", env.WallClockStart)
	}
	if env.OS == "" || env.Arch == "" {
		t.Error("host OS and architecture must be recorded")
	}
}

// TestCaptureWarnsRatherThanGuessing: kube-proxy and CNI are allowed to be
// unreadable, but the report must then say "unknown" and carry a warning.
func TestCaptureWarnsRatherThanGuessing(t *testing.T) {
	cs := newFake(node("worker", "containerd://1.7.18"))

	env, err := Capture(context.Background(), cs, time.Now())
	if err != nil {
		t.Fatalf("Capture: %v", err)
	}
	if env.KubeProxyMode != UnknownValue {
		t.Errorf("kube-proxy mode = %q, want %q", env.KubeProxyMode, UnknownValue)
	}
	if env.CNI != UnknownValue {
		t.Errorf("cni = %q, want %q", env.CNI, UnknownValue)
	}
	if len(env.Warnings) != 2 {
		t.Errorf("expected one warning each for kube-proxy and cni, got %v", env.Warnings)
	}
	for _, w := range env.Warnings {
		if !strings.Contains(w, "unknown") {
			t.Errorf("warning should name the unknown value: %q", w)
		}
	}
}

// TestCaptureRefusesAClusterWithNoNodes: a run that cannot record its
// environment refuses to start.
func TestCaptureRefusesAClusterWithNoNodes(t *testing.T) {
	cs := newFake()
	_, err := Capture(context.Background(), cs, time.Now())
	if err == nil {
		t.Fatal("expected Capture to refuse a cluster with zero nodes")
	}
	if !strings.Contains(err.Error(), "invariant") {
		t.Errorf("error should name the invariant, got: %v", err)
	}
}
