// Package envrecord captures the cluster and host context of a run.
//
// A run that cannot record its environment refuses to start. The distinction
// this package enforces is between properties that must be known for a report to
// mean anything (Kubernetes version, node inventory) and properties that may
// legitimately be unreadable (kube-proxy mode behind RBAC, CNI on a cluster with
// unfamiliar DaemonSet names). The first kind is a hard error. The second is
// recorded as "unknown" with a warning, never guessed.
package envrecord

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"sort"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/jaynirmal15/drainwatch/internal/report"
)

// KubeProxyConfigMapKey is the ConfigMap key holding the kube-proxy
// KubeProxyConfiguration document.
const KubeProxyConfigMapKey = "config.conf"

// Capture builds the environment record. Errors returned here abort the run.
func Capture(ctx context.Context, cs kubernetes.Interface, now time.Time) (report.Environment, error) {
	env := report.Environment{
		DrainwatchVersion: report.Version,
		GitCommit:         report.GitCommit,
		OS:                runtime.GOOS,
		Arch:              runtime.GOARCH,
		WallClockStart:    now.Format(time.RFC3339Nano),
		Warnings:          []string{},
	}

	ver, err := cs.Discovery().ServerVersion()
	if err != nil {
		return env, fmt.Errorf("cannot read the Kubernetes server version: %w (invariant: a report without a cluster version is uninterpretable, so drainwatch refuses to run; check that your kubeconfig points at a reachable cluster)", err)
	}
	env.KubernetesVersion = ver.GitVersion

	nodes, err := cs.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return env, fmt.Errorf("cannot list nodes: %w (invariant: the node inventory and container runtime must be recorded before measuring; check that your credentials can list nodes)", err)
	}
	env.NodeCount = len(nodes.Items)
	for _, n := range nodes.Items {
		env.Nodes = append(env.Nodes, report.NodeInfo{
			Name:             n.Name,
			ContainerRuntime: n.Status.NodeInfo.ContainerRuntimeVersion,
			KubeletVersion:   n.Status.NodeInfo.KubeletVersion,
			OSImage:          n.Status.NodeInfo.OSImage,
			Architecture:     n.Status.NodeInfo.Architecture,
		})
	}
	sort.Slice(env.Nodes, func(i, j int) bool { return env.Nodes[i].Name < env.Nodes[j].Name })
	if env.NodeCount == 0 {
		return env, fmt.Errorf("the cluster reports zero nodes (invariant: a measurable cluster has at least one node; check that you are pointed at the intended cluster)")
	}

	env.KubeProxyMode, env.Warnings = captureKubeProxyMode(ctx, cs, env.Warnings)
	env.CNI, env.Warnings = captureCNI(ctx, cs, env.Warnings)
	return env, nil
}

func captureKubeProxyMode(ctx context.Context, cs kubernetes.Interface, warnings []string) (string, []string) {
	cm, err := cs.CoreV1().ConfigMaps("kube-system").Get(ctx, "kube-proxy", metav1.GetOptions{})
	if err != nil {
		return UnknownValue, append(warnings, fmt.Sprintf("kube-proxy mode recorded as %q: the kube-system/kube-proxy ConfigMap could not be read (%v). drainwatch does not guess the proxy mode.", UnknownValue, err))
	}
	mode, warn := ParseKubeProxyMode(cm.Data[KubeProxyConfigMapKey])
	if warn != "" {
		warnings = append(warnings, fmt.Sprintf("kube-proxy mode recorded as %q: %s", mode, warn))
	}
	return mode, warnings
}

func captureCNI(ctx context.Context, cs kubernetes.Interface, warnings []string) (string, []string) {
	dsList, err := cs.AppsV1().DaemonSets("kube-system").List(ctx, metav1.ListOptions{})
	if err != nil {
		return UnknownValue, append(warnings, fmt.Sprintf("CNI recorded as %q: kube-system DaemonSets could not be listed (%v).", UnknownValue, err))
	}
	names := make([]string, 0, len(dsList.Items))
	for _, ds := range dsList.Items {
		names = append(names, ds.Name)
	}
	cni, warn := IdentifyCNI(names)
	if warn != "" {
		warnings = append(warnings, fmt.Sprintf("CNI recorded as %q: %s", cni, warn))
	}
	return cni, warnings
}

// PrintWarnings writes each environment warning to stderr. Warnings are printed
// as well as recorded so that an operator watching a run sees immediately which
// parts of the environment are not fully known.
func PrintWarnings(env report.Environment) {
	for _, w := range env.Warnings {
		fmt.Fprintf(os.Stderr, "drainwatch: WARNING: %s\n", w)
	}
}
