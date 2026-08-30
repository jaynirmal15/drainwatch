// Package orchestrate owns the run lifecycle: capture the environment, deploy
// the probe, verify every precondition, hold flows steady, trigger termination,
// observe until every flow has an outcome, and merge four independent event
// sources into one timeline.
package orchestrate

import (
	"fmt"
	"os"
	"path/filepath"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// Labels applied to everything drainwatch creates, and used by every watch.
const (
	LabelApp      = "app"
	LabelAppValue = "drainwatch-probe"
	// ServiceNameLabel is the well-known label the EndpointSlice controller sets
	// on slices it manages.
	ServiceNameLabel = "kubernetes.io/service-name"
)

// NewClient builds a client-go clientset from an explicit kubeconfig path, the
// KUBECONFIG environment variable, or ~/.kube/config, honouring an optional
// context override.
func NewClient(kubeconfigPath, contextName string) (*kubernetes.Clientset, *rest.Config, error) {
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	if kubeconfigPath != "" {
		rules = &clientcmd.ClientConfigLoadingRules{ExplicitPath: kubeconfigPath}
	} else if env := os.Getenv("KUBECONFIG"); env == "" {
		if home, err := os.UserHomeDir(); err == nil {
			candidate := filepath.Join(home, ".kube", "config")
			if _, statErr := os.Stat(candidate); statErr == nil {
				rules.ExplicitPath = candidate
			}
		}
	}

	overrides := &clientcmd.ConfigOverrides{}
	if contextName != "" {
		overrides.CurrentContext = contextName
	}

	cfg, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, overrides).ClientConfig()
	if err != nil {
		return nil, nil, fmt.Errorf("cannot load a kubeconfig: %w (invariant: drainwatch needs cluster credentials before it can record an environment; check --kubeconfig, KUBECONFIG, or ~/.kube/config)", err)
	}
	// drainwatch issues a small number of API calls and relies on watches for
	// everything else; the default client-side rate limit is generous enough,
	// but naming the user agent makes drainwatch's traffic identifiable in
	// audit logs.
	cfg.UserAgent = "drainwatch"

	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, nil, fmt.Errorf("cannot build a Kubernetes client: %w (invariant: the kubeconfig must yield a usable client)", err)
	}
	return cs, cfg, nil
}
