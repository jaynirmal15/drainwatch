package orchestrate

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
)

// ProbeContainerName is the container the orchestrator streams logs from.
const ProbeContainerName = "probe"

// manifestObjects is the decoded content of deploy/manifests/probe.yaml.
type manifestObjects struct {
	Deployment *appsv1.Deployment
	Service    *corev1.Service
}

// loadManifest reads and decodes the probe manifest. v0.1 expects exactly one
// Deployment and one Service; anything else is rejected rather than partially
// applied, because a half-understood manifest is a half-understood experiment.
func loadManifest(path string) (*manifestObjects, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("cannot read the probe manifest %s: %w (invariant: `drainwatch run --workload deploy` needs the manifest on disk; run from the repository root or pass --manifest)", path, err)
	}

	out := &manifestObjects{}
	decoder := scheme.Codecs.UniversalDeserializer()
	for i, doc := range splitYAMLDocuments(string(raw)) {
		if strings.TrimSpace(doc) == "" {
			continue
		}
		obj, _, err := decoder.Decode([]byte(doc), nil, nil)
		if err != nil {
			return nil, fmt.Errorf("cannot decode document %d of %s: %w (invariant: every document in the probe manifest must be a known Kubernetes object)", i+1, path, err)
		}
		switch o := obj.(type) {
		case *appsv1.Deployment:
			if out.Deployment != nil {
				return nil, fmt.Errorf("%s contains more than one Deployment (invariant: v0.1 deploys exactly one probe Deployment)", path)
			}
			out.Deployment = o
		case *corev1.Service:
			if out.Service != nil {
				return nil, fmt.Errorf("%s contains more than one Service (invariant: v0.1 deploys exactly one probe Service)", path)
			}
			out.Service = o
		default:
			return nil, fmt.Errorf("%s contains an unsupported object of type %T (invariant: v0.1 understands only a Deployment and a Service)", path, obj)
		}
	}
	if out.Deployment == nil || out.Service == nil {
		return nil, fmt.Errorf("%s must contain exactly one Deployment and one Service (invariant: the probe workload is a Deployment fronted by a Service so that EndpointSlice transitions are observable)", path)
	}
	return out, nil
}

// splitYAMLDocuments splits on lines consisting solely of `---`.
func splitYAMLDocuments(s string) []string {
	lines := strings.Split(s, "\n")
	var docs []string
	var cur []string
	for _, ln := range lines {
		if strings.TrimSpace(strings.TrimRight(ln, "\r")) == "---" {
			docs = append(docs, strings.Join(cur, "\n"))
			cur = nil
			continue
		}
		cur = append(cur, ln)
	}
	return append(docs, strings.Join(cur, "\n"))
}

// deployOptions are the trial-specific mutations applied to the manifest.
type deployOptions struct {
	Namespace       string
	Image           string
	GracePeriod     int
	DrainBehavior   string
	DrainMaxSeconds int
	ExitNowForceRST bool
}

// applyManifest mutates the decoded manifest for this trial and applies it,
// replacing any previous probe so that each trial starts from a known state.
func applyManifest(ctx context.Context, cs kubernetes.Interface, m *manifestObjects, opts deployOptions) (*appsv1.Deployment, error) {
	if err := ensureNamespace(ctx, cs, opts.Namespace); err != nil {
		return nil, err
	}

	dep := m.Deployment.DeepCopy()
	svc := m.Service.DeepCopy()
	dep.Namespace = opts.Namespace
	svc.Namespace = opts.Namespace

	if err := mutateDeployment(dep, opts); err != nil {
		return nil, err
	}

	// Remove any previous probe and wait for its pods to disappear. Without
	// this, a trial can observe the tail of the previous trial's termination
	// and attribute it to its own trigger.
	if err := deleteProbeWorkload(ctx, cs, opts.Namespace, dep.Name); err != nil {
		return nil, err
	}

	created, err := cs.AppsV1().Deployments(opts.Namespace).Create(ctx, dep, metav1.CreateOptions{})
	if err != nil {
		return nil, fmt.Errorf("cannot create the probe Deployment %s/%s: %w (invariant: the probe workload must exist before flows can be established; check RBAC for deployments in this namespace)", opts.Namespace, dep.Name, err)
	}

	if _, err := cs.CoreV1().Services(opts.Namespace).Get(ctx, svc.Name, metav1.GetOptions{}); err != nil {
		if !apierrors.IsNotFound(err) {
			return nil, fmt.Errorf("cannot read the probe Service %s/%s: %w", opts.Namespace, svc.Name, err)
		}
		if _, err := cs.CoreV1().Services(opts.Namespace).Create(ctx, svc, metav1.CreateOptions{}); err != nil {
			return nil, fmt.Errorf("cannot create the probe Service %s/%s: %w (invariant: the Service must exist so that the EndpointSlice controller produces the slice drainwatch watches)", opts.Namespace, svc.Name, err)
		}
	}
	return created, nil
}

// mutateDeployment maps run flags onto the probe pod spec.
func mutateDeployment(dep *appsv1.Deployment, opts deployOptions) error {
	replicas := int32(1)
	dep.Spec.Replicas = &replicas
	dep.ResourceVersion = ""

	grace := int64(opts.GracePeriod)
	dep.Spec.Template.Spec.TerminationGracePeriodSeconds = &grace

	if len(dep.Spec.Template.Spec.Containers) != 1 {
		return fmt.Errorf("the probe Deployment must have exactly one container, found %d (invariant: v0.1 streams logs from a single probe container)", len(dep.Spec.Template.Spec.Containers))
	}
	c := &dep.Spec.Template.Spec.Containers[0]
	if c.Name != ProbeContainerName {
		return fmt.Errorf("the probe container must be named %q, found %q (invariant: the log stream is opened by container name)", ProbeContainerName, c.Name)
	}
	if opts.Image != "" {
		c.Image = opts.Image
	}
	setEnv(c, "DRAIN_BEHAVIOR", opts.DrainBehavior)
	setEnv(c, "DRAIN_MAX_SECONDS", strconv.Itoa(opts.DrainMaxSeconds))
	setEnv(c, "EXIT_NOW_FORCE_RST", strconv.FormatBool(opts.ExitNowForceRST))

	if dep.Labels == nil {
		dep.Labels = map[string]string{}
	}
	dep.Labels[LabelApp] = LabelAppValue
	if dep.Spec.Template.Labels == nil {
		dep.Spec.Template.Labels = map[string]string{}
	}
	dep.Spec.Template.Labels[LabelApp] = LabelAppValue
	return nil
}

func setEnv(c *corev1.Container, name, value string) {
	for i := range c.Env {
		if c.Env[i].Name == name {
			c.Env[i].Value = value
			c.Env[i].ValueFrom = nil
			return
		}
	}
	c.Env = append(c.Env, corev1.EnvVar{Name: name, Value: value})
}

func ensureNamespace(ctx context.Context, cs kubernetes.Interface, ns string) error {
	_, err := cs.CoreV1().Namespaces().Get(ctx, ns, metav1.GetOptions{})
	if err == nil {
		return nil
	}
	if !apierrors.IsNotFound(err) {
		return fmt.Errorf("cannot read namespace %s: %w (invariant: drainwatch must know whether its namespace exists before deploying; check RBAC for namespaces)", ns, err)
	}
	nsObj := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns, Labels: map[string]string{LabelApp: LabelAppValue}}}
	if _, err := cs.CoreV1().Namespaces().Create(ctx, nsObj, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("cannot create namespace %s: %w (invariant: the probe needs a namespace; create it yourself or point --namespace at an existing one)", ns, err)
	}
	return nil
}

// deleteProbeWorkload removes a previous Deployment and blocks until both the
// Deployment object and every pod carrying the probe label are gone.
//
// Waiting for the pods alone is not enough. Foreground propagation keeps the
// Deployment object alive until its dependents are collected, so there is a
// window in which the pods have gone but the Deployment is still finalizing;
// creating into that window fails with "object is being deleted". Both
// conditions must hold before the next trial starts.
func deleteProbeWorkload(ctx context.Context, cs kubernetes.Interface, ns, name string) error {
	policy := metav1.DeletePropagationForeground
	err := cs.AppsV1().Deployments(ns).Delete(ctx, name, metav1.DeleteOptions{PropagationPolicy: &policy})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("cannot delete the previous probe Deployment %s/%s: %w (invariant: each trial starts from a clean workload)", ns, name, err)
	}

	deadline := time.Now().Add(2 * time.Minute)
	for {
		blockers, err := probeWorkloadRemnants(ctx, cs, ns, name)
		if err != nil {
			return err
		}
		if len(blockers) == 0 {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("the previous probe workload is still present after 2m: %s (invariant: a trial must not observe the tail of the previous trial's termination, and cannot create a workload that is still being deleted; remove these and retry)", strings.Join(blockers, ", "))
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("cancelled while waiting for the previous probe workload to clear: %w", ctx.Err())
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// probeWorkloadRemnants names everything from a previous trial that still
// exists. An empty result means the namespace is clean.
func probeWorkloadRemnants(ctx context.Context, cs kubernetes.Interface, ns, name string) ([]string, error) {
	var blockers []string

	dep, err := cs.AppsV1().Deployments(ns).Get(ctx, name, metav1.GetOptions{})
	switch {
	case err == nil:
		state := "still exists"
		if dep.DeletionTimestamp != nil {
			state = "is still being deleted"
		}
		blockers = append(blockers, fmt.Sprintf("deployment/%s %s", name, state))
	case !apierrors.IsNotFound(err):
		return nil, fmt.Errorf("cannot read the probe Deployment %s/%s while waiting for the previous trial to clear: %w", ns, name, err)
	}

	pods, err := cs.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{LabelSelector: LabelApp + "=" + LabelAppValue})
	if err != nil {
		return nil, fmt.Errorf("cannot list probe pods in %s while waiting for the previous trial to clear: %w", ns, err)
	}
	for _, p := range pods.Items {
		blockers = append(blockers, "pod/"+p.Name)
	}
	return blockers, nil
}
