package envrecord

import (
	"sort"
	"strings"
)

// UnknownValue is what drainwatch records for a property it could not read. It
// is never used for a property it could read but found empty, and it is never a
// substitute for a guess.
const UnknownValue = "unknown"

// UnspecifiedProxyMode is recorded when the kube-proxy ConfigMap is readable but
// leaves `mode` empty. kube-proxy then picks a platform default, and drainwatch
// does not guess which one that is.
const UnspecifiedProxyMode = "unspecified (kube-proxy left at its platform default)"

// ParseKubeProxyMode extracts the `mode` field from the kube-proxy
// KubeProxyConfiguration document (the config.conf key of the kube-proxy
// ConfigMap).
//
// It reads only top-level keys, so nested blocks such as conntrack or winkernel
// cannot be mistaken for the proxy mode. It returns a warning string when the
// value could not be determined; an empty warning means the answer is
// authoritative.
func ParseKubeProxyMode(configConf string) (mode string, warning string) {
	if strings.TrimSpace(configConf) == "" {
		return UnknownValue, "kube-proxy ConfigMap contained no config.conf document"
	}
	found := false
	raw := ""
	for _, line := range strings.Split(configConf, "\n") {
		trimmed := strings.TrimRight(line, "\r")
		if trimmed == "" || strings.HasPrefix(strings.TrimSpace(trimmed), "#") {
			continue
		}
		// Only top-level keys: any leading whitespace means the key belongs to
		// a nested block.
		if strings.HasPrefix(trimmed, " ") || strings.HasPrefix(trimmed, "\t") || strings.HasPrefix(trimmed, "-") {
			continue
		}
		key, value, ok := strings.Cut(trimmed, ":")
		if !ok || strings.TrimSpace(key) != "mode" {
			continue
		}
		found = true
		raw = strings.TrimSpace(value)
		break
	}
	if !found {
		return UnknownValue, "kube-proxy config.conf has no top-level `mode` key"
	}
	raw = strings.Trim(raw, `"'`)
	if raw == "" {
		return UnspecifiedProxyMode, "kube-proxy `mode` is empty; the effective mode is a platform default that drainwatch does not infer"
	}
	return raw, ""
}

// knownCNIDaemonSets maps a DaemonSet name substring in kube-system to the CNI
// it indicates. This is explicitly a best-effort heuristic and the caller
// records it as such.
var knownCNIDaemonSets = map[string]string{
	"kindnet":      "kindnet",
	"calico-node":  "calico",
	"cilium":       "cilium",
	"kube-flannel": "flannel",
	"flannel":      "flannel",
	"weave-net":    "weave-net",
	"aws-node":     "aws-vpc-cni",
	"azure-cni":    "azure-cni",
	"ovnkube":      "ovn-kubernetes",
	"antrea":       "antrea",
}

// IdentifyCNI matches kube-system DaemonSet names against known CNI plugins.
// It returns UnknownValue when nothing matches, plus a warning explaining that
// the identification is name-based and best effort.
func IdentifyCNI(daemonSetNames []string) (cni string, warning string) {
	seen := map[string]bool{}
	for _, name := range daemonSetNames {
		lower := strings.ToLower(name)
		for substr, plugin := range knownCNIDaemonSets {
			if strings.Contains(lower, substr) {
				seen[plugin] = true
			}
		}
	}
	if len(seen) == 0 {
		return UnknownValue, "no kube-system DaemonSet name matched a known CNI plugin; CNI identification in drainwatch is name-based and best effort"
	}
	out := make([]string, 0, len(seen))
	for k := range seen {
		out = append(out, k)
	}
	sort.Strings(out)
	return strings.Join(out, "+") + " (identified by DaemonSet name; best effort)", ""
}
