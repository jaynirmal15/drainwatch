package envrecord

import (
	"strings"
	"testing"
)

func TestParseKubeProxyMode(t *testing.T) {
	iptablesConf := `apiVersion: kubeproxy.config.k8s.io/v1alpha1
bindAddress: 0.0.0.0
clientConnection:
  kubeconfig: /var/lib/kube-proxy/kubeconfig.conf
conntrack:
  maxPerCore: 32768
  mode: "should-not-be-picked-up"
kind: KubeProxyConfiguration
mode: "iptables"
`
	tests := []struct {
		name        string
		in          string
		wantMode    string
		wantWarning bool
	}{
		{"iptables, nested keys ignored", iptablesConf, "iptables", false},
		{"ipvs unquoted", "kind: KubeProxyConfiguration\nmode: ipvs\n", "ipvs", false},
		{"nftables", "mode: 'nftables'\n", "nftables", false},
		{"empty mode is unspecified, not guessed", "kind: KubeProxyConfiguration\nmode: \"\"\n", UnspecifiedProxyMode, true},
		{"no mode key at all", "kind: KubeProxyConfiguration\nbindAddress: 0.0.0.0\n", UnknownValue, true},
		{"empty document", "", UnknownValue, true},
		{"comments are skipped", "# mode: ipvs\nmode: iptables\n", "iptables", false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mode, warning := ParseKubeProxyMode(tc.in)
			if mode != tc.wantMode {
				t.Errorf("mode = %q, want %q", mode, tc.wantMode)
			}
			if tc.wantWarning && warning == "" {
				t.Error("expected a warning explaining why the mode is not authoritative")
			}
			if !tc.wantWarning && warning != "" {
				t.Errorf("unexpected warning: %s", warning)
			}
		})
	}
}

// TestParseKubeProxyModeNeverGuesses is the point of the whole function: an
// unreadable or unset mode must never come back as a plausible-looking value.
func TestParseKubeProxyModeNeverGuesses(t *testing.T) {
	for _, in := range []string{"", "kind: KubeProxyConfiguration\n", "mode: \"\"\n"} {
		mode, _ := ParseKubeProxyMode(in)
		if mode == "iptables" || mode == "ipvs" || mode == "nftables" {
			t.Fatalf("input %q produced a concrete mode %q; drainwatch must not guess", in, mode)
		}
	}
}

func TestIdentifyCNI(t *testing.T) {
	tests := []struct {
		name        string
		names       []string
		wantContain string
		wantWarning bool
	}{
		{"kind", []string{"kindnet", "kube-proxy"}, "kindnet", false},
		{"calico", []string{"calico-node", "kube-proxy"}, "calico", false},
		{"cilium", []string{"cilium", "cilium-node-init"}, "cilium", false},
		{"flannel", []string{"kube-flannel-ds"}, "flannel", false},
		{"nothing recognisable", []string{"kube-proxy", "node-exporter"}, UnknownValue, true},
		{"empty", nil, UnknownValue, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cni, warning := IdentifyCNI(tc.names)
			if !strings.Contains(cni, tc.wantContain) {
				t.Errorf("cni = %q, want it to contain %q", cni, tc.wantContain)
			}
			if tc.wantWarning != (warning != "") {
				t.Errorf("warning = %q, wantWarning = %t", warning, tc.wantWarning)
			}
			if !tc.wantWarning && !strings.Contains(cni, "best effort") {
				t.Errorf("a name-based identification must be labelled best effort, got %q", cni)
			}
		})
	}
}

func TestIdentifyCNIIsDeterministicWithMultipleMatches(t *testing.T) {
	first, _ := IdentifyCNI([]string{"cilium", "calico-node"})
	second, _ := IdentifyCNI([]string{"calico-node", "cilium"})
	if first != second {
		t.Errorf("identification depends on input order: %q vs %q", first, second)
	}
}
