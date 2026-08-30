package orchestrate

import (
	"errors"
	"strings"
	"testing"

	"github.com/jaynirmal15/drainwatch/internal/probe"
)

func validOptions() Options {
	return Options{
		Namespace:     "drainwatch",
		ManifestPath:  "deploy/manifests/probe.yaml",
		Workload:      WorkloadDeploy,
		Trigger:       TriggerDelete,
		DrainBehavior: probe.BehaviorDrain,
		GracePeriod:   30,
		TCPFlows:      10,
		UDPFlows:      10,
		TargetHost:    "127.0.0.1",
		TCPPort:       7001,
		UDPPort:       7002,
		SettleSeconds: 10,
		Repeat:        1,
		OutDir:        "out",
	}
}

func TestNormalizeAppliesDocumentedDefaults(t *testing.T) {
	o := validOptions()
	if err := o.Normalize(); err != nil {
		t.Fatalf("Normalize: %v", err)
	}
	if o.ObserveTimeoutSeconds != 60 {
		t.Errorf("observe timeout = %d, want grace+30 = 60", o.ObserveTimeoutSeconds)
	}
	if o.DrainMaxSeconds != 25 {
		t.Errorf("drain max = %d, want grace-5 = 25", o.DrainMaxSeconds)
	}
	if o.FlowTimeout.Seconds() != 10 {
		t.Errorf("flow timeout = %s, want 10s", o.FlowTimeout)
	}
	if o.UDPSilenceDatagrams != 6 {
		t.Errorf("udp silence datagrams = %d, want 6", o.UDPSilenceDatagrams)
	}
	if o.PostTerminalGraceSecond != 5 {
		t.Errorf("post-terminal grace = %d, want 5", o.PostTerminalGraceSecond)
	}
}

// TestNormalizeKeepsDrainInsideTheGracePeriod: if the drain window is not
// shorter than the grace period, the drain arm measures a SIGKILL rather than a
// drain, and the run must refuse rather than mislabel the result.
func TestNormalizeKeepsDrainInsideTheGracePeriod(t *testing.T) {
	o := validOptions()
	o.DrainMaxSeconds = 30
	err := o.Normalize()
	if err == nil {
		t.Fatal("expected an error when the drain window is not shorter than the grace period")
	}
	if !strings.Contains(err.Error(), "invariant") {
		t.Errorf("error should name the invariant, got: %v", err)
	}

	// The same setting is legitimate under exit-now and ignore, where the drain
	// window is not used.
	o = validOptions()
	o.DrainBehavior = probe.BehaviorIgnore
	o.DrainMaxSeconds = 30
	if err := o.Normalize(); err != nil {
		t.Errorf("drain window should be irrelevant under ignore: %v", err)
	}
}

func TestNormalizeClampsTinyGracePeriods(t *testing.T) {
	o := validOptions()
	o.GracePeriod = 3
	o.DrainBehavior = probe.BehaviorExitNow
	if err := o.Normalize(); err != nil {
		t.Fatalf("Normalize: %v", err)
	}
	if o.DrainMaxSeconds < 1 {
		t.Errorf("drain max = %d, want at least 1", o.DrainMaxSeconds)
	}
}

func TestNormalizeRejectsAttachWithASentinelError(t *testing.T) {
	o := validOptions()
	o.Workload = WorkloadAttach
	err := o.Normalize()
	if !errors.Is(err, ErrAttachNotImplemented) {
		t.Fatalf("got %v, want ErrAttachNotImplemented so that main can exit 2", err)
	}
}

func TestNormalizeRejectsUnknownEnums(t *testing.T) {
	cases := map[string]func(*Options){
		"workload":       func(o *Options) { o.Workload = "adopt" },
		"trigger":        func(o *Options) { o.Trigger = "kill" },
		"drain behavior": func(o *Options) { o.DrainBehavior = "graceful" },
		"grace period":   func(o *Options) { o.GracePeriod = 0 },
		"repeat":         func(o *Options) { o.Repeat = 0 },
		"namespace":      func(o *Options) { o.Namespace = "" },
		"out dir":        func(o *Options) { o.OutDir = "" },
		"manifest":       func(o *Options) { o.ManifestPath = "" },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			o := validOptions()
			mutate(&o)
			if err := o.Normalize(); err == nil {
				t.Fatal("expected an error")
			}
		})
	}
}

func TestFlowConfigMirrorsOptions(t *testing.T) {
	o := validOptions()
	if err := o.Normalize(); err != nil {
		t.Fatalf("Normalize: %v", err)
	}
	c := o.flowConfig()
	if err := c.Validate(); err != nil {
		t.Fatalf("a normalized Options produced an invalid flow config: %v", err)
	}
	if c.TCPFlows != o.TCPFlows || c.UDPFlows != o.UDPFlows || c.Host != o.TargetHost {
		t.Errorf("flow config does not mirror options: %+v", c)
	}
}
