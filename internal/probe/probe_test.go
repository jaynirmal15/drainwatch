package probe

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/jaynirmal15/drainwatch/internal/report"
)

func envFrom(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

func TestConfigFromEnvDefaults(t *testing.T) {
	c, err := ConfigFromEnv(envFrom(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if c.Behavior != BehaviorDrain {
		t.Errorf("behavior = %q, want %q", c.Behavior, BehaviorDrain)
	}
	if c.DrainMaxSeconds != DefaultDrainMaxSeconds {
		t.Errorf("drain max = %d, want %d", c.DrainMaxSeconds, DefaultDrainMaxSeconds)
	}
	if c.TCPPort != 7001 || c.UDPPort != 7002 || c.ReadyPort != 7003 {
		t.Errorf("ports = %d/%d/%d, want 7001/7002/7003", c.TCPPort, c.UDPPort, c.ReadyPort)
	}
	if !c.ExitNowForceRST {
		t.Error("EXIT_NOW_FORCE_RST should default to true")
	}
}

func TestConfigFromEnvRejectsBadValues(t *testing.T) {
	cases := map[string]map[string]string{
		"unknown behaviour":     {"DRAIN_BEHAVIOR": "graceful"},
		"negative drain window": {"DRAIN_MAX_SECONDS": "-1"},
		"non-numeric window":    {"DRAIN_MAX_SECONDS": "sixty"},
		"non-boolean rst flag":  {"EXIT_NOW_FORCE_RST": "yes-please"},
		"port out of range":     {"PROBE_TCP_PORT": "70000"},
	}
	for name, env := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := ConfigFromEnv(envFrom(env))
			if err == nil {
				t.Fatal("expected an error; a probe running a behaviour the report does not claim would contaminate the trial")
			}
			if !strings.Contains(err.Error(), "invariant") {
				t.Errorf("error should name the invariant, got: %v", err)
			}
		})
	}
}

func TestConfigFromEnvAcceptsEveryBehaviour(t *testing.T) {
	for _, b := range []string{BehaviorDrain, BehaviorExitNow, BehaviorIgnore} {
		c, err := ConfigFromEnv(envFrom(map[string]string{"DRAIN_BEHAVIOR": b}))
		if err != nil {
			t.Fatalf("%s: %v", b, err)
		}
		if c.Behavior != b {
			t.Errorf("behavior = %q, want %q", c.Behavior, b)
		}
	}
}

// TestLoggerEmitsParseableJSONLines: these lines are the only route by which
// SIGTERM delivery time reaches the report, so their shape is load bearing.
func TestLoggerEmitsParseableJSONLines(t *testing.T) {
	var buf bytes.Buffer
	start := time.Now()
	l := NewLogger(&buf, start)

	l.Simple(EventStarted, "tcp=7001")
	seq := int64(41)
	l.Flow(EventHeartbeat, "srv-tcp-0001", &seq, "")
	l.Event(report.ProbeEvent{Event: EventSigtermReceived, DrainBehavior: BehaviorDrain, Detail: "signal=terminated"})

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 3 {
		t.Fatalf("got %d lines, want 3: %q", len(lines), buf.String())
	}

	var got []report.ProbeEvent
	for i, line := range lines {
		var e report.ProbeEvent
		if err := json.Unmarshal([]byte(line), &e); err != nil {
			t.Fatalf("line %d is not valid JSON: %v (%s)", i, err, line)
		}
		if e.Wall == "" {
			t.Errorf("line %d has no wall clock", i)
		}
		if _, err := e.WallTime(); err != nil {
			t.Errorf("line %d wall clock does not parse: %v", i, err)
		}
		got = append(got, e)
	}

	if got[1].Seq == nil || *got[1].Seq != 41 {
		t.Errorf("heartbeat sequence = %v, want 41", got[1].Seq)
	}
	if got[2].Event != EventSigtermReceived || got[2].DrainBehavior != BehaviorDrain {
		t.Errorf("sigterm line = %+v", got[2])
	}
	if got[0].MonoMs < 0 {
		t.Errorf("monotonic offset must not be negative, got %d", got[0].MonoMs)
	}
}
