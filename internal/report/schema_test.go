package report

import (
	"bytes"
	"encoding/json"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func sampleReport() Report {
	return Report{
		DrainwatchVersion: "0.1.0",
		GitCommit:         "abc1234",
		Environment: Environment{
			KubernetesVersion: "v1.31.0",
			NodeCount:         2,
			Nodes: []NodeInfo{
				{Name: "drainwatch-control-plane", ContainerRuntime: "containerd://1.7.18", KubeletVersion: "v1.31.0", OSImage: "Debian GNU/Linux 12", Architecture: "arm64"},
				{Name: "drainwatch-worker", ContainerRuntime: "containerd://1.7.18", KubeletVersion: "v1.31.0", OSImage: "Debian GNU/Linux 12", Architecture: "arm64"},
			},
			KubeProxyMode:     "iptables",
			CNI:               "kindnet (identified by DaemonSet name; best effort)",
			DrainwatchVersion: "0.1.0",
			GitCommit:         "abc1234",
			OS:                "darwin",
			Arch:              "arm64",
			WallClockStart:    "2026-01-01T12:00:00.000000001Z",
			Warnings:          []string{},
		},
		Trial: Trial{
			ID: "trial-001",
			Config: Config{
				TCPFlows: 10, UDPFlows: 10, GracePeriodSeconds: 30,
				DrainBehavior: "drain", DrainMaxSeconds: 25, Trigger: "delete",
				Workload: "deploy", Namespace: "drainwatch", TargetHost: "127.0.0.1",
				TCPPort: 7001, UDPPort: 7002, SettleSeconds: 10,
				ObserveTimeoutSeconds: 60, FlowTimeoutSeconds: 10, UDPSilenceDatagrams: 6,
			},
			Timeline: []Event{
				{TMs: 0, Source: SourceOrchestrator, Event: EventTriggerIssued},
				{TMs: 112, Source: SourceProbe, Event: EventSigtermReceived, Approximate: true},
				{TMs: 340, Source: SourceK8s, Event: EventEndpointSliceReadyFalse},
				{TMs: 8100, Source: SourceK8s, Event: EventEndpointSliceEndpointGone},
			},
			Flows: []Flow{
				{ID: "tcp-0001", Proto: "tcp", Outcome: OutcomeDrainedCleanClose,
					ConnectedTMs: Ptr(int64(-10500)), TTerminalMs: Ptr(int64(4210)),
					LastHeartbeatSeq: Ptr(int64(41)), LastDataTMs: Ptr(int64(4200)),
					Detail: "fin after drain announcement"},
				{ID: "udp-0004", Proto: "udp", Outcome: OutcomeSevered,
					ConnectedTMs: Ptr(int64(-10500)), TTerminalMs: Ptr(int64(3550)),
					LastAnsweredSeq: Ptr(int64(17)), LastDataTMs: Ptr(int64(50)),
					Detail: "udp-silence-6-datagrams"},
				{ID: "tcp-0002", Proto: "tcp", Outcome: OutcomeSurvivedObservationWindow},
			},
			Summary: Summary{
				TCP:                         ProtoSummary{Total: 2, Drained: 1, SurvivedWindow: 1},
				UDP:                         ProtoSummary{Total: 1, Severed: 1},
				SigtermToReadyFalseMs:       Ptr(int64(228)),
				SigtermToLastFlowTerminalMs: Ptr(int64(4098)),
				TriggerToSigtermMs:          Ptr(int64(112)),
				Notes:                       []string{},
			},
			ClockNote:            ClockNoteText,
			ProbeClockOffsetNote: ProbeClockOffsetNoteText,
		},
	}
}

// TestReportRoundTrip: a report written by this binary must be readable by this
// binary, field for field, with no unknown fields on either side.
func TestReportRoundTrip(t *testing.T) {
	want := sampleReport()
	path := filepath.Join(t.TempDir(), "nested", "report.json")

	if err := WriteJSON(path, want); err != nil {
		t.Fatalf("WriteJSON: %v", err)
	}
	got, err := ReadReport(path)
	if err != nil {
		t.Fatalf("ReadReport: %v", err)
	}
	if !reflect.DeepEqual(*got, want) {
		t.Errorf("round trip changed the report.\n got: %+v\nwant: %+v", *got, want)
	}
}

// TestUnobservedValuesSerialiseAsNull is the schema half of "the absence of an
// observation is not measured, never zero".
func TestUnobservedValuesSerialiseAsNull(t *testing.T) {
	f := Flow{ID: "tcp-0002", Proto: "tcp", Outcome: OutcomeSurvivedObservationWindow}
	buf, err := json.Marshal(f)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	for _, field := range []string{`"t_terminal_ms":null`, `"connected_t_ms":null`, `"last_data_t_ms":null`} {
		if !bytes.Contains(buf, []byte(field)) {
			t.Errorf("expected %s in %s", field, buf)
		}
	}
	if bytes.Contains(buf, []byte(`"t_terminal_ms":0`)) {
		t.Error("an unobserved terminal time must never serialise as 0")
	}

	s := Summary{}
	sbuf, err := json.Marshal(s)
	if err != nil {
		t.Fatalf("Marshal summary: %v", err)
	}
	for _, field := range []string{`"sigterm_to_ready_false_ms":null`, `"sigterm_to_last_flow_terminal_ms":null`, `"trigger_to_sigterm_ms":null`} {
		if !bytes.Contains(sbuf, []byte(field)) {
			t.Errorf("expected %s in %s", field, sbuf)
		}
	}
}

// TestReadReportRejectsForeignFields: a report from a different schema must be
// rejected loudly rather than silently half-parsed.
func TestReadReportRejectsForeignFields(t *testing.T) {
	path := filepath.Join(t.TempDir(), "report.json")
	if err := WriteJSON(path, map[string]any{
		"drainwatch_version": "0.1.0",
		"invented_field":     true,
	}); err != nil {
		t.Fatalf("WriteJSON: %v", err)
	}
	if _, err := ReadReport(path); err == nil {
		t.Fatal("expected ReadReport to reject a document with unknown fields")
	} else if !strings.Contains(err.Error(), "schema") {
		t.Errorf("error should name the schema invariant, got: %v", err)
	}
}

func TestOutcomeEnumIsClosed(t *testing.T) {
	for _, o := range Outcomes() {
		if !o.Valid() {
			t.Errorf("%q is listed by Outcomes but not Valid", o)
		}
	}
	for _, bad := range []Outcome{"", "drained", "ok", "DRAINED-CLEAN-CLOSE"} {
		if bad.Valid() {
			t.Errorf("%q must not be a valid outcome", bad)
		}
	}
}
