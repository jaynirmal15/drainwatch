package report

import "sort"

// FirstEvent returns the first timeline event matching source and name, or nil.
// The timeline is expected to be sorted by t_ms; FirstEvent does not assume it.
func FirstEvent(timeline []Event, source, name string) *Event {
	var best *Event
	for i := range timeline {
		e := &timeline[i]
		if e.Source != source || e.Event != name {
			continue
		}
		if best == nil || e.TMs < best.TMs {
			best = e
		}
	}
	return best
}

// SortTimeline orders events by t_ms, breaking ties by source then event name so
// that report output is byte-stable across runs with identical observations.
func SortTimeline(timeline []Event) {
	sort.SliceStable(timeline, func(i, j int) bool {
		a, b := timeline[i], timeline[j]
		if a.TMs != b.TMs {
			return a.TMs < b.TMs
		}
		if a.Source != b.Source {
			return a.Source < b.Source
		}
		return a.Event < b.Event
	})
}

// CountFlows tallies flows of one protocol by outcome.
func CountFlows(flows []Flow, proto string) ProtoSummary {
	var s ProtoSummary
	for _, f := range flows {
		if f.Proto != proto {
			continue
		}
		s.Total++
		switch f.Outcome {
		case OutcomeDrainedCleanClose:
			s.Drained++
		case OutcomeSevered:
			s.Severed++
		case OutcomeReadTimeout:
			s.ReadTimeout++
		case OutcomeSurvivedObservationWindow:
			s.SurvivedWindow++
		default:
			// Anything not in the enum is counted as not-measured rather than
			// silently dropped, so that Total always equals the sum of buckets.
			s.NotMeasured++
		}
	}
	return s
}

// BuildSummary derives the trial summary from the timeline and flow records.
// Every derived interval is nil unless both of its endpoints were observed.
func BuildSummary(timeline []Event, flows []Flow) Summary {
	s := Summary{
		TCP:   CountFlows(flows, "tcp"),
		UDP:   CountFlows(flows, "udp"),
		Notes: []string{},
	}

	sigterm := FirstEvent(timeline, SourceProbe, EventSigtermReceived)
	readyFalse := FirstEvent(timeline, SourceK8s, EventEndpointSliceReadyFalse)
	removed := FirstEvent(timeline, SourceK8s, EventEndpointSliceEndpointGone)
	terminated := FirstEvent(timeline, SourceK8s, EventContainerTerminated)

	if sigterm == nil {
		s.Notes = append(s.Notes, "sigterm_received was not observed in the probe log stream; every interval measured from SIGTERM is not-measured")
	} else {
		s.TriggerToSigtermMs = Ptr(sigterm.TMs)
	}
	if readyFalse == nil {
		s.Notes = append(s.Notes, "endpointslice_ready_false was not observed; the endpoint may have been removed without an intermediate not-ready state")
	}
	if sigterm != nil && readyFalse != nil {
		s.SigtermToReadyFalseMs = Ptr(readyFalse.TMs - sigterm.TMs)
	}
	if removed != nil {
		s.TriggerToEndpointRemovedMs = Ptr(removed.TMs)
	}
	if terminated != nil {
		s.TriggerToContainerTerminated = Ptr(terminated.TMs)
	}

	// The last flow terminal is the maximum observed terminal time. Flows with
	// no terminal (survived / not-measured) contribute nothing, and their
	// presence is called out so the number is not read as "everything ended".
	var last *int64
	unterminated := 0
	for _, f := range flows {
		if f.TTerminalMs == nil {
			unterminated++
			continue
		}
		if last == nil || *f.TTerminalMs > *last {
			v := *f.TTerminalMs
			last = &v
		}
	}
	if unterminated > 0 {
		s.Notes = append(s.Notes, "flows without an observed terminal event are excluded from sigterm_to_last_flow_terminal_ms")
	}
	if sigterm != nil && last != nil {
		s.SigtermToLastFlowTerminalMs = Ptr(*last - sigterm.TMs)
	}
	return s
}

// ClockNoteText is the standard clock_note. It is a constant sentence rather
// than a computed one because the precision characteristics it describes are
// structural, not per-run.
const ClockNoteText = "t_ms is milliseconds relative to trigger_issued. " +
	"Events from sources 'orchestrator' and 'client' are taken from a single monotonic clock in the orchestrator process and are exact relative to the trigger. " +
	"Events from source 'k8s' are stamped with the orchestrator's monotonic clock at the moment the watch event was received, so they include API-server write latency and watch dispatch latency and are an upper bound on when the change actually occurred. " +
	"Events from source 'probe' are marked approximate: their t_ms is obtained by differencing wall clocks on two different hosts with no synchronisation guarantee, and should be read as accurate to tens of milliseconds, not sub-millisecond."

// ClockNoteClientText is the clock_note for a standalone `drainwatch client`
// run, where there is no trigger and therefore no cluster-relative zero.
const ClockNoteClientText = "This trial was produced by `drainwatch client`, which has no orchestrator and issues no trigger. " +
	"t_ms is milliseconds relative to the instant the flow generator started, taken from a single monotonic clock in the client process. " +
	"There is no probe-sourced or k8s-sourced event in this record, so no cross-host alignment is involved."

// ProbeClockOffsetNoteText documents the probe alignment method.
const ProbeClockOffsetNoteText = "Probe timestamps were mapped onto the orchestrator timebase as (probe wall clock - orchestrator wall clock at trigger_issued). " +
	"Deltas between two probe events are exact because they share the probe's monotonic clock; deltas between a probe event and any other source carry the unmeasured host-to-host clock offset."
