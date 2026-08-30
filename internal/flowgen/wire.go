package flowgen

// The drainwatch wire protocol. It is deliberately line-oriented ASCII so that a
// packet capture or a `nc` session is readable without tooling.
//
// This file is the single definition of the protocol; internal/probe imports it
// rather than restating the literals, so the server and the client cannot drift.
//
// TCP (probe :7001)
//
//	client -> probe   "ping <seq>\n"        every 500ms, seq from 0
//	probe  -> client  "hb <seq> <unixnano>\n"  every 500ms, seq from 0, per flow
//	probe  -> client  "bye <seq> <reason>\n"   exactly once, immediately before the
//	                                           probe closes the connection as part
//	                                           of a graceful drain
//
// The client's pings are not required by the measurement, but they make the flow
// bidirectional, which is what a real keepalive connection looks like and what
// makes an abrupt server exit observable as a write error rather than silence.
//
// A "bye" line is the only thing that licenses the outcome drained-clean-close.
// A FIN without a preceding "bye" is an unannounced close and is classified as
// severed, because the server ended the flow without completing it.
//
// UDP (probe :7002)
//
//	client -> probe   "ping <seq>\n"
//	probe  -> client  "ack <seq>\n"          echoes the client's sequence number
//
// UDP has no close handshake and therefore no drain semantics. The only honest
// UDP outcomes in v0.1 are severed (silence or ICMP error) and
// survived-observation-window.
const (
	// PrefixPing is sent by the client on both protocols.
	PrefixPing = "ping "
	// PrefixHeartbeat is the probe's TCP heartbeat.
	PrefixHeartbeat = "hb "
	// PrefixBye is the probe's graceful-close announcement.
	PrefixBye = "bye "
	// PrefixAck is the probe's UDP reply.
	PrefixAck = "ack "
)

// Reasons carried by a "bye" line.
const (
	// ReasonDrainDeadline: the probe's drain window expired with the flow still
	// open, so the probe closed it gracefully.
	ReasonDrainDeadline = "drain-deadline"
	// ReasonDrainComplete: every other flow closed and the probe is shutting
	// down.
	ReasonDrainComplete = "drain-complete"
)
