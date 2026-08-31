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
//	probe  -> client  "hb <seq> <unixnano> <instance>\n"  every 500ms, per flow
//	probe  -> client  "bye <seq> <reason>\n"   exactly once, immediately before the
//	                                           probe closes the connection as part
//	                                           of a graceful drain
//
// <instance> identifies the probe *process* (its pod name). It exists because a
// flow can be answered by a different pod than the one under test: deleting a
// Deployment-managed pod causes a replacement to be created, and a connectionless
// protocol will happily be re-homed onto it mid-flow. See PrefixAck below.
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
//	probe  -> client  "ack <seq> <instance>\n"   echoes the client's sequence number
//
// UDP has no close handshake and therefore no drain semantics. The only honest
// UDP outcomes in v0.1 are severed (silence, ICMP error, or re-homing onto a
// different probe instance) and survived-observation-window.
//
// The instance check is what stops a re-homed UDP flow from being reported as an
// undisturbed one. When the pod under test is deleted, its replacement answers
// the same NodePort from a new process; without comparing instances, the client
// would keep receiving acks throughout and record the flow as having survived
// the observation window, which reads as "nothing happened to it". What actually
// happened is that the flow to the pod under test ended and something else
// picked it up, so the flow is classified severed with the instance change in
// its detail.
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
