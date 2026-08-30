# drainwatch experiments

This file holds the methodology and the recorded findings. The methodology is complete.
The findings section is a template with no results in it yet, because none have been
recorded on a real cluster — see [Status](#status).

---

## Status

| | |
| --- | --- |
| Harness | v0.1, complete |
| Unit and loopback tests | passing (`make test`) |
| Cluster runs recorded | **none yet** |

The loopback tests in `internal/orchestrate/loopback_test.go` verify the part of the
mechanism that does not need Kubernetes: that the probe's three SIGTERM behaviours put
the expected bytes on the wire, and that the classifier turns those bytes into the
expected outcomes. What they cannot verify is anything involving kube-proxy,
EndpointSlices, the kubelet, or the grace-period boundary. Those numbers only exist once
`make reproduce` has been run on a real cluster, and this file will carry them, with the
environment record from the run that produced them.

Nothing in this file is an estimate.

---

## The question

When a pod is deleted, three things happen at roughly the same time and in an order that
is not guaranteed:

1. the kubelet sends SIGTERM to the container,
2. the EndpointSlice controller marks the endpoint not-ready and then removes it,
3. kube-proxy on every node updates its rules, and existing conntrack entries do or do
   not survive that update.

Most guidance about graceful shutdown is written as if step 2 reliably precedes step 1.
The observable question is narrower and answerable: **for a connection that was already
established when the trigger was issued, what ends it, and when?**

drainwatch does not attempt to explain the mechanism inside kube-proxy or conntrack. It
records what happened to the flows and the order the cluster published its own state
changes.

## Method

### Workload

A single-replica Deployment fronted by a NodePort Service (`deploy/manifests/probe.yaml`).
The container is the same `drainwatch` binary in `probe` mode, on `scratch`.

- **TCP :7001** — accepts connections and writes `hb <seq> <unixnano>` every 500ms per
  flow. It also reads the client's pings, which is what makes the flow bidirectional.
- **UDP :7002** — replies `ack <seq>` to every datagram, echoing the client's sequence
  number.
- **:7003 `/readyz`** — 200 normally; 503 after SIGTERM under `drain`; always 200 under
  `ignore`. The readiness probe runs with `periodSeconds: 1, failureThreshold: 1`, so the
  interval between the application answering 503 and the EndpointSlice showing
  `ready:false` is measured rather than hidden behind a slow probe period.
- **`/healthz`** — unconditionally 200, including during a drain, so the kubelet can never
  restart the probe in the middle of a measured termination.

The probe writes one JSON object per line to stdout, each carrying both a wall clock and a
monotonic offset from probe start. The `sigterm_received` line is emitted before any other
handling work, so its timestamp reflects delivery rather than processing.

### Client

Runs on the host and reaches the pod through NodePorts mapped to `localhost` by kind
(`deploy/kind/kind-config.yaml`). The mappings are on the **worker** node, and the probe
lands there too because the control plane carries the standard `NoSchedule` taint. The
measured path is therefore `host → worker:nodePort → kube-proxy DNAT → pod on the same
node`, with no second node hop to muddy the result.

- TCP: N flows, each reading heartbeats and sending a ping every 500ms. Terminal on FIN,
  RST, a write error, or `--flow-timeout` (default 10s) without bytes.
- UDP: M flows, each a *connected* socket — `net.Dial`, not `ListenPacket` — so that ICMP
  port-unreachable is delivered to us as `ECONNREFUSED` and so that each flow occupies a
  distinct source port. Severed after 6 consecutive unanswered datagrams (3s), recorded
  with the sequence number of the last answered datagram.

### Trial sequence

1. **Environment record.** Kubernetes version, node inventory with container runtime,
   kube-proxy mode from the `kube-system/kube-proxy` ConfigMap, CNI from kube-system
   DaemonSet names, drainwatch version and commit, host OS/arch, wall-clock start. A run
   that cannot read the version or the node list aborts. A run that cannot read the
   kube-proxy mode records `"unknown"` and warns; it does not guess.
2. **Deploy.** The manifest is applied with `--grace-period` mapped onto
   `terminationGracePeriodSeconds` and `--drain-behavior` onto `DRAIN_BEHAVIOR`. Any
   previous probe is deleted first and its pods waited out, so a trial cannot observe the
   tail of the previous trial's termination.
3. **Preflight.** Six checks, in order. The first failure aborts.
4. **Steady state.** Flows are held for `--settle-seconds` (default 10). If any flow ends
   during this window the run aborts: a flow dying before the trigger means the harness or
   the network path is unhealthy, and the trial would be measuring that instead.
5. **Trigger.** `t = 0` is stamped immediately before the API call. `delete` uses the pod
   API with no grace override, so the pod spec's grace period is what is measured;
   `evict` uses the eviction subresource; `scale` sets the Deployment's replicas to 0.
6. **Observe.** Until every flow has a terminal event or `--observe-timeout` (default
   grace + 30s). When all flows end early, drainwatch keeps watching for five more seconds
   to catch trailing cluster events. Flows still alive when the window closes are recorded
   as `survived-observation-window` — never as drained.
7. **Collect and report.** Orchestrator events, client per-flow records, probe stdout, and
   the two API watches are merged into one timeline, sorted, and written.

### Preflight checks

| Check | Invariant |
| --- | --- |
| `probe-pod-ready` | exactly one probe pod exists and its Ready condition is true |
| `k8s-watches-delivering` | the pod and EndpointSlice watches have synced *and each has delivered at least one object* |
| `service-endpoint-ready` | the Service has a ready endpoint, so an endpoint leaving rotation is observable |
| `flows-dialed` | every requested TCP connection and UDP socket opens |
| `tcp-flows-established` | every TCP flow has received at least one heartbeat |
| `udp-flows-replying` | every UDP flow has received at least one ack |

A synced watch that has seen nothing is not evidence that events will arrive, which is why
the second check tests delivery and not just sync.

### Clocks

- `orchestrator` and `client` events share one monotonic clock and are exact relative to
  the trigger.
- `k8s` events are stamped when the watch event is *received*. That includes API-server
  write latency and watch dispatch latency, so each is an upper bound on when the change
  actually occurred.
- `probe` events are mapped by differencing wall clocks on two hosts with no
  synchronisation guarantee. They are marked `"approximate": true` and should be read as
  accurate to tens of milliseconds.

Every report restates this in its `clock_note` field. No number in a drainwatch report is
presented at a precision the method does not support.

## Design decisions worth arguing with

### A FIN without an announcement is `severed`, not `drained`

The probe writes `bye <seq> <reason>` before closing a flow gracefully. The client
classifies a clean EOF as `drained-clean-close` **only** if that announcement preceded it.
An unannounced FIN arriving mid-heartbeat is classified `severed`, with the detail
`eof-without-drain-announcement`.

The reason: at the TCP layer, a server finishing its work and a server dying mid-stream
both produce a FIN. Calling both "drained" would make the drain arm and a crash
indistinguishable in the report, which is the exact confusion drainwatch exists to remove.
The cost is that this definition is drainwatch's own — a real application that closes
cleanly without announcing anything would be recorded as severed here. That is a
deliberate choice, and it is why the mechanism is always in the `detail` field.

### `exit-now` forces RST, and says so

`exit-now` models an application that dies and abandons its connections. A bare `exit(0)`
does not reliably produce RST: the kernel sends FIN unless unread client data happens to
be sitting in the receive buffer, which makes the observed outcome depend on 500ms ping
timing rather than on the behaviour under test.

So under `exit-now` the probe sets `SO_LINGER=0` on every established connection and
closes it, producing a deterministic RST, and logs `tcp_flow_reset_forced` for each one.
The harness caused it, and the record says the harness caused it.

Set `EXIT_NOW_FORCE_RST=false` (or `--exit-now-force-rst=false`) for the bare-`exit(0)`
variant, which is more faithful to a crash and less deterministic. Both are legitimate;
only one of them is the default.

### The drain window is forced inside the grace period

Under `--drain-behavior drain`, `--drain-max-seconds` defaults to `grace-period - 5` and
the run refuses to start if it is configured longer than the grace period. Otherwise the
drain arm would be terminated by SIGKILL mid-drain and would be measuring a kill while
being labelled a drain.

### UDP can never be `drained-clean-close`

UDP has no close handshake, so there is nothing for the probe to announce. In v0.1 the
only honest UDP outcomes are `severed` and `survived-observation-window`. A UDP severance
record always carries the sequence number of the last answered datagram, which is the
useful quantity: how far the flow got before the far end stopped answering.

### One replica

With more than one replica, a flow's fate depends on which pod it landed on and on
whether the Service rebalanced it. v0.1 runs exactly one, so every flow outcome is
attributable to one pod's termination. Multi-pod is listed as out of scope for exactly
this reason, not because it is uninteresting.

---

## Findings

> **No cluster runs have been recorded yet.** The tables below are the shape the results
> take; they contain no data. Run `make reproduce` and paste the environment block and the
> three summaries in.

### Run metadata

| | |
| --- | --- |
| Date | _not recorded_ |
| drainwatch version / commit | _not recorded_ |
| Kubernetes version | _not recorded_ |
| Nodes / container runtime | _not recorded_ |
| kube-proxy mode | _not recorded_ |
| CNI | _not recorded_ |
| Host OS / arch | _not recorded_ |

### Arm 1 — `drain`

Expected shape, from the loopback tests and the design: TCP flows receive a `bye` and a
FIN and are recorded `drained-clean-close`; UDP flows go silent when the pod's networking
goes away and are recorded `severed`. What is genuinely unknown until measured: the
interval from SIGTERM to `endpointslice_ready_false`, whether endpoint removal precedes or
follows the last flow terminal, and how much the probe's approximate clock offset moves
the `sigterm_received` entry.

| Metric | Value |
| --- | --- |
| tcp drained / severed / read-timeout / survived | _not recorded_ |
| udp drained / severed / read-timeout / survived | _not recorded_ |
| trigger → sigterm | _not recorded_ |
| sigterm → endpoint `ready:false` | _not recorded_ |
| trigger → endpoint removed | _not recorded_ |
| trigger → container terminated | _not recorded_ |
| sigterm → last flow terminal | _not recorded_ |

### Arm 2 — `exit-now`

| Metric | Value |
| --- | --- |
| tcp drained / severed / read-timeout / survived | _not recorded_ |
| udp drained / severed / read-timeout / survived | _not recorded_ |
| trigger → sigterm | _not recorded_ |
| sigterm → endpoint `ready:false` | _not recorded_ |
| trigger → container terminated | _not recorded_ |
| sigterm → last flow terminal | _not recorded_ |

### Arm 3 — `ignore`

The arm where the grace-period boundary is the whole point: the application never exits,
so the kubelet SIGKILLs it and `container_terminated` should carry `exitCode=137`
(128 + SIGKILL) at roughly `trigger + grace-period`.

| Metric | Value |
| --- | --- |
| tcp drained / severed / read-timeout / survived | _not recorded_ |
| udp drained / severed / read-timeout / survived | _not recorded_ |
| trigger → container terminated (SIGKILL boundary) | _not recorded_ |
| container exit code | _not recorded_ |
| sigterm → last flow terminal | _not recorded_ |

### Observations

_To be written from the recorded runs. Anything in this section must be traceable to a
`report.json` in the repository or to a pasted timeline._

---

## Reproducing

```bash
make reproduce
```

Six to eight minutes. Creates a clean kind cluster, builds the image, runs the three arms,
and leaves:

```
out/drain/report.json
out/exit-now/report.json
out/ignore/report.json
out/<arm>.log
```

Then `make kind-down`.

To vary one thing at a time:

```bash
bin/drainwatch run --drain-behavior drain --grace-period 60 --out out/grace60
bin/drainwatch run --trigger evict --out out/evict
bin/drainwatch run --tcp-flows 50 --udp-flows 50 --out out/50flows
bin/drainwatch run --repeat 5 --out out/repeat5     # 5 trials plus summary.json
```

## Known limitations of the method

- **The client is on the host, not in the cluster.** Every flow traverses the kind port
  mapping and a NodePort DNAT. That is a realistic path for external traffic, but it is
  not the pod-to-pod path, and pod-to-pod results may differ.
- **kind is not a production cluster.** Nodes are containers on one machine; there is no
  real network between them. Absolute latencies are not transferable.
- **`k8s` timestamps are receipt times.** drainwatch measures when it learned about a
  change, not when the API server made it. The difference is small on a local cluster and
  is not measured.
- **Probe timestamps cross an unsynchronised clock boundary.** Marked `approximate`
  throughout; do not read `trigger → sigterm` as a sub-millisecond figure.
- **UDP severance is a client-side inference.** Six unanswered datagrams is silence, not
  proof of a torn-down path. The record says `severed` and names the rule that produced it.
- **No conntrack inspection.** Whether a conntrack entry survived a kube-proxy rule update
  is not observed. Only the flow's fate is.
