# drainwatch experiments

This file holds the methodology and the recorded findings. Two runs are recorded: an
initial three-arm run at n=1, and a five-arm matrix at n=5 per arm.

---

## Status

| | |
| --- | --- |
| Harness | v0.1, complete |
| Unit and loopback tests | passing (`make test`) |
| Cluster runs recorded | **3 arms, n=1 each**, 2026-08-31; **5 arms, n=5 each**, 2026-09-01 |
| Raw reports (n=1) | [`results/2026-08-31-kind-v1.34.0/`](results/2026-08-31-kind-v1.34.0/) |
| Raw reports (n=5) | [`results/2026-09-01-repeat5-kind-v1.34.0/`](results/2026-09-01-repeat5-kind-v1.34.0/) |

Every number below is copied from a `report.json` in one of those directories. Nothing
here is an estimate. The n=1 numbers are single observations; the n=5 numbers are
medians with their full observed range, computed by `drainwatch aggregate`.

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

Recorded 2026-08-31 by `make reproduce`. Raw reports:
[`results/2026-08-31-kind-v1.34.0/`](results/2026-08-31-kind-v1.34.0/), with the full
console output in `reproduce.log`.

### Run metadata

| | |
| --- | --- |
| Date | 2026-08-31 (wall clock start 01:03:21Z) |
| drainwatch version / commit | 0.1.0 / `d36d40d` |
| Kubernetes version | v1.34.0 (kind, 2 nodes) |
| Nodes / container runtime | drainwatch-control-plane, drainwatch-worker — containerd://2.1.3, kubelet v1.34.0, Debian 12 (bookworm), amd64 |
| kube-proxy mode | `iptables` (read from the ConfigMap) |
| CNI | kindnet (identified by DaemonSet name; best effort) |
| Orchestrator host | linux/amd64 — running inside the kind worker node, see [the macOS note](#the-orchestrator-ran-inside-the-node) |
| Environment warnings | none: every field was readable |
| Config held constant | 10 TCP flows, 10 UDP flows, grace period 30s, `--trigger delete`, settle 10s, observation window 60s |

### Results

| | `drain` | `exit-now` | `ignore` |
| --- | --- | --- | --- |
| TCP outcome | **10/10 drained-clean-close** | **10/10 severed** (`econnreset`) | **10/10 severed** (unannounced FIN) |
| TCP terminal | 25065–25069 ms | **46 ms** | 30057–30058 ms |
| UDP outcome | 10/10 severed (re-homed) | 10/10 severed (re-homed) | 10/10 severed (re-homed) |
| UDP terminal | 2381–2384 ms | 2410 ms | 30428 ms |
| trigger → SIGTERM | 63 ms | 45 ms | 31 ms |
| SIGTERM → endpoint `ready:false` | −8 ms | −16 ms | 0 ms |
| trigger → endpoint removed | 25739 ms | 376 ms | 30328 ms |
| trigger → container terminated | 25599 ms | 340 ms | **30323 ms** |
| container exit | `exitCode=0 reason=Completed` | `exitCode=0 reason=Completed` | **`exitCode=137 reason=Error`** |

### Observations

**1. SIGTERM arrives within ~30–60 ms of the delete call, and the endpoint leaves rotation
at essentially the same moment.** `trigger → sigterm` was 31–63 ms across the three arms.
`sigterm → ready:false` came out at −8, −16 and 0 ms. The negative values are not evidence
that the endpoint left rotation before SIGTERM: the two timestamps come from different
hosts' clocks, and the report says so in a note on each affected trial. The honest reading
is that on this cluster the two events are simultaneous to within the measurement's
resolution — which is itself the useful result, because the common assumption is that
endpoint removal reliably *precedes* SIGTERM by a usable margin. It does not here.

**2. What the application does on SIGTERM determines the TCP outcome completely, and the
spread is three orders of magnitude.** Same cluster, same trigger, same grace period:

- `exit-now` killed every connection at **46 ms**, with RST.
- `drain` held all ten to **25.07 s** and closed them cleanly.
- `ignore` held them to **30.06 s**, where SIGKILL cut them mid-stream.

**3. In `exit-now`, connections died ~330 ms before the endpoint was removed.** TCP flows
were severed at 46 ms; the endpoint was removed from the EndpointSlice at 376 ms. For that
window the Service still advertised an endpoint whose process had already gone. This is the
concrete shape of the race that connection-draining guidance is meant to address, measured
rather than asserted.

**4. `ignore` shows the grace-period boundary exactly where it should be.** TCP flows were
severed at 30057 ms and the container terminated at 30323 ms with `exitCode=137` — 128 +
SIGKILL — against a 30 s grace period. An application that never exits does not get to
choose when its connections die.

**5. UDP flows were re-homed onto a replacement pod in every arm, and no application-level
drain can prevent it.** `--trigger delete` on a Deployment-managed pod causes the
ReplicaSet to create a replacement. Because UDP is connectionless, the client's flows were
picked up by that replacement, which the probe made visible by putting its pod name on the
wire.

The timing differs between arms in a way worth noting. In `drain` and `exit-now`,
re-homing happened at ~2.4 s — roughly when the replacement became ready — even though in
`drain` the original probe was still serving UDP for another 22 seconds. In `ignore`,
re-homing happened at 30.4 s, just after the original was killed. The pattern is consistent
with kube-proxy steering new datagrams away from an endpoint once it leaves rotation,
independently of whether the application is still willing to serve them; the `ignore` arm's
endpoint stayed in service until the process died. drainwatch did not inspect kube-proxy or
conntrack to confirm that mechanism, so treat the explanation as inference and the
timestamps as the measurement.

The practical point stands regardless of mechanism: **a graceful shutdown routine buys a
TCP connection 25 seconds and buys a UDP flow nothing.** Draining is a property of the
connection, and UDP does not have one.

**6. This finding was originally hidden by a defect in drainwatch itself.** In the first
cluster run, the UDP flows were reported as `survived-observation-window`, because the
client kept receiving acks for the full 60 seconds and had no way to know they were coming
from a different pod. That reads as "nothing happened to these flows", which is the
opposite of the truth. The probe now identifies its process on every heartbeat and ack, and
a flow answered by a new instance is classified `severed`. Two other defects surfaced the
same way and are recorded in commit `d36d40d`.

---

## Repeat-5 matrix

Recorded 2026-09-01 by [`scripts/repeat5-matrix.sh`](scripts/repeat5-matrix.sh). Raw
reports: [`results/2026-09-01-repeat5-kind-v1.34.0/`](results/2026-09-01-repeat5-kind-v1.34.0/),
one directory per arm, five `trial-NNN/report.json` each, plus `aggregate.json` and
`aggregate.txt` from `drainwatch aggregate`.

### Run metadata

| | |
| --- | --- |
| Date | 2026-09-01, 00:45Z to 01:15Z |
| drainwatch version / commit | `0.1.0-10-g173d102` / `173d102` — one frozen build for all 25 trials |
| Kubernetes version | v1.34.0 (kind v0.30.0, 2 nodes) — identical across all 25 |
| kube-proxy mode | `iptables` — identical across all 25 |
| Flows per trial | 10 TCP, 10 UDP |
| Grace period | 30 s; probe drain window 25 s (`grace - 5`) |
| Cluster | recreated fresh per arm; creation times in each `arm-*/cluster.txt` |

All 25 trials carry the same `drainwatch_version`, `git_commit`, Kubernetes version and
kube-proxy mode. The aggregator checks this and would have flagged any drift.

### Core table — does the application's SIGTERM behaviour change the outcome?

Median (min–max) in ms across 5 repeats. All intervals here are **same-clock**: both
endpoints come from the orchestrator's own clock, exact relative to the trigger.

| arm | behavior | trigger | TCP | UDP | first TCP end | last TCP end | endpoint removed | container exit | exit code |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| A | `exit-now` | delete | 10/10 severed | 10/10 severed | 41 (30–96) | 45 (30–98) | 293 (251–591) | 285 (246–584) | 0 |
| B | `drain` | delete | 10/10 drained | 10/10 severed | 25041 (25027–25110) | 25043 (25029–25113) | 25296 (25243–25466) | 25289 (25237–25460) | 0 |
| C | `ignore` | delete | 10/10 severed | 10/10 severed | 30055 (30050–30062) | 30056 (30050–30062) | 30259 (30244–30312) | 30255 (30240–30305) | 137 |

Every repeat in every arm agreed on outcome counts and exit code. The three arms differ
only in `DRAIN_BEHAVIOR`, and they separate cleanly:

* **A** — TCP is gone in a median of 45 ms, by RST. Nothing drains.
* **B** — TCP survives to the end of the 25 s drain window and closes cleanly, announced.
* **C** — TCP dies at 30056 ms, i.e. within 62 ms of the 30 s grace boundary, and the
  container exits **137** (128 + SIGKILL) in all five repeats. This is the kubelet
  killing the process, not the application ending anything.

### Secondary table — does the trigger change the physics?

| arm | behavior | trigger | TCP | UDP | last TCP end | endpoint removed | container exit | exit code |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| B | `drain` | delete | 10/10 drained | 10/10 severed | 25043 (25029–25113) | 25296 (25243–25466) | 25289 (25237–25460) | 0 |
| D | `drain` | evict | 10/10 drained | 10/10 severed | 25037 (25034–25065) | 25243 (25223–25780) | 25239 (25216–25773) | 0 |
| E | `drain` | scale | 10/10 drained | 10/10 severed | 25046 (25042–25051) | 25256 (25220–25356) | 25252 (25215–25320) | 0 |

**For TCP, the trigger does not change the physics.** Delete, eviction and scale-to-zero
produce last-TCP-end medians of 25043, 25037 and 25046 ms — a spread of 9 ms between
arms, against a within-arm range of up to 84 ms. Whatever the API verb, the pod gets a
SIGTERM and the drain runs to its window.

**For UDP, the trigger changes everything.** See below.

### The UDP finding, restated against arms D and E

The n=1 run recorded UDP flows as severed with the detail *"answered by probe instance
X, was Y"* — the flow was re-homed onto a **different pod** than the one it started
with. The matrix confirms this and, through arm E, isolates its cause.

| arm | trigger | replacement pod? | UDP severance route (all 5 repeats) | first UDP end | container exit | UDP end − container exit |
| --- | --- | --- | --- | --- | --- | --- |
| B | delete | yes | rehomed onto a different probe instance | 2417 (2411–2433) | 25289 (25237–25460) | **−22861 (−23045 to −22806)** |
| D | evict | yes | rehomed onto a different probe instance | 2420 (2419–2436) | 25239 (25216–25773) | **−22811 (−23353 to −22790)** |
| E | scale | **no** | `econnrefused` (ICMP port unreachable) | 25426 (25404–25431) | 25252 (25215–25320) | **+177 (+111 to +189)** |

The last column is computed per repeat and then aggregated — it is the median of each
repeat's own difference, not the difference between the two medians beside it. Both of
its endpoints come from the orchestrator's clock, so it is exact.

Read the last column carefully. In arms B and D the UDP flows stopped being served by
their original pod roughly **23 seconds before that pod exited**. The probe was still
alive, still answering TCP heartbeats, still inside its drain window — and the UDP
datagrams were already being answered by a different pod. Eviction re-homes UDP exactly
the way delete does; the median difference between the two is 3 ms.

Arm E is the control. Scaling to zero creates no replacement, so there is nothing to
re-home onto, and the re-homing path cannot occur. It does not. Instead every UDP flow
in all five repeats died by **ICMP port unreachable**, a median of 177 ms *after* the
container exited — kube-proxy rejecting traffic to a Service with no endpoints. Not silence: an
explicit rejection, which is why it lands a few hundred milliseconds after the exit
rather than at the 3 s silence threshold the harness would otherwise have applied.

The conclusion is not "UDP drains badly". It is that **a UDP flow has no drain at all**:
with a replacement present it is silently migrated to a different backend while the old
one is still running and still healthy, and with no replacement present it is rejected
outright. Neither is a drain, and the application cannot influence either — all three of
arms B, D and E ran `DRAIN_BEHAVIOR=drain`.

Traceable to: [`arm-B/trial-001/report.json`](results/2026-09-01-repeat5-kind-v1.34.0/arm-B/trial-001/report.json),
[`arm-D/trial-001/report.json`](results/2026-09-01-repeat5-kind-v1.34.0/arm-D/trial-001/report.json),
[`arm-E/trial-001/report.json`](results/2026-09-01-repeat5-kind-v1.34.0/arm-E/trial-001/report.json)
and their four siblings each.

One qualification the aggregator raised and this file will not bury: in arm A,
`trial-001` recorded **both** routes across its ten UDP flows (`econnrefused` on some,
re-homing on others), while `trial-002` through `trial-005` recorded re-homing only. The
outcome counts are identical in all five (10/10 severed); only the route differed. In
arm A the pod exits within ~285 ms, so the window in which the endpoint is gone but the
replacement is not yet ready is open long enough for some flows to be rejected before
others are re-homed. It is a race, and it resolved differently in one repeat out of five.

### The endpoint-lag finding, restated with spread

In the `exit-now` arm, the process is dead long before the Service stops advertising it.

| metric (arm A, n=5) | median | min | max |
| --- | --- | --- | --- |
| last TCP flow dead at | 45 ms | 30 ms | 98 ms |
| endpoint removed from EndpointSlice at | 293 ms | 251 ms | 591 ms |
| **window where the Service advertised a dead process** | **221 ms** | **195 ms** | **521 ms** |

The bottom row is measured per repeat and then aggregated, not derived by subtracting the
two medians above it. Both of its endpoints come from the orchestrator's clock, so the
window is exact.

For a median of 221 ms, and in the worst of five repeats 521 ms, the EndpointSlice named
an endpoint whose process had already sent RST to every connection it held. Any traffic
routed on that advertisement in that window had nowhere to land. The same window exists
in the other arms (arm B 253 ms, arm C 206 ms, arm D 206 ms, arm E 211 ms), but it only
matters in `exit-now`, because that is the only arm where the process is already dead
while it is open.

### What changed from n=1

Most numbers did not move. The `exit-now` last-TCP-end was 46 ms at n=1 and 45 (30–98) at
n=5; the `ignore` boundary was 30058 ms and is 30056 (30050–30062); the `drain` TCP
outcome, the UDP re-homing mechanism and every outcome count are unchanged.

Two things moved enough to state plainly:

1. **The n=1 `drain` arm's cluster-side timings sat above the n=5 range.** Endpoint
   removal was 25739 ms at n=1 against 25296 (25243–25466) at n=5 — 273 ms above the n=5
   maximum. Container exit was 25599 ms against 25289 (25237–25460), 139 ms above the
   maximum. The likely reason is procedural: the n=1 run executed all three arms on a
   single cluster via `make reproduce`, so the `drain` arm ran third on a cluster that had
   already torn down two workloads, whereas the matrix gives every arm a fresh cluster.
   That is a hypothesis about the difference, not a measurement of it; nothing here tested
   it.

2. **The spread is wider than a single run suggests.** `exit-now` endpoint removal was
   376 ms at n=1, which reads like a stable figure until five repeats put it at 293
   (251–591) — a range of 340 ms, with the n=1 observation sitting near the middle. Any
   single-run number from this harness should be read as one draw from a distribution
   that is a few hundred milliseconds wide on the cluster-side events, and much tighter
   (tens of ms) on the flow-side ones.

Nothing in the n=1 findings was contradicted.

---

## Reproducing

The three-arm run at n=1:

```bash
make reproduce
```

The five-arm matrix at n=5 per arm (roughly 30 minutes; fresh cluster per arm):

```bash
scripts/repeat5-matrix.sh
```

It refuses to start from a dirty working tree, freezes one binary and one probe image for
all 25 trials, and aggregates with:

```bash
drainwatch aggregate results/<dir>/arm-A results/<dir>/arm-B results/<dir>/arm-C results/<dir>/arm-D results/<dir>/arm-E
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

### The orchestrator ran inside the node

On macOS, `scripts/dwrun.sh` runs the orchestrator inside the kind worker container rather
than on the host, and the recorded run above used that backend. The reason is a Docker
Desktop limitation, not a Kubernetes one: published **UDP** ports are not forwarded
reliably. In testing, the `127.0.0.1:7002 -> 30072/udp` mapping carried the first ~50
datagrams and then stopped delivering entirely, while the TCP mapping on the same node kept
working. UDP through the same NodePort worked perfectly from inside the cluster, and from
inside the node, at the same moment the host could not reach it at all.

The measured path is therefore `node -> NodePort -> kube-proxy DNAT -> pod on the same
node`, which is the path the kind config was designed around, minus Docker Desktop's
userland proxy hop. On Linux the host backend is used and the extra hop is present; that
difference is recorded in each report's `environment.os`.
