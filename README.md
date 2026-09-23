# drainwatch

**When a Kubernetes pod terminates, what actually happens to its established connections?**

drainwatch answers that with recorded evidence. It holds open long-lived TCP and UDP
flows against a workload, triggers termination, and emits a machine-readable timeline
showing exactly when SIGTERM was delivered, when the endpoint left rotation, and when —
and how — each flow died or drained.

```bash
make demo
```

That creates a two-node kind cluster, builds and side-loads the probe image, runs one
trial with the defaults, prints the table below, and leaves `report.json` in `./out/`.

```
SUMMARY
  PROTO  TOTAL  DRAINED  SEVERED  READ_TIMEOUT  SURVIVED_WINDOW  NOT_MEASURED
  tcp    10     10       0        0             0                0
  udp    10     0        10       0             0                0

  trigger -> sigterm               63 ms
  sigterm -> endpoint ready:false  -8 ms
  trigger -> endpoint removed      25739 ms
  trigger -> container terminated  25599 ms
  sigterm -> last flow terminal    25006 ms

FLOWS
  ID        PROTO  OUTCOME              T_TERMINAL  LAST_SEQ  DETAIL
  tcp-0001  tcp    drained-clean-close  25066 ms    70        fin after drain announcement
  udp-0001  udp    severed              2382 ms     24        answered by probe instance drainwatch-probe-5497cd7d46-tvb75, was drainwatch-probe-5497cd7d46-nj9t4
```

(The real output has one row per flow and a TIMELINE section above; 18 flow rows are
elided here. The rest is verbatim.)

That is a real recorded run, not a mock-up: it is the `drain` arm of
[`results/2026-08-31-kind-v1.34.0/`](results/2026-08-31-kind-v1.34.0/), on kind with
Kubernetes v1.34.0 and kube-proxy in iptables mode. Two things in it are worth a second
look, and both are the point of the tool:

- **`sigterm -> endpoint ready:false` is −8 ms.** Not a time machine — the two timestamps
  come from different hosts' clocks, and the report says so in a note. The honest reading
  is "simultaneous to within measurement resolution", which already contradicts the common
  assumption that endpoint removal precedes SIGTERM by a usable margin.
- **The UDP flows were severed at 2.38 s while the probe was still serving.** They were
  silently re-homed onto the replacement pod the ReplicaSet created. A graceful shutdown
  routine bought the TCP connections 25 seconds and bought the UDP flows nothing.

---

## What it measures

For every trial, drainwatch records four independent event sources and merges them into
one timeline:

| Source | What it contributes | Precision |
| --- | --- | --- |
| `orchestrator` | the trigger instant (t=0), settle and observation-window boundaries | exact (one monotonic clock) |
| `client` | per-flow terminal events: when and how each flow ended | exact (same clock) |
| `k8s` | pod `deletionTimestamp`, container termination and exit code, EndpointSlice `ready:false`, endpoint removal | receipt time — an upper bound on when it happened |
| `probe` | SIGTERM delivery, readiness flip, drain start/deadline, per-flow close | approximate — cross-host wall clocks, tens of ms |

The per-flow outcome is a **closed enum**. There is no "other", and no empty string:

| Outcome | Meaning |
| --- | --- |
| `drained-clean-close` | the server announced the close on the wire, then sent FIN. The only outcome that licenses the word "drained". |
| `severed` | the flow ended without the server completing it: RST, an unannounced FIN mid-heartbeat, EPIPE, ICMP port unreachable, or UDP silence. The mechanism is in the flow's `detail`. |
| `read-timeout` | no bytes for the whole flow timeout, and no socket error. The flow went quiet without provably dying. |
| `survived-observation-window` | the flow was still alive when the window closed. **Not** a drained flow — a flow whose fate was not observed. |
| `not-measured` | no usable observation exists for this flow. |

### Three arms, one variable

`make reproduce` runs three trials that differ only in what the application does on
SIGTERM (`DRAIN_BEHAVIOR` on the probe):

- **`drain`** — stop accepting, fail readiness, keep serving established flows, then
  announce and close them when the drain window expires.
- **`exit-now`** — abandon established flows and `exit(0)`.
- **`ignore`** — keep serving, never exit; the kubelet sends SIGKILL at the grace-period
  boundary, which appears in the timeline as `container_terminated exitCode=137`.

Flow counts, grace period, trigger and settle time are held constant, so any difference
between the three reports is attributable to the SIGTERM behaviour and nothing else.

## Design rules

These are not aspirations; they are enforced in code and covered by tests.

1. **Evidence over assertion.** Every run emits a full trial record: environment,
   configuration, method, timeline, per-flow outcomes. A run that cannot record its
   environment refuses to start.
2. **Preflight, and fail loudly.** Six checks run before anything is measured. The first
   failure aborts the run and names the check, the invariant it enforces, and what to
   look at. A broken harness never emits a report.
3. **Deterministic where possible.** No RNG anywhere in the measurement path. Flow IDs
   are sequential (`tcp-0001`, `udp-0001`, …). Timestamps are monotonic wherever the
   comparison is local. Outcome classification is a pure function of the event history.
4. **The absence of an observation is `not-measured`, never zero.** Every optional
   quantity is a JSON `null`, in the report and in the table alike.

Where cross-source clock alignment is approximate, the report says so in a `clock_note`
field rather than pretending to sub-millisecond precision, and every affected timeline
entry carries `"approximate": true`.

## Install and run

Requirements: Go 1.22+, Docker, and [kind](https://kind.sigs.k8s.io/). No other runtime
dependencies — drainwatch is a single static binary built from the standard library plus
`k8s.io/client-go`.

**On macOS**, `make demo` and `make reproduce` run the orchestrator *inside* the kind node
rather than on your host. Docker Desktop does not reliably forward published UDP ports — in
testing the mapping carried the first ~50 datagrams and then stopped, while TCP kept
working — so a host-side run fails the `udp-flows-replying` preflight check. UDP through
the same NodePort works fine from inside the cluster, so `scripts/dwrun.sh` moves the
orchestrator across that boundary and copies the reports back. This is automatic; set
`DRAINWATCH_RUNNER=host` to force the host-side path anyway.

```bash
git clone https://github.com/jaynirmal15/drainwatch
cd drainwatch
make demo
```

Individual steps, if you would rather run them yourself:

```bash
make build          # bin/drainwatch, with version and commit stamped in
make kind-up        # create the 2-node cluster and side-load the probe image
bin/drainwatch run --out out
make kind-down      # tear the cluster down
```

### Installing the binary on its own

```bash
go install github.com/jaynirmal15/drainwatch/cmd/drainwatch@latest
```

If `drainwatch` isn't found after installing, add Go's bin directory to your PATH:

```bash
export PATH="$PATH:$(go env GOPATH)/bin"
```

`drainwatch run` still expects to be started from a clone: it reads
`deploy/manifests/probe.yaml` from the working directory (override with `--manifest`), and
the probe image it deploys is built by `make kind-up`.

### The three subcommands

One binary.

```bash
drainwatch run     # the orchestrator: deploy, verify, hold, trigger, observe, report
drainwatch probe   # the in-cluster workload whose termination is measured
drainwatch client  # the flow generator, exposed for manual runs
```

`drainwatch run --help` lists every flag. The ones you are most likely to change:

```
--drain-behavior   drain | exit-now | ignore     (default drain)
--trigger          delete | evict | scale        (default delete)
--grace-period     terminationGracePeriodSeconds (default 30)
--tcp-flows        long-lived TCP flows          (default 10)
--udp-flows        UDP flows                     (default 10)
--settle-seconds   steady state before trigger   (default 10)
--observe-timeout  observation window            (default grace-period + 30)
--repeat           trials into one --out dir     (default 1)
--out              output directory              (default ./out)
```

## The report

`report.json` is one trial plus the context needed to interpret it.

```json
{
  "drainwatch_version": "0.1.0",
  "git_commit": "abc1234",
  "environment": {
    "kubernetes_version": "v1.31.0",
    "node_count": 2,
    "nodes": [{ "name": "drainwatch-worker", "container_runtime": "containerd://1.7.18" }],
    "kube_proxy_mode": "iptables",
    "cni": "kindnet (identified by DaemonSet name; best effort)",
    "os": "darwin", "arch": "arm64",
    "wall_clock_start": "2026-01-01T12:00:00Z",
    "warnings": []
  },
  "trial": {
    "id": "trial-001",
    "config": { "tcp_flows": 10, "udp_flows": 10, "grace_period_seconds": 30,
                "drain_behavior": "drain", "trigger": "delete" },
    "timeline": [
      { "t_ms": 0,    "source": "orchestrator", "event": "trigger_issued" },
      { "t_ms": 112,  "source": "probe",        "event": "sigterm_received", "approximate": true },
      { "t_ms": 340,  "source": "k8s",          "event": "endpointslice_ready_false" },
      { "t_ms": 25390,"source": "k8s",          "event": "endpointslice_endpoint_removed" }
    ],
    "flows": [
      { "id": "tcp-0001", "proto": "tcp",
        "outcome": "drained-clean-close", "t_terminal_ms": 25133,
        "last_heartbeat_seq": 71 },
      { "id": "udp-0004", "proto": "udp",
        "outcome": "severed", "t_terminal_ms": 3550,
        "last_answered_seq": 17 }
    ],
    "summary": {
      "tcp": { "total": 10, "drained": 10, "severed": 0, "read_timeout": 0,
               "survived_window": 0, "not_measured": 0 },
      "udp": { "total": 10, "drained": 0, "severed": 10, "read_timeout": 0,
               "survived_window": 0, "not_measured": 0 },
      "sigterm_to_ready_false_ms": 228,
      "sigterm_to_last_flow_terminal_ms": 25022
    },
    "clock_note": "t_ms is milliseconds relative to trigger_issued. ..."
  }
}
```

With `--repeat N`, each trial lands in `out/trial-00N/report.json` and a `summary.json`
collects the whole run.

## Findings

Recorded results, the full methodology, and the reasoning behind each design choice live
in **[EXPERIMENTS.md](EXPERIMENTS.md)**. The raw reports they are drawn from are committed
under [`results/`](results/2026-08-31-kind-v1.34.0/).

The headline from the first recorded run — same cluster, same trigger, same 30 s grace
period, one variable changed:

| SIGTERM behaviour | TCP flows | When they ended | Container exit |
| --- | --- | --- | --- |
| `exit-now` | 10/10 severed (RST) | **46 ms** | `exitCode=0` |
| `drain` | 10/10 drained cleanly | **25.07 s** | `exitCode=0` |
| `ignore` | 10/10 severed (SIGKILL) | **30.06 s** | **`exitCode=137`** |

UDP was severed in all three arms by being re-homed onto the replacement pod. In
`exit-now`, connections died ~330 ms *before* the endpoint was removed from the
EndpointSlice.

Companion articles:

- _Article 1 — placeholder link, forthcoming._
- _Article 2 — placeholder link, forthcoming._

## Explicitly out of scope for v0.1

These are not oversights. They are stated so that no result from drainwatch is read as
covering them.

- **Attaching to an arbitrary existing workload** (`--workload attach`). The flag exists,
  prints "not implemented in v0.1", and exits 2. v0.1 measures a workload it deploys
  itself, so that the probe's SIGTERM handling and readiness behaviour are known
  quantities rather than assumptions.
- **NLB / cloud load-balancer deregistration timing.** drainwatch observes the
  EndpointSlice, not any cloud provider's target group.
- **ECS, service meshes, and Windows nodes.**
- **Multi-pod and rolling-update scenarios.** v0.1 runs exactly one replica, so that a
  flow's fate is attributable to one pod's termination.
- **Any claim about conntrack internals** beyond what the flow outcomes show. drainwatch
  records what happened to the flows; it does not read or interpret conntrack tables.

## Development

```bash
make test    # unit tests: flow state machine, outcome classification, report schema
make lint    # gofmt check plus go vet
make help    # every target
```

The test suite includes loopback integration tests that wire the real probe to the real
flow generator with no Kubernetes involved, asserting that each of the three SIGTERM
behaviours produces the wire events the classifier expects — a drain puts an
announcement on the wire before its FIN, and an abrupt exit does not.

Those tests cannot cover kube-proxy, EndpointSlices or the grace-period boundary. Three
defects in drainwatch were found only by running against a real cluster — including one
where re-homed UDP flows were reported as having survived untouched. They are described
in [EXPERIMENTS.md](EXPERIMENTS.md).

## Licence

MIT. See [LICENSE](LICENSE).
