#!/usr/bin/env bash
#
# scripts/dwrun.sh - run one `drainwatch run` invocation, from wherever the
# flow generator can actually reach the probe.
#
# Two backends:
#
#   host  the orchestrator runs on your machine and reaches the probe through
#         kind's extraPortMappings (127.0.0.1:7001 / :7002). This is the
#         straightforward path and it is what the README describes.
#
#   node  the orchestrator runs inside the kind worker container and reaches
#         the probe through the NodePort directly (127.0.0.1:30071 / :30072).
#
# Why "node" exists: Docker Desktop for Mac does not reliably forward published
# UDP ports. In practice the mapping carries the first few dozen datagrams and
# then silently stops, while TCP keeps working. That is a host-to-VM boundary
# problem, not a Kubernetes one - UDP through the same NodePort works perfectly
# from inside the cluster. Rather than let the udp-flows-replying preflight check
# fail on macOS, this script moves the orchestrator to the other side of that
# boundary, which also removes a userland proxy hop from the measured path.
#
# Backend selection: $DRAINWATCH_RUNNER = auto (default) | host | node.
# "auto" picks node on Darwin and host everywhere else.
#
# Usage: scripts/dwrun.sh --out out/drain --drain-behavior drain [...]
#        Any flags are passed through to `drainwatch run`, except that the
#        backend supplies --kubeconfig, --manifest, --target-host and the ports.

set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

CLUSTER="${KIND_CLUSTER:-drainwatch}"
WORKER="${CLUSTER}-worker"
CONTROL="${CLUSTER}-control-plane"
RUNNER="${DRAINWATCH_RUNNER:-auto}"

# NodePorts as declared in deploy/manifests/probe.yaml. The node backend targets
# these directly; the host backend targets the host ports that
# deploy/kind/kind-config.yaml maps onto them.
NODEPORT_TCP="${NODEPORT_TCP:-30071}"
NODEPORT_UDP="${NODEPORT_UDP:-30072}"
HOSTPORT_TCP="${HOSTPORT_TCP:-7001}"
HOSTPORT_UDP="${HOSTPORT_UDP:-7002}"

fail() { echo "dwrun: $*" >&2; exit 1; }

if [ "$RUNNER" = "auto" ]; then
  if [ "$(uname -s)" = "Darwin" ]; then RUNNER="node"; else RUNNER="host"; fi
fi

# --- host backend -----------------------------------------------------------
if [ "$RUNNER" = "host" ]; then
  [ -x bin/drainwatch ] || fail "bin/drainwatch is missing (invariant: the host backend runs the local binary; run 'make build')"
  exec bin/drainwatch run \
    --target-host 127.0.0.1 \
    --tcp-port "$HOSTPORT_TCP" \
    --udp-port "$HOSTPORT_UDP" \
    "$@"
fi

[ "$RUNNER" = "node" ] || fail "DRAINWATCH_RUNNER=$RUNNER is not recognised (invariant: must be auto, host or node)"

# --- node backend -----------------------------------------------------------
command -v docker >/dev/null 2>&1 || fail "docker is not on PATH (invariant: the node backend execs into the kind node container)"
docker inspect "$WORKER" >/dev/null 2>&1 || fail "container $WORKER does not exist (invariant: the node backend needs a running kind cluster; run 'make kind-up')"

# The orchestrator must be a Linux binary; the node container is Linux even when
# your machine is not.
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
  -trimpath -tags netgo,osusergo \
  -ldflags "-X github.com/jaynirmal15/drainwatch/internal/report.Version=${VERSION:-$(git describe --tags 2>/dev/null | sed 's/^v//' || echo 0.1.0-dev)} -X github.com/jaynirmal15/drainwatch/internal/report.GitCommit=$(git rev-parse --short HEAD 2>/dev/null || echo unknown)" \
  -o bin/drainwatch-linux ./cmd/drainwatch

# The node needs credentials pointing at the API server's address on the kind
# network, not the loopback address your kubeconfig uses.
CP_IP="$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$CONTROL")"
[ -n "$CP_IP" ] || fail "cannot determine the control-plane container IP for $CONTROL"
TMP_KC="$(mktemp)"
trap 'rm -f "$TMP_KC"' EXIT
docker exec "$CONTROL" cat /etc/kubernetes/admin.conf > "$TMP_KC"
sed -i.bak "s#server: https://.*#server: https://${CP_IP}:6443#" "$TMP_KC" && rm -f "${TMP_KC}.bak"

docker cp bin/drainwatch-linux "$WORKER":/drainwatch >/dev/null
docker cp "$TMP_KC" "$WORKER":/kubeconfig >/dev/null
docker cp deploy/manifests/probe.yaml "$WORKER":/probe.yaml >/dev/null

# Translate the caller's --out into a path inside the container, so the reports
# can be copied back to where the caller expects them.
OUT_HOST="out"
# Bash 3.2, which is what macOS ships, treats an empty array as unbound under
# `set -u`, so every expansion of ARGS below uses the ${a[@]+"${a[@]}"} form.
ARGS=()
while [ $# -gt 0 ]; do
  case "$1" in
    --out) OUT_HOST="$2"; shift 2 ;;
    --out=*) OUT_HOST="${1#--out=}"; shift ;;
    *) ARGS+=("$1"); shift ;;
  esac
done
OUT_NODE="/out/$(basename "$OUT_HOST")"

docker exec "$WORKER" rm -rf "$OUT_NODE"
set +e
docker exec "$WORKER" /drainwatch run \
  --kubeconfig /kubeconfig \
  --manifest /probe.yaml \
  --target-host 127.0.0.1 \
  --tcp-port "$NODEPORT_TCP" \
  --udp-port "$NODEPORT_UDP" \
  --out "$OUT_NODE" \
  ${ARGS[@]+"${ARGS[@]}"}
rc=$?
set -e

# Copy the reports back even when the run failed: a failed run's partial output
# is still evidence about what went wrong.
# Replace only what this run produces. Wiping the whole directory would destroy
# anything a caller placed alongside the reports - the matrix driver writes each
# arm's cluster.txt and tees its run.log there - and those are exactly the files
# needed to interpret, or to discard, the run.
mkdir -p "$OUT_HOST"
rm -rf "$OUT_HOST"/trial-* "$OUT_HOST"/report.json "$OUT_HOST"/summary.json
if docker cp "$WORKER":"$OUT_NODE/." "$OUT_HOST/" >/dev/null 2>&1; then
  # The orchestrator printed its own paths, which are inside the node. Say where
  # the files actually are on this machine.
  echo
  echo "dwrun: the orchestrator ran inside $WORKER; its output paths above are container paths."
  echo "dwrun: reports copied to $OUT_HOST/ on this machine."
else
  echo "dwrun: WARNING: could not copy $OUT_NODE out of $WORKER; reports remain in the container" >&2
fi

exit $rc
