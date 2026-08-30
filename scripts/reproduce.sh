#!/usr/bin/env bash
#
# scripts/reproduce.sh - the drainwatch v0.1 experiment.
#
# Creates a clean two-node kind cluster and runs three trials that differ in
# exactly one variable: what the application does when it receives SIGTERM.
#
#   drain     stop accepting, keep serving established flows, announce the close
#   exit-now  abandon established flows and exit(0)
#   ignore    keep serving and never exit; the kubelet SIGKILLs at the boundary
#
# Everything else - flow counts, grace period, trigger, settle time - is held
# constant, so any difference between the three reports is attributable to the
# SIGTERM behaviour and nothing else.
#
# Expect this to take roughly six to eight minutes.

set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

OUT="${OUT:-out}"
CLUSTER="${KIND_CLUSTER:-drainwatch}"
GRACE="${GRACE:-30}"
TCP_FLOWS="${TCP_FLOWS:-10}"
UDP_FLOWS="${UDP_FLOWS:-10}"
BIN="bin/drainwatch"

fail() { echo "reproduce: $*" >&2; exit 1; }

# ---------------------------------------------------------------------------
# Preconditions. Fail loudly, before anything is created.
# ---------------------------------------------------------------------------
for tool in docker kind go; do
  command -v "$tool" >/dev/null 2>&1 || fail "$tool is not on PATH (invariant: reproduce needs Docker, kind and Go; install it and retry)"
done
docker info >/dev/null 2>&1 || fail "the Docker daemon is not reachable (invariant: kind needs a running Docker daemon; start Docker and retry)"

echo "reproduce: recreating a clean kind cluster '$CLUSTER'"
kind delete cluster --name "$CLUSTER" >/dev/null 2>&1 || true
kind create cluster --name "$CLUSTER" --config deploy/kind/kind-config.yaml --wait 120s

echo "reproduce: building drainwatch and the probe image"
make --no-print-directory build
make --no-print-directory probe-image

rm -rf "$OUT"
mkdir -p "$OUT"

# ---------------------------------------------------------------------------
# Three arms, one variable.
# ---------------------------------------------------------------------------
for behavior in drain exit-now ignore; do
  echo
  echo "================================================================"
  echo "reproduce: trial arm '$behavior'"
  echo "================================================================"
  "$BIN" run \
    --drain-behavior "$behavior" \
    --grace-period "$GRACE" \
    --tcp-flows "$TCP_FLOWS" \
    --udp-flows "$UDP_FLOWS" \
    --out "$OUT/$behavior" \
    2>&1 | tee "$OUT/$behavior.log"
done

# ---------------------------------------------------------------------------
# Collected results.
# ---------------------------------------------------------------------------
echo
echo "================================================================"
echo "reproduce: all three arms complete"
echo "================================================================"
for behavior in drain exit-now ignore; do
  echo
  echo "--- $behavior ---"
  # The per-arm summary table is the last block of each run's stdout.
  sed -n '/^SUMMARY$/,/^CLOCKS$/p' "$OUT/$behavior.log" | sed '$d'
done

echo
echo "reports:"
for behavior in drain exit-now ignore; do
  echo "  $behavior:  $OUT/$behavior/report.json"
done
echo "  logs:      $OUT/<arm>.log"
echo
echo "The cluster is left running. Remove it with: make kind-down"
