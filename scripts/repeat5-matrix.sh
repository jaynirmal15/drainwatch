#!/usr/bin/env bash
#
# scripts/repeat5-matrix.sh - the drainwatch v0.1.0 repeat-5 experiment matrix.
#
# Five arms, five repeats each. The core table varies only the application's
# SIGTERM behaviour; the secondary table holds the behaviour fixed at `drain`
# and varies the trigger, to test whether the trigger changes the physics.
#
#   A  exit-now / delete    D  drain / evict
#   B  drain    / delete    E  drain / scale
#   C  ignore   / delete
#
# Environment discipline, deliberate and load-bearing:
#
#   * A FRESH CLUSTER PER ARM. The Docker VM on the recording machine has under
#     4 GiB and its API server was observed timing out under sustained load. A
#     cluster that has already run five trials is not the same measurement
#     substrate as a fresh one, so each arm gets its own and the creation time
#     is recorded alongside the arm's reports.
#   * TRIALS ARE STRICTLY SEQUENTIAL. drainwatch runs its repeats in-process,
#     one after another, and this script runs one arm at a time. Two trials are
#     never in flight.
#   * A PARTIAL ARM IS NOT AN ARM. If any trial aborts, the whole arm is
#     discarded rather than averaged over. The abort is preserved under
#     discarded/ with a reason, the defect is fixed in code, and the arm is
#     rerun from repeat 1.

set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

CLUSTER="${KIND_CLUSTER:-drainwatch}"
REPEATS="${REPEATS:-5}"
DATE="${MATRIX_DATE:-$(date -u +%F)}"  # UTC, to match the timestamps inside the reports

fail() { echo "matrix: $*" >&2; exit 1; }

for tool in docker kind go; do
  command -v "$tool" >/dev/null 2>&1 || fail "$tool is not on PATH"
done
docker info >/dev/null 2>&1 || fail "the Docker daemon is not reachable"

# Provenance discipline. Every report records the commit its binaries were built
# from. If the tree is dirty that commit is stamped "-dirty", which tells a
# reader the numbers came from code that is not in the history and therefore
# cannot be reproduced from it. A recorded experiment must be traceable to a
# commit, so a dirty tree aborts here rather than quietly producing 25 reports
# nobody can tie to source. Set ALLOW_DIRTY=1 for a throwaway run.
if [ -z "${ALLOW_DIRTY:-}" ] && [ -n "$(git status --porcelain 2>/dev/null)" ]; then
  echo "matrix: the working tree has uncommitted changes:" >&2
  git status --short >&2
  fail "refusing to record an experiment from a dirty tree (invariant: every report's git_commit must name a commit in the history; commit your changes, or set ALLOW_DIRTY=1 for a throwaway run)"
fi

# arm : drain_behavior : trigger
# Freeze the build BEFORE any arm runs. Everything below uses this one binary
# and this one image. Rebuilding per arm is how the first attempt at this matrix
# ended up recording arm A from one commit and arms B-E from another.
FROZEN_COMMIT="$(git rev-parse --short HEAD)"
FROZEN_VERSION="$(git describe --tags 2>/dev/null | sed 's/^v//')"
echo "matrix: freezing the build at ${FROZEN_VERSION} (${FROZEN_COMMIT})"
mkdir -p bin
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
  -trimpath -tags netgo,osusergo \
  -ldflags "-X github.com/jaynirmal15/drainwatch/internal/report.Version=${FROZEN_VERSION} -X github.com/jaynirmal15/drainwatch/internal/report.GitCommit=${FROZEN_COMMIT}" \
  -o bin/drainwatch-matrix ./cmd/drainwatch
make --no-print-directory probe-image-build VERSION="$FROZEN_VERSION" GIT_COMMIT="$FROZEN_COMMIT" GIT_DIRTY=""
export DRAINWATCH_LINUX_BIN="$PWD/bin/drainwatch-matrix"
export VERSION="$FROZEN_VERSION"

ARMS=(
  "A:exit-now:delete"
  "B:drain:delete"
  "C:ignore:delete"
  "D:drain:evict"
  "E:drain:scale"
)

recreate_cluster() {
  echo "matrix: tearing down any existing cluster '$CLUSTER'"
  kind delete cluster --name "$CLUSTER" >/dev/null 2>&1 || true
  leftovers="$(docker ps -aq --filter "label=io.x-k8s.kind.cluster=${CLUSTER}" 2>/dev/null || true)"
  [ -n "$leftovers" ] && docker rm -f $leftovers >/dev/null 2>&1 || true

  echo "matrix: creating a fresh cluster for this arm"
  CLUSTER_CREATE_START="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  CLUSTER_CREATE_EPOCH="$(date +%s)"
  kind create cluster --name "$CLUSTER" --config deploy/kind/kind-config.yaml --wait 120s
  CLUSTER_CREATE_SECONDS=$(( $(date +%s) - CLUSTER_CREATE_EPOCH ))
  # Load the frozen image; do not rebuild it.
  make --no-print-directory probe-image-load
}

OUTDIR=""

for spec in "${ARMS[@]}"; do
  ARM="${spec%%:*}"; rest="${spec#*:}"
  BEHAVIOR="${rest%%:*}"; TRIGGER="${rest##*:}"

  echo
  echo "================================================================"
  echo "matrix: arm $ARM  behavior=$BEHAVIOR  trigger=$TRIGGER  repeats=$REPEATS"
  echo "================================================================"

  recreate_cluster

  # The output directory is named for the Kubernetes version actually running,
  # which is only knowable once a cluster exists.
  if [ -z "$OUTDIR" ]; then
    K8S="$(docker exec "${CLUSTER}-control-plane" kubelet --version 2>/dev/null | awk '{print $2}')"
    [ -n "$K8S" ] || fail "cannot determine the Kubernetes version from the control-plane node"
    OUTDIR="results/${DATE}-repeat5-kind-${K8S}"
    echo "matrix: recording into $OUTDIR"
    mkdir -p "$OUTDIR"
  fi

  ARMDIR="$OUTDIR/arm-${ARM}"
  mkdir -p "$ARMDIR"

  cat > "$ARMDIR/cluster.txt" <<EOF
arm: $ARM
drain_behavior: $BEHAVIOR
trigger: $TRIGGER
repeats: $REPEATS
cluster_name: $CLUSTER
cluster_created_utc: $CLUSTER_CREATE_START
cluster_create_seconds: $CLUSTER_CREATE_SECONDS
kind_version: $(kind version 2>/dev/null | head -1)
drainwatch_version: $FROZEN_VERSION
drainwatch_commit: $FROZEN_COMMIT
EOF

  set +e
  scripts/dwrun.sh \
    --drain-behavior "$BEHAVIOR" \
    --trigger "$TRIGGER" \
    --repeat "$REPEATS" \
    --out "$ARMDIR" \
    2>&1 | tee "$ARMDIR/run.log"
  rc=${PIPESTATUS[0]}
  set -e

  if [ "$rc" -ne 0 ]; then
    # A partial arm must never be averaged over. Preserve it and stop.
    mkdir -p "$OUTDIR/discarded/arm-${ARM}"
    cp -R "$ARMDIR/." "$OUTDIR/discarded/arm-${ARM}/" 2>/dev/null || true
    echo "arm $ARM aborted with exit code $rc; see run.log. The arm must be rerun from repeat 1 after the defect is fixed." \
      > "$OUTDIR/discarded/arm-${ARM}/REASON.txt"
    rm -rf "$ARMDIR"
    fail "arm $ARM aborted (exit $rc). Preserved under $OUTDIR/discarded/arm-${ARM}/. Fix the defect in code, then rerun."
  fi

  echo "matrix: arm $ARM complete -> $ARMDIR"
done

# Provenance self-check. Every recorded report must name the frozen commit; if
# any does not, the matrix says so rather than leaving it to be noticed later.
echo
echo "matrix: verifying every report came from the frozen build ${FROZEN_COMMIT}"
bad=0
for f in "$OUTDIR"/arm-*/trial-*/report.json; do
  if ! grep -q "\"git_commit\": \"${FROZEN_COMMIT}\"" "$f"; then
    echo "matrix: PROVENANCE MISMATCH in $f" >&2
    bad=$((bad+1))
  fi
done
if [ "$bad" -ne 0 ]; then
  fail "$bad report(s) were not produced by the frozen build (invariant: every trial in a matrix must come from one binary)"
fi
echo "matrix: all reports carry ${FROZEN_COMMIT}"

echo
echo "matrix: all arms complete. Tearing the cluster down."
kind delete cluster --name "$CLUSTER" >/dev/null 2>&1 || true
echo "matrix: results in $OUTDIR"
