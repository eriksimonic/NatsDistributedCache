#!/usr/bin/env bash
# Runs the milestone-1 spike against one NATS version (default: the pinned latest stable release).
# Usage: ./run-spike.sh [image]   e.g. ./run-spike.sh nats:2.15.0-alpine
set -euo pipefail
cd "$(dirname "$0")"

IMAGE="${1:-nats:2.15.0-alpine}"
TAG="${IMAGE#*:}"
VERSION="${TAG%%-alpine}"
case "$VERSION" in *-RC*|*-rc*|*beta*) echo "refusing release candidate $IMAGE (design section 3)"; exit 2 ;; esac
LABEL="$VERSION"
PROJECT="spike-${LABEL//./-}"
export NATS_IMAGE="$IMAGE" SPIKE_LABEL="$LABEL" SPIKE_RESULTS="$PWD/results" SPIKE_PROJECT="$PROJECT"
DC=(docker compose -p "$PROJECT")

cleanup() { "${DC[@]}" down -v --remove-orphans >/dev/null 2>&1 || true; }
trap cleanup EXIT

wait_healthy() {
  for port in 8222 8223 8224; do
    local ok=0
    for _ in $(seq 1 90); do
      if curl -fs "http://localhost:$port/healthz?js-enabled-only=true" >/dev/null; then ok=1; break; fi
      sleep 1
    done
    [ "$ok" = 1 ] || { echo "NATS on monitor port $port not healthy after 90 s"; exit 3; }
  done
}

check_version() {
  local got
  got="$(curl -fs http://localhost:8222/varz | python3 -c 'import json,sys; print(json.load(sys.stdin)["version"])')"
  [ "$got" = "$VERSION" ] || { echo "server reports $got, expected $VERSION"; exit 4; }
}

echo "== NATS $IMAGE =="
cleanup
mkdir -p results
rm -f results/"$LABEL"-*
"${DC[@]}" up -d
wait_healthy
check_version

dotnet build Spike -v q -nologo >/dev/null
RUN=(dotnet run --no-build --project Spike --)

status=0
"${RUN[@]}" all || status=1

"${RUN[@]}" lock-throughput-kv || true   # evidence: NATS.Net CreateAsync fails spuriously on tombstones

# NATS CPU while the helper lock-throughput test runs: three samples (design: K3 gates on lock ops/s and NATS CPU)
( for s in 4 8 12; do sleep 4; echo "t+${s}s"; docker stats --no-stream --format '{{.Name}} {{.CPUPerc}}' $("${DC[@]}" ps -q); done ) \
  > "results/$LABEL-lock-cpu.txt" &
stats_pid=$!
"${RUN[@]}" lock-throughput-helper || status=1
wait "$stats_pid" || true

# Unknown outcomes: the locks stream leader is restarted twice while workers cycle locks
"${RUN[@]}" leader-failover || status=1
wait_healthy

# Full cluster restart: data must survive, TTL timers must resume
"${RUN[@]}" restart-setup || status=1
"${DC[@]}" stop
sleep 5
"${DC[@]}" start
wait_healthy
"${RUN[@]}" restart-check || status=1

echo "== done, status $status =="
exit $status
