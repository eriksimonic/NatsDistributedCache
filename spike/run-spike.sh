#!/usr/bin/env bash
# Runs the milestone-1 spike against one NATS version.
# Usage: ./run-spike.sh [image]   e.g. ./run-spike.sh nats:2.11.2-alpine
set -euo pipefail
cd "$(dirname "$0")"

IMAGE="${1:-nats:2.12.15-alpine}"
LABEL="$(echo "$IMAGE" | sed 's/[^0-9.]//g')"
export NATS_IMAGE="$IMAGE" SPIKE_LABEL="$LABEL" SPIKE_RESULTS="$PWD/results"
PROJECT="spike-${LABEL//./-}"
DC=(docker compose -p "$PROJECT")
mkdir -p results

wait_healthy() {
  for port in 8222 8223 8224; do
    for _ in $(seq 1 60); do
      curl -fs "http://localhost:$port/healthz?js-enabled-only=true" >/dev/null && break
      sleep 1
    done
  done
}

echo "== NATS $IMAGE =="
"${DC[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
"${DC[@]}" up -d
wait_healthy

dotnet build Spike -v q -nologo >/dev/null
RUN=(dotnet run --no-build --project Spike --)

status=0
"${RUN[@]}" all || status=1

"${RUN[@]}" lock-throughput-kv || true   # evidence: NATS.Net CreateAsync fails spuriously on tombstones

# NATS CPU while the helper lock-throughput test runs (design: K3 gates on lock ops/s and NATS CPU)
( sleep 5; docker stats --no-stream --format '{{.Name}} {{.CPUPerc}}' $("${DC[@]}" ps -q) ) > "results/$LABEL-lock-cpu.txt" &
"${RUN[@]}" lock-throughput-helper || status=1
wait

# Full cluster restart: data must survive, TTL timers must resume
"${RUN[@]}" restart-setup
"${DC[@]}" stop
sleep 5
"${DC[@]}" start
wait_healthy
"${RUN[@]}" restart-check || status=1

"${DC[@]}" down -v
echo "== done, status $status =="
exit $status
