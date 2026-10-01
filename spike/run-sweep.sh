#!/usr/bin/env bash
# Helper lock-throughput sweep (design section 9): 8/16/32/64 closed-loop workers, CPU sampled per run.
# Usage: ./run-sweep.sh [scaled|full] [image]
set -euo pipefail
cd "$(dirname "$0")"

PROFILE="${1:-scaled}"
IMAGE="${2:-nats:2.15.0-alpine}"
case "$IMAGE" in *-RC*|*-rc*|*beta*) echo "refusing release candidate $IMAGE (design section 3)"; exit 2 ;; esac
FILES=(-f docker-compose.yml)
[ "$PROFILE" = full ] && FILES+=(-f docker-compose.full.yml)
PROJECT="sweep-$PROFILE"
export NATS_IMAGE="$IMAGE" SPIKE_RESULTS="$PWD/results" SPIKE_PROJECT="$PROJECT"
DC=(docker compose -p "$PROJECT" "${FILES[@]}")

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

cleanup
mkdir -p results
"${DC[@]}" up -d
wait_healthy
dotnet build Spike -v q -nologo >/dev/null

status=0
for workers in 8 16 32 64; do
  export SPIKE_WORKERS="$workers" SPIKE_LABEL="sweep-$PROFILE-w$workers"
  ( for s in 4 8 12; do sleep 4; echo "t+${s}s"; docker stats --no-stream --format '{{.Name}} {{.CPUPerc}}' $("${DC[@]}" ps -q); done ) \
    > "results/$SPIKE_LABEL-cpu.txt" &
  stats_pid=$!
  dotnet run --no-build --project Spike -- lock-throughput-helper || status=1
  wait "$stats_pid" || true
done

echo "== sweep $PROFILE done, status $status =="
exit $status
