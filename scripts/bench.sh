#!/usr/bin/env bash
# Benchmarks a running Shardwell (make up): PUT and GET at 4KiB/1MiB/32MiB with
# concurrency 1/8/32, BENCH_RUNS runs of BENCH_DURATION each, median per
# cell, printed as a Markdown table. Then PUT 1MiB c=8 under the default
# RF=3/W=2 against RF=1/W=1 by recreating the coordinator with that env.
set -euo pipefail

CAIRN_URL="${CAIRN_URL:-http://localhost:8080}"
CAIRN_COMPOSE="${CAIRN_COMPOSE:-docker compose -f $(dirname "$0")/../deploy/docker-compose.yml}"
BENCH_DURATION="${BENCH_DURATION:-30s}"
BENCH_WARMUP="${BENCH_WARMUP:-2s}"
BENCH_RUNS="${BENCH_RUNS:-3}"
BENCH="${BENCH:-$(dirname "$0")/../bin/bench}"
BUCKET="bench-$$"
SIZES="4KiB 1MiB 32MiB"
CONCURRENCY="1 8 32"

COORD_ENV_CHANGED=""
FAILED=0
restore_coordinator() {
  if [ -n "$COORD_ENV_CHANGED" ]; then
    echo "restoring coordinator to RF=3/W=2" >&2
    recreate_coordinator 3 2 || true
  fi
}
trap restore_coordinator EXIT

wait_healthy() {
  for _ in $(seq 1 60); do
    if curl -sf -o /dev/null "$CAIRN_URL/healthz"; then
      return 0
    fi
    sleep 1
  done
  echo "FAIL: coordinator not healthy at $CAIRN_URL" >&2
  exit 1
}

# recreate_coordinator RF W: restarts only the coordinator with that env.
# Nodes keep running and re-register on their next heartbeat.
recreate_coordinator() {
  CAIRN_REPLICATION_FACTOR="$1" CAIRN_WRITE_QUORUM="$2" $CAIRN_COMPOSE up -d --no-deps --wait coordinator >&2
  wait_healthy
}

# field NAME JSON: prints the numeric value of a top-level JSON field.
field() {
  sed -n "s/.*\"$1\":\([0-9.]*\).*/\1/p" <<< "$2"
}

# median VALUES...: prints the middle value (upper middle for even counts).
median() {
  printf '%s\n' "$@" | sort -g | sed -n "$(( ($# + 1) / 2 ))p"
}

# error_count JSON: sums the counts in the trailing "errors" map.
error_count() {
  local n=0 v
  for v in $(grep -o ":[0-9]*" <<< "${1#*\"errors\":}" | tr -d :); do
    n=$(( n + v ))
  done
  echo "$n"
}

# cell OP SIZE C: runs BENCH_RUNS benches and prints
# "ops/s MiB/s p50 p99 errors" as medians (errors summed), or FAIL when a
# run could not start (the bench already said why on stderr).
cell() {
  local op="$1" size="$2" c="$3"
  local ops=() mib=() p50=() p99=() errs=0 out
  for _ in $(seq 1 "$BENCH_RUNS"); do
    out="$("$BENCH" -url "$CAIRN_URL" -bucket "$BUCKET" -op "$op" -size "$size" -concurrency "$c" \
      -duration "$BENCH_DURATION" -warmup "$BENCH_WARMUP" -keys 64 -json)" || { echo FAIL; return; }
    ops+=("$(field ops_per_sec "$out")")
    mib+=("$(field mib_per_sec "$out")")
    p50+=("$(field p50_ms "$out")")
    p99+=("$(field p99_ms "$out")")
    errs=$(( errs + $(error_count "$out") ))
  done
  printf '%.1f %.2f %.1f %.1f %s\n' "$(median "${ops[@]}")" "$(median "${mib[@]}")" "$(median "${p50[@]}")" "$(median "${p99[@]}")" "$errs"
}

# row LABEL OP SIZE C: prints one Markdown table row for cell.
row() {
  local label="$1"
  shift
  local r
  r="$(cell "$@")"
  if [ "$r" = FAIL ]; then
    printf '| %s | FAIL | | | | |\n' "$label"
    FAILED=1
    return
  fi
  # shellcheck disable=SC2086
  set -- $r
  printf '| %s | %s | %s | %s | %s | %s |\n' "$label" "$1" "$2" "$3" "$4" "$5"
}
[ -x "$BENCH" ] || { echo "FAIL: $BENCH not built (make build)" >&2; exit 1; }
wait_healthy

echo "## Shardwell bench: $BENCH_RUNS × $BENCH_DURATION per cell, median (warmup $BENCH_WARMUP)"
echo
echo "| op / size / c | ops/s | MiB/s | p50 ms | p99 ms | errors |"
echo "|---|---:|---:|---:|---:|---:|"
for op in put get; do
  for size in $SIZES; do
    for c in $CONCURRENCY; do
      echo "bench $op $size c=$c" >&2
      row "$op $size c=$c" "$op" "$size" "$c"
    done
  done
done

echo
echo "## PUT 1MiB c=8: replication factor"
echo
echo "| config | ops/s | MiB/s | p50 ms | p99 ms | errors |"
echo "|---|---:|---:|---:|---:|---:|"
echo "bench RF=3/W=2" >&2
row "RF=3/W=2" put 1MiB 8
echo "recreating coordinator with RF=1/W=1" >&2
COORD_ENV_CHANGED=1
recreate_coordinator 1 1
row "RF=1/W=1" put 1MiB 8

[ "$FAILED" = 0 ] || { echo "FAIL: some cells could not run" >&2; exit 1; }

[ "$FAILED" = 0 ] || { echo "FAIL: some cells could not run" >&2; exit 1; }
