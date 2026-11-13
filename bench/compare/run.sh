#!/usr/bin/env bash
# fastka against rdkafka (librdkafka), rskafka and kafka-rust, on the Redpanda of scripts/brokers.sh.
# Each run is a process of its own; a produce run writes a topic of its own, a consume run reads a
# backlog written once (by fastka, in batches of up to 10,000 messages). Prints JSON lines; the
# median of RUNS per row is what the README reports (bench/compare/table.py).
#   N=1000000 SIZE=100 PARTS=3 RUNS=5 bench/compare/run.sh > bench/results/compare.jsonl
set -euo pipefail
cd "$(dirname "$0")"
N=${N:-1000000} SIZE=${SIZE:-100} PARTS=${PARTS:-3} RUNS=${RUNS:-5}
BROKERS=${BROKERS:-127.0.0.1:19092} RPK=${RPK:-fastka-rp}
CLIENTS=${CLIENTS:-fastka rdkafka rskafka kafka-rust}
bin=target/release/fastka-compare
cargo build --release -q
# a run that fails is a row that says so (its first line of panic)
one() {
  local out
  if ! out=$(timeout 300 "$bin" "$1" "$2" "$BROKERS" "$3" "$PARTS" "$N" "$SIZE" "$4" 2>&1); then
    why=$(grep -m1 -A1 panicked <<<"$out" | tail -1 | tr -d '"\\' | cut -c1-160)
    echo "{\"workload\":\"$1\",\"client\":\"$2\",\"codec\":\"$4\",\"n\":$N,\"size\":$SIZE,\"error\":\"$why\"}"
  else
    tail -1 <<<"$out"
  fi
}
topic() { docker exec "$RPK" rpk topic create "$1" -p "$PARTS" -r 1 >/dev/null; }
for codec in none lz4; do
  backlog="cmp-backlog-$codec-$RANDOM$RANDOM"
  topic "$backlog"
  "$bin" produce fastka "$BROKERS" "$backlog" "$PARTS" "$N" "$SIZE" "$codec" >/dev/null
  for r in $(seq "$RUNS"); do
    for c in $CLIENTS; do
      [[ $codec == lz4 && $c == kafka-rust ]] && continue # kafka-rust: gzip and snappy only
      t="cmp-$c-$codec-$r-$RANDOM$RANDOM"; topic "$t"
      one produce "$c" "$t" "$codec"
      docker exec "$RPK" rpk topic delete "$t" >/dev/null
      one consume "$c" "$backlog" "$codec"
    done
  done
  docker exec "$RPK" rpk topic delete "$backlog" >/dev/null
done
# latency: produce -> consume at LATENCY_RATE msgs/s (default 5000) on one partition, linger 0
for r in $(seq "$RUNS"); do
  for c in fastka rdkafka; do
    t="cmp-latency-$c-$r-$RANDOM$RANDOM"; docker exec "$RPK" rpk topic create "$t" -p 1 -r 1 >/dev/null
    N=${LATENCY_N:-20000} PARTS=1 one latency "$c" "$t" none
    docker exec "$RPK" rpk topic delete "$t" >/dev/null
  done
done
