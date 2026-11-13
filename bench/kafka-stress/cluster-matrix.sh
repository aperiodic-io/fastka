#!/usr/bin/env bash
# The matrix of run-matrix.sh on a 3-node cluster (cluster.sh), topics of replication 3, and each
# run's leader elections past the first of each partition: what staging saw as step-downs.
set -u
cd "$(dirname "$0")"
S=target/release/kafka-stress
PARTS=${PARTS:-24}
elections() { # the topic's won elections, every node's
  for i in 0 1 2; do curl -s --noproxy '*' "127.0.0.1:964$i/public_metrics"; done |
    grep "^redpanda_raft_leadership_changes{.*redpanda_topic=\"$1\"" | awk '{s+=$2} END {print s+0}'
}
run() {
  docker exec rp0 rpk topic delete -r 'stress-.*' -X brokers=127.0.0.1:19090 >/dev/null 2>&1
  out=$("$S" --upstream 127.0.0.1:19190,127.0.0.1:19191,127.0.0.1:19192 --listen 19290,19291,19292 \
    --replicas 3 --partitions "$PARTS" "$@" 2>&1 | grep -v '^stall:')
  echo "$out"
  topic=$(echo "$out" | sed -n 's/.*| topic \([^ ]*\) .*/\1/p')
  sleep 2
  echo " leader elections past the first: $(( $(elections "$topic") - PARTS ))"
  echo
}
run --client fastka "$@"
run --client fastka -F max.in.flight=5 -F max.request.size=1048576 "$@"
run --client rdkafka "$@"
