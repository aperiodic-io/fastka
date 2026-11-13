#!/usr/bin/env bash
# Each client under the same burst: fastka by default, fastka with librdkafka's request
# shape (max.in.flight=5, max.request.size=1 MiB), librdkafka. Pass the scenario's flags.
# CLEAN: a command deleting the stress-* topics (default: rpk in the container `rp`).
# Needs a broker advertising 127.0.0.1:$LISTEN and listening on $UPSTREAM, e.g.:
#   docker run -d --name rp --network host mirror.gcr.io/redpandadata/redpanda:v24.2.7 redpanda start \
#     --mode dev-container --smp 1 --memory 1G --reserve-memory 0M --check=false \
#     --kafka-addr direct://0.0.0.0:19092,proxied://0.0.0.0:19094 \
#     --advertise-kafka-addr direct://127.0.0.1:19092,proxied://127.0.0.1:19093
set -u
cd "$(dirname "$0")"
UPSTREAM=${UPSTREAM:-127.0.0.1:19094} LISTEN=${LISTEN:-19093}
S=target/release/kafka-stress
# the earlier runs' topics deleted first: a dev-container Redpanda holds few partitions
CLEAN=${CLEAN:-docker exec rp rpk topic delete -r 'stress-.*' -X brokers=127.0.0.1:19092}
run() { $CLEAN >/dev/null 2>&1; "$S" --upstream "$UPSTREAM" --listen "$LISTEN" "$@" 2>&1 | grep -v '^stall:'; echo; }
run --client fastka "$@"
run --client fastka -F max.in.flight=5 -F max.request.size=1048576 "$@"
run --client rdkafka "$@"
