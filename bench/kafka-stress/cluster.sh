#!/usr/bin/env bash
# A 3-node Redpanda on this host, each node's CPU bounded (CPUS, 0.6 by default): node i takes
# Kafka on 1909i (direct) and 1919i (advertised as 127.0.0.1:1929i, the harness's proxy), admin 964i.
set -eu
IMAGE=${IMAGE:-mirror.gcr.io/redpandadata/redpanda:v24.2.7}
docker rm -f rp0 rp1 rp2 >/dev/null 2>&1 || true
for i in 0 1 2; do
  docker run -d --name rp$i --network host --cpus "${CPUS:-0.6}" "$IMAGE" redpanda start --mode dev-container \
    --smp 1 --memory 1G --reserve-memory 0M --check=false \
    --kafka-addr direct://0.0.0.0:1909$i,proxied://0.0.0.0:1919$i \
    --advertise-kafka-addr direct://127.0.0.1:1909$i,proxied://127.0.0.1:1929$i \
    --rpc-addr 0.0.0.0:3314$i --advertise-rpc-addr 127.0.0.1:3314$i --seeds 127.0.0.1:33140 \
    --pandaproxy-addr 0.0.0.0:1808$i --schema-registry-addr 0.0.0.0:1818$i \
    --set redpanda.admin="[{\"address\":\"0.0.0.0\",\"port\":964$i}]" >/dev/null
  sleep 5 # the seed first
done
timeout 120 bash -c 'until [ "$(docker exec rp0 rpk cluster info -X brokers=127.0.0.1:19090 2>/dev/null | grep -c "^[0-9]")" = 3 ]; do sleep 2; done'
echo "3 nodes up"
