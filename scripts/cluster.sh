#!/usr/bin/env bash
# A 3-node Redpanda cluster for the acceptance tests' failover scenarios: node i (container
# fastka-c$i) takes Kafka on 127.0.0.1:4909i, RPC 4314i, admin 4964i. `scripts/cluster.sh down`
# removes it. Prints the environment the acceptance tests read.
set -eu
IMAGE=${IMAGE:-redpandadata/redpanda:v24.2.7}
names=(fastka-c0 fastka-c1 fastka-c2)
docker rm -fv "${names[@]}" >/dev/null 2>&1 || true
[[ "${1:-up}" == down ]] && exit 0
for i in 0 1 2; do
  docker run -d --name fastka-c$i --network host "$IMAGE" redpanda start --mode dev-container \
    --smp 1 --memory 1G --reserve-memory 0M --check=false --node-id $i \
    --kafka-addr 0.0.0.0:4909$i --advertise-kafka-addr 127.0.0.1:4909$i \
    --rpc-addr 0.0.0.0:4314$i --advertise-rpc-addr 127.0.0.1:4314$i --seeds 127.0.0.1:43140 \
    --pandaproxy-addr 0.0.0.0:4808$i --schema-registry-addr 0.0.0.0:4818$i \
    --set redpanda.admin="[{\"address\":\"0.0.0.0\",\"port\":4964$i}]" >/dev/null
  sleep 3 # the seed first
done
timeout 180 bash -c 'until [ "$(docker exec fastka-c0 rpk cluster info -X brokers=127.0.0.1:49090 2>/dev/null | grep -cE "^[0-9]+\*? ")" = 3 ]; do sleep 2; done'
echo "export FASTKA_IT_CLUSTER=127.0.0.1:49090,127.0.0.1:49091,127.0.0.1:49092 FASTKA_IT_CLUSTER_NODES=fastka-c0,fastka-c1,fastka-c2"
