#!/usr/bin/env bash
# The stress harness against a Redpanda of its own, which advertises the proxy's address
# (127.0.0.1:19095) and listens on 19094: every connection the client makes passes the faults.
#   STRESS_SECS=600 STRESS_SEED=42 stress/run.sh
# Release build with overflow checks and debug assertions on. Exits non-zero on any violation.
set -euo pipefail
cd "$(dirname "$0")"
docker rm -f fastka-chaos >/dev/null 2>&1 || true
docker run -d --name fastka-chaos -p 19094:19094 redpandadata/redpanda:v24.2.7 redpanda start \
  --mode dev-container --smp 1 --memory 1G --reserve-memory 0M --check=false \
  --kafka-addr 0.0.0.0:19094 --advertise-kafka-addr 127.0.0.1:19095 >/dev/null
trap 'docker rm -f fastka-chaos >/dev/null 2>&1 || true' EXIT
timeout 180 bash -c "until docker exec fastka-chaos rpk cluster info -X brokers=127.0.0.1:19094 >/dev/null 2>&1; do sleep 1; done"
cargo build --release -q
for codec in ${STRESS_CODECS:-lz4 none zstd}; do
  STRESS_CODEC=$codec target/release/fastka-stress
done
# harsh: stalls past message.timeout.ms, many cuts: batches time out in flight (possibly persisted),
# producers go fatal and are replaced, and every report must still be true
STRESS_CODEC=lz4 STRESS_CUT_PPM=30000 STRESS_STALL_PPM=20000 STRESS_STALL_MS=6000 \
  STRESS_MESSAGE_TIMEOUT_MS=3000 target/release/fastka-stress
