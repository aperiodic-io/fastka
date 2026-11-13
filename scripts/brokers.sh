#!/usr/bin/env bash
# Starts the brokers the integration tests, the stress harness and the benchmarks run against:
#   fastka-rp      Redpanda v24.2         127.0.0.1:19092  (rpk inside: franz-go, another client)
#   fastka-apache  Apache Kafka 4.1 KRaft 127.0.0.1:29092  (its own traffic, transaction markers
#                  included, on an internal listener: the advertised address is the host's)
#   fastka-sasl    Redpanda v26.2 + SCRAM 127.0.0.1:39092  (it/it-secret SHA-256, it512/it-secret SHA-512,
#                  noacl/noacl-secret authenticated but granted nothing)
# `scripts/brokers.sh down` removes them. Prints the environment the tests read.
set -euo pipefail
names=(fastka-rp fastka-apache fastka-sasl)
if [[ "${1:-up}" == down ]]; then docker rm -fv "${names[@]}" >/dev/null 2>&1 || true; exit 0; fi
docker rm -fv "${names[@]}" >/dev/null 2>&1 || true
rp() { # name port image
  docker run -d --name "$1" -p "$2:$2" "$3" redpanda start --mode dev-container --smp 1 --memory 1G \
    --reserve-memory 0M --check=false --kafka-addr "0.0.0.0:$2" --advertise-kafka-addr "127.0.0.1:$2" >/dev/null
}
rp fastka-rp 19092 redpandadata/redpanda:v24.2.7
docker run -d --name fastka-apache -p 29092:9092 -e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller \
  -e KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093,INTERNAL://:9094 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://127.0.0.1:29092,INTERNAL://localhost:9094 -e KAFKA_INTER_BROKER_LISTENER_NAME=INTERNAL \
  -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,INTERNAL:PLAINTEXT \
  -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
  -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 apache/kafka:4.1.1 >/dev/null
rp fastka-sasl 39092 redpandadata/redpanda:v26.2.1
for p in 19092 29092 39092; do timeout 180 bash -c "until echo > /dev/tcp/127.0.0.1/$p; do sleep 1; done" 2>/dev/null; done
for c in fastka-rp fastka-sasl; do
  timeout 180 bash -c "until docker exec $c rpk cluster info >/dev/null 2>&1; do sleep 1; done"
done
r="docker exec fastka-sasl rpk"
$r security user create it -p it-secret --mechanism SCRAM-SHA-256 >/dev/null
$r security user create it512 -p it-secret --mechanism SCRAM-SHA-512 >/dev/null
# authenticated, granted nothing: what a writer the cluster has not authorized meets
$r security user create noacl -p noacl-secret --mechanism SCRAM-SHA-256 >/dev/null
$r cluster config set superusers '["it","it512"]' >/dev/null
$r cluster config set enable_sasl true >/dev/null
cat <<ENV
export FASTKA_IT=127.0.0.1:19092 FASTKA_IT_RPK=fastka-rp FASTKA_IT_APACHE=127.0.0.1:29092 FASTKA_IT_SASL=127.0.0.1:39092
ENV
