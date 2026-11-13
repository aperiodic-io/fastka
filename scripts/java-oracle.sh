#!/usr/bin/env bash
# Builds the acceptance tests' reference (tests/java/Oracle.java, Apache Kafka's own Java client)
# against the kafka-clients jars of the Apache Kafka image scripts/brokers.sh runs, into
# target/oracle. Needs a JDK (javac). Prints the environment the acceptance tests read.
set -euo pipefail
cd "$(dirname "$0")/.."
out=target/oracle
rm -rf "$out" && mkdir -p "$out/libs"
c=$(docker create apache/kafka:4.1.1)
for j in kafka-clients-4.1.1.jar lz4-java-1.8.0.jar snappy-java-1.1.10.7.jar zstd-jni-1.5.6-10.jar slf4j-api-1.7.36.jar; do
  docker cp "$c:/opt/kafka/libs/$j" "$out/libs/" >/dev/null
done
docker rm "$c" >/dev/null
javac -nowarn -d "$out" -cp "$out/libs/*" tests/java/Oracle.java
echo "export FASTKA_IT_JAVA=$PWD/$out"
