#!/usr/bin/env bash
# Create Kafka topics + HDFS medallion dirs after the stack is healthy. Idempotent (re-runnable).
# Reads topic/partition declarations from contracts/avro/topics.yml. (Nagy, plan Task 2.)
set -euo pipefail

COMPOSE="docker compose -f infra/docker/docker-compose.yml"

echo "==> Waiting for Kafka to be healthy..."
until $COMPOSE exec -T kafka kafka-broker-api-versions --bootstrap-server localhost:9092 >/dev/null 2>&1; do
  sleep 3; echo "   ...still waiting for kafka"
done

echo "==> Creating Kafka topics (idempotent)..."
# name:partitions pairs mirror contracts/avro/topics.yml
declare -a TOPICS=(
  "live_web_traffic:6"
  "inventory_updates:3"
  "system_alerts:3"
  "automated_pricing_updates:6"
)
for t in "${TOPICS[@]}"; do
  name="${t%%:*}"; parts="${t##*:}"
  $COMPOSE exec -T kafka kafka-topics --create --if-not-exists \
    --bootstrap-server localhost:9092 \
    --topic "$name" --partitions "$parts" --replication-factor 1
  echo "   topic ready: $name ($parts partitions)"
done

echo "==> Creating HDFS medallion directories (idempotent)..."
for zone in bronze silver gold; do
  $COMPOSE exec -T namenode hdfs dfs -mkdir -p "/data/$zone" || true
  echo "   hdfs dir ready: /data/$zone"
done

echo "==> Bootstrap complete."
