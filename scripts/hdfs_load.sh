#!/usr/bin/env bash
# Stage the Retailrocket train split + dimension tables into HDFS bronze. Idempotent.
# (Hatem, plan Task 5.) Per decisions D-B/D-C: dimension tables staged in full; events bronze
# holds the 80% TRAIN split only (latest 20% is replayed to Kafka, not stored in bronze).
set -euo pipefail
COMPOSE="docker compose -f infra/docker/docker-compose.yml"
# Source directory for the raw/split CSVs. Override with DATA_DIR=<path> for hermetic test runs.
DATA_DIR="${DATA_DIR:-data/raw}"

# Precondition: the train split must exist. Produce it via the split CLI first.
if [ ! -f "$DATA_DIR/events_train.csv" ]; then
  echo "ERROR: $DATA_DIR/events_train.csv not found." >&2
  echo "       run 'python -m ingestion.generator.split' first (after download_data.sh)." >&2
  exit 1
fi

stage() { # $1 = local file, $2 = hdfs bronze subdir
  local f="$1" dir="$2"
  [ -f "$DATA_DIR/$f" ] || { echo "   skip (missing): $DATA_DIR/$f"; return 0; }
  $COMPOSE exec -T namenode hdfs dfs -mkdir -p "/data/bronze/$dir"
  $COMPOSE cp "$DATA_DIR/$f" namenode:"/tmp/$f"
  $COMPOSE exec -T namenode hdfs dfs -put -f "/tmp/$f" "/data/bronze/$dir/"
  echo "   staged: $f -> /data/bronze/$dir/"
}

echo "==> Staging Retailrocket into HDFS bronze..."
stage events_train.csv events
stage item_properties_part1.csv item_properties
stage item_properties_part2.csv item_properties
stage category_tree.csv category_tree
echo "==> HDFS staging complete."
