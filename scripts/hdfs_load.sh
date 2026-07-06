#!/usr/bin/env bash
# Stage the raw Retailrocket CSVs into HDFS bronze. Idempotent. (Hatem, plan Task 5.)
set -euo pipefail
COMPOSE="docker compose -f infra/docker/docker-compose.yml"

stage() { # $1 = local file, $2 = hdfs bronze subdir
  local f="$1" dir="$2"
  [ -f "data/raw/$f" ] || { echo "   skip (missing): data/raw/$f"; return 0; }
  $COMPOSE exec -T namenode hdfs dfs -mkdir -p "/data/bronze/$dir"
  $COMPOSE cp "data/raw/$f" namenode:"/tmp/$f"
  $COMPOSE exec -T namenode hdfs dfs -put -f "/tmp/$f" "/data/bronze/$dir/"
  echo "   staged: $f -> /data/bronze/$dir/"
}

echo "==> Staging Retailrocket into HDFS bronze..."
stage events.csv events
stage item_properties_part1.csv item_properties
stage item_properties_part2.csv item_properties
stage category_tree.csv category_tree
echo "==> HDFS staging complete."
