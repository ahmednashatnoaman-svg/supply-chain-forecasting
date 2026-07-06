#!/usr/bin/env bash
# Download the free Retailrocket dataset from Kaggle (needs a free Kaggle API token in ~/.kaggle).
# (Hatem, plan Task 5.)
set -euo pipefail
mkdir -p data/raw
if command -v kaggle >/dev/null 2>&1; then
  echo "==> Downloading Retailrocket dataset (free) via Kaggle API..."
  kaggle datasets download -d retailrocket/ecommerce-dataset -p data/raw --unzip
else
  cat <<'EOF'
Kaggle CLI not found. To fetch the free dataset:
  1) pip install kaggle
  2) Create a free Kaggle account, download kaggle.json to ~/.kaggle/kaggle.json (chmod 600)
  3) Re-run: bash scripts/download_data.sh
Or download manually: https://www.kaggle.com/datasets/retailrocket/ecommerce-dataset
Place events.csv, item_properties_part*.csv, category_tree.csv into data/raw/
EOF
fi
