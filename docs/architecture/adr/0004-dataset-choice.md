# ADR 0004 — Dataset: Retailrocket over Instacart

**Status:** Accepted · **Date:** 2026-07-06

## Context
The original brief ([`docs/reference/project-brief.md`](../../reference/project-brief.md)) offered two
free Kaggle datasets: **Instacart Market Basket Analysis** and the **Retailrocket Recommender System
Dataset**. The system needs both (a) historical transactions to train the MLlib demand forecast and
GraphFrames elasticity graph, and (b) a realistic clickstream to replay as live traffic for the
Structured Streaming + LSTM surge-pricing engine.

## Decision
Use **Retailrocket** — direct link:
**https://www.kaggle.com/datasets/retailrocket/ecommerce-dataset**

## Why (not Instacart)
- **`item_properties.csv` is the deciding factor.** It's a time-series file recording how each item's
  `price` and `available` status changed over time — exactly the price-history signal a *dynamic
  pricing* system needs. Instacart's dataset has no price field or time-series at all, only static
  basket contents.
- **`events.csv` is clickstream-shaped** (`view` / `addtocart` / `transaction`, each timestamped per
  visitor) — this maps directly onto `contracts/avro/live_web_traffic.avsc` and is what the ingestion
  layer (Hatem) replays into Kafka to simulate live traffic and surges. Instacart is order-level only
  (a finished basket), with no pre-purchase browsing signal to feed the LSTM surge classifier.
- **`category_tree.csv`** gives the product hierarchy GraphFrames uses alongside co-purchase edges for
  cross-elasticity/cannibalization checks.
- Both are free (Kaggle account + API token, zero-cost per
  [cost-and-licensing.md](../../reference/cost-and-licensing.md)) — cost was not a differentiator.

## Consequences
- ✅ One dataset serves both the batch (bronze/silver/gold) and streaming (replay) layers — no need to
  source a second dataset for clickstream simulation.
- ✅ `item_properties.csv`'s price history can validate the MLlib forecast and backtest the pricing
  formula against what prices *actually* did historically.
- ⚠️ Retailrocket is anonymized/obfuscated (item and category IDs are hashed integers, no real product
  names) — fine for a technical demo, not for customer-facing copy.
- Referenced by: `scripts/download_data.sh`, `scripts/hdfs_load.sh`, `contracts/hdfs/medallion.md`,
  `ingestion/config/generator.yaml`, [hatem-plan.md](../../plans/hatem-plan.md).
