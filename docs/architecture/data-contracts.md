# Data Contracts (human-readable reference)

The machine-readable sources of truth live in [`contracts/`](../../contracts/). Layer code MUST access
them via `libs/scf_common/contracts` — never hard-code a topic, key, or path. Changing any contract is a
PR reviewed by both adjacent owners (see [master plan §1](../master-plan.md)).

## Kafka topics

| Topic | Key | Partitions | Producer | Consumer | Schema |
|---|---|---|---|---|---|
| `live_web_traffic` | `item_id` | 6 | Hatem | Nashat, Emad | [live_web_traffic.avsc](../../contracts/avro/live_web_traffic.avsc) |
| `inventory_updates` | `item_id` | 3 | Hatem | Nashat | [inventory_updates.avsc](../../contracts/avro/inventory_updates.avsc) |
| `system_alerts` | `item_id` | 3 | Nashat | Ziad | [system_alerts.avsc](../../contracts/avro/system_alerts.avsc) |
| `automated_pricing_updates` | `item_id` | 6 | Nashat | Ziad | [automated_pricing_updates.avsc](../../contracts/avro/automated_pricing_updates.avsc) |

Partition counts are declared in [topics.yml](../../contracts/avro/topics.yml) and consumed by
`bootstrap.sh`.

## Redis keys (serving layer)

| Key pattern | Type | Written by | Read by | Meaning |
|---|---|---|---|---|
| `forecast:{sku}` | string(float) | Emad (batch) | Nashat, Ziad | Baseline 30d demand forecast |
| `graph:elasticity:{sku}` | string(JSON list) | Emad (batch) | Nashat | `[{related, weight}, ...]` |
| `graph:community:{sku}` | string(int) | Emad (batch) | Nashat | Community id for cannibalization guard |
| `price:current:{sku}` | string(float) | Nashat (speed) | Ziad | Latest dynamic price |
| `inventory:{sku}` | string(int) | Hatem/Nashat | Nashat, Ziad | Current stock level |
| `velocity:{sku}:{window}` | string(float) | Nashat (speed) | Ziad | Rolling sales velocity |

Accessor examples: `RedisKeys.forecast("10") == "forecast:10"`,
`RedisKeys.elasticity("10") == "graph:elasticity:10"`.

## HDFS medallion

| Zone | Path | Format | Owner |
|---|---|---|---|
| bronze | `/data/bronze/{events,item_properties,category_tree}` | Parquet (raw) | Hatem stages |
| silver | `/data/silver/events_cleaned` | Parquet (typed, deduped) | Emad |
| gold | `/data/gold/{features,forecast,product_graph}` | Parquet | Emad |

Zone schemas: [contracts/hdfs/medallion.md](../../contracts/hdfs/medallion.md).

## Model signatures

- Forecast output: [contracts/models/forecast_output.json](../../contracts/models/forecast_output.json)
  → `{item_id: long, forecast_demand: double}`.
- LSTM signature: [contracts/models/lstm_signature.json](../../contracts/models/lstm_signature.json)
  → input `float[seq_len=50, features=4]`, output `float surge_prob ∈ [0,1]`.
