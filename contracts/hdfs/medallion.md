# HDFS Medallion Contract

Access paths **only** via `libs/scf_common.contracts.HdfsPaths`. Base dirs come from `.env`
(`HDFS_BRONZE`, `HDFS_SILVER`, `HDFS_GOLD`). All data is Parquet except raw bronze staging.

## Bronze (raw, staged by Hatem)

| Path | Source file | Schema (columns) |
|---|---|---|
| `/data/bronze/events` | Retailrocket `events.csv` | `timestamp:long, visitorid:long, event:string, itemid:long, transactionid:long?` |
| `/data/bronze/item_properties` | `item_properties_part*.csv` | `timestamp:long, itemid:long, property:string, value:string` |
| `/data/bronze/category_tree` | `category_tree.csv` | `categoryid:long, parentid:long?` |

## Silver (cleansed, by Emad)

| Path | Schema |
|---|---|
| `/data/silver/events_cleaned` | `event_time:timestamp, visitor_id:long, event:string, item_id:long, price:double?` |

Rules: deduped, `event` ∈ {view, addtocart, transaction}, `item_id > 0`, `event_time` non-null.

## Gold (features + models, by Emad)

| Path | Schema |
|---|---|
| `/data/gold/features` | `item_id:long, ds:date, velocity_7d:double, velocity_30d:double, dow:int, is_weekend:boolean` |
| `/data/gold/forecast` | `item_id:long, forecast_demand:double` (matches `contracts/models/forecast_output.json`) |
| `/data/gold/product_graph` | `item_id:long, related_item_id:long, elasticity_weight:double, community:int` |

## Accessor mapping (`HdfsPaths`)
```
HdfsPaths.bronze("events")   -> "hdfs://.../data/bronze/events"
HdfsPaths.silver("events_cleaned") -> "hdfs://.../data/silver/events_cleaned"
HdfsPaths.gold("forecast")   -> "hdfs://.../data/gold/forecast"
```
