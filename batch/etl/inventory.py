"""Item-availability -> inventory gold table.

Fills a real pipeline gap: `check_low_stock_alert` (streaming/pipeline/pricing_stream.py) and the
dashboard's low-stock KPI both read `RedisKeys.inventory(sku)`, but nothing in the real pipeline
ever wrote it -- only `scripts/seed_redis_stub.py` did, hardcoded to 5 SKUs. Retailrocket's
item_properties has a real, time-varying `available` (0/1) signal per item; this derives a stock
count from it instead of inventing unrelated numbers.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

# Retailrocket's `available` property is boolean (in-stock or not), not a quantity. When an item
# is available, seed a baseline count matching automation/n8n/alert_bridge.py's own
# DEFAULT_TARGET_STOCK (200), so a freshly-published SKU starts right at the automation layer's
# restock target rather than an arbitrary number. When unavailable, 0 -- which correctly trips
# check_low_stock_alert immediately.
IN_STOCK_BASELINE = 200


def build_inventory(item_properties: DataFrame) -> DataFrame:
    """Derive a per-item stock count from the most recent `available` property value.

    Args:
        item_properties: bronze item_properties, Retailrocket's raw CSV schema
            (timestamp, itemid, property, value -- all staged as strings).

    Returns:
        Gold inventory: item_id (long), stock (int).
    """
    latest = Window.partitionBy("itemid").orderBy(F.col("timestamp").cast("long").desc())
    return (
        item_properties.filter(F.col("property") == "available")
        .withColumn("rn", F.row_number().over(latest))
        .filter(F.col("rn") == 1)
        .select(
            F.col("itemid").cast("long").alias("item_id"),
            F.when(F.col("value").cast("int") == 1, F.lit(IN_STOCK_BASELINE))
            .otherwise(F.lit(0))
            .alias("stock"),
        )
    )
