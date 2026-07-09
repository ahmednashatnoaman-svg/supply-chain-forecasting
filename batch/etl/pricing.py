"""Catalog price -> pricing gold table.

Fills the same class of pipeline gap `batch/etl/inventory.py` fixed for stock: the streaming
pricing engine (`streaming/pipeline/pricing_stream.py::price_row`) needs a real starting
`price:current:*` to multiplicatively adjust, but Retailrocket's clickstream events carry no price
field at all -- price only exists in item_properties, under the dataset's documented property code
"790". Nothing in the pipeline ever extracted it, so every SKU's catalog price was implicitly 0.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

# Retailrocket obfuscates large numeric property values by prefixing them with "n" (its published
# anonymization convention -- see the dataset's own documentation). Property "790" is price; every
# value under it carries this prefix (e.g. "n15360.000"), unlike category/other property codes.
_PRICE_PROPERTY = "790"


def build_pricing(item_properties: DataFrame) -> DataFrame:
    """Derive a per-item catalog price from the most recent `790` property value.

    Args:
        item_properties: bronze item_properties, Retailrocket's raw CSV schema
            (timestamp, itemid, property, value -- all staged as strings).

    Returns:
        Gold pricing: item_id (long), catalog_price (double).
    """
    latest = Window.partitionBy("itemid").orderBy(F.col("timestamp").cast("long").desc())
    return (
        item_properties.filter(F.col("property") == _PRICE_PROPERTY)
        .withColumn("rn", F.row_number().over(latest))
        .filter(F.col("rn") == 1)
        .select(
            F.col("itemid").cast("long").alias("item_id"),
            F.regexp_replace(F.col("value"), "^n", "").cast("double").alias("catalog_price"),
        )
        .filter(F.col("catalog_price").isNotNull())
    )
