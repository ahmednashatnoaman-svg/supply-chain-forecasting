"""Bronze -> Silver event cleansing. Implements Emad plan Task 1.

Pure DataFrame transform (I/O handled by the DAG) so it is unit-testable with chispa.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

ALLOWED_EVENTS = ("view", "addtocart", "transaction")
SILVER_COLUMNS = ["event_time", "visitor_id", "event", "item_id", "price"]


def clean_events(df: DataFrame) -> DataFrame:
    """Deduplicate, type-cast, and filter raw bronze events into the silver contract shape.

    Args:
        df: bronze events with columns [event_time(long millis), visitor_id, event, item_id, price?].

    Returns:
        Silver DataFrame: event_time cast to timestamp, deduped, only allowed events, item_id > 0.
    """
    price_col = F.col("price") if "price" in df.columns else F.lit(None).cast("double")
    return (
        df.withColumn("event_time", (F.col("event_time") / 1000).cast("timestamp"))
        .withColumn("price", price_col.cast("double"))
        .filter(F.col("item_id") > 0)
        .filter(F.col("event").isin(*ALLOWED_EVENTS))
        .filter(F.col("event_time").isNotNull())
        .dropDuplicates(SILVER_COLUMNS)
        .select(*SILVER_COLUMNS)
    )
