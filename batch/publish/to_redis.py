"""Publish gold tables to the Redis serving layer. Implements Emad plan Task 6.

Writes forecast + elasticity + community keys atomically per SKU (pipeline) so the streaming engine
never reads a half-updated pair. Keys built via contract accessors.
"""

from __future__ import annotations

import json

from pyspark.sql import DataFrame

from libs.scf_common.contracts import RedisKeys
from libs.scf_common.io import get_redis
from libs.scf_common.observability import RECORDS_PROCESSED, get_logger

log = get_logger("batch.publish")


def publish_forecasts(
    forecast_df: DataFrame, graph_df: DataFrame, inventory_df: DataFrame | None = None
) -> int:
    """Publish forecasts + elasticity + inventory to Redis; return number of SKUs written.

    Args:
        forecast_df: [item_id, forecast_demand].
        graph_df: [item_id, related_item_id, elasticity_weight, community].
        inventory_df: optional [item_id, stock] from batch.etl.inventory.build_inventory. When
            omitted, inventory keys are left untouched (existing values, if any, are preserved).
    """
    r = get_redis()
    # Collect elasticity into per-SKU JSON lists (small after aggregation).
    elasticity_map: dict[int, list[dict]] = {}
    community_map: dict[int, int] = {}
    for row in graph_df.collect():
        elasticity_map.setdefault(row["item_id"], []).append(
            {"related": row["related_item_id"], "weight": float(row["elasticity_weight"])}
        )
        if "community" in row and row["community"] is not None:
            community_map[row["item_id"]] = int(row["community"])

    inventory_map: dict[int, int] = {}
    if inventory_df is not None:
        inventory_map = {row["item_id"]: int(row["stock"]) for row in inventory_df.collect()}

    forecast_map: dict[int, float] = {
        row["item_id"]: float(row["forecast_demand"]) for row in forecast_df.collect()
    }

    # forecast_df only covers each item's *latest* unlabeled day (see train_forecast), a much
    # smaller set than every item that ever appears in the graph's transaction history -- looping
    # over forecast_df alone silently dropped elasticity/community for every SKU outside that
    # narrow overlap. Union all four sources so each SKU gets whichever data is available for it.
    all_skus = set(forecast_map) | set(elasticity_map) | set(community_map) | set(inventory_map)

    count = 0
    for sku in all_skus:
        pipe = r.pipeline()
        if sku in forecast_map:
            pipe.set(RedisKeys.forecast(sku), forecast_map[sku])
        if sku in elasticity_map:
            pipe.set(RedisKeys.elasticity(sku), json.dumps(elasticity_map[sku]))
        if sku in community_map:
            pipe.set(RedisKeys.community(sku), community_map[sku])
        if sku in inventory_map:
            pipe.set(RedisKeys.inventory(sku), inventory_map[sku])
        pipe.execute()  # atomic per SKU
        count += 1
    RECORDS_PROCESSED.labels(component="batch.publish").inc(count)
    log.info("batch.publish.done", skus=count)
    return count
