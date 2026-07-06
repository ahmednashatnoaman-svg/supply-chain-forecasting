"""GraphFrames product cross-elasticity + communities. Implements Emad plan Task 5."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def cooccurrence_edges(transactions: DataFrame) -> DataFrame:
    """Co-purchase edges from basket self-join.

    Args:
        transactions: silver transactions with columns [visitor_id, item_id].

    Returns:
        edges: item_id (src), related_item_id (dst), elasticity_weight (normalized co-count).
    """
    left = transactions.select("visitor_id", F.col("item_id").alias("src"))
    right = transactions.select("visitor_id", F.col("item_id").alias("dst"))
    pairs = (
        left.join(right, "visitor_id")
        .filter(F.col("src") < F.col("dst"))
        .groupBy("src", "dst")
        .agg(F.count("*").alias("co_count"))
    )
    max_c = pairs.agg(F.max("co_count")).first()[0] or 1
    return pairs.select(
        F.col("src").alias("item_id"),
        F.col("dst").alias("related_item_id"),
        (F.col("co_count") / F.lit(max_c)).alias("elasticity_weight"),
    )


def build_graph(transactions: DataFrame) -> DataFrame:
    """Return the elasticity edge table (thin wrapper; PageRank/communities added by owner).

    TODO(emad-plan Task 5): construct a GraphFrame from vertices+edges, run `pageRank` and
    `labelPropagation`, and join community ids to produce the product_graph gold table.
    """
    return cooccurrence_edges(transactions)
