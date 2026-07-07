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
    """Construct a GraphFrame, run PageRank & LabelPropagation, and return the gold table.

    Returns:
        DataFrame with columns [item_id, related_item_id, elasticity_weight, community, pagerank]
    """
    edges = cooccurrence_edges(transactions)
    
    # Vertices must have an 'id' column
    vertices = (
        edges.select(F.col("item_id").alias("id"))
        .union(edges.select(F.col("related_item_id").alias("id")))
        .distinct()
    )
    
    # Rename edges to src and dst for GraphFrame
    gf_edges = edges.select(
        F.col("item_id").alias("src"), 
        F.col("related_item_id").alias("dst"), 
        "elasticity_weight"
    )
    
    from graphframes import GraphFrame
    g = GraphFrame(vertices, gf_edges)
    
    # Run PageRank (measure of product importance)
    pr = g.pageRank(resetProbability=0.15, maxIter=5)
    
    # Run Label Propagation (community detection)
    communities = g.labelPropagation(maxIter=5)
    
    # Join results back to edges
    res = (
        edges
        .join(communities.select(F.col("id").alias("item_id"), F.col("label").alias("community")), "item_id", "left")
        .join(pr.vertices.select(F.col("id").alias("item_id"), F.col("pagerank")), "item_id", "left")
    )
    
    return res
