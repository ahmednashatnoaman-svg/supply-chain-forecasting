"""Tests for GraphFrames cross-elasticity and communities (Emad plan Task 5)."""

from batch.graph.elasticity import build_graph


def test_build_graph_computes_communities_and_pagerank(spark):
    # Enable checkpointing for GraphFrames
    spark.sparkContext.setCheckpointDir("/tmp/graphframes_checkpoints")

    transactions = spark.createDataFrame(
        [
            # visitor 1 buys items 10 and 20
            (1, 10),
            (1, 20),
            # visitor 2 buys items 10, 20, 30
            (2, 10),
            (2, 20),
            (2, 30),
            # visitor 3 buys items 40, 50 (separate cluster)
            (3, 40),
            (3, 50),
        ],
        ["visitor_id", "item_id"],
    )

    graph_df = build_graph(transactions)

    # Verify expected columns
    expected_cols = {"item_id", "related_item_id", "elasticity_weight", "community", "pagerank"}
    assert expected_cols.issubset(set(graph_df.columns))

    rows = graph_df.collect()

    # Check that there are edges
    assert len(rows) > 0

    # Check that community and pagerank are assigned
    for row in rows:
        assert row["community"] is not None
        assert row["pagerank"] is not None
        assert row["elasticity_weight"] > 0
