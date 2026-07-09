"""Tests for GBT demand forecast model (Emad plan Task 4)."""

from batch.mllib.forecast import train_forecast


def test_train_forecast_schema_and_clamping(spark):
    # label=None is build_features' contract for "today" (no next-day count yet, see
    # features.py) -- train_forecast trains on the labeled history and forecasts forward onto
    # exactly those rows, so item 9 here is the one row that ends up in `preds`.
    features = spark.createDataFrame(
        [
            (1, 10.0, 30.0, 1, 5.0),
            (2, 5.0, 15.0, 2, 0.0),  # label 0 to test clamping if prediction goes negative
            (3, 20.0, 60.0, 3, 10.0),
            (4, 30.0, 90.0, 4, 15.0),
            (5, 12.0, 32.0, 5, 6.0),
            (6, 15.0, 45.0, 6, 8.0),
            (7, 22.0, 66.0, 7, 11.0),
            (8, 25.0, 75.0, 1, 12.0),
            (9, 18.0, 50.0, 3, None),  # "today": no label yet -- this is what gets forecast
        ],
        ["item_id", "velocity_7d", "velocity_30d", "dow", "label"],
    )

    model, preds = train_forecast(features)

    assert "item_id" in preds.columns
    assert "forecast_demand" in preds.columns

    rows = preds.collect()
    assert [r["item_id"] for r in rows] == [9]
    for row in rows:
        assert row["forecast_demand"] >= 0.0
