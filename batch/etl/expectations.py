"""Great Expectations silver quality gate. Implements Emad plan Task 3."""

from __future__ import annotations

from great_expectations.dataset.sparkdf_dataset import SparkDFDataset
from pyspark.sql import DataFrame


def validate_silver(df: DataFrame) -> bool:
    """Validate that silver events meet contract invariants.
    
    Raises:
        ValueError if any expectation fails.
    """
    gdf = SparkDFDataset(df)
    results = [
        gdf.expect_column_values_to_not_be_null("event_time"),
        gdf.expect_column_values_to_be_between("item_id", min_value=1),
        gdf.expect_column_values_to_be_in_set("event", ["view", "addtocart", "transaction"])
    ]
    
    for result in results:
        if not result.success:
            raise ValueError(f"Silver data quality check failed: {result.expectation_config.expectation_type}")
    
    return True
