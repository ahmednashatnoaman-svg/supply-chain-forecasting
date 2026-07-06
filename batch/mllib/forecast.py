"""MLlib GBT demand forecast. Implements Emad plan Task 4.

Output schema matches contracts/models/forecast_output.json: (item_id: long, forecast_demand: double).
"""

from __future__ import annotations

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import GBTRegressor
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

FEATURE_COLS = ["velocity_7d", "velocity_30d", "dow"]


def train_forecast(features: DataFrame):
    """Train a GBT regressor and produce clamped (>=0) per-SKU demand forecasts.

    Args:
        features: gold features from build_features, with a `label` column = next-period demand.

    Returns:
        (model, predictions_df) where predictions_df has columns [item_id, forecast_demand].

    TODO(emad-plan Task 4): add MLflow logging (`mlflow.spark.log_model(...)`) and cross-validation.
    """
    assembler = VectorAssembler(inputCols=FEATURE_COLS, outputCol="features_vec")
    train = assembler.transform(features.na.fill(0.0))
    gbt = GBTRegressor(featuresCol="features_vec", labelCol="label", maxIter=20)
    model = gbt.fit(train)
    preds = (
        model.transform(train)
        .withColumn("forecast_demand", F.greatest(F.col("prediction"), F.lit(0.0)))
        .select("item_id", "forecast_demand")
    )
    return model, preds
