"""MLlib GBT demand forecast. Implements Emad plan Task 4.

Output schema matches contracts/models/forecast_output.json: (item_id: long, forecast_demand: double).
"""

from __future__ import annotations

import mlflow
import mlflow.spark
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

FEATURE_COLS = ["velocity_7d", "velocity_30d", "dow"]


def train_forecast(features: DataFrame):
    """Train a GBT regressor and produce clamped (>=0) per-SKU demand forecasts.

    Args:
        features: gold features from build_features, with a `label` column = next-period demand.

    Returns:
        (model, predictions_df) where predictions_df has columns [item_id, forecast_demand].
    """
    assembler = VectorAssembler(inputCols=FEATURE_COLS, outputCol="features_vec")
    train = assembler.transform(features.na.fill(0.0))

    gbt = GBTRegressor(featuresCol="features_vec", labelCol="label")

    param_grid = (
        ParamGridBuilder().addGrid(gbt.maxDepth, [3, 5]).addGrid(gbt.maxIter, [10, 20]).build()
    )

    evaluator = RegressionEvaluator(predictionCol="prediction", labelCol="label", metricName="rmse")

    cv = CrossValidator(
        estimator=gbt,
        estimatorParamMaps=param_grid,
        evaluator=evaluator,
        numFolds=2,  # 2 folds for fast tests
        seed=42,
    )

    mlflow.set_experiment("scf-demand-forecast")
    with mlflow.start_run():
        cv_model = cv.fit(train)
        best_model = cv_model.bestModel

        # Log best params
        mlflow.log_param("maxDepth", best_model.getOrDefault("maxDepth"))
        mlflow.log_param("maxIter", best_model.getOrDefault("maxIter"))

        mlflow.spark.log_model(best_model, "gbt_forecast_model")

        preds = (
            best_model.transform(train)
            .withColumn("forecast_demand", F.greatest(F.col("prediction"), F.lit(0.0)))
            .select("item_id", "forecast_demand")
        )
        return best_model, preds
