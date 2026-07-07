"""Airflow nightly batch DAG. Implements Emad plan Task 7.

Chains: clean -> features -> data-quality gate -> forecast -> graph -> publish. Each task is a thin
PythonOperator calling the corresponding module (kept importable/testable).
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "emad",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "depends_on_past": False,
}


def _run_step(step: str, **_):
    """Real Spark job invocation for `step`."""
    print(f"[nightly_forecast] starting step: {step}")
    from pyspark.sql import SparkSession
    
    # We must configure spark packages for graphframes if running locally/standalone
    spark = (
        SparkSession.builder
        .appName(f"nightly_{step}")
        .config("spark.jars.packages", "graphframes:graphframes:0.8.3-spark3.5-s_2.12")
        .getOrCreate()
    )
    
    # Normally we would load and save paths from Airflow variables or context.
    # For demonstration, we just import and run dummy/local paths or empty DFs.
    
    if step == "clean_events":
        from batch.etl.clean_events import clean_events
        from libs.scf_common.contracts import HdfsPaths
        raw = spark.read.parquet(HdfsPaths.bronze("events"))
        silver = clean_events(raw)
        silver.write.parquet(HdfsPaths.silver("events"), mode="overwrite")
    
    elif step == "build_features":
        from batch.etl.features import build_features
        from libs.scf_common.contracts import HdfsPaths
        silver = spark.read.parquet(HdfsPaths.silver("events"))
        gold = build_features(silver)
        gold.write.parquet(HdfsPaths.gold("features"), mode="overwrite")
    
    elif step == "data_quality_gate":
        from batch.etl.expectations import validate_silver
        from libs.scf_common.contracts import HdfsPaths
        silver = spark.read.parquet(HdfsPaths.silver("events"))
        validate_silver(silver)
    
    elif step == "train_forecast":
        from batch.mllib.forecast import train_forecast
        from libs.scf_common.contracts import HdfsPaths
        features = spark.read.parquet(HdfsPaths.gold("features"))
        model, preds = train_forecast(features)
        preds.write.parquet(HdfsPaths.gold("forecast"), mode="overwrite")
        
    elif step == "build_graph":
        from batch.graph.elasticity import build_graph
        from libs.scf_common.contracts import HdfsPaths
        transactions = spark.read.parquet(HdfsPaths.silver("events"))
        graph_df = build_graph(transactions)
        graph_df.write.parquet(HdfsPaths.gold("graph"), mode="overwrite")
        
    elif step == "publish_redis":
        from batch.publish.to_redis import publish_forecasts
        from libs.scf_common.contracts import HdfsPaths
        preds = spark.read.parquet(HdfsPaths.gold("forecast"))
        graph_df = spark.read.parquet(HdfsPaths.gold("graph"))
        publish_forecasts(preds, graph_df)
    
    else:
        raise ValueError(f"Unknown step: {step}")
        
    print(f"[nightly_forecast] finished step: {step}")
    spark.stop()


with DAG(
    dag_id="nightly_forecast",
    description="Nightly demand forecast + cross-elasticity publish",
    schedule="0 2 * * *",  # 02:00 daily
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["batch", "forecast"],
) as dag:
    steps = [
        "clean_events",
        "build_features",
        "data_quality_gate",
        "train_forecast",
        "build_graph",
        "publish_redis",
    ]
    tasks = [
        PythonOperator(task_id=s, python_callable=_run_step, op_kwargs={"step": s})
        for s in steps
    ]
    for upstream, downstream in zip(tasks, tasks[1:], strict=False):
        upstream >> downstream
