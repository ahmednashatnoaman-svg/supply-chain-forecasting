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


def _placeholder(step: str, **_):  # pragma: no cover
    """TODO(emad-plan): replace with real Spark job invocation for `step`."""
    print(f"[nightly_forecast] running step: {step}")


with DAG(
    dag_id="nightly_forecast",
    description="Nightly demand forecast + cross-elasticity publish",
    schedule="0 2 * * *",  # 02:00 daily
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["batch", "forecast"],
) as dag:
    steps = ["clean_events", "build_features", "data_quality_gate", "train_forecast", "build_graph", "publish_redis"]
    tasks = [
        PythonOperator(task_id=s, python_callable=_placeholder, op_kwargs={"step": s})
        for s in steps
    ]
    for upstream, downstream in zip(tasks, tasks[1:]):
        upstream >> downstream
