"""Airflow nightly batch DAG. Implements Emad plan Task 7.

Launches the real batch pipeline (`batch.airflow_dags.run_batch_once`, the same clean -> features
-> gate -> forecast -> graph -> inventory -> pricing -> publish chain `make batch` runs) inside the
scf-batch-pipeline container via DockerOperator, instead of running Spark in-process inside this
Airflow container. Airflow's own image has no pyspark, no matching JDK, and no SPARK_MASTER
pointed at the cluster -- the previous version of this DAG tried to `SparkSession.builder
.getOrCreate()` directly here and failed immediately with `ModuleNotFoundError: No module named
'pyspark'`. It also split the pipeline into six separate PythonOperator steps that quietly drifted
from run_batch_once.py's real fixes (CSV bronze read, the build_graph transaction filter,
build_inventory, build_pricing) since nothing enforced the two implementations staying in sync. One
DockerOperator task now IS the single source of truth -- there is no second implementation to drift.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

default_args = {
    "owner": "emad",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "depends_on_past": False,
}

# Must match the network docker-compose creates (project name `scf` + service network `scf-net`)
# so the container can resolve kafka/namenode/spark-master/redis by their compose service names.
_NETWORK = "scf_scf-net"

with DAG(
    dag_id="nightly_forecast",
    description="Nightly demand forecast + cross-elasticity + pricing publish",
    schedule="0 2 * * *",  # 02:00 daily
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["batch", "forecast"],
) as dag:
    run_batch_pipeline = DockerOperator(
        task_id="run_batch_pipeline",
        image="scf-batch-pipeline:latest",
        # No fixed container_name: Docker auto-generates a unique one per run. A fixed name
        # collides (409 Conflict) with any leftover container from a previous run, a manual
        # `docker run` of the same image, or the next scheduled/retried run before this one's
        # cleaned up -- observed repeatedly failing real DAG runs for exactly this reason. The
        # cost is Prometheus's static `scf-batch-pipeline-run:8001` scrape target won't resolve
        # during Airflow-launched runs (already a no-op the rest of the time -- see
        # docs/runbooks/orchestration-guide.md §3, this is a one-shot job Prometheus is expected
        # to see "down" between runs regardless).
        api_version="auto",
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        network_mode=_NETWORK,
        mount_tmp_dir=False,
        environment={
            "SPARK_MASTER": "spark://spark-master:7077",
            "SPARK_EXECUTOR_MEMORY": "4g",
            "SPARK_DRIVER_MEMORY": "2g",
            "REDIS_HOST": "redis",
            "REDIS_PORT": "6379",
            "REDIS_DB": "0",
            "HDFS_NAMENODE": "hdfs://namenode:9000",
            "HDFS_BRONZE": "/data/bronze",
            "HDFS_SILVER": "/data/silver",
            "HDFS_GOLD": "/data/gold",
        },
    )
