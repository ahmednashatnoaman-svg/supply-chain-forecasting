# Airflow orchestrator, extended with the Docker provider only. nightly_dag.py does NOT run Spark
# in-process here (this image intentionally has no pyspark/JDK) -- it uses DockerOperator to launch
# the real scf-batch-pipeline image against the live cluster, the same image/command verified by
# `make batch`. Keeps Airflow itself lightweight and avoids duplicating JDK/jar version-matching
# (see pricing-stream.Dockerfile's comment) in a third custom image.
#
# Build from the repo root:
#   docker build -f infra/docker/airflow.Dockerfile -t scf-airflow .
FROM apache/airflow:2.9.2

RUN pip install --no-cache-dir apache-airflow-providers-docker
