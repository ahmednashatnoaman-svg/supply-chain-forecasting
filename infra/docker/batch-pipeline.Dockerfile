# Nightly batch pipeline (batch/airflow_dags/run_batch_once.py): clean -> features -> forecast ->
# graph -> publish, run once against a live Spark + HDFS cluster. Point it at one via SPARK_MASTER
# and HDFS_NAMENODE (see libs/scf_common/config). Build from the repo root:
#   docker build -f infra/docker/batch-pipeline.Dockerfile -t scf-batch-pipeline .
FROM python:3.10-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-21-jre-headless \
    && rm -rf /var/lib/apt/lists/* \
    && ln -sfn "$(dirname "$(dirname "$(readlink -f "$(which java)")")")" /opt/java
ENV JAVA_HOME=/opt/java

WORKDIR /app
COPY pyproject.toml README.md ./
COPY libs ./libs
COPY batch ./batch
COPY streaming ./streaming
COPY ingestion ./ingestion
COPY automation ./automation

RUN pip install --no-cache-dir .

ENTRYPOINT ["python", "-m", "batch.airflow_dags.run_batch_once"]
