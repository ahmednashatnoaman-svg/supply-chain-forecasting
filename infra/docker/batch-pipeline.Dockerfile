# Nightly batch pipeline (batch/airflow_dags/run_batch_once.py): clean -> features -> forecast ->
# graph -> publish, run once against a live Spark + HDFS cluster. Point it at one via SPARK_MASTER
# and HDFS_NAMENODE (see libs/scf_common/config). Build from the repo root:
#   docker build -f infra/docker/batch-pipeline.Dockerfile -t scf-batch-pipeline .
#
# Both /opt/spark AND /opt/java/openjdk are copied from the SAME apache/spark:3.5.1 image used by
# spark-master/spark-worker in docker-compose -- see pricing-stream.Dockerfile's comment for why
# both the jars (PyPI's pyspark wheel vs. the official binary distribution) and the JVM major
# version (Temurin 11 vs. whatever python:3.10-slim's apt repo installs) must match the cluster's
# executors, or every collect()/take() surfaces as an EOFException deserializing task results.
FROM apache/spark:3.5.1 AS spark-base

FROM python:3.10-slim

COPY --from=spark-base /opt/java/openjdk /opt/java/openjdk
ENV JAVA_HOME=/opt/java/openjdk
ENV PATH="${JAVA_HOME}/bin:${PATH}"

COPY --from=spark-base /opt/spark /opt/spark
ENV SPARK_HOME=/opt/spark
ENV PYTHONPATH=/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip

WORKDIR /app
COPY pyproject.toml README.md ./
COPY libs ./libs
COPY contracts ./contracts
COPY batch ./batch
COPY streaming ./streaming
COPY ingestion ./ingestion
COPY automation ./automation

RUN pip install --no-cache-dir .

ENTRYPOINT ["python", "-m", "batch.airflow_dags.run_batch_once"]
