# Streaming pricing engine (streaming/pipeline/pricing_stream.py). Spark Structured Streaming
# client that submits to a remote cluster -- point it at one via SPARK_MASTER (e.g. spark://spark-master:7077)
# and the rest of the *_HOST/*_BOOTSTRAP_SERVERS env vars from libs/scf_common/config.
#
# /opt/spark is copied from the SAME apache/spark:3.5.1 image used by spark-master/spark-worker
# in docker-compose, and SPARK_HOME points at it -- PyPI's pyspark wheel and the official binary
# distribution are independent builds of "3.5.1" that can disagree at the driver/executor
# wire-protocol level (observed as EOFException deserializing task results). Running the exact
# same jars on both sides avoids that; `pip install .` still provides the pyspark Python package,
# but PYTHONPATH puts the copied (jar-matched) one ahead of it.
#
# Build from the repo root:
#   docker build -f infra/docker/pricing-stream.Dockerfile -t scf-pricing-stream .
FROM apache/spark:3.5.1 AS spark-base

FROM python:3.10-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-21-jre-headless \
    && rm -rf /var/lib/apt/lists/* \
    && ln -sfn "$(dirname "$(dirname "$(readlink -f "$(which java)")")")" /opt/java
ENV JAVA_HOME=/opt/java

COPY --from=spark-base /opt/spark /opt/spark
ENV SPARK_HOME=/opt/spark
ENV PYTHONPATH=/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip

WORKDIR /app
COPY pyproject.toml README.md ./
COPY libs ./libs
COPY contracts ./contracts
COPY streaming ./streaming
COPY ingestion ./ingestion
COPY automation ./automation
COPY batch ./batch

RUN pip install --no-cache-dir .

ENTRYPOINT ["python", "-m", "streaming.pipeline.pricing_stream"]
