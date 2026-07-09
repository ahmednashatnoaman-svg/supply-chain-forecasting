# Streaming pricing engine (streaming/pipeline/pricing_stream.py). Spark Structured Streaming
# client that submits to a remote cluster -- point it at one via SPARK_MASTER (e.g. spark://spark-master:7077)
# and the rest of the *_HOST/*_BOOTSTRAP_SERVERS env vars from libs/scf_common/config.
#
# Both /opt/spark AND /opt/java/openjdk are copied from the SAME apache/spark:3.5.1 image used by
# spark-master/spark-worker in docker-compose. Two independent mismatches will produce the same
# symptom (EOFException deserializing task results between driver and executor) if not pinned:
#   1. Jars: PyPI's pyspark wheel and the official binary distribution are independent builds of
#      "3.5.1" that can disagree at the wire-protocol level. `pip install .` still provides the
#      pyspark Python package, but PYTHONPATH puts the copied (jar-matched) one ahead of it.
#   2. JVM: apache/spark:3.5.1 bundles Temurin Java 11. Installing a JRE from python:3.10-slim's
#      own apt repo (e.g. openjdk-21-jre-headless) runs those same 3.5.1 jars on a different major
#      JVM version than the executors use on spark-worker -- copying the base image's JDK directly
#      is what actually guarantees a matching runtime, not just matching jars.
#
# Build from the repo root:
#   docker build -f infra/docker/pricing-stream.Dockerfile -t scf-pricing-stream .
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
COPY streaming ./streaming
COPY ingestion ./ingestion
COPY automation ./automation
COPY batch ./batch

RUN pip install --no-cache-dir .

ENTRYPOINT ["python", "-m", "streaming.pipeline.pricing_stream"]
