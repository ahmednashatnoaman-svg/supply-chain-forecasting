# Streaming pricing engine (streaming/pipeline/pricing_stream.py). Spark Structured Streaming
# client that submits to a remote cluster -- point it at one via SPARK_MASTER (e.g. spark://spark-master:7077)
# and the rest of the *_HOST/*_BOOTSTRAP_SERVERS env vars from libs/scf_common/config.
# Build from the repo root:
#   docker build -f infra/docker/pricing-stream.Dockerfile -t scf-pricing-stream .
FROM python:3.10-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

WORKDIR /app
COPY pyproject.toml README.md ./
COPY libs ./libs
COPY streaming ./streaming
COPY ingestion ./ingestion
COPY automation ./automation
COPY batch ./batch

RUN pip install --no-cache-dir .

ENTRYPOINT ["python", "-m", "streaming.pipeline.pricing_stream"]
