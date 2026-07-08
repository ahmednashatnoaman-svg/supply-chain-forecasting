# Kafka traffic generator (ingestion/generator). Build from the repo root:
#   docker build -f infra/docker/traffic-generator.Dockerfile -t scf-traffic-generator .
FROM python:3.10-slim

WORKDIR /app
COPY pyproject.toml README.md ./
COPY libs ./libs
COPY ingestion ./ingestion
COPY automation ./automation
COPY batch ./batch
COPY streaming ./streaming

RUN pip install --no-cache-dir .

ENTRYPOINT ["python", "-m", "ingestion.generator.traffic_generator"]
