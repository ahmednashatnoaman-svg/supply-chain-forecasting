# Streamlit command center (automation/dashboard). Build from the repo root:
#   docker build -f infra/docker/dashboard.Dockerfile -t scf-dashboard .
FROM python:3.10-slim

WORKDIR /app
COPY pyproject.toml README.md ./
COPY libs ./libs
COPY automation ./automation
COPY batch ./batch
COPY streaming ./streaming
COPY ingestion ./ingestion

RUN pip install --no-cache-dir .

EXPOSE 8501
ENTRYPOINT ["streamlit", "run", "automation/dashboard/app.py", "--server.address=0.0.0.0"]
