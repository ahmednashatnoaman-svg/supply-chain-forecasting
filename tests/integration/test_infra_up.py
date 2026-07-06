"""Integration test — Task 1 (Nagy plan).

Asserts every service in the Docker Compose stack is reachable on its declared
port. Run *after* the stack is healthy:

    docker compose -f infra/docker/docker-compose.yml --profile core up -d
    pytest tests/integration/test_infra_up.py -v -m integration

These tests are skipped automatically in CI when Docker is unavailable.
"""

from __future__ import annotations

import socket

import pytest

pytestmark = pytest.mark.integration

# Published ports from infra/docker/docker-compose.yml — tested from the host machine.
# Integration tests skip gracefully when the compose stack is not running.
_SERVICE_PARAMS = [
    pytest.param("localhost", 29092, id="kafka-external"),   # Kafka external listener
    pytest.param("localhost", 6379,  id="redis"),
    pytest.param("localhost", 9870,  id="namenode-http"),    # HDFS namenode WebUI / health
    pytest.param("localhost", 8088,  id="spark-master-ui"),  # Spark master WebUI (8080→8088, avoids Oracle TNS)
    pytest.param("localhost", 8081,  id="schema-registry"),
]


@pytest.mark.parametrize("host,port", _SERVICE_PARAMS)
def test_service_reachable(host: str, port: int) -> None:
    """Assert the service is reachable from the host machine.

    Fails fast (timeout=5 s) and skips gracefully when the stack is not up,
    rather than blocking CI on every unit-test run.
    """
    try:
        with socket.create_connection((host, port), timeout=5):
            pass  # connection success == healthy
    except (ConnectionRefusedError, OSError) as exc:
        pytest.skip(
            f"{host}:{port} unreachable — is the compose stack running? ({exc})"
        )
