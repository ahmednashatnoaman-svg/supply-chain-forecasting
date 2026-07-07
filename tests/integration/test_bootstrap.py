"""Integration test — Task 2 (Nagy plan).

Asserts that ``scripts/bootstrap.sh`` has been run and produced:
  - All four Kafka topics declared in ``contracts/avro/topics.yml``.
  - HDFS medallion directories ``/data/{bronze,silver,gold}``.

Run *after* the stack is healthy and bootstrap.sh has executed:

    bash scripts/bootstrap.sh
    pytest tests/integration/test_bootstrap.py -v -m integration

The tests shell out to docker-compose exec so they require Docker to be running.
They are skipped gracefully when Docker is unavailable.
"""

from __future__ import annotations

import subprocess

import pytest

pytestmark = pytest.mark.integration

COMPOSE_CMD = [
    "docker",
    "compose",
    "-f",
    "infra/docker/docker-compose.yml",
    "exec",
    "-T",
]

EXPECTED_TOPICS = [
    "live_web_traffic",
    "inventory_updates",
    "system_alerts",
    "automated_pricing_updates",
]

HDFS_ZONES = ["bronze", "silver", "gold"]


def _run(args: list[str], **kwargs) -> subprocess.CompletedProcess:  # type: ignore[type-arg]
    """Run a subprocess command, returning the result (not raising on failure)."""
    return subprocess.run(
        args,
        capture_output=True,
        text=True,
        timeout=30,
        **kwargs,
    )


def _skip_if_docker_down() -> None:
    """Skip the test if Docker is not reachable."""
    result = _run(["docker", "info"])
    if result.returncode != 0:
        pytest.skip("Docker not available — compose stack not running")


@pytest.mark.parametrize("topic", EXPECTED_TOPICS)
def test_kafka_topic_exists(topic: str) -> None:
    """Assert each topic declared in contracts/avro/topics.yml exists in Kafka."""
    _skip_if_docker_down()

    result = _run(
        COMPOSE_CMD
        + [
            "kafka",
            "kafka-topics",
            "--list",
            "--bootstrap-server",
            "localhost:9092",
        ]
    )

    if result.returncode != 0:
        pytest.skip(f"kafka exec failed (stack may not be up): {result.stderr.strip()}")

    listed_topics = result.stdout.splitlines()
    assert topic in listed_topics, (
        f"Topic '{topic}' not found in Kafka. "
        f"Re-run: bash scripts/bootstrap.sh\n"
        f"Found topics: {listed_topics}"
    )


@pytest.mark.parametrize("zone", HDFS_ZONES)
def test_hdfs_medallion_dir_exists(zone: str) -> None:
    """Assert each HDFS medallion directory (bronze/silver/gold) exists."""
    _skip_if_docker_down()

    result = _run(
        COMPOSE_CMD
        + [
            "namenode",
            "hdfs",
            "dfs",
            "-test",
            "-d",
            f"/data/{zone}",
        ]
    )

    stderr = result.stderr.strip().lower()
    if result.returncode == 255 or "is not running" in stderr:
        pytest.skip("namenode exec failed (stack may not be up): " + result.stderr.strip())

    assert result.returncode == 0, (
        f"HDFS directory /data/{zone} does not exist. " f"Re-run: bash scripts/bootstrap.sh"
    )
