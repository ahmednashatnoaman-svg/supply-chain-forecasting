"""Integration test — Task 3 (Nagy plan).

Asserts Prometheus is scraping correctly:
  - Queries ``/api/v1/targets`` and verifies every configured job has at least
    one target that is **up**.

Run *after* the full-profile stack is healthy:

    docker compose -f infra/docker/docker-compose.yml --profile full up -d
    pytest tests/integration/test_monitoring.py -v -m integration

Tests are skipped gracefully when Prometheus is not reachable.
"""

from __future__ import annotations

import urllib.error
import urllib.request
import json

import pytest

pytestmark = pytest.mark.integration

PROMETHEUS_URL = "http://localhost:9090"

# Jobs declared in infra/monitoring/prometheus/prometheus.yml.
# The Python-layer jobs (streaming/batch/ingestion) target host.docker.internal
# and will only be up when those apps are running — we mark them as "optional"
# so CI doesn't fail before those layers are implemented.
REQUIRED_JOBS: list[str] = ["prometheus", "redis", "kafka"]
OPTIONAL_JOBS: list[str] = ["streaming", "batch", "ingestion"]


def _fetch_targets() -> dict:  # type: ignore[type-arg]
    """Fetch Prometheus /api/v1/targets. Returns parsed JSON or raises."""
    url = f"{PROMETHEUS_URL}/api/v1/targets"
    try:
        with urllib.request.urlopen(url, timeout=10) as resp:
            return json.loads(resp.read())
    except (urllib.error.URLError, OSError) as exc:
        pytest.skip(f"Prometheus unreachable at {PROMETHEUS_URL}: {exc}")


def test_prometheus_api_reachable() -> None:
    """Assert the Prometheus HTTP API responds."""
    data = _fetch_targets()
    assert data.get("status") == "success", (
        f"Prometheus targets API returned non-success: {data}"
    )


@pytest.mark.parametrize("job", REQUIRED_JOBS)
def test_required_scrape_job_has_active_targets(job: str) -> None:
    """Assert each required scrape job has ≥1 active target registered."""
    data = _fetch_targets()
    active = data.get("data", {}).get("activeTargets", [])
    job_targets = [t for t in active if t.get("labels", {}).get("job") == job]
    assert job_targets, (
        f"No active targets found for required Prometheus job '{job}'. "
        f"Check infra/monitoring/prometheus/prometheus.yml and ensure the stack "
        f"is running with --profile full."
    )


@pytest.mark.parametrize("job", REQUIRED_JOBS)
def test_required_scrape_job_is_up(job: str) -> None:
    """Assert every required job target reports health == 'up'."""
    data = _fetch_targets()
    active = data.get("data", {}).get("activeTargets", [])
    job_targets = [t for t in active if t.get("labels", {}).get("job") == job]

    if not job_targets:
        pytest.skip(f"No active targets for job '{job}' — run the full stack first")

    down = [t for t in job_targets if t.get("health") != "up"]
    assert not down, (
        f"Some targets for job '{job}' are DOWN:\n"
        + "\n".join(
            f"  {t['labels'].get('instance', '?')} → {t.get('lastError', 'no error')}"
            for t in down
        )
    )


@pytest.mark.parametrize("job", OPTIONAL_JOBS)
def test_optional_scrape_job_up_if_present(job: str) -> None:
    """If an optional job target is present, assert it is healthy (skip otherwise)."""
    data = _fetch_targets()
    active = data.get("data", {}).get("activeTargets", [])
    job_targets = [t for t in active if t.get("labels", {}).get("job") == job]

    if not job_targets:
        pytest.skip(f"Optional job '{job}' has no targets (layer not started)")

    down = [t for t in job_targets if t.get("health") != "up"]
    assert not down, (
        f"Optional job '{job}' targets are DOWN: "
        + str([t.get("lastError") for t in down])
    )
