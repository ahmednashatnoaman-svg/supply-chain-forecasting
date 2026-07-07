"""Unit test — Task 4 (Nagy plan).

Asserts ``helm lint infra/helm/supply-chain`` exits 0.
Skipped automatically when ``helm`` is not installed (CI without Helm).

Run:
    pytest tests/unit/test_helm_lint.py -v
"""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

CHART_DIR = Path(__file__).resolve().parents[2] / "infra" / "helm" / "supply-chain"


def test_helm_lint() -> None:
    """helm lint must pass with zero errors.

    Skips when helm binary is absent so the unit suite stays green in
    environments that don't have Helm installed (pure-Python CI).
    """
    helm = shutil.which("helm")
    if helm is None:
        pytest.skip("helm binary not found — install Helm to run this test")

    result = subprocess.run(
        [helm, "lint", str(CHART_DIR)],
        capture_output=True,
        text=True,
        timeout=60,
    )

    assert result.returncode == 0, (
        f"helm lint failed with exit code {result.returncode}.\n"
        f"STDOUT:\n{result.stdout}\n"
        f"STDERR:\n{result.stderr}"
    )
    # Warn but don't fail on lint warnings (non-zero only on errors).
    if "[WARNING]" in result.stdout:
        # Print to stdout so it's visible in verbose pytest output.
        print(f"\nHelm lint warnings:\n{result.stdout}")


def test_chart_yaml_exists() -> None:
    """Chart.yaml must exist and be non-empty (static check, no helm needed)."""
    chart_yaml = CHART_DIR / "Chart.yaml"
    assert chart_yaml.exists(), f"Chart.yaml not found at {chart_yaml}"
    assert chart_yaml.stat().st_size > 0, "Chart.yaml is empty"


def test_values_yaml_exists() -> None:
    """values.yaml must exist and contain key service definitions."""
    values_yaml = CHART_DIR / "values.yaml"
    assert values_yaml.exists(), f"values.yaml not found at {values_yaml}"
    content = values_yaml.read_text()
    for key in ("redis", "kafka", "sparkWorker"):
        assert key in content, f"values.yaml missing '{key}' section"


def test_templates_directory_not_empty() -> None:
    """templates/ must contain ≥1 .yaml file mirroring compose services."""
    templates = list((CHART_DIR / "templates").glob("*.yaml"))
    assert templates, (
        "infra/helm/supply-chain/templates/ is empty — "
        "add Deployment + Service manifests mirroring the compose stack"
    )
