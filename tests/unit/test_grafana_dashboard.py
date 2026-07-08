"""Unit test: the Grafana platform-overview dashboard is valid and references real metrics.

Guards against the metric-name-as-string-literal drift between the dashboard JSON and the Python
code that actually emits each metric (libs/scf_common/observability + the handful of call sites
that increment SURGE_EVENTS_TOTAL / LOW_STOCK_ALERTS_TOTAL) -- a typo in either place would
otherwise only surface as a silently-empty Grafana panel.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

DASHBOARD_PATH = (
    Path(__file__).resolve().parents[2]
    / "infra"
    / "monitoring"
    / "grafana"
    / "dashboards"
    / "platform-overview.json"
)


@pytest.fixture(scope="module")
def dashboard():
    return json.loads(DASHBOARD_PATH.read_text())


def _metric_names_in(dashboard: dict) -> set[str]:
    names: set[str] = set()
    for panel in dashboard["panels"]:
        for target in panel.get("targets", []):
            names.update(re.findall(r"\bscf_[a-z_]+\b", target["expr"]))
    return names


def test_dashboard_is_valid_json_with_unique_panel_ids(dashboard):
    ids = [p["id"] for p in dashboard["panels"]]
    assert len(ids) == len(set(ids)), "duplicate panel id would silently break Grafana rendering"


def test_dashboard_references_only_real_scf_metrics(dashboard):
    """Every scf_* metric name in a panel's PromQL must be one this codebase actually emits."""
    from libs.scf_common.observability import (
        ERRORS_TOTAL,
        LATENCY_SECONDS,
        LOW_STOCK_ALERTS_TOTAL,
        RECORDS_PROCESSED,
        SURGE_EVENTS_TOTAL,
    )

    # prometheus_client's Counter._name strips the trailing "_total" it adds at exposition time
    # (e.g. "scf_records_processed", not "...processed_total") -- add it back for the counters.
    real_metric_names = {
        RECORDS_PROCESSED._name + "_total",
        ERRORS_TOTAL._name + "_total",
        LATENCY_SECONDS._name,
        SURGE_EVENTS_TOTAL._name + "_total",
        LOW_STOCK_ALERTS_TOTAL._name + "_total",
    }
    # Histograms expose derived series (_bucket/_sum/_count) in PromQL, not the base name.
    referenced = {
        (
            name.split("_bucket")[0].split("_sum")[0].split("_count")[0]
            if name.endswith(("_bucket", "_sum", "_count"))
            else name
        )
        for name in _metric_names_in(dashboard)
    }
    assert (
        referenced <= real_metric_names
    ), f"unknown metrics referenced: {referenced - real_metric_names}"


def test_dashboard_has_a_pricing_and_inventory_panel(dashboard):
    """Business-level panels (Grafana audit follow-up) -- not just infra plumbing health."""
    titles = [p["title"] for p in dashboard["panels"]]
    assert any("Surge" in t for t in titles)
    assert any("Low-Stock" in t for t in titles)
