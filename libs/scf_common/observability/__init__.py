"""Structured logging + Prometheus metrics shared by all layers.

Every job should: `log = get_logger(__name__)` and increment the standard metrics
(`records_processed`, `errors_total`, `latency_seconds`).
"""

from __future__ import annotations

import logging
import sys

import structlog
from prometheus_client import Counter, Histogram, start_http_server

from libs.scf_common.config import settings

# ---- Standard metrics (label by `component`) ----
RECORDS_PROCESSED = Counter("scf_records_processed_total", "Records processed", ["component"])
ERRORS_TOTAL = Counter("scf_errors_total", "Errors encountered", ["component"])
LATENCY_SECONDS = Histogram("scf_latency_seconds", "Processing latency in seconds", ["component"])

# ---- Business metrics (Grafana "Pricing & Inventory" row) ----
# Unlike the generic metrics above, these surface the project's actual value proposition (surge
# pricing, automated reordering) rather than plumbing health -- see pricing_stream.price_row /
# check_low_stock_alert, the only two places these are incremented.
SURGE_EVENTS_TOTAL = Counter(
    "scf_surge_events_total", "Surge-priced windows detected", ["component"]
)
LOW_STOCK_ALERTS_TOTAL = Counter(
    "scf_low_stock_alerts_total", "Low-stock alerts raised", ["component"]
)


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    """Return a structlog JSON logger configured at the settings log level."""
    logging.basicConfig(format="%(message)s", stream=sys.stdout, level=settings.log_level)
    structlog.configure(
        processors=[
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            logging.getLevelName(settings.log_level)
        ),
        cache_logger_on_first_use=True,
    )
    return structlog.get_logger(name)


def serve_metrics(port: int) -> None:
    """Expose /metrics for Prometheus to scrape (call once at app start)."""
    start_http_server(port)


__all__ = [
    "get_logger",
    "serve_metrics",
    "RECORDS_PROCESSED",
    "ERRORS_TOTAL",
    "LATENCY_SECONDS",
    "SURGE_EVENTS_TOTAL",
    "LOW_STOCK_ALERTS_TOTAL",
]
