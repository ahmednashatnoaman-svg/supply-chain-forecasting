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
]
