"""Integration test: system_alerts -> n8n reorder bridge (Ziad plan Task 3).

Produces a LOW_STOCK alert onto real Kafka (testcontainers + Apicurio ccompat), runs the bridge
consumer loop pointed at an in-process stub HTTP server, and asserts the stub receives exactly one
POST whose `qty` refills stock to the target. Exercises the real AvroKafkaConsumer/Producer path
plus the retry/backoff POST helper — no mocks of the IO layer.
"""

from __future__ import annotations

import json
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

pytest.importorskip("confluent_kafka")
pytestmark = pytest.mark.integration


class _StubHandler(BaseHTTPRequestHandler):
    """Records every POST body. ``log_message`` is silenced to keep test output clean."""

    received: list[dict] = []

    def do_POST(self) -> None:  # noqa: N802 - http.server API
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length)
        try:
            self.received.append(json.loads(body))
        except json.JSONDecodeError:
            self.received.append({"raw": body.decode(errors="replace")})
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b'{"ok":true}')

    def log_message(self, fmt, *args):  # noqa: ARG002 - silence default request logging
        pass


@pytest.fixture()
def stub_webhook():
    """Start an HTTP server on a free port in a daemon thread; yield (url, received_list)."""
    _StubHandler.received = []
    server = HTTPServer(("127.0.0.1", 0), _StubHandler)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{port}/webhook/reorder", _StubHandler.received
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


@pytest.fixture()
def system_alerts_topic(kafka_env, kafka_bootstrap: str):
    """Create the ``system_alerts`` topic (auto-create is disabled in the dev stack)."""
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient({"bootstrap.servers": kafka_bootstrap})
    fs = admin.create_topics([NewTopic("system_alerts", num_partitions=3, replication_factor=1)])
    for _, future in fs.items():
        future.result(timeout=30)
    yield "system_alerts"


def _wait_for_stub(received: list[dict], count: int, timeout: float = 30.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline and len(received) < count:
        time.sleep(0.2)
    assert len(received) >= count, f"stub received {len(received)} POSTs, expected {count}"


def test_bridge_posts_reorder_for_low_stock_alert(kafka_env, system_alerts_topic, stub_webhook):
    from libs.scf_common.io.kafka import AvroKafkaProducer

    stub_url, received = stub_webhook

    # --- produce one LOW_STOCK alert via the real Avro producer (auto-registers schema) ---
    producer = AvroKafkaProducer(system_alerts_topic, "system_alerts.avsc", key_field="item_id")
    alert = {
        "event_time": int(time.time() * 1000),
        "item_id": 10,
        "alert_type": "LOW_STOCK",
        "stock_level": 5,
        "reorder_threshold": 50,
        "message": "stock critically low during surge",
    }
    producer.produce(alert)
    producer.flush(10)

    # --- run the bridge for exactly one message, pointed at the stub ---
    from automation.n8n.alert_bridge import run_bridge

    posted = run_bridge(max_messages=1, webhook_url=stub_url, poll_timeout=1.0)

    # --- the stub received exactly one POST with the expected refill qty ---
    _wait_for_stub(received, count=1)
    assert len(received) == 1
    po = received[0]
    assert po["sku"] == 10
    # target default = 200, stock = 5 -> qty = 195
    assert po["qty"] == 195
    assert po["supplier_email"]

    # the bridge also returns the posted payloads
    assert len(posted) == 1
    assert posted[0]["qty"] == 195


def test_post_with_retry_returns_false_when_unreachable():
    """The retry helper returns False after exhausting retries (no server up)."""
    from automation.n8n.alert_bridge import _post_with_retry

    ok = _post_with_retry("http://127.0.0.1:1/no-such-port", {"sku": 1}, retries=2, timeout=0.5)
    assert ok is False
