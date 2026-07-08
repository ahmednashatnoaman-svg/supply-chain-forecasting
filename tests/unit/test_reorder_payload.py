"""Unit tests for the reorder payload builder (Ziad plan Task 2). Pure."""

from __future__ import annotations

import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

from automation.n8n.alert_bridge import _post_with_retry, build_reorder

pytestmark = pytest.mark.unit


def test_build_reorder_refills_to_target():
    alert = {"item_id": 42, "stock_level": 5, "reorder_threshold": 50}
    po = build_reorder(alert, target_stock=200)
    assert po["sku"] == 42
    assert po["qty"] == 195
    assert "42" in po["subject"]
    assert po["supplier_email"]


def test_build_reorder_no_negative_qty():
    alert = {"item_id": 1, "stock_level": 500, "reorder_threshold": 50}
    assert build_reorder(alert, target_stock=200)["qty"] == 0


class _HeaderCapturingHandler(BaseHTTPRequestHandler):
    received_headers: list[dict] = []

    def do_POST(self) -> None:  # noqa: N802 - http.server API
        length = int(self.headers.get("Content-Length", "0"))
        self.rfile.read(length)
        self.received_headers.append(dict(self.headers))
        self.send_response(200)
        self.end_headers()

    def log_message(self, fmt, *args):  # noqa: ARG002 - silence default request logging
        pass


@pytest.fixture()
def stub_server():
    _HeaderCapturingHandler.received_headers = []
    server = HTTPServer(("127.0.0.1", 0), _HeaderCapturingHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}/webhook/reorder"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def test_post_with_retry_sends_shared_secret_header_when_configured(stub_server):
    """Security audit follow-up: a reachable-but-unauthenticated n8n shouldn't be triggerable by
    anyone who guesses the webhook URL -- alert_bridge sends this header so a downstream shared-
    secret check (see reorder_workflow.json's "Verify Shared Secret" node) can reject the rest.
    """
    ok = _post_with_retry(stub_server, {"sku": 1}, retries=1, timeout=2.0, secret="s3cr3t-value")
    assert ok is True
    assert _HeaderCapturingHandler.received_headers[0]["X-Webhook-Secret"] == "s3cr3t-value"


def test_post_with_retry_omits_header_when_secret_not_configured(stub_server):
    """Default (empty) secret -- matches the zero-config demo, no header sent at all."""
    ok = _post_with_retry(stub_server, {"sku": 1}, retries=1, timeout=2.0)
    assert ok is True
    assert "X-Webhook-Secret" not in _HeaderCapturingHandler.received_headers[0]
