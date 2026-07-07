"""Unit test: to_avro_record output validates against the real contract schema (Hatem plan Task 2)."""

from __future__ import annotations

import json

import pytest

fastavro = pytest.importorskip("fastavro")

from ingestion.generator.traffic_generator import to_avro_record  # noqa: E402
from libs.scf_common.io.kafka import load_schema  # noqa: E402

pytestmark = pytest.mark.unit


@pytest.fixture(scope="module")
def schema():
    return fastavro.parse_schema(json.loads(load_schema("live_web_traffic.avsc")))


def test_to_avro_record_validates_against_contract_schema(schema):
    row = {
        "timestamp": 1609459200000,
        "visitorid": "42",
        "event": "transaction",
        "itemid": "10",
        "price": "9.99",
    }
    record = to_avro_record(row)
    assert fastavro.validate(record, schema) is True
    assert record == {
        "event_time": 1609459200000,
        "visitor_id": 42,
        "event": "transaction",
        "item_id": 10,
        "price": 9.99,
    }


def test_to_avro_record_handles_missing_price(schema):
    row = {
        "timestamp": 1609459200000,
        "visitorid": "1",
        "event": "view",
        "itemid": "5",
        "price": "",
    }
    record = to_avro_record(row)
    assert fastavro.validate(record, schema) is True
    assert record["price"] is None


def test_to_avro_record_rejects_unknown_event_type():
    row = {"timestamp": 1609459200000, "visitorid": "1", "event": "bogus", "itemid": "5"}
    with pytest.raises(ValueError, match="unknown event type"):
        to_avro_record(row)


def test_to_avro_record_bad_type_fails_schema_validation(schema):
    # A hand-built record with the wrong type for item_id (string instead of long) must fail
    # fastavro's schema validation -- proving the schema genuinely constrains the wire format.
    bad_record = {
        "event_time": 1609459200000,
        "visitor_id": 1,
        "event": "view",
        "item_id": "not-a-long",
        "price": None,
    }
    assert fastavro.validate(bad_record, schema, raise_errors=False) is False
