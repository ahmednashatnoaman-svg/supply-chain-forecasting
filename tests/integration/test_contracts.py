"""Contract test suite (master-plan §4). Ensures every Avro schema loads and every contract file
resolves. Pure file checks — runs in CI on every PR."""
import json
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit  # pure file validation, no services — runs in the fast CI lane

CONTRACTS = Path(__file__).resolve().parents[2] / "contracts"


def test_all_avro_schemas_are_valid_json():
    avsc_files = list((CONTRACTS / "avro").glob("*.avsc"))
    assert avsc_files, "no avro schemas found"
    for f in avsc_files:
        schema = json.loads(f.read_text())
        assert schema["type"] == "record"
        assert "name" in schema and "fields" in schema


def test_topics_yaml_matches_schema_files():
    import re

    topics_text = (CONTRACTS / "avro" / "topics.yml").read_text()
    named = set(re.findall(r"schema:\s*([\w.]+\.avsc)", topics_text))
    on_disk = {f.name for f in (CONTRACTS / "avro").glob("*.avsc")}
    assert named.issubset(on_disk), f"topics.yml references missing schema files: {named - on_disk}"


def test_model_signatures_present():
    assert (CONTRACTS / "models" / "forecast_output.json").exists()
    assert (CONTRACTS / "models" / "lstm_signature.json").exists()
