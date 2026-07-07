"""Shared testcontainers fixtures for ingestion integration tests (Hatem Tasks 3 & 4).

Starts a real Kafka (KRaft mode, no Zookeeper -- the testcontainers KafkaContainer default) plus
an Apicurio Registry exposing its Confluent-compatibility API at /apis/ccompat/v6, so
``confluent_kafka``'s ``AvroSerializer``/``AvroDeserializer`` work unchanged (decision D-D).

The dev ``docker-compose.yml`` ships Confluent ``cp-schema-registry``; integration tests run
Apicurio instead. This divergence is intentional per the plan and tracked as OPEN-1 (Nagy).

Both fixtures are module-scoped so the (slow) container startup happens once per test module.
Tests point ``settings.kafka`` at the containers by setting env vars BEFORE the config singleton
is materialized -- the producing/consuming code reads ``settings`` lazily, so we reset the
``lru_cache`` and replace the module-level ``settings`` object.
"""

from __future__ import annotations

import time
import urllib.error
import urllib.request

import pytest


def _wait_http(url: str, timeout: float = 60.0) -> None:
    """Poll a URL until it returns 200, or raise after ``timeout`` seconds."""
    deadline = time.time() + timeout
    last_err: Exception | None = None
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=3) as resp:
                if resp.status == 200:
                    return
        except (urllib.error.URLError, ConnectionError, OSError) as exc:
            last_err = exc
        time.sleep(1)
    raise RuntimeError(f"{url} not healthy within {timeout}s: {last_err}")


@pytest.fixture(scope="module")
def kafka_bootstrap() -> str:
    """Start a KRaft Kafka container; yield its host bootstrap address."""
    pytest.importorskip("testcontainers")
    from testcontainers.kafka import KafkaContainer

    with KafkaContainer("confluentinc/cp-kafka:7.6.1") as kafka:
        yield kafka.get_bootstrap_server()


@pytest.fixture(scope="module")
def schema_registry_url() -> str:
    """Start Apicurio Registry with in-memory storage; yield its ccompat v6 URL.

    The dev ``docker-compose.yml`` ships Confluent ``cp-schema-registry`` backed by Kafka; tests
    use Apicurio instead (decision D-D). Wiring Apicurio's Kafka-SQL storage to the
    testcontainers KafkaContainer's internal Docker network is nontrivial and version-sensitive
    (see the SCOPE NOTE in tests/e2e/test_walking_skeleton.py). For an integration test we only
    need the schema registry to *serve* schemas over the Confluent-compatibility API -- in-memory
    storage is sufficient and sidesteps the cross-container networking entirely.
    """
    from testcontainers.core.container import DockerContainer

    # Pin Apicurio 2.x because `latest` can point at newer major versions with a different
    # Confluent-compatibility API layout; the tests intentionally use /apis/ccompat/v6.
    apicurio = DockerContainer("quay.io/apicurio/apicurio-registry-mem:2.6.2.Final")
    # Default storage is in-memory (no APICURIO_STORAGE_KIND set) -- no Kafka dependency.
    apicurio.with_exposed_ports(8080)
    apicurio.start()
    try:
        host = apicurio.get_container_host_ip()
        port = int(apicurio.get_exposed_port(8080))
        ccompat = f"http://{host}:{port}/apis/ccompat/v6"
        # Apicurio image log text has changed across releases, so poll the actual ccompat
        # endpoint instead of waiting for a brittle startup log substring.
        _wait_http(f"{ccompat}/subjects", timeout=90)
        yield ccompat
    finally:
        apicurio.stop()


@pytest.fixture()
def kafka_env(monkeypatch, kafka_bootstrap: str, schema_registry_url: str):
    """Point ``settings.kafka`` at the containers for the duration of one test.

    The config module caches a singleton via ``lru_cache``; we set the env vars it reads and
    reset the cache so the producing/consuming code picks up the container addresses.
    """
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", kafka_bootstrap)
    monkeypatch.setenv("KAFKA_SCHEMA_REGISTRY_URL", schema_registry_url)
    from libs.scf_common.config import get_settings

    get_settings.cache_clear()
    import libs.scf_common.config as cfg

    monkeypatch.setattr(cfg, "settings", get_settings())
    # The io.kafka module imports `settings` by name; patch the attribute there too.
    import libs.scf_common.io.kafka as iok

    monkeypatch.setattr(iok, "settings", get_settings())
    yield {"bootstrap": kafka_bootstrap, "schema_registry": schema_registry_url}


@pytest.fixture()
def live_topic(kafka_env, kafka_bootstrap: str):
    """Create the ``live_web_traffic`` topic (6 partitions, rf=1) before the test runs.

    The dev stack disables auto-create (KAFKA_AUTO_CREATE_TOPICS_ENABLE=false), and we mirror
    that contract here so the test exercises the real bootstrap path.
    """
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient({"bootstrap.servers": kafka_bootstrap})
    fs = admin.create_topics([NewTopic("live_web_traffic", num_partitions=6, replication_factor=1)])
    for _, future in fs.items():
        future.result(timeout=30)
    yield "live_web_traffic"


@pytest.fixture()
def make_row():
    """Shared builder for a string-typed Retailrocket CSV row that exercises the producer's
    ``to_avro_record`` contract. Used by both the producer round-trip and verifier tests so a
    contract change is applied in one place.

    Timestamps are ascending and close together (negligible inter-event sleep at speed=100 so
    records stream fast); itemid cycles through 6 values to spread across the 6 partitions.
    """

    def _row(i: int) -> dict:
        return {
            "timestamp": str(1442300000000 + i),
            "visitorid": str(100 + (i % 5)),
            "event": "view" if i % 2 == 0 else "transaction",
            "itemid": str(10 + (i % 6)),
            "price": "9.99" if i % 2 else "",
        }

    return _row
