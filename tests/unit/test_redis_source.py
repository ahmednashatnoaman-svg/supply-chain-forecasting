"""Unit test for the dashboard Redis source (Ziad plan Task 1). Uses fakeredis (free)."""
import pytest

fakeredis = pytest.importorskip("fakeredis")

pytestmark = pytest.mark.unit


@pytest.fixture
def fake_redis(monkeypatch):
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    monkeypatch.setattr("libs.scf_common.io.redis_client.get_redis", lambda: client, raising=True)
    monkeypatch.setattr("automation.dashboard.components.redis_source.get_redis", lambda: client, raising=True)
    return client


def test_get_price_and_missing(fake_redis):
    from automation.dashboard.components import redis_source

    fake_redis.set("price:current:10", 120.99)
    assert redis_source.get_price("10") == 120.99
    assert redis_source.get_price("999") is None
