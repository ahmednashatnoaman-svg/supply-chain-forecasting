"""Unit test for the dashboard Redis source (Ziad plan Task 1). Uses fakeredis (free)."""

import pytest

fakeredis = pytest.importorskip("fakeredis")

pytestmark = pytest.mark.unit


@pytest.fixture
def fake_redis(monkeypatch):
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    monkeypatch.setattr("libs.scf_common.io.redis_client.get_redis", lambda: client, raising=True)
    monkeypatch.setattr(
        "automation.dashboard.components.redis_source.get_redis", lambda: client, raising=True
    )
    return client


def test_get_price_and_missing(fake_redis):
    from automation.dashboard.components import redis_source

    fake_redis.set("price:current:10", 120.99)
    assert redis_source.get_price("10") == 120.99
    assert redis_source.get_price("999") is None


def test_get_velocity_and_missing(fake_redis):
    from automation.dashboard.components import redis_source

    fake_redis.set("velocity:10:60s", 42.5)
    assert redis_source.get_velocity("10") == 42.5
    assert redis_source.get_velocity("10", "300s") is None
    assert redis_source.get_velocity("999") is None


def test_list_skus_discovers_active_prices(fake_redis):
    from automation.dashboard.components import redis_source

    assert redis_source.list_skus() == []
    for sku, price in [("10", 9.99), ("30", 4.5), ("20", 19.0)]:
        fake_redis.set(f"price:current:{sku}", price)
    assert redis_source.list_skus() == ["10", "20", "30"]
