"""Walking-skeleton e2e — the integration canary (Nashat plan Task 2 / master-plan §4).

Proves all five layers still talk: seed Redis -> publish a surge event -> assert a price update lands
on automated_pricing_updates within the window. Marked xfail until M3 flips it to required.
"""

import pytest

pytest.importorskip("testcontainers")
pytestmark = pytest.mark.e2e


@pytest.mark.xfail(
    reason="Enabled at M3 once streaming pricing engine is implemented (nashat-plan Task 5)"
)
@pytest.mark.skip(reason="Scaffold — full stack e2e; implement per nashat-plan Task 5")
def test_surge_event_produces_price_update():
    """
    Plan:
      1. Bring up (or reuse) Kafka + Redis + Schema Registry.
      2. python scripts/seed_redis_stub.py.
      3. Produce a burst of `transaction` events for one SKU to live_web_traffic.
      4. Run pricing_stream.run(trigger_once=True).
      5. Consume automated_pricing_updates; assert a PricingUpdate for that SKU with new_price > base.
      6. Assert a LOW_STOCK system_alert fires when inventory is seeded low.
    """
