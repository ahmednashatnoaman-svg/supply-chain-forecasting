"""Seed Redis with stub forecasts/elasticity so downstream members are never blocked on the batch
layer (master-plan §3 mitigation; supports the M0 walking-skeleton). (Nashat, plan Task 2.)

Usage:  python scripts/seed_redis_stub.py
"""

from __future__ import annotations

import json

from libs.scf_common.contracts import RedisKeys
from libs.scf_common.io import get_redis

STUB_SKUS = [10, 20, 30, 40, 50]


def main() -> None:
    r = get_redis()
    for sku in STUB_SKUS:
        r.set(RedisKeys.forecast(sku), 100.0)
        r.set(RedisKeys.elasticity(sku), json.dumps([{"related": sku + 1, "weight": 0.5}]))
        r.set(RedisKeys.community(sku), sku % 3)
        r.set(RedisKeys.inventory(sku), 500)
    print(f"Seeded {len(STUB_SKUS)} stub SKUs into Redis: {STUB_SKUS}")


if __name__ == "__main__":
    main()
