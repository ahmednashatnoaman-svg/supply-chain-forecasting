"""Redis client factory. All serving-layer reads/writes go through here."""
from __future__ import annotations

from functools import lru_cache

import redis

from libs.scf_common.config import settings


@lru_cache(maxsize=1)
def get_redis() -> redis.Redis:
    """Return a process-wide Redis client (decode_responses=True → str values)."""
    return redis.Redis(
        host=settings.redis.host,
        port=settings.redis.port,
        db=settings.redis.db,
        password=settings.redis.password,
        decode_responses=True,
    )
