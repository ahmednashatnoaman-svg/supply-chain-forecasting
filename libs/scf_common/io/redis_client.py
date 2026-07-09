"""Redis client factory. All serving-layer reads/writes go through here."""

from __future__ import annotations

from functools import lru_cache

import redis

from libs.scf_common.config import settings


@lru_cache(maxsize=1)
def get_redis() -> redis.Redis:
    """Return a process-wide Redis client (decode_responses=True → str values).

    Without explicit timeouts, a dropped/restarted Redis leaves this client hanging
    indefinitely on the next call instead of failing fast -- every layer that reads/writes
    through here (streaming's per-window price updates, batch's publish loop) would stall with
    it. socket_connect_timeout covers the initial TCP handshake; socket_timeout covers each
    individual command; retry_on_timeout retries a command once after a timeout (covers a
    single transient blip, e.g. Redis mid-restart) rather than failing on the first hiccup.
    """
    return redis.Redis(
        host=settings.redis.host,
        port=settings.redis.port,
        db=settings.redis.db,
        password=settings.redis.password,
        decode_responses=True,
        socket_connect_timeout=5.0,
        socket_timeout=5.0,
        retry_on_timeout=True,
    )
