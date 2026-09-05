"""
Core HotCache manager orchestrating Redis interactions, atomic Lua script execution,
and in-memory/fakeredis fallback support.
"""

from __future__ import annotations
import asyncio
import time
import uuid
import logging
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple, Union

try:
    import redis.asyncio as aioredis
except ImportError:
    aioredis = None

from .policy import CachePolicy
from .serializer import CacheSerializer
from .lua_scripts import (
    LUA_ADAPTIVE_GET_OR_TRACK,
    LUA_ADAPTIVE_SET,
    LUA_ACQUIRE_LOCK,
    LUA_RELEASE_LOCK,
    LUA_SLIDING_RATE_LIMIT,
)

logger = logging.getLogger("hotcache")


@dataclass
class CacheReadResult:
    """Result returned by atomic get_or_track."""
    status: str  # 'HIT', 'EXPIRED', 'MISS'
    value: Optional[Any]
    current_ttl: int
    current_load: int
    low_load_streak: int
    last_extended: int
    is_stale: bool


class HotCache:
    """
    Manager class for HotCache.
    Executes atomic load-aware TTL adjustments, stampede locking, and SWR.
    """

    def __init__(
        self,
        redis_url: Optional[str] = None,
        redis_client: Optional[Any] = None,
        prefix: str = "hotcache",
        use_fake_redis_fallback: bool = True,
    ):
        self.redis_url = redis_url
        self.prefix = prefix
        self.use_fake_redis_fallback = use_fake_redis_fallback
        self._redis = redis_client
        self._scripts: Dict[str, Any] = {}
        self._stats: Dict[str, Any] = {
            "total_requests": 0,
            "hits": 0,
            "misses": 0,
            "stale_hits": 0,
            "ttl_extensions": 0,
            "ttl_decays": 0,
            "stampede_locks_acquired": 0,
            "stampede_locks_contended": 0,
            "keys": {},
        }
        self._serializer = CacheSerializer

    async def get_client(self):
        """Retrieve or lazily initialize the async Redis connection."""
        if self._redis is not None:
            return self._redis

        if self.redis_url:
            try:
                self._redis = aioredis.from_url(
                    self.redis_url,
                    decode_responses=True,
                    socket_connect_timeout=2.0,
                )
                # Verify ping
                await self._redis.ping()
                logger.info("Connected to Redis at %s", self.redis_url)
                return self._redis
            except Exception as e:
                logger.warning("Failed to connect to real Redis: %s", e)
                if not self.use_fake_redis_fallback:
                    raise

        # Fallback to fakeredis with Lua support
        logger.info("Using embedded FakeRedis with Lua scripting support")
        import fakeredis.aioredis
        self._redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
        return self._redis

    def _get_keys(self, resource_id: str) -> Tuple[str, str, str]:
        """Generate Redis keys for request count, cache hash, and lock."""
        reqcount_key = f"{self.prefix}:reqcount:{resource_id}"
        cache_key = f"{self.prefix}:cache:{resource_id}"
        lock_key = f"{self.prefix}:lock:{resource_id}"
        return reqcount_key, cache_key, lock_key

    async def get_or_track(
        self, resource_id: str, policy: CachePolicy
    ) -> CacheReadResult:
        """
        Atomically increments request counter, computes adaptive TTL,
        and retrieves fresh or stale cache value.
        """
        client = await self.get_client()
        reqcount_key, cache_key, _ = self._get_keys(resource_id)
        now = int(time.time())

        # Execute atomic Lua script
        raw_res = await client.eval(
            LUA_ADAPTIVE_GET_OR_TRACK,
            2,
            reqcount_key,
            cache_key,
            policy.base_ttl,
            policy.max_ttl,
            1 if policy.adaptive_ttl else 0,
            1 if policy.strict_freshness else 0,
            policy.load_threshold,
            policy.decay_window,
            now,
            60,  # 60-second counter window
        )

        status = raw_res[0]
        raw_val = raw_res[1]
        current_ttl = int(raw_res[2])
        current_load = int(raw_res[3])
        low_load_streak = int(raw_res[4])
        last_extended = int(raw_res[5])
        is_stale = raw_res[6] == "1"

        # Update in-memory introspection stats
        self._stats["total_requests"] += 1
        key_stat = self._stats["keys"].setdefault(
            resource_id,
            {
                "hits": 0,
                "misses": 0,
                "stale_hits": 0,
                "current_ttl": current_ttl,
                "current_load": current_load,
                "last_extended": last_extended,
                "base_ttl": policy.base_ttl,
                "max_ttl": policy.max_ttl,
                "adaptive_ttl": policy.adaptive_ttl,
                "strict_freshness": policy.strict_freshness,
            },
        )
        key_stat["current_ttl"] = current_ttl
        key_stat["current_load"] = current_load
        key_stat["last_extended"] = last_extended

        val = self._serializer.loads(raw_val) if raw_val else None

        if status == "HIT":
            self._stats["hits"] += 1
            key_stat["hits"] += 1
        elif status == "EXPIRED":
            if is_stale and policy.swr_enabled:
                self._stats["stale_hits"] += 1
                key_stat["stale_hits"] += 1
            else:
                self._stats["misses"] += 1
                key_stat["misses"] += 1
        else:  # MISS
            self._stats["misses"] += 1
            key_stat["misses"] += 1

        return CacheReadResult(
            status=status,
            value=val,
            current_ttl=current_ttl,
            current_load=current_load,
            low_load_streak=low_load_streak,
            last_extended=last_extended,
            is_stale=is_stale,
        )

    async def set(
        self,
        resource_id: str,
        value: Any,
        policy: CachePolicy,
        current_ttl: Optional[int] = None,
    ) -> None:
        """
        Atomically saves serialized payload with TTL metadata and expiry.
        Preserves current adaptive TTL (hotness memory) if provided.
        """
        client = await self.get_client()
        _, cache_key, _ = self._get_keys(resource_id)
        now = int(time.time())
        ttl_to_set = current_ttl if current_ttl is not None else policy.base_ttl
        serialized_val = self._serializer.dumps(value)

        await client.eval(
            LUA_ADAPTIVE_SET,
            1,
            cache_key,
            serialized_val,
            policy.base_ttl,
            policy.max_ttl,
            ttl_to_set,
            1 if policy.adaptive_ttl else 0,
            1 if policy.strict_freshness else 0,
            now,
            policy.stale_ttl_buffer,
        )

    async def acquire_lock(
        self, resource_id: str, token: str, timeout: float = 3.0
    ) -> bool:
        """Acquires a distributed stampede lock using SETNX with TTL."""
        client = await self.get_client()
        _, _, lock_key = self._get_keys(resource_id)
        res = await client.eval(
            LUA_ACQUIRE_LOCK,
            1,
            lock_key,
            token,
            int(timeout) if timeout >= 1 else 1,
        )
        acquired = bool(res == 1)
        if acquired:
            self._stats["stampede_locks_acquired"] += 1
        else:
            self._stats["stampede_locks_contended"] += 1
        return acquired

    async def release_lock(self, resource_id: str, token: str) -> bool:
        """Releases the distributed stampede lock if token matches."""
        client = await self.get_client()
        _, _, lock_key = self._get_keys(resource_id)
        res = await client.eval(
            LUA_RELEASE_LOCK,
            1,
            lock_key,
            token,
        )
        return bool(res == 1)

    async def sliding_rate_limit(
        self, key: str, window_seconds: int = 60, max_requests: int = 100
    ) -> Tuple[bool, int, int, int]:
        """
        Sliding window rate limit check.
        Returns (allowed, remaining, current_count, reset_in_seconds).
        """
        client = await self.get_client()
        rate_key = f"{self.prefix}:ratelimit:{key}"
        now = int(time.time())
        member_id = f"{now}:{uuid.uuid4().hex[:8]}"

        res = await client.eval(
            LUA_SLIDING_RATE_LIMIT,
            1,
            rate_key,
            now,
            window_seconds,
            max_requests,
            member_id,
        )
        allowed = bool(res[0] == 1)
        remaining = int(res[1])
        current_count = int(res[2])
        reset_in = int(res[3]) if len(res) > 3 else window_seconds
        return allowed, remaining, current_count, reset_in

    async def get_stats(self, resource_id: Optional[str] = None) -> Dict[str, Any]:
        """Return runtime metrics and per-resource cache state."""
        if resource_id:
            return self._stats["keys"].get(resource_id, {})
        return dict(self._stats)

    async def clear_cache(self, resource_id: Optional[str] = None) -> None:
        """Purge cache entries from Redis."""
        client = await self.get_client()
        if resource_id:
            reqcount_key, cache_key, lock_key = self._get_keys(resource_id)
            await client.delete(reqcount_key, cache_key, lock_key)
            self._stats["keys"].pop(resource_id, None)
        else:
            # Delete all matching keys
            keys = await client.keys(f"{self.prefix}:*")
            if keys:
                await client.delete(*keys)
            self._stats["keys"].clear()


# Default singleton instance
_default_instance: Optional[HotCache] = None


def get_hotcache(redis_url: Optional[str] = None) -> HotCache:
    """Retrieve global default HotCache instance."""
    global _default_instance
    if _default_instance is None:
        _default_instance = HotCache(redis_url=redis_url)
    return _default_instance


def set_hotcache(instance: HotCache) -> None:
    """Set custom HotCache singleton instance."""
    global _default_instance
    _default_instance = instance
