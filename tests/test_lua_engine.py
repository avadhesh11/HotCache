"""
Unit tests for HotCache atomic Lua scripts and adaptive TTL logic.
"""

import pytest
import time
from hotcache.core import HotCache
from hotcache.policy import CachePolicy


@pytest.fixture
def hc():
    """Create a clean HotCache instance using FakeRedis."""
    return HotCache(use_fake_redis_fallback=True)


@pytest.mark.asyncio
async def test_initial_cache_miss(hc):
    policy = CachePolicy(base_ttl=10, max_ttl=80, load_threshold=5)
    res = await hc.get_or_track("test:item1", policy)
    assert res.status == "MISS"
    assert res.value is None
    assert res.current_ttl == 10
    assert res.current_load == 1


@pytest.mark.asyncio
async def test_set_and_hit(hc):
    policy = CachePolicy(base_ttl=10, max_ttl=80, load_threshold=5)
    await hc.set("test:item2", {"name": "Laptop", "price": 999}, policy)
    
    res = await hc.get_or_track("test:item2", policy)
    assert res.status == "HIT"
    assert res.value == {"name": "Laptop", "price": 999}
    assert res.current_ttl == 10
    assert not res.is_stale


@pytest.mark.asyncio
async def test_adaptive_ttl_doubling_under_load(hc):
    policy = CachePolicy(base_ttl=10, max_ttl=80, load_threshold=3, adaptive_ttl=True)
    await hc.set("test:hot_post", "Viral Post Content", policy)

    # 1st request -> load=1 <= 3 -> TTL stays 10
    res1 = await hc.get_or_track("test:hot_post", policy)
    assert res1.current_ttl == 10

    # 2nd request -> load=2 <= 3 -> TTL stays 10
    res2 = await hc.get_or_track("test:hot_post", policy)
    assert res2.current_ttl == 10

    # 3rd request -> load=3 <= 3 -> TTL stays 10
    res3 = await hc.get_or_track("test:hot_post", policy)
    assert res3.current_ttl == 10

    # 4th request -> load=4 > 3 -> TTL DOUBLES to 20!
    res4 = await hc.get_or_track("test:hot_post", policy)
    assert res4.current_ttl == 20
    assert res4.current_load == 4

    # 5th request -> load=5 > 3 -> TTL DOUBLES to 40!
    res5 = await hc.get_or_track("test:hot_post", policy)
    assert res5.current_ttl == 40

    # 6th request -> load=6 > 3 -> TTL DOUBLES to 80 (max_ttl)!
    res6 = await hc.get_or_track("test:hot_post", policy)
    assert res6.current_ttl == 80

    # 7th request -> load=7 > 3 -> TTL stays clamped at max_ttl (80)
    res7 = await hc.get_or_track("test:hot_post", policy)
    assert res7.current_ttl == 80


@pytest.mark.asyncio
async def test_adaptive_ttl_decay_after_decay_window(hc):
    policy = CachePolicy(base_ttl=10, max_ttl=80, load_threshold=5, decay_window=3, adaptive_ttl=True)
    await hc.set("test:decay_post", "Post Data", policy, current_ttl=40)

    # Load is currently 0. Simulating request when counter is below threshold (load <= 5)
    # 1st check under threshold -> streak = 1, ttl = 40
    r1 = await hc.get_or_track("test:decay_post", policy)
    assert r1.current_ttl == 40
    assert r1.low_load_streak == 1

    # 2nd check under threshold -> streak = 2, ttl = 40
    r2 = await hc.get_or_track("test:decay_post", policy)
    assert r2.current_ttl == 40
    assert r2.low_load_streak == 2

    # 3rd check under threshold -> streak reaches decay_window (3) -> TTL HALVES to 20!
    r3 = await hc.get_or_track("test:decay_post", policy)
    assert r3.current_ttl == 20
    assert r3.low_load_streak == 0


@pytest.mark.asyncio
async def test_strict_freshness_never_adapts(hc):
    policy = CachePolicy(base_ttl=5, max_ttl=100, load_threshold=2, strict_freshness=True)
    await hc.set("test:order_status", "PENDING", policy)

    for i in range(10):
        res = await hc.get_or_track("test:order_status", policy)
        assert res.current_ttl == 5
