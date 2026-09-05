"""
Concurrency tests for Stampede Locking and Stale-While-Revalidate (SWR).
"""

import asyncio
import pytest
from fastapi import FastAPI, Response
from httpx import AsyncClient, ASGITransport
from hotcache.core import HotCache, set_hotcache
from hotcache.policy import CachePolicy
from hotcache.decorators import cached


@pytest.mark.asyncio
async def test_cache_stampede_protection():
    """Verify that 30 concurrent requests on an empty cache trigger only 1 DB query."""
    app = FastAPI()
    hc = HotCache(use_fake_redis_fallback=True)
    set_hotcache(hc)

    db_counter = {"calls": 0}

    @app.get("/items/{item_id}")
    @cached(
        key_fn=lambda item_id: f"item:{item_id}",
        policy=CachePolicy(base_ttl=10, lock_timeout=2.0, lock_wait_timeout=1.0),
    )
    async def get_item(item_id: str, response: Response):
        db_counter["calls"] += 1
        # Artificial slow DB delay to simulate heavy query
        await asyncio.sleep(0.1)
        return {"item_id": item_id, "data": "computed"}

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # Fire 30 concurrent requests
        tasks = [client.get("/items/special-42") for _ in range(30)]
        responses = await asyncio.gather(*tasks)

        for res in responses:
            assert res.status_code == 200
            assert res.json()["item_id"] == "special-42"

        # Crucial assertion: Exactly 1 DB call should have happened!
        assert db_counter["calls"] == 1


@pytest.mark.asyncio
async def test_stale_while_revalidate():
    """Verify that expired cache returns stale data immediately and revalidates in background."""
    app = FastAPI()
    hc = HotCache(use_fake_redis_fallback=True)
    set_hotcache(hc)

    db_counter = {"calls": 0}

    @app.get("/swr-test/{val_id}")
    @cached(
        key_fn=lambda val_id: f"swr:{val_id}",
        policy=CachePolicy(base_ttl=1, max_ttl=10, swr_enabled=True, stale_ttl_buffer=5),
    )
    async def get_swr(val_id: str, response: Response):
        db_counter["calls"] += 1
        await asyncio.sleep(0.05)
        return {"val_id": val_id, "version": db_counter["calls"]}

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # 1. Initial write
        r1 = await client.get("/swr-test/foo")
        assert r1.status_code == 200
        assert r1.json()["version"] == 1
        assert r1.headers["X-HotCache-Status"] == "MISS"
        assert db_counter["calls"] == 1

        # Wait 1.1s so current_ttl (1s) expires, but within stale_ttl_buffer (5s)
        await asyncio.sleep(1.1)

        # 2. Next request should receive STALE value version 1 immediately
        r2 = await client.get("/swr-test/foo")
        assert r2.status_code == 200
        assert r2.json()["version"] == 1
        assert r2.headers["X-HotCache-Status"] == "STALE_SWR"

        # Wait briefly for background revalidation task to finish
        await asyncio.sleep(0.2)

        # 3. Subsequent request gets the newly updated fresh value version 2
        r3 = await client.get("/swr-test/foo")
        assert r3.status_code == 200
        assert r3.json()["version"] == 2
        assert r3.headers["X-HotCache-Status"] == "HIT"
        assert db_counter["calls"] == 2
