"""
Unit tests for @cached decorator and FastAPI integration.
"""

import pytest
from fastapi import FastAPI, Response
from httpx import AsyncClient, ASGITransport
from hotcache.core import HotCache, set_hotcache
from hotcache.policy import CachePolicy
from hotcache.decorators import cached


@pytest.fixture
def app_with_cache():
    app = FastAPI()
    hc = HotCache(use_fake_redis_fallback=True)
    set_hotcache(hc)

    db_counter = {"calls": 0}

    @app.get("/posts/{post_id}")
    @cached(
        key_fn=lambda post_id: f"post:{post_id}",
        policy=CachePolicy(base_ttl=30, max_ttl=120, adaptive_ttl=True, load_threshold=2),
    )
    async def get_post(post_id: str, response: Response):
        db_counter["calls"] += 1
        return {"id": post_id, "title": f"Post Title {post_id}", "db_version": db_counter["calls"]}

    return app, db_counter


@pytest.mark.asyncio
async def test_cached_decorator_hit_and_miss(app_with_cache):
    app, db_counter = app_with_cache
    transport = ASGITransport(app=app)
    
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # 1st request -> Cache MISS, hits DB
        r1 = await client.get("/posts/101")
        assert r1.status_code == 200
        assert r1.json()["db_version"] == 1
        assert r1.headers["X-HotCache-Status"] == "MISS"
        assert db_counter["calls"] == 1

        # 2nd request -> Cache HIT, DB not called
        r2 = await client.get("/posts/101")
        assert r2.status_code == 200
        assert r2.json()["db_version"] == 1
        assert r2.headers["X-HotCache-Status"] == "HIT"
        assert db_counter["calls"] == 1

        # 3rd request -> load > threshold (2) -> TTL doubles, still cache HIT
        r3 = await client.get("/posts/101")
        assert r3.status_code == 200
        assert r3.headers["X-HotCache-Status"] == "HIT"
        assert int(r3.headers["X-HotCache-TTL"]) == 60
        assert db_counter["calls"] == 1
