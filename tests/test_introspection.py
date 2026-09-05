"""
Unit tests for HotCache introspection endpoints.
"""

import pytest
from fastapi import FastAPI, Response
from httpx import AsyncClient, ASGITransport
from hotcache.core import HotCache, set_hotcache
from hotcache.policy import CachePolicy
from hotcache.decorators import cached
from hotcache.introspection import router as introspection_router


@pytest.fixture
def app_with_introspection():
    app = FastAPI()
    hc = HotCache(use_fake_redis_fallback=True)
    set_hotcache(hc)
    app.include_router(introspection_router)

    @app.get("/items/{item_id}")
    @cached(key_fn=lambda item_id: f"item:{item_id}", policy=CachePolicy(base_ttl=60))
    async def get_item(item_id: str, response: Response):
        return {"id": item_id}

    return app


@pytest.mark.asyncio
async def test_introspection_endpoints(app_with_introspection):
    transport = ASGITransport(app=app_with_introspection)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # Generate some cache traffic
        await client.get("/items/abc")
        await client.get("/items/abc")

        # Global stats
        r1 = await client.get("/_cache/stats")
        assert r1.status_code == 200
        data1 = r1.json()
        assert data1["summary"]["total_requests"] == 2
        assert data1["summary"]["hits"] == 1
        assert data1["summary"]["misses"] == 1

        # Key specific stats
        r2 = await client.get("/_cache/stats/item:abc")
        assert r2.status_code == 200
        data2 = r2.json()
        assert data2["resource_id"] == "item:abc"
        assert data2["hits"] == 1
        assert data2["misses"] == 1

        # Reset cache
        r3 = await client.post("/_cache/reset")
        assert r3.status_code == 200
        assert r3.json()["status"] == "success"
