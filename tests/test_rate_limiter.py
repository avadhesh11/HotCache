"""
Unit tests for sliding-window RateLimiter.
"""

import pytest
from fastapi import FastAPI, Depends, Request, Response
from httpx import AsyncClient, ASGITransport
from hotcache.core import HotCache, set_hotcache
from hotcache.rate_limiter import RateLimiter


@pytest.fixture
def app_with_limiter():
    app = FastAPI()
    hc = HotCache(use_fake_redis_fallback=True)
    set_hotcache(hc)

    limiter = RateLimiter(max_requests=3, window_seconds=10, key_func=lambda req: "test_client")

    @app.get("/limited")
    async def limited_endpoint(request: Request, response: Response, _=Depends(limiter)):
        return {"status": "ok"}

    return app


@pytest.mark.asyncio
async def test_sliding_rate_limiter(app_with_limiter):
    transport = ASGITransport(app=app_with_limiter)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # 1st request -> Allowed (remaining: 2)
        r1 = await client.get("/limited")
        assert r1.status_code == 200
        assert r1.headers["X-RateLimit-Remaining"] == "2"

        # 2nd request -> Allowed (remaining: 1)
        r2 = await client.get("/limited")
        assert r2.status_code == 200
        assert r2.headers["X-RateLimit-Remaining"] == "1"

        # 3rd request -> Allowed (remaining: 0)
        r3 = await client.get("/limited")
        assert r3.status_code == 200
        assert r3.headers["X-RateLimit-Remaining"] == "0"

        # 4th request -> 429 Too Many Requests
        r4 = await client.get("/limited")
        assert r4.status_code == 429
        assert "Too Many Requests" in r4.json()["detail"]
