"""
Sliding-window Rate Limiter for FastAPI built on atomic Redis Lua scripts.
"""

from __future__ import annotations
from typing import Callable, Optional
from fastapi import Request, HTTPException, Response

from .core import HotCache, get_hotcache


class RateLimiter:
    """
    FastAPI dependency for sliding-window rate limiting.
    
    Usage:
        @app.get("/api/data", dependencies=[Depends(RateLimiter(max_requests=60, window_seconds=60))])
        async def get_data():
            ...
    """

    def __init__(
        self,
        max_requests: int = 100,
        window_seconds: int = 60,
        key_func: Optional[Callable[[Request], str]] = None,
        hotcache_instance: Optional[HotCache] = None,
    ):
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self.key_func = key_func or self._default_key_func
        self.hc = hotcache_instance

    def _default_key_func(self, request: Request) -> str:
        """Extract client IP as default rate limit key."""
        client = request.client
        ip = client.host if client else "127.0.0.1"
        return f"{request.url.path}:{ip}"

    async def __call__(self, request: Request, response: Response) -> None:
        hc = self.hc or get_hotcache()
        key = self.key_func(request)

        allowed, remaining, current_count, reset_in = await hc.sliding_rate_limit(
            key=key,
            window_seconds=self.window_seconds,
            max_requests=self.max_requests,
        )

        response.headers["X-RateLimit-Limit"] = str(self.max_requests)
        response.headers["X-RateLimit-Remaining"] = str(max(0, remaining))
        response.headers["X-RateLimit-Reset"] = str(reset_in)

        if not allowed:
            raise HTTPException(
                status_code=429,
                detail="Too Many Requests: Rate limit exceeded.",
                headers={
                    "Retry-After": str(reset_in),
                    "X-RateLimit-Limit": str(self.max_requests),
                    "X-RateLimit-Remaining": "0",
                    "X-RateLimit-Reset": str(reset_in),
                },
            )
