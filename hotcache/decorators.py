"""
FastAPI route and async function caching decorator for HotCache.
"""

from __future__ import annotations
import asyncio
import functools
import inspect
import uuid
import logging
from typing import Any, Callable, Optional
from fastapi import Response, BackgroundTasks

from .core import HotCache, get_hotcache, CacheReadResult
from .policy import CachePolicy

logger = logging.getLogger("hotcache")


def cached(
    key_fn: Optional[Callable[..., str]] = None,
    prefix: Optional[str] = None,
    policy: Optional[CachePolicy] = None,
    hotcache_instance: Optional[HotCache] = None,
):
    """
    Decorator for caching FastAPI endpoints and coroutines with adaptive TTL,
    stampede locking, and stale-while-revalidate.
    
    Example:
        @app.get("/posts/{post_id}")
        @cached(
            key_fn=lambda post_id: f"post:{post_id}",
            policy=CachePolicy(base_ttl=30, max_ttl=300, adaptive_ttl=True)
        )
        async def get_post(post_id: str):
            return await db.fetch(post_id)
    """
    cache_policy = policy or CachePolicy()

    def decorator(func: Callable):
        sig = inspect.signature(func)

        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            hc = hotcache_instance or get_hotcache()

            # 1. Resolve cache resource key
            if key_fn is not None:
                # Bind function arguments for key_fn evaluation
                try:
                    bound = sig.bind_partial(*args, **kwargs)
                    bound.apply_defaults()
                    # Filter out internal FastAPI objects if needed
                    key_args = {
                        k: v for k, v in bound.arguments.items()
                        if not isinstance(v, (Response, BackgroundTasks))
                    }
                    resource_id = key_fn(**key_args)
                except Exception:
                    # Fallback to direct positional/keyword evaluation
                    resource_id = key_fn(*args, **kwargs)
            else:
                func_name = f"{func.__module__}.{func.__qualname__}"
                arg_repr = str(args) + str(sorted(kwargs.items()))
                resource_id = f"{func_name}:{arg_repr}"

            if prefix:
                resource_id = f"{prefix}:{resource_id}"

            # Check if Response is passed in kwargs for setting headers
            response_obj: Optional[Response] = kwargs.get("response")

            def set_headers(status: str, ttl: int, load: int, streak: int):
                if response_obj is not None:
                    response_obj.headers["X-HotCache-Status"] = status
                    response_obj.headers["X-HotCache-TTL"] = str(ttl)
                    response_obj.headers["X-HotCache-Load"] = str(load)
                    response_obj.headers["X-HotCache-Streak"] = str(streak)

            # 2. Atomic read and load check via Lua script
            read_result: CacheReadResult = await hc.get_or_track(resource_id, cache_policy)

            # Case A: Cache HIT
            if read_result.status == "HIT" and read_result.value is not None:
                set_headers("HIT", read_result.current_ttl, read_result.current_load, read_result.low_load_streak)
                return read_result.value

            # Case B: Cache EXPIRED with Stale Value (SWR candidate)
            if read_result.status == "EXPIRED" and read_result.value is not None and cache_policy.swr_enabled:
                token = uuid.uuid4().hex
                acquired = await hc.acquire_lock(resource_id, token, timeout=cache_policy.lock_timeout)
                if acquired:
                    # We won the lock -> revalidate in background task
                    async def _revalidate_bg():
                        try:
                            fresh_data = await func(*args, **kwargs)
                            await hc.set(
                                resource_id,
                                fresh_data,
                                cache_policy,
                                current_ttl=read_result.current_ttl,
                            )
                        except Exception as e:
                            logger.error("Error during background revalidation for %s: %s", resource_id, e)
                        finally:
                            await hc.release_lock(resource_id, token)

                    # Trigger async revalidation
                    asyncio.create_task(_revalidate_bg())

                # Return stale data immediately!
                set_headers("STALE_SWR", read_result.current_ttl, read_result.current_load, read_result.low_load_streak)
                return read_result.value

            # Case C: Cache MISS or Expired without SWR
            token = uuid.uuid4().hex
            acquired = await hc.acquire_lock(resource_id, token, timeout=cache_policy.lock_timeout)

            if acquired:
                try:
                    # Compute fresh value
                    result = await func(*args, **kwargs)
                    # Persist in cache (preserving hotness memory)
                    await hc.set(
                        resource_id,
                        result,
                        cache_policy,
                        current_ttl=read_result.current_ttl,
                    )
                    set_headers("MISS", read_result.current_ttl, read_result.current_load, read_result.low_load_streak)
                    return result
                finally:
                    await hc.release_lock(resource_id, token)
            else:
                # Another process holds the lock
                # If we have stale data as fallback:
                if read_result.value is not None and cache_policy.swr_enabled:
                    set_headers("STALE_SWR", read_result.current_ttl, read_result.current_load, read_result.low_load_streak)
                    return read_result.value

                # Otherwise, poll until the lock winner completes and fills the cache
                elapsed = 0.0
                while elapsed < cache_policy.lock_wait_timeout:
                    await asyncio.sleep(cache_policy.lock_wait_interval)
                    elapsed += cache_policy.lock_wait_interval
                    subsequent_read = await hc.get_or_track(resource_id, cache_policy)
                    if subsequent_read.status == "HIT" and subsequent_read.value is not None:
                        set_headers("HIT", subsequent_read.current_ttl, subsequent_read.current_load, subsequent_read.low_load_streak)
                        return subsequent_read.value

                # If wait timeout exceeded, execute directly as fallback
                result = await func(*args, **kwargs)
                set_headers("MISS_FALLBACK", read_result.current_ttl, read_result.current_load, read_result.low_load_streak)
                return result

        return wrapper

    return decorator
