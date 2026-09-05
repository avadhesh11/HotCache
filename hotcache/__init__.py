"""
HotCache: Adaptive Load-Aware Caching Middleware for FastAPI.
"""

from .policy import CachePolicy
from .core import HotCache, get_hotcache, set_hotcache, CacheReadResult
from .decorators import cached
from .rate_limiter import RateLimiter
from .introspection import router as introspection_router

__version__ = "0.1.0"

__all__ = [
    "CachePolicy",
    "HotCache",
    "get_hotcache",
    "set_hotcache",
    "CacheReadResult",
    "cached",
    "RateLimiter",
    "introspection_router",
]
