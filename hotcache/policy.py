"""
Cache Policy configuration for HotCache.
"""

from dataclasses import dataclass
from typing import Optional


@dataclass
class CachePolicy:
    """
    Configuration policy defining how a resource is cached and adaptively scaled.
    
    Attributes:
        base_ttl: Default cache lifetime in seconds under normal load.
        max_ttl: Hard ceiling - TTL will never be extended past this regardless of load.
        adaptive_ttl: If True, TTL can be doubled under load exceeding load_threshold.
        strict_freshness: If True, strictly respects base_ttl and disables adaptive extensions.
        load_threshold: Requests per minute threshold on this key to trigger TTL doubling.
        decay_window: Consecutive checks under load_threshold before TTL decays (halves).
        swr_enabled: If True, serves last-known stale value during cache stampede revalidation.
        lock_timeout: Duration in seconds to hold the stampede lock.
        stale_ttl_buffer: Additional seconds stale data is retained in Redis for SWR.
        lock_wait_timeout: Seconds non-lock winners wait for fresh computation before fallback.
        lock_wait_interval: Sleep interval in seconds when polling for lock release.
    """
    base_ttl: int = 60
    max_ttl: int = 3600
    adaptive_ttl: bool = True
    strict_freshness: bool = False
    load_threshold: int = 100
    decay_window: int = 3
    swr_enabled: bool = True
    lock_timeout: float = 3.0
    stale_ttl_buffer: int = 300
    lock_wait_timeout: float = 0.5
    lock_wait_interval: float = 0.05

    def __post_init__(self):
        if self.strict_freshness:
            self.adaptive_ttl = False
        if self.max_ttl < self.base_ttl:
            self.max_ttl = self.base_ttl
        if self.decay_window < 1:
            self.decay_window = 1
        if self.load_threshold < 1:
            self.load_threshold = 1
