"""
Introspection and telemetry endpoints for HotCache.
"""

from typing import Optional
from fastapi import APIRouter, HTTPException

from .core import HotCache, get_hotcache

router = APIRouter(prefix="/_cache", tags=["HotCache Introspection"])


@router.get("/stats")
async def get_all_stats():
    """Retrieve global cache telemetry and metrics across all resources."""
    hc = get_hotcache()
    stats = await hc.get_stats()
    total_ops = stats["hits"] + stats["misses"] + stats["stale_hits"]
    hit_ratio = round((stats["hits"] + stats["stale_hits"]) / total_ops, 4) if total_ops > 0 else 0.0
    
    return {
        "summary": {
            "total_requests": stats["total_requests"],
            "hits": stats["hits"],
            "misses": stats["misses"],
            "stale_hits": stats["stale_hits"],
            "hit_ratio": hit_ratio,
            "stampede_locks_acquired": stats["stampede_locks_acquired"],
            "stampede_locks_contended": stats["stampede_locks_contended"],
        },
        "keys": stats["keys"],
    }


@router.get("/stats/{resource_id:path}")
async def get_resource_stats(resource_id: str):
    """Retrieve runtime state and adaptive parameters for a specific resource key."""
    hc = get_hotcache()
    key_stats = await hc.get_stats(resource_id)
    if not key_stats:
        raise HTTPException(status_code=404, detail=f"No stats found for resource: {resource_id}")

    total = key_stats["hits"] + key_stats["misses"] + key_stats["stale_hits"]
    hit_ratio = round((key_stats["hits"] + key_stats["stale_hits"]) / total, 4) if total > 0 else 0.0

    return {
        "resource_id": resource_id,
        "current_ttl": key_stats.get("current_ttl"),
        "base_ttl": key_stats.get("base_ttl"),
        "max_ttl": key_stats.get("max_ttl"),
        "current_load": key_stats.get("current_load"),
        "last_extended": key_stats.get("last_extended"),
        "hits": key_stats.get("hits"),
        "misses": key_stats.get("misses"),
        "stale_hits": key_stats.get("stale_hits"),
        "hit_ratio": hit_ratio,
        "adaptive_ttl": key_stats.get("adaptive_ttl"),
        "strict_freshness": key_stats.get("strict_freshness"),
    }


@router.post("/reset")
async def reset_cache(resource_id: Optional[str] = None):
    """Purge cache and reset metrics (for demo/testing)."""
    hc = get_hotcache()
    await hc.clear_cache(resource_id)
    return {"status": "success", "message": f"Cache cleared for {resource_id or 'all keys'}"}
