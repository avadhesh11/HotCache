"""
Interactive Demo Application for HotCache.
Includes simulated social feed, DB latency mock, real-time WebSocket telemetry,
and bot traffic generator.
"""

import asyncio
import os
import time
from typing import Dict, Any, List
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Response, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, FileResponse
import httpx

from hotcache import CachePolicy, cached, get_hotcache, RateLimiter, introspection_router

app = FastAPI(title="HotCache Live Demo")
app.include_router(introspection_router)

# Global DB query instrumentation
db_metrics = {
    "total_queries": 0,
    "queries_by_key": {},
    "last_query_time": 0.0,
}

# Real-time event log
event_log: List[Dict[str, Any]] = []


def record_event(key: str, event_type: str, message: str, current_ttl: int = 0, load: int = 0):
    entry = {
        "timestamp": time.time(),
        "key": key,
        "type": event_type,
        "message": message,
        "current_ttl": current_ttl,
        "load": load,
    }
    event_log.append(entry)
    if len(event_log) > 100:
        event_log.pop(0)


# Simulated Database Layer with 150ms artificial latency
async def mock_fetch_post_from_db(post_id: str) -> Dict[str, Any]:
    db_metrics["total_queries"] += 1
    db_metrics["queries_by_key"][post_id] = db_metrics["queries_by_key"].get(post_id, 0) + 1
    db_metrics["last_query_time"] = time.time()
    
    # Simulate DB I/O latency
    await asyncio.sleep(0.15)

    posts_database = {
        "celebrity-post": {
            "id": "celebrity-post",
            "author": "Taylor Nova 🌟 (Celebrity)",
            "avatar": "🌟",
            "content": "Just announced our world tour stadium dates! 🚀 Tickets go live in 10 minutes!",
            "likes": 1420950,
            "shares": 89200,
            "created_at": "Just now",
        },
        "post-1": {
            "id": "post-1",
            "author": "Alex Dev",
            "avatar": "💻",
            "content": "Testing out async Redis Lua scripts today for cache stampede protection. Incredible results!",
            "likes": 42,
            "shares": 3,
            "created_at": "5m ago",
        },
        "post-2": {
            "id": "post-2",
            "author": "Sarah Chen",
            "avatar": "☕",
            "content": "Morning coffee & reviewing FastAPI microservice telemetry. Smooth sailing!",
            "likes": 88,
            "shares": 12,
            "created_at": "12m ago",
        },
        "post-3": {
            "id": "post-3",
            "author": "Jordan Tech",
            "avatar": "⚡",
            "content": "Why static TTL caches fail under viral spikes: a thread 🧵👇",
            "likes": 310,
            "shares": 54,
            "created_at": "25m ago",
        },
    }

    return posts_database.get(post_id, {
        "id": post_id,
        "author": f"User {post_id}",
        "avatar": "👤",
        "content": f"Custom simulated post content for {post_id}",
        "likes": 10,
        "shares": 1,
        "created_at": "1h ago",
    })


# 1. Celebrity Post (Adaptive TTL enabled: base_ttl=5s, max_ttl=60s, threshold=20 req/min)
@app.get("/posts/celebrity-post")
@cached(
    key_fn=lambda: "post:celebrity-post",
    policy=CachePolicy(
        base_ttl=5,
        max_ttl=60,
        adaptive_ttl=True,
        load_threshold=20,
        decay_window=3,
        swr_enabled=True,
    ),
)
async def get_celebrity_post(response: Response):
    data = await mock_fetch_post_from_db("celebrity-post")
    return data


# 2. Normal Posts (Adaptive TTL disabled: static base_ttl=10s)
@app.get("/posts/{post_id}")
@cached(
    key_fn=lambda post_id: f"post:{post_id}",
    policy=CachePolicy(
        base_ttl=10,
        max_ttl=10,
        adaptive_ttl=False,
        strict_freshness=False,
        swr_enabled=True,
    ),
)
async def get_normal_post(post_id: str, response: Response):
    data = await mock_fetch_post_from_db(post_id)
    return data


# 3. Order Status (Strict Freshness: base_ttl=3s, never extends)
@app.get("/orders/{order_id}/status")
@cached(
    key_fn=lambda order_id: f"order:{order_id}",
    policy=CachePolicy(
        base_ttl=3,
        strict_freshness=True,
    ),
)
async def get_order_status(order_id: str, response: Response):
    db_metrics["total_queries"] += 1
    await asyncio.sleep(0.12)
    return {
        "order_id": order_id,
        "status": "PROCESSING",
        "last_updated": time.strftime("%H:%M:%S"),
    }


# Simulated Baseline Endpoint (NO adaptive cache / NO stampede lock for side-by-side comparison)
baseline_db_count = 0
baseline_cache: Dict[str, Any] = {}

@app.get("/baseline/posts/{post_id}")
async def get_baseline_post(post_id: str):
    global baseline_db_count
    now = time.time()
    cached_entry = baseline_cache.get(post_id)

    # Static 5s cache without stampede lock or adaptive logic
    if cached_entry and now < cached_entry["expires_at"]:
        return {"data": cached_entry["data"], "source": "STATIC_CACHE", "ttl": 5}
    
    # Cache Miss -> Hits DB
    baseline_db_count += 1
    await asyncio.sleep(0.15)
    data = await mock_fetch_post_from_db(post_id)
    baseline_cache[post_id] = {"data": data, "expires_at": now + 5}
    return {"data": data, "source": "DB_MISS", "ttl": 5}


# Traffic Simulator Controller
traffic_state = {
    "active": False,
    "mode": "idle",
}
active_tasks: List[asyncio.Task] = []
last_traffic_time = 0.0


async def run_traffic_simulator(scenario: str, count: int, rps: float, target_url: str):
    global last_traffic_time
    traffic_state["active"] = True
    traffic_state["mode"] = scenario
    
    try:
        async with httpx.AsyncClient(base_url="http://127.0.0.1:8000") as client:
            interval = 1.0 / rps if rps > 0 else 0.1
            for i in range(count):
                if not traffic_state["active"]:
                    break
                last_traffic_time = time.time()
                try:
                    res = await client.get(target_url)
                    ttl = int(res.headers.get("X-HotCache-TTL", 0))
                    load = int(res.headers.get("X-HotCache-Load", 0))
                    status = res.headers.get("X-HotCache-Status", "UNKNOWN")
                    
                    if i % 10 == 0 or status != "HIT":
                        if status == "HIT" and ttl > 5:
                            record_event(target_url, "ADAPTATION", f"⚡ Viral load ({load} req/min) -> Adaptive TTL scaled to {ttl}s", ttl, load)
                        elif status == "STALE_SWR":
                            record_event(target_url, "SWR", f"🛡️ Stampede avoided: Served Stale-While-Revalidate", ttl, load)
                        elif status == "MISS":
                            record_event(target_url, "DB_FETCH", f"💾 Cache Miss -> Fetched fresh from DB", ttl, load)
                except Exception:
                    pass
                await asyncio.sleep(interval)
    except asyncio.CancelledError:
        pass
    finally:
        traffic_state["active"] = False
        traffic_state["mode"] = "idle"


@app.post("/api/traffic/start")
async def start_traffic(request: Request):
    body = await request.json()
    scenario = body.get("scenario", "viral_spike")
    url = body.get("target_url", "/posts/celebrity-post")
    
    # Cancel any currently active tasks first
    for t in active_tasks:
        if not t.done():
            t.cancel()
    active_tasks.clear()

    if scenario == "viral_spike":
        task = asyncio.create_task(run_traffic_simulator("Viral Spike", count=150, rps=30, target_url=url))
        active_tasks.append(task)
    elif scenario == "stampede":
        async def burst():
            traffic_state["active"] = True
            traffic_state["mode"] = "Stampede Burst"
            try:
                async with httpx.AsyncClient(base_url="http://127.0.0.1:8000") as client:
                    record_event(url, "STAMPEDE", "💥 Simulating 50 concurrent requests at exact expiry moment!")
                    tasks = [client.get(url) for _ in range(50)]
                    await asyncio.gather(*tasks, return_exceptions=True)
            except asyncio.CancelledError:
                pass
            finally:
                traffic_state["active"] = False
                traffic_state["mode"] = "idle"
        task = asyncio.create_task(burst())
        active_tasks.append(task)
    elif scenario == "cooldown":
        task = asyncio.create_task(run_traffic_simulator("Cooldown Decay", count=20, rps=0.5, target_url=url))
        active_tasks.append(task)
    elif scenario == "steady":
        task = asyncio.create_task(run_traffic_simulator("Steady Load", count=60, rps=5, target_url=url))
        active_tasks.append(task)

    return {"status": "started", "scenario": scenario}


@app.post("/api/traffic/stop")
async def stop_traffic():
    traffic_state["active"] = False
    traffic_state["mode"] = "idle"
    for t in active_tasks:
        if not t.done():
            t.cancel()
    active_tasks.clear()
    return {"status": "stopped"}


@app.post("/api/metrics/reset")
async def reset_all_metrics():
    global baseline_db_count, baseline_cache
    baseline_db_count = 0
    baseline_cache.clear()
    db_metrics["total_queries"] = 0
    db_metrics["queries_by_key"].clear()
    event_log.clear()
    hc = get_hotcache()
    await hc.clear_cache()
    return {"status": "reset"}


# WebSocket Live Stream for Dashboard Telemetry
@app.websocket("/ws/stats")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    hc = get_hotcache()
    try:
        while True:
            stats = await hc.get_stats()
            keys_data = {}
            is_idle = (time.time() - last_traffic_time) > 2.0 and not traffic_state["active"]

            for k, v in stats.get("keys", {}).items():
                k_copy = dict(v)
                if is_idle:
                    k_copy["current_load"] = 0
                keys_data[k] = k_copy

            payload = {
                "summary": {
                    "total_requests": stats.get("total_requests", 0),
                    "hits": stats.get("hits", 0),
                    "misses": stats.get("misses", 0),
                    "stale_hits": stats.get("stale_hits", 0),
                    "db_queries": db_metrics["total_queries"],
                    "baseline_db_queries": baseline_db_count,
                    "stampede_locks_acquired": stats.get("stampede_locks_acquired", 0),
                    "stampede_locks_contended": stats.get("stampede_locks_contended", 0),
                },
                "keys": keys_data,
                "traffic_state": traffic_state,
                "events": event_log[-15:],
            }
            await websocket.send_json(payload)
            await asyncio.sleep(0.5)
    except WebSocketDisconnect:
        pass


# Mount static assets
static_dir = os.path.join(os.path.dirname(__file__), "static")
os.makedirs(static_dir, exist_ok=True)
app.mount("/static", StaticFiles(directory=static_dir), name="static")


@app.get("/")
async def get_index():
    return FileResponse(os.path.join(static_dir, "index.html"))
