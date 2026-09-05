# HotCache 🔥

> **Adaptive Load-Aware Caching Middleware for FastAPI**  
> *Dynamic TTL scaling, zero-race atomic Redis Lua engine, distributed stampede locking, and Stale-While-Revalidate (SWR).*

---

## 🚀 Overview

Standard caching solutions (e.g. `fastapi-cache`, static Redis wrappers) assign a single static TTL to routes. Under real-world traffic spikes, this creates two major failure modes:
1. **Cold data is over-cached**: rarely requested keys linger needlessly in memory.
2. **Hot data is under-cached**: when a resource goes viral, fixed TTLs expire on schedule. The moment it expires, hundreds of concurrent requests simultaneously miss the cache and hammer the database (**Cache Stampede**).

**HotCache** solves this with **demand-aware adaptive caching**:
- **Atomic TTL Scaling**: Atomically tracks request frequency and doubles cache lifetime (`5s &rarr; 10s &rarr; 20s &rarr; 40s &rarr; 60s`) during spikes exceeding configured load thresholds.
- **Controlled Decay**: Gradually halves TTL back toward `base_ttl` across a configurable `decay_window` when traffic normalizes.
- **Distributed Stampede Lock**: Uses atomic `SETNX` with TTL so only **1 worker** recomputes expired content while concurrent requests are shielded.
- **Stale-While-Revalidate (SWR)**: Instantly serves the last-known stale payload while asynchronously recomputing fresh data in the background.
- **Strict Freshness Guarantee**: Critical endpoints (e.g. order status, live scores) declare `strict_freshness=True` to strictly respect `base_ttl` and never extend under any load.
- **Sliding-Window Rate Limiter**: Independent high-throughput rate limiter built on atomic Redis sorted sets (`ZSET`).

---

## 📊 Empirical Benchmark Results

*Hardware/Env: High-concurrency async load test (600 requests, 40 concurrency) against a simulated 150ms database latency layer.*

| Metric | Static TTL Baseline | HotCache (Adaptive + SWR) | Validated Outcome |
| :--- | :--- | :--- | :--- |
| **Total DB Queries (Viral Spike)** | `40 queries` | `1 query` | **97.5% DB query reduction** |
| **Stampede Expiry (50 Concurrent Req)** | `50 DB hits` | `1 DB hit` | **100% stampede prevention** (49 served via SWR) |
| **p95 Tail Latency** | `302.3 ms` | `144.3 ms` | **52.3% faster** |
| **p99 Tail Latency** | `342.9 ms` | `231.9 ms` | **32.4% faster** |
| **p50 Hit Latency** | `0.76 ms` | `1.54 ms` | *~0.7ms trade-off for Lua metadata atomicity* |

### 🔍 Engineering Trade-Offs & Key Learnings
- **The Tail Latency Win**: Baseline static cache suffers severe latency spikes during cache stampedes because dozens of requests get blocked on database I/O. HotCache cuts p95 by **52.3%** and p99 by **32.4%** by serving stale data via SWR during background revalidation.
- **Zero Cache Stampedes**: During exact cache expiry, 50 concurrent requests produced **50 simultaneous DB hits** under baseline, vs. **only 1 DB hit** under HotCache.
- **The p50 Trade-Off**: Raw static cache hits are sub-millisecond (`0.76ms`). HotCache's atomic Lua script introduces a negligible `~0.7ms` overhead (`1.54ms`) to atomically track load and compute dynamic TTL adjustments in a single round-trip.

---

## 🛠️ Installation

```bash
# In your virtual environment
pip install -e .
```

---

## ⚡ Quickstart

### 1. Decorate FastAPI Endpoints

```python
from fastapi import FastAPI, Response
from hotcache import CachePolicy, cached, RateLimiter

app = FastAPI()

# 1. Viral Resource (Adaptive TTL)
@app.get("/posts/{post_id}")
@cached(
    key_fn=lambda post_id: f"post:{post_id}",
    policy=CachePolicy(
        base_ttl=5,          # Starts at 5 seconds
        max_ttl=120,        # Hard ceiling (2 minutes)
        adaptive_ttl=True,
        load_threshold=30,  # 30 req/min triggers TTL doubling
        decay_window=3,     # 3 low-load checks before stepping down
        swr_enabled=True,   # Serve stale while revalidating
    )
)
async def get_post(post_id: str, response: Response):
    return await db.fetch_post(post_id)

# 2. Strict Freshness Resource (Never extends)
@app.get("/orders/{order_id}/status")
@cached(
    key_fn=lambda order_id: f"order:{order_id}",
    policy=CachePolicy(base_ttl=3, strict_freshness=True)
)
async def get_order_status(order_id: str):
    return await db.fetch_order_status(order_id)
```

### 2. Add Sliding-Window Rate Limiting

```python
from fastapi import Depends
from hotcache import RateLimiter

@app.get("/api/sensitive-action", dependencies=[Depends(RateLimiter(max_requests=60, window_seconds=60))])
async def sensitive_action():
    return {"status": "ok"}
```

### 3. Expose Live Telemetry & Introspection

```python
from hotcache import introspection_router

app.include_router(introspection_router)

# Available Endpoints:
# GET  /_cache/stats
# GET  /_cache/stats/{resource_id}
# POST /_cache/reset
```

---

## 🖥️ Live Visual Dashboard & Traffic Simulator

HotCache includes a built-in demo server with an interactive dark-mode glassmorphic telemetry dashboard:

```bash
# Start the demo application
uvicorn demo.app:app --port 8000 --reload
```

Open `http://localhost:8000` to interact with:
- **Real-Time Dynamic Adaptation Graphs**: Watch req/min cross threshold & trigger atomic TTL doubling.
- **Bot Traffic Generator**: One-click synthetic loads (*Viral Spike*, *Cache Stampede*, *Cooldown Decay*, *Steady Traffic*).
- **Side-by-Side Comparator**: Live comparison of Static Baseline vs HotCache.
- **Live Event Stream**: Real-time event log with color-coded alerts.

---

## 🧪 Testing & Verification

Run the test suite:
```bash
pytest -v
```

Run automated load benchmarks:
```bash
python benchmarks/run_benchmarks.py
```

---


