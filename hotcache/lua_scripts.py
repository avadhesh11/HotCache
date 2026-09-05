"""
Atomic Redis Lua scripts for HotCache.
Guarantees zero-race-condition execution of adaptive TTL extensions, decays,
distributed stampede locking, and sliding window rate limiting.
"""

# Script 1: Atomic Read, Load Increment, and Adaptive TTL Evaluation
LUA_ADAPTIVE_GET_OR_TRACK = """
local reqcount_key = KEYS[1]
local cache_key = KEYS[2]

local base_ttl = tonumber(ARGV[1])
local max_ttl = tonumber(ARGV[2])
local adaptive_ttl = tonumber(ARGV[3])
local strict_freshness = tonumber(ARGV[4])
local load_threshold = tonumber(ARGV[5])
local decay_window = tonumber(ARGV[6])
local now = tonumber(ARGV[7])
local window_size = tonumber(ARGV[8]) or 60

-- 1. Increment request counter
local current_load = redis.call('INCR', reqcount_key)
if current_load == 1 then
    redis.call('EXPIRE', reqcount_key, window_size)
end

-- 2. Check if cache entry exists
local exists = redis.call('EXISTS', cache_key)
if exists == 0 then
    -- Cache MISS (no data or metadata)
    return {
        "MISS",
        "",                  -- value
        tostring(base_ttl),  -- current_ttl
        tostring(current_load),
        "0",                 -- low_load_streak
        "0",                 -- last_extended
        "0"                  -- is_stale
    }
end

-- Read metadata from Redis Hash
local metadata = redis.call('HMGET', cache_key, 
    'value', 'current_ttl', 'expires_at', 'low_load_streak', 'last_extended', 'base_ttl', 'max_ttl'
)

local val = metadata[1] or ""
local current_ttl = tonumber(metadata[2]) or base_ttl
local expires_at = tonumber(metadata[3]) or 0
local low_load_streak = tonumber(metadata[4]) or 0
local last_extended = tonumber(metadata[5]) or 0

-- 3. Adaptive TTL computation
local ttl_updated = 0
if strict_freshness == 1 then
    if current_ttl ~= base_ttl then
        current_ttl = base_ttl
        ttl_updated = 1
    end
    low_load_streak = 0
elseif adaptive_ttl == 1 then
    if current_load > load_threshold then
        local new_ttl = math.min(current_ttl * 2, max_ttl)
        if new_ttl > current_ttl then
            current_ttl = new_ttl
            last_extended = now
            ttl_updated = 1
        end
        low_load_streak = 0
    else
        low_load_streak = low_load_streak + 1
        if low_load_streak >= decay_window then
            local decayed_ttl = math.max(math.floor(current_ttl / 2), base_ttl)
            if decayed_ttl ~= current_ttl then
                current_ttl = decayed_ttl
                ttl_updated = 1
            end
            low_load_streak = 0
        end
    end
end

-- Save updated TTL metadata in hash
redis.call('HSET', cache_key, 
    'current_ttl', current_ttl, 
    'low_load_streak', low_load_streak, 
    'last_extended', last_extended
)

-- 4. Determine freshness
local is_expired = (now >= expires_at)

if is_expired then
    return {
        "EXPIRED",
        val,                 -- Stale value for SWR
        tostring(current_ttl),
        tostring(current_load),
        tostring(low_load_streak),
        tostring(last_extended),
        "1"                  -- is_stale = true
    }
else
    return {
        "HIT",
        val,                 -- Fresh value
        tostring(current_ttl),
        tostring(current_load),
        tostring(low_load_streak),
        tostring(last_extended),
        "0"                  -- is_stale = false
    }
end
"""

# Script 2: Atomic Cache Set with Metadata and Expiration
LUA_ADAPTIVE_SET = """
local cache_key = KEYS[1]

local val = ARGV[1]
local base_ttl = tonumber(ARGV[2])
local max_ttl = tonumber(ARGV[3])
local current_ttl = tonumber(ARGV[4])
local adaptive_ttl = tonumber(ARGV[5])
local strict_freshness = tonumber(ARGV[6])
local now = tonumber(ARGV[7])
local stale_buffer = tonumber(ARGV[8]) or 300

local expires_at = now + current_ttl
local redis_ttl = current_ttl + stale_buffer

redis.call('HSET', cache_key,
    'value', val,
    'base_ttl', base_ttl,
    'max_ttl', max_ttl,
    'current_ttl', current_ttl,
    'adaptive_ttl', adaptive_ttl,
    'strict_fresh', strict_freshness,
    'expires_at', expires_at,
    'last_updated', now
)

redis.call('EXPIRE', cache_key, redis_ttl)

return {
    "OK",
    tostring(current_ttl),
    tostring(expires_at)
}
"""

# Script 3: Stampede Lock Acquire (SETNX with TTL)
LUA_ACQUIRE_LOCK = """
local lock_key = KEYS[1]
local token = ARGV[1]
local ttl_seconds = tonumber(ARGV[2])

local res = redis.call('SET', lock_key, token, 'NX', 'EX', ttl_seconds)
if res then
    return 1
else
    return 0
end
"""

# Script 4: Stampede Lock Release (Only if token matches)
LUA_RELEASE_LOCK = """
local lock_key = KEYS[1]
local token = ARGV[1]

if redis.call('GET', lock_key) == token then
    return redis.call('DEL', lock_key)
else
    return 0
end
"""

# Script 5: Sliding-Window Rate Limiter
LUA_SLIDING_RATE_LIMIT = """
local rate_key = KEYS[1]
local now = tonumber(ARGV[1])
local window_seconds = tonumber(ARGV[2])
local max_requests = tonumber(ARGV[3])
local member_id = ARGV[4]

local clear_before = now - window_seconds
redis.call('ZREMRANGEBYSCORE', rate_key, '-inf', clear_before)

local current_requests = redis.call('ZCARD', rate_key)

if current_requests < max_requests then
    redis.call('ZADD', rate_key, now, member_id)
    redis.call('EXPIRE', rate_key, window_seconds + 1)
    local remaining = max_requests - current_requests - 1
    return {1, remaining, current_requests + 1}
else
    local oldest = redis.call('ZRANGE', rate_key, 0, 0, 'WITHSCORES')
    local reset_in = window_seconds
    if oldest and #oldest >= 2 then
        reset_in = math.max(0, math.floor(tonumber(oldest[2]) + window_seconds - now))
    end
    return {0, 0, current_requests, reset_in}
end
"""
