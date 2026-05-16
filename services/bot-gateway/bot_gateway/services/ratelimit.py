"""Redis token bucket rate limiter for Telegram message sending.

Telegram limits:
  - 30 messages/second globally
  - 1 message/second per chat

Uses an atomic Lua script to avoid race conditions across multiple bot pods.
"""

import time

import redis.asyncio as redis
import structlog

logger = structlog.get_logger(__name__)

# Atomic token bucket implementation in Lua.
# Returns 1 if the request is allowed, 0 if rate-limited.
_TOKEN_BUCKET_SCRIPT = """
local key        = KEYS[1]
local max_tokens = tonumber(ARGV[1])
local refill_rate = tonumber(ARGV[2])   -- tokens per second
local now        = tonumber(ARGV[3])    -- current time (float seconds)
local requested  = tonumber(ARGV[4])    -- tokens to consume (usually 1)

local bucket = redis.call("HMGET", key, "tokens", "last_refill")
local tokens     = tonumber(bucket[1]) or max_tokens
local last_refill = tonumber(bucket[2]) or now

-- Refill tokens based on elapsed time
local elapsed = math.max(0, now - last_refill)
tokens = math.min(max_tokens, tokens + elapsed * refill_rate)

if tokens >= requested then
    tokens = tokens - requested
    redis.call("HMSET", key, "tokens", tokens, "last_refill", now)
    redis.call("EXPIRE", key, 60)
    return 1
else
    redis.call("HMSET", key, "tokens", tokens, "last_refill", now)
    redis.call("EXPIRE", key, 60)
    return 0
end
"""

_GLOBAL_KEY = "ratelimit:global"
_CHAT_KEY_PREFIX = "ratelimit:chat:"


class RateLimiter:
    """Token bucket rate limiter backed by Redis.

    Args:
        redis_client: Async Redis client (dedicated DB for rate limiting).
        global_per_second: Max messages per second globally (default 30).
        per_chat_per_second: Max messages per second per chat (default 1).
    """

    def __init__(
        self,
        redis_client: redis.Redis,
        global_per_second: int = 30,
        per_chat_per_second: int = 1,
    ) -> None:
        self._redis = redis_client
        self._global_rate = global_per_second
        self._chat_rate = per_chat_per_second
        self._script: redis.client.Script | None = None

    async def _get_script(self) -> redis.client.Script:
        if self._script is None:
            self._script = self._redis.register_script(_TOKEN_BUCKET_SCRIPT)
        return self._script

    async def is_allowed(self, chat_id: int | str) -> bool:
        """Check and consume one token from both global and per-chat buckets.

        Returns True if the message is allowed, False if rate-limited.
        """
        now = time.time()
        script = await self._get_script()

        # Check global bucket
        global_ok = await script(
            keys=[_GLOBAL_KEY],
            args=[self._global_rate, self._global_rate, now, 1],
        )
        if not global_ok:
            logger.warning("rate_limit_global_hit")
            return False

        # Check per-chat bucket
        chat_key = f"{_CHAT_KEY_PREFIX}{chat_id}"
        chat_ok = await script(
            keys=[chat_key],
            args=[self._chat_rate * 5, self._chat_rate, now, 1],  # burst = 5x rate
        )
        if not chat_ok:
            logger.warning("rate_limit_chat_hit", chat_id=chat_id)
            return False

        return True
