"""Unit tests for RateLimiter."""

import pytest


@pytest.mark.asyncio
async def test_first_request_allowed(rate_limiter):
    """First request should always be allowed."""
    result = await rate_limiter.is_allowed(chat_id=123456)
    assert result is True


@pytest.mark.asyncio
async def test_different_chats_independent(rate_limiter):
    """Rate limits for different chats should be independent."""
    assert await rate_limiter.is_allowed(chat_id=111) is True
    assert await rate_limiter.is_allowed(chat_id=222) is True


@pytest.mark.asyncio
async def test_global_rate_limit_enforced():
    """Global rate limit should be enforced after burst is exhausted."""
    import fakeredis.aioredis
    from bot_gateway.services.ratelimit import RateLimiter

    redis_client = fakeredis.aioredis.FakeRedis()
    # Very tight rate limit: 2 per second globally
    limiter = RateLimiter(redis_client, global_per_second=2, per_chat_per_second=100)

    # First 2 should pass (burst = max_tokens = 2)
    assert await limiter.is_allowed(chat_id=1) is True
    assert await limiter.is_allowed(chat_id=2) is True
    # 3rd should be rate-limited
    assert await limiter.is_allowed(chat_id=3) is False

    await redis_client.aclose()
