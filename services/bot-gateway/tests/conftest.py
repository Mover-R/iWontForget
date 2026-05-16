"""Shared pytest fixtures for bot-gateway tests (python-telegram-bot).

Provides:
  - fake_redis: fakeredis async client (in-memory Redis, no real Redis needed)
  - rate_limiter: RateLimiter backed by fake_redis
  - dedup: Deduplicator backed by fake_redis
  - mock_bot: PTB Bot with mocked send_message
  - notification_sender: NotificationSender backed by mock_bot
  - mock_user_client: UserClient with mocked gRPC stub
"""

from __future__ import annotations

import pytest
import pytest_asyncio
from unittest.mock import AsyncMock, MagicMock

import fakeredis.aioredis

from bot_gateway.services.dedup import Deduplicator
from bot_gateway.services.ratelimit import RateLimiter
from bot_gateway.services.notification import NotificationSender


# ---------------------------------------------------------------------------
# Redis
# ---------------------------------------------------------------------------

@pytest_asyncio.fixture
async def fake_redis():
    """In-memory Redis client (fakeredis) — no real Redis needed."""
    client = fakeredis.aioredis.FakeRedis()
    yield client
    await client.aclose()


# ---------------------------------------------------------------------------
# Services
# ---------------------------------------------------------------------------

@pytest_asyncio.fixture
async def rate_limiter(fake_redis):
    """RateLimiter backed by fakeredis."""
    return RateLimiter(
        redis_client=fake_redis,
        global_per_second=30,
        per_chat_per_second=1,
    )


@pytest_asyncio.fixture
async def dedup(fake_redis):
    """Deduplicator backed by fakeredis."""
    return Deduplicator(redis_client=fake_redis, ttl_seconds=3600)


# ---------------------------------------------------------------------------
# PTB Bot (mocked — no real Telegram API calls)
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_bot():
    """PTB Bot with mocked send_message."""
    from telegram import Message
    bot = MagicMock()
    mock_message = MagicMock(spec=Message)
    mock_message.message_id = 42
    bot.send_message = AsyncMock(return_value=mock_message)
    return bot


@pytest.fixture
def notification_sender(mock_bot):
    """NotificationSender backed by mock_bot."""
    return NotificationSender(bot=mock_bot)


# ---------------------------------------------------------------------------
# gRPC client mocks
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_user_client():
    """Mocked UserClient — returns a fake UserRecord."""
    from bot_gateway.grpc_clients.user import UserRecord

    client = MagicMock()
    client.get_or_create = AsyncMock(
        return_value=UserRecord(
            user_id="tg_123456789",
            telegram_id="123456789",
            telegram_username="testuser",
            first_name="Test",
            last_name="User",
            language_code="en",
            timezone="UTC",
        )
    )
    client.get_user = AsyncMock(
        return_value=UserRecord(
            user_id="tg_123456789",
            telegram_id="123456789",
            telegram_username="testuser",
            first_name="Test",
            last_name="User",
            language_code="en",
            timezone="UTC",
        )
    )
    return client
