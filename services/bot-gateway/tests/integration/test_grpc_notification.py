"""Integration tests for NotificationGatewayServicer (PTB version)."""

import pytest
from unittest.mock import AsyncMock, MagicMock

from bot_gateway.grpc_server.notification_service import NotificationGatewayServicer
from bot_gateway.services.dedup import Deduplicator
from bot_gateway.services.notification import NotificationSender
from bot_gateway.services.ratelimit import RateLimiter


def _make_text_request(user_id: str, key: str, text: str):
    req = MagicMock()
    req.user_id = user_id
    req.idempotency_key = key
    req.WhichOneof = MagicMock(return_value="text")
    req.text = MagicMock()
    req.text.text = text
    req.text.parse_mode = "HTML"
    return req


@pytest.mark.asyncio
async def test_send_notification_success(fake_redis, mock_bot):
    """Successful notification delivery returns sent=True."""
    sender = NotificationSender(bot=mock_bot)
    rate_limiter = RateLimiter(fake_redis, global_per_second=30, per_chat_per_second=1)
    dedup = Deduplicator(fake_redis)

    servicer = NotificationGatewayServicer(
        sender=sender,
        rate_limiter=rate_limiter,
        dedup=dedup,
    )

    request = _make_text_request("123456789", "test:key:1", "Hello!")
    context = MagicMock()
    context.abort = AsyncMock()

    response = await servicer.SendNotification(request, context)

    assert response.sent is True
    assert response.message_id == "42"
    mock_bot.send_message.assert_called_once()


@pytest.mark.asyncio
async def test_send_notification_deduplicated(fake_redis, mock_bot):
    """Duplicate notification (same idempotency_key) returns sent=False."""
    sender = NotificationSender(bot=mock_bot)
    rate_limiter = RateLimiter(fake_redis, global_per_second=30, per_chat_per_second=1)
    dedup = Deduplicator(fake_redis)

    servicer = NotificationGatewayServicer(
        sender=sender,
        rate_limiter=rate_limiter,
        dedup=dedup,
    )

    request = _make_text_request("123456789", "test:key:dup", "Hello!")
    context = MagicMock()
    context.abort = AsyncMock()

    resp1 = await servicer.SendNotification(request, context)
    assert resp1.sent is True

    resp2 = await servicer.SendNotification(request, context)
    assert resp2.sent is False

    assert mock_bot.send_message.call_count == 1
