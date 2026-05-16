"""NotificationGateway gRPC servicer.

Called by Temporal workflow activities to deliver notifications to Telegram users.

Flow:
  Temporal activity → SendNotification RPC → dedup check → rate limit check
                    → NotificationSender.send_text() → Telegram API

The servicer is idempotent: duplicate calls with the same idempotency_key
return sent=False without re-sending.
"""

from __future__ import annotations

from typing import Any

import grpc
import structlog

from bot_gateway.services.dedup import Deduplicator
from bot_gateway.services.formatter import format_reminder
from bot_gateway.services.notification import NotificationSender
from bot_gateway.services.ratelimit import RateLimiter

logger = structlog.get_logger(__name__)


class NotificationGatewayServicer:
    """Implements the NotificationGateway gRPC service.

    Args:
        sender: Telegram message sender.
        rate_limiter: Redis token bucket rate limiter.
        dedup: Redis SET NX deduplicator.
    """

    def __init__(
        self,
        sender: NotificationSender,
        rate_limiter: RateLimiter,
        dedup: Deduplicator,
    ) -> None:
        self._sender = sender
        self._rate_limiter = rate_limiter
        self._dedup = dedup

    def register(self, server: grpc.aio.Server) -> None:
        """Register this servicer with the gRPC server.

        TODO: Replace with generated registration once proto stubs exist:
            from bot_gateway.proto_gen.bot.v1 import bot_pb2_grpc
            bot_pb2_grpc.add_NotificationGatewayServicer_to_server(self, server)
        """
        # Temporary: no-op until proto stubs are generated.
        # The server will start but won't serve any RPCs yet.
        logger.warning(
            "grpc_servicer_not_registered",
            reason="proto stubs not yet generated — run `buf generate` or `grpc_tools.protoc`",
        )

    async def SendNotification(
        self,
        request: Any,
        context: grpc.aio.ServicerContext,
    ) -> Any:
        """Handle a SendNotification RPC call from a Temporal activity.

        Request fields (from bot/v1/bot.proto):
          - user_id: str
          - idempotency_key: str
          - content: oneof { text, reminder }

        Returns:
          SendNotificationResponse { sent: bool, message_id: str }
        """
        user_id: str = request.user_id
        idempotency_key: str = request.idempotency_key

        log = logger.bind(user_id=user_id, idempotency_key=idempotency_key)

        # 1. Deduplication check
        if not await self._dedup.is_first_delivery(idempotency_key):
            log.info("notification_deduplicated")
            return _make_response(sent=False, message_id="")

        # 2. Rate limit check
        if not await self._rate_limiter.is_allowed(user_id):
            log.warning("notification_rate_limited")
            await context.abort(
                grpc.StatusCode.RESOURCE_EXHAUSTED,
                "Rate limit exceeded — retry after 1 second",
            )
            return None

        # 3. Build message text from content oneof
        text, parse_mode = _build_message(request)

        # 4. Send via Telegram
        message_id = await self._sender.send_text(
            chat_id=user_id,
            text=text,
            parse_mode=parse_mode or "HTML",
        )

        if message_id is None:
            # User blocked the bot — mark as delivered to avoid infinite retries
            log.info("notification_bot_blocked")
            return _make_response(sent=False, message_id="")

        log.info("notification_delivered", message_id=message_id)
        return _make_response(sent=True, message_id=str(message_id))


def _build_message(request: Any) -> tuple[str, str]:
    """Extract text and parse_mode from the request content oneof."""
    content_type = request.WhichOneof("content") if hasattr(request, "WhichOneof") else None

    if content_type == "text":
        return request.text.text, request.text.parse_mode or "HTML"

    if content_type == "reminder":
        r = request.reminder
        text = format_reminder(
            title=r.title,
            due_at=r.due_at,
            description=r.description,
        )
        return text, "HTML"

    # Fallback for unknown content types
    return str(getattr(request, "text", "You have a new notification.")), "HTML"


def _make_response(sent: bool, message_id: str) -> Any:
    """Build a SendNotificationResponse.

    TODO: Replace with generated proto message once stubs exist:
        from bot_gateway.proto_gen.bot.v1 import bot_pb2
        return bot_pb2.SendNotificationResponse(sent=sent, message_id=message_id)
    """

    class _Resp:
        pass

    r = _Resp()
    r.sent = sent  # type: ignore[attr-defined]
    r.message_id = message_id  # type: ignore[attr-defined]
    return r
