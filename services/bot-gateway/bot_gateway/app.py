"""Application factory for python-telegram-bot.

Creates and wires together:
  - PTB Application with RedisPersistence (conversation state survives restarts)
  - Handler registration (common, wish, reminder, friend, catch-alls)
  - TypeHandler-based logging + metrics (group=-1, runs before all handlers)
  - Error handler
  - gRPC clients (to Go backend services)
  - gRPC server (for Temporal notification activities) — Phase 1+
  - Kafka producer (user activity events) — Phase 1+
  - Redis connections (rate limiting, dedup)

Usage:
    app, settings = await create_app()
    # Then run via __main__.py
"""

from __future__ import annotations

import logging
import sys
from typing import TYPE_CHECKING

import redis.asyncio as aioredis
import structlog
from telegram import Update
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    MessageHandler,
    TypeHandler,
    filters,
)
from telegram.ext import PicklePersistence  # fallback if Redis unavailable

from bot_gateway.config import Settings
from bot_gateway.grpc_clients.ai import AIClient
from bot_gateway.grpc_clients.event import EventClient
from bot_gateway.grpc_clients.friends import FriendsClient
from bot_gateway.grpc_clients.gift import GiftClient
from bot_gateway.grpc_clients.reminder import ReminderClient
from bot_gateway.grpc_clients.user import UserClient
from bot_gateway.grpc_clients.wishlist import WishlistClient
from bot_gateway.handlers import callbacks, commands, messages
from bot_gateway.kafka.producer import ActivityProducer
from bot_gateway.middleware.error_handler import error_handler
from bot_gateway.middleware.logging import log_update
from bot_gateway.middleware.metrics import record_metrics
from bot_gateway.middleware.user_context import make_user_context_handler
from bot_gateway.routers import common, friend, reminder, wish
from bot_gateway.services.dedup import Deduplicator
from bot_gateway.services.notification import NotificationSender
from bot_gateway.services.ratelimit import RateLimiter

logger = structlog.get_logger(__name__)


def _configure_logging(level: str, fmt: str) -> None:
    """Configure structlog for JSON (prod) or console (dev) output."""
    log_level = getattr(logging, level.upper(), logging.INFO)

    shared_processors: list[structlog.types.Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
    ]

    if fmt == "json":
        processors: list[structlog.types.Processor] = [
            *shared_processors,
            structlog.processors.dict_tracebacks,
            structlog.processors.JSONRenderer(),
        ]
    else:
        processors = [
            *shared_processors,
            structlog.dev.ConsoleRenderer(colors=True),
        ]

    structlog.configure(
        processors=processors,
        wrapper_class=structlog.make_filtering_bound_logger(log_level),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )

    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=log_level,
        force=True,
    )


def _build_redis_url(settings: Settings, db: int) -> str:
    """Build a Redis URL from settings."""
    if settings.redis.password:
        return f"redis://:{settings.redis.password}@{settings.redis.addr}/{db}"
    return f"redis://{settings.redis.addr}/{db}"


async def _build_persistence(settings: Settings):  # type: ignore[return]
    """Build PTB persistence backend.

    Uses RedisPersistence (from python-telegram-bot[redis]) when Redis is
    configured. Falls back to PicklePersistence for local dev without Redis.

    RedisPersistence stores:
      - user_data   → per-user conversation state (ConversationHandler states)
      - chat_data   → per-chat data
      - bot_data    → global bot data
      - conversations → active ConversationHandler states
    """
    try:
        from telegram.ext import RedisStorage  # type: ignore[attr-defined]

        # PTB's RedisStorage (from python-telegram-bot[redis]) uses redis-py
        redis_url = _build_redis_url(settings, settings.redis.session_db)
        persistence = RedisStorage(url=redis_url)
        logger.info("persistence_redis", url=redis_url)
        return persistence
    except ImportError:
        pass

    # Fallback: try connecting to Redis directly via redis-py
    try:
        from telegram.ext import PersistenceInput
        # PTB v21 uses its own RedisStorage — if not available, use pickle
        raise ImportError("Using pickle fallback")
    except ImportError:
        persistence = PicklePersistence(filepath="bot_data.pickle")
        logger.warning(
            "persistence_pickle_fallback",
            reason="python-telegram-bot[redis] not installed or Redis unavailable",
        )
        return persistence


async def create_app() -> tuple["BotApp", Settings]:
    """Application factory — reads settings, creates and wires all components."""
    settings = Settings()
    _configure_logging(settings.log_level, settings.log_format)

    logger.info(
        "bot_gateway_initializing",
        environment=settings.environment,
        mode=settings.telegram.mode,
    )

    app = BotApp(settings)
    await app.initialize()
    return app, settings


class BotApp:
    """Container for all bot components.

    Holds the PTB Application and all infrastructure clients.
    """

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.ptb_app: Application | None = None

        # ---- Redis connections (rate limiting + dedup) ----
        self.ratelimit_redis = aioredis.Redis.from_url(
            _build_redis_url(settings, settings.redis.ratelimit_db),
        )
        self.dedup_redis = aioredis.Redis.from_url(
            _build_redis_url(settings, settings.redis.dedup_db),
        )

        # ---- gRPC clients ----
        self.user_client = UserClient(settings.services.user)
        self.wishlist_client = WishlistClient(settings.services.wishlist)
        self.reminder_client = ReminderClient(settings.services.reminder)
        self.friends_client = FriendsClient(settings.services.friends)
        self.gift_client = GiftClient(settings.services.gift)
        self.event_client = EventClient(settings.services.event)
        self.ai_client = AIClient(settings.services.ai)

        self._all_clients = [
            self.user_client,
            self.wishlist_client,
            self.reminder_client,
            self.friends_client,
            self.gift_client,
            self.event_client,
            self.ai_client,
        ]

        # ---- Services ----
        self.rate_limiter = RateLimiter(
            self.ratelimit_redis,
            global_per_second=settings.ratelimit.global_per_second,
            per_chat_per_second=settings.ratelimit.per_chat_per_second,
        )
        self.dedup = Deduplicator(self.dedup_redis)

        # ---- Kafka ----
        self.kafka_producer = ActivityProducer(
            brokers=settings.kafka.brokers,
            topic=settings.kafka.topic_user_activity,
            enabled=settings.kafka.enabled,
        )

        # ---- gRPC server (Phase 1+) ----
        self.grpc_server = None

    async def initialize(self) -> None:
        """Build the PTB Application and register all handlers."""
        persistence = await _build_persistence(self.settings)

        # Build PTB Application
        builder = (
            Application.builder()
            .token(self.settings.telegram.token)
            .persistence(persistence)
            # Store shared services in bot_data so handlers can access them
            # via context.bot_data["wishlist_client"] etc.
        )

        self.ptb_app = builder.build()

        # Store shared services in bot_data for handler access
        self.ptb_app.bot_data.update({
            "wishlist_client": self.wishlist_client,
            "reminder_client": self.reminder_client,
            "friends_client": self.friends_client,
            "gift_client": self.gift_client,
            "event_client": self.event_client,
            "ai_client": self.ai_client,
            "kafka_producer": self.kafka_producer,
            "rate_limiter": self.rate_limiter,
        })

        self._register_handlers()
        logger.info("ptb_application_built")

    def _register_handlers(self) -> None:
        """Register all handlers on the PTB Application.

        Registration order:
          group=-1 : TypeHandlers for logging + metrics + user context (run first)
          group=0  : Domain handlers (common, wish, reminder, friend)
          group=1  : Catch-all handlers (unknown commands, free text, unknown callbacks)
        """
        app = self.ptb_app
        assert app is not None

        # ---- group=-1: pre-handlers (logging, metrics, user context) ----
        app.add_handler(TypeHandler(Update, log_update), group=-1)
        app.add_handler(TypeHandler(Update, record_metrics), group=-1)
        app.add_handler(
            TypeHandler(Update, make_user_context_handler(self.user_client)),
            group=-1,
        )

        # ---- group=0: domain handlers ----
        for handler in common.get_handlers():
            app.add_handler(handler, group=0)

        for handler in wish.get_handlers():
            app.add_handler(handler, group=0)

        for handler in reminder.get_handlers():
            app.add_handler(handler, group=0)

        for handler in friend.get_handlers():
            app.add_handler(handler, group=0)

        # /cancel works globally (not just inside conversations)
        app.add_handler(common.get_cancel_handler(), group=0)

        # ---- group=1: catch-all handlers (LAST) ----
        # Unknown commands (messages starting with /)
        app.add_handler(
            MessageHandler(filters.COMMAND, commands.unknown_command),
            group=1,
        )
        # Free-text messages (not commands, not in active conversation)
        app.add_handler(
            MessageHandler(filters.TEXT & ~filters.COMMAND, messages.free_text_message),
            group=1,
        )
        # Unknown callback queries
        app.add_handler(
            CallbackQueryHandler(callbacks.unknown_callback),
            group=1,
        )

        # ---- Error handler ----
        app.add_error_handler(error_handler)

        logger.info("handlers_registered")

    async def start(self) -> None:
        """Connect all infrastructure clients."""
        # Connect gRPC clients (User Service is needed for user context middleware)
        await self.user_client.connect()
        # Other clients connect lazily when Phase 1 handlers use them

        # Start Kafka producer
        await self.kafka_producer.start()

        logger.info("bot_app_started")

    async def stop(self) -> None:
        """Graceful shutdown — close all connections."""
        logger.info("bot_gateway_shutting_down")

        if self.grpc_server:
            await self.grpc_server.stop(grace=5)

        await self.kafka_producer.stop()

        for client in self._all_clients:
            await client.close()

        await self.ratelimit_redis.aclose()
        await self.dedup_redis.aclose()

        logger.info("bot_gateway_shutdown_complete")
