"""Async Kafka producer for user activity events.

Publishes to the `user.activity` topic whenever a user interacts with the bot.
Uses aiokafka for native asyncio support.

Event schema (JSON):
{
  "event_type": "command_executed" | "wish_created" | "reminder_created" | ...,
  "user_id": "tg_123456789",
  "telegram_id": "123456789",
  "timestamp": "2024-01-15T10:30:00Z",
  "metadata": { ... }
}
"""

import json
from datetime import UTC, datetime
from typing import Any

import structlog
from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaConnectionError

logger = structlog.get_logger(__name__)


class ActivityProducer:
    """Async Kafka producer for user activity events.

    Args:
        brokers: List of Kafka broker addresses.
        topic: Topic name for user activity events.
        enabled: Set to False to disable Kafka (useful in local dev).
    """

    def __init__(
        self,
        brokers: list[str],
        topic: str = "user.activity",
        enabled: bool = True,
    ) -> None:
        self._brokers = brokers
        self._topic = topic
        self._enabled = enabled
        self._producer: AIOKafkaProducer | None = None

    async def start(self) -> None:
        """Connect to Kafka. Call once at application startup."""
        if not self._enabled:
            logger.info("kafka_producer_disabled")
            return

        self._producer = AIOKafkaProducer(
            bootstrap_servers=self._brokers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            # Reliability settings
            acks="all",
            enable_idempotence=True,
            max_batch_size=16384,
            linger_ms=5,  # small batching window
        )
        try:
            await self._producer.start()
            logger.info("kafka_producer_started", brokers=self._brokers, topic=self._topic)
        except KafkaConnectionError as exc:
            logger.error("kafka_producer_start_failed", error=str(exc))
            self._producer = None
            # Don't raise — Kafka failure should not prevent bot from starting

    async def stop(self) -> None:
        """Flush and close the Kafka producer."""
        if self._producer:
            await self._producer.stop()
            self._producer = None
            logger.info("kafka_producer_stopped")

    async def publish_activity(
        self,
        event_type: str,
        user_id: str,
        telegram_id: str,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Publish a user activity event.

        Args:
            event_type: Event type string (e.g., "command_executed", "wish_created").
            user_id: Internal user ID.
            telegram_id: Telegram user ID (for partitioning).
            metadata: Optional additional event data.
        """
        if not self._producer:
            return  # Kafka disabled or not connected — silently skip

        event = {
            "event_type": event_type,
            "user_id": user_id,
            "telegram_id": telegram_id,
            "timestamp": datetime.now(UTC).isoformat(),
            "metadata": metadata or {},
        }

        try:
            await self._producer.send_and_wait(
                self._topic,
                value=event,
                key=telegram_id,  # partition by telegram_id for ordering
            )
            logger.debug("activity_event_published", event_type=event_type, user_id=user_id)
        except Exception as exc:
            # Fire-and-forget: log but don't raise
            logger.warning("activity_event_publish_failed", event_type=event_type, error=str(exc))
