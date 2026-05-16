"""Notification deduplication via Redis SET NX.

Temporal retries the same activity multiple times on failure.
We use an idempotency key (provided by the caller) to ensure
each notification is delivered at most once.

Pattern:
    SET dedup:<key> 1 NX EX <ttl>
    → returns True  → first delivery, proceed
    → returns False → already delivered, skip
"""

import redis.asyncio as redis
import structlog

logger = structlog.get_logger(__name__)

_KEY_PREFIX = "dedup:"
_DEFAULT_TTL_SECONDS = 86400  # 24 hours — covers Temporal retry window


class Deduplicator:
    """Redis SET NX deduplication for notification delivery.

    Args:
        redis_client: Async Redis client (dedicated DB for dedup).
        ttl_seconds: How long to remember a delivered notification.
    """

    def __init__(
        self,
        redis_client: redis.Redis,
        ttl_seconds: int = _DEFAULT_TTL_SECONDS,
    ) -> None:
        self._redis = redis_client
        self._ttl = ttl_seconds

    async def is_first_delivery(self, idempotency_key: str) -> bool:
        """Attempt to claim the idempotency key.

        Returns True if this is the first delivery (key was not set).
        Returns False if the notification was already delivered.
        """
        full_key = f"{_KEY_PREFIX}{idempotency_key}"
        result = await self._redis.set(full_key, "1", nx=True, ex=self._ttl)
        if result:
            logger.debug("dedup_first_delivery", key=idempotency_key)
            return True
        logger.info("dedup_duplicate_skipped", key=idempotency_key)
        return False

    async def mark_delivered(self, idempotency_key: str) -> None:
        """Explicitly mark a key as delivered (idempotent — safe to call multiple times)."""
        full_key = f"{_KEY_PREFIX}{idempotency_key}"
        await self._redis.set(full_key, "1", ex=self._ttl)

    async def revoke(self, idempotency_key: str) -> None:
        """Remove a dedup key — allows re-delivery (use with caution)."""
        full_key = f"{_KEY_PREFIX}{idempotency_key}"
        await self._redis.delete(full_key)
        logger.info("dedup_key_revoked", key=idempotency_key)
