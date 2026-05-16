"""Reminder Service gRPC client (stub — Phase 1)."""

from __future__ import annotations

from typing import Any

import grpc
import structlog

from bot_gateway.grpc_clients.base import BaseGrpcClient

logger = structlog.get_logger(__name__)


class ReminderClient(BaseGrpcClient):
    """Client for the Reminder Service gRPC API."""

    def __init__(self, address: str) -> None:
        super().__init__(address)
        self._stub: Any = None

    def _init_stub(self, channel: grpc.aio.Channel) -> None:
        # TODO: replace with generated stub after `buf generate`
        self._stub = None

    async def create_reminder(
        self,
        user_id: str,
        text: str,
        due_at: str,
        recurrence: str = "once",
    ) -> dict[str, Any]:
        """Create a new reminder."""
        self._check_connected()
        raise NotImplementedError("ReminderClient.create_reminder — awaiting proto codegen")

    async def list_reminders(
        self,
        user_id: str,
        page: int = 1,
        page_size: int = 10,
    ) -> list[dict[str, Any]]:
        """List upcoming reminders for a user."""
        self._check_connected()
        raise NotImplementedError("ReminderClient.list_reminders — awaiting proto codegen")
