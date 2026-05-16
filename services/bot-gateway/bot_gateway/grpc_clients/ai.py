"""AI Service gRPC client (stub — Phase 4)."""

from __future__ import annotations

from typing import Any

import grpc

from bot_gateway.grpc_clients.base import BaseGrpcClient


class AIClient(BaseGrpcClient):
    def __init__(self, address: str) -> None:
        super().__init__(address)
        self._stub: Any = None

    def _init_stub(self, channel: grpc.aio.Channel) -> None:
        self._stub = None  # TODO: replace after proto codegen

    async def classify_intent(self, text: str, user_id: str) -> str:
        """Classify free-text message intent. Returns intent string."""
        # TODO: implement after proto codegen
        # For now, return "unknown" so free-text falls through to help message
        return "unknown"
