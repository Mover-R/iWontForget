"""Gift Service gRPC client (stub — Phase 5)."""

from __future__ import annotations

from typing import Any

import grpc

from bot_gateway.grpc_clients.base import BaseGrpcClient


class GiftClient(BaseGrpcClient):
    def __init__(self, address: str) -> None:
        super().__init__(address)
        self._stub: Any = None

    def _init_stub(self, channel: grpc.aio.Channel) -> None:
        self._stub = None  # TODO: replace after proto codegen
