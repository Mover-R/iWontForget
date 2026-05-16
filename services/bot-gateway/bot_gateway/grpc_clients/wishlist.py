"""Wishlist Service gRPC client (stub — Phase 1)."""

from __future__ import annotations

from typing import Any

import grpc
import structlog

from bot_gateway.grpc_clients.base import BaseGrpcClient

logger = structlog.get_logger(__name__)


class WishlistClient(BaseGrpcClient):
    """Client for the Wishlist Service gRPC API."""

    def __init__(self, address: str) -> None:
        super().__init__(address)
        self._stub: Any = None

    def _init_stub(self, channel: grpc.aio.Channel) -> None:
        # TODO: replace with generated stub after `buf generate`
        # from bot_gateway.proto_gen.wishlist.v1 import wishlist_pb2_grpc
        # self._stub = wishlist_pb2_grpc.WishlistServiceStub(channel)
        self._stub = None

    async def create_wish(
        self,
        user_id: str,
        name: str,
        category: str | None = None,
        price_min: int = 0,
        price_max: int = 0,
        url: str = "",
        notes: str = "",
    ) -> dict[str, Any]:
        """Create a new wish. Returns wish dict with wish_id."""
        self._check_connected()
        # TODO: implement after proto stubs are generated
        raise NotImplementedError("WishlistClient.create_wish — awaiting proto codegen")

    async def list_wishes(
        self,
        user_id: str,
        page: int = 1,
        page_size: int = 10,
    ) -> list[dict[str, Any]]:
        """List wishes for a user."""
        self._check_connected()
        raise NotImplementedError("WishlistClient.list_wishes — awaiting proto codegen")
