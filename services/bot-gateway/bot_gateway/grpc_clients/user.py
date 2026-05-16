"""User Service gRPC client.

Wraps the generated UserServiceStub with typed methods.

NOTE: The generated protobuf stubs live in bot_gateway/proto_gen/ (created by
      `buf generate` or `grpc_tools.protoc` during the Docker build).
      Until stubs are generated, this module uses TYPE_CHECKING guards so the
      rest of the codebase can import it without errors.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import grpc
import structlog

from bot_gateway.grpc_clients.base import BaseGrpcClient

if TYPE_CHECKING:
    pass  # generated stub types would be imported here

logger = structlog.get_logger(__name__)


# ---------------------------------------------------------------------------
# Lightweight data class returned to callers (avoids proto leaking everywhere)
# ---------------------------------------------------------------------------
class UserRecord:
    """Simplified user record returned by UserClient methods."""

    __slots__ = (
        "user_id",
        "telegram_id",
        "telegram_username",
        "first_name",
        "last_name",
        "language_code",
        "timezone",
    )

    def __init__(
        self,
        user_id: str,
        telegram_id: str,
        telegram_username: str,
        first_name: str,
        last_name: str,
        language_code: str,
        timezone: str,
    ) -> None:
        self.user_id = user_id
        self.telegram_id = telegram_id
        self.telegram_username = telegram_username
        self.first_name = first_name
        self.last_name = last_name
        self.language_code = language_code
        self.timezone = timezone


class UserClient(BaseGrpcClient):
    """Client for the User Service gRPC API."""

    def __init__(self, address: str) -> None:
        super().__init__(address)
        self._stub: Any = None  # type: ignore[assignment]

    def _init_stub(self, channel: grpc.aio.Channel) -> None:
        # Once proto stubs are generated, replace with:
        # from bot_gateway.proto_gen.users.v1 import users_pb2_grpc
        # self._stub = users_pb2_grpc.UserServiceStub(channel)
        self._stub = _MockUserStub(channel)

    async def get_or_create(
        self,
        telegram_id: str,
        telegram_username: str,
        first_name: str,
        last_name: str,
        language_code: str,
    ) -> UserRecord:
        """Get existing user or create a new one. Idempotent."""
        self._check_connected()
        try:
            resp = await self._stub.GetOrCreateUser(
                _make_get_or_create_request(
                    telegram_id=telegram_id,
                    telegram_username=telegram_username,
                    first_name=first_name,
                    last_name=last_name,
                    language_code=language_code,
                ),
                timeout=self._timeout,
            )
            return _proto_to_record(resp.user)
        except grpc.aio.AioRpcError as exc:
            logger.error(
                "user_service_get_or_create_failed",
                code=exc.code(),
                details=exc.details(),
                telegram_id=telegram_id,
            )
            raise

    async def get_user(self, user_id: str) -> UserRecord:
        """Fetch a user by internal user_id."""
        self._check_connected()
        try:
            resp = await self._stub.GetUser(
                _make_get_request(user_id=user_id),
                timeout=self._timeout,
            )
            return _proto_to_record(resp.user)
        except grpc.aio.AioRpcError as exc:
            logger.error(
                "user_service_get_failed",
                code=exc.code(),
                details=exc.details(),
                user_id=user_id,
            )
            raise


# ---------------------------------------------------------------------------
# Helpers — replaced by generated proto code once stubs exist
# ---------------------------------------------------------------------------

def _make_get_or_create_request(**kwargs: str) -> Any:
    """Build a GetOrCreateUserRequest. Replaced by proto stub after codegen."""

    class _Req:
        def __init__(self, **kw: str) -> None:
            for k, v in kw.items():
                setattr(self, k, v)

    return _Req(**kwargs)


def _make_get_request(user_id: str) -> Any:
    class _Req:
        pass

    r = _Req()
    r.user_id = user_id  # type: ignore[attr-defined]
    return r


def _proto_to_record(proto: Any) -> UserRecord:
    """Convert proto User message to UserRecord. Replaced after codegen."""
    return UserRecord(
        user_id=getattr(proto, "user_id", ""),
        telegram_id=getattr(proto, "telegram_id", ""),
        telegram_username=getattr(proto, "telegram_username", ""),
        first_name=getattr(proto, "first_name", ""),
        last_name=getattr(proto, "last_name", ""),
        language_code=getattr(proto, "language_code", "en"),
        timezone=getattr(proto, "timezone", "UTC"),
    )


class _MockUserStub:
    """Temporary mock stub — returns a fake user until real stubs are generated."""

    def __init__(self, channel: grpc.aio.Channel) -> None:
        pass

    async def GetOrCreateUser(self, request: Any, timeout: float = 5.0) -> Any:
        class _User:
            user_id = f"tg_{request.telegram_id}"
            telegram_id = request.telegram_id
            telegram_username = request.telegram_username
            first_name = request.first_name
            last_name = request.last_name
            language_code = request.language_code
            timezone = "UTC"

        class _Resp:
            user = _User()
            created = False

        return _Resp()

    async def GetUser(self, request: Any, timeout: float = 5.0) -> Any:
        class _User:
            user_id = request.user_id
            telegram_id = ""
            telegram_username = ""
            first_name = ""
            last_name = ""
            language_code = "en"
            timezone = "UTC"

        class _Resp:
            user = _User()

        return _Resp()
