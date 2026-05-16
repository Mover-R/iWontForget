"""Base async gRPC channel manager.

All service clients inherit from BaseGrpcClient which handles:
- Channel lifecycle (connect / close)
- Default call timeout
- Structured logging on errors
"""

from abc import ABC, abstractmethod

import grpc
import structlog

logger = structlog.get_logger(__name__)

_DEFAULT_TIMEOUT_SECONDS = 5.0


class BaseGrpcClient(ABC):
    """Base class for async gRPC service clients.

    Args:
        address: Service address in "host:port" format.
        timeout: Default RPC call timeout in seconds.
    """

    def __init__(self, address: str, timeout: float = _DEFAULT_TIMEOUT_SECONDS) -> None:
        self._address = address
        self._timeout = timeout
        self._channel: grpc.aio.Channel | None = None

    async def connect(self) -> None:
        """Open the gRPC channel. Call once at application startup."""
        self._channel = grpc.aio.insecure_channel(self._address)
        self._init_stub(self._channel)
        logger.info("grpc_client_connected", service=self.__class__.__name__, address=self._address)

    async def close(self) -> None:
        """Close the gRPC channel. Call during graceful shutdown."""
        if self._channel:
            await self._channel.close()
            self._channel = None
            logger.info("grpc_client_closed", service=self.__class__.__name__)

    @abstractmethod
    def _init_stub(self, channel: grpc.aio.Channel) -> None:
        """Initialise the generated stub from the channel. Called by connect()."""
        ...

    def _check_connected(self) -> None:
        if self._channel is None:
            raise RuntimeError(
                f"{self.__class__.__name__} is not connected. Call connect() first."
            )
