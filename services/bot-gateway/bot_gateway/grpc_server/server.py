"""gRPC server factory.

Creates and starts a grpc.aio.Server with the NotificationGateway servicer.
Runs alongside the aiogram bot in the same asyncio event loop.
"""

import grpc
import structlog

from bot_gateway.grpc_server.notification_service import NotificationGatewayServicer

logger = structlog.get_logger(__name__)


async def create_grpc_server(
    servicer: NotificationGatewayServicer,
    port: int = 50051,
    max_recv_msg_size: int = 4 * 1024 * 1024,
) -> grpc.aio.Server:
    """Create, configure, and start the gRPC server.

    Args:
        servicer: The NotificationGateway servicer implementation.
        port: Port to listen on.
        max_recv_msg_size: Maximum incoming message size in bytes.

    Returns:
        A started grpc.aio.Server instance.
    """
    options = [
        ("grpc.max_receive_message_length", max_recv_msg_size),
        ("grpc.max_send_message_length", max_recv_msg_size),
        # Keep-alive settings for long-lived connections from Temporal workers
        ("grpc.keepalive_time_ms", 30_000),
        ("grpc.keepalive_timeout_ms", 10_000),
        ("grpc.keepalive_permit_without_calls", True),
    ]

    server = grpc.aio.server(options=options)

    # Register the servicer
    # Once proto stubs are generated, replace with:
    # from bot_gateway.proto_gen.bot.v1 import bot_pb2_grpc
    # bot_pb2_grpc.add_NotificationGatewayServicer_to_server(servicer, server)
    servicer.register(server)

    listen_addr = f"[::]:{port}"
    server.add_insecure_port(listen_addr)

    await server.start()
    logger.info("grpc_server_started", address=listen_addr)

    return server
