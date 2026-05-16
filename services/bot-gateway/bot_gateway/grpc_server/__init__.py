"""gRPC server — exposes NotificationGateway service for Temporal activities.

Temporal workflow activities call SendNotification to deliver messages to users.
The server runs concurrently with the aiogram bot in the same asyncio event loop.
"""
