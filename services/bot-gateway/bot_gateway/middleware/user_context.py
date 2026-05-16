"""User context loader for PTB.

PTB doesn't have middleware, so we use a TypeHandler (group=-1) that runs
before domain handlers and loads/creates the user record from User Service.

The user record is stored in context.user_data["user"] for downstream handlers.
"""

import structlog
from telegram import Update
from telegram.ext import ContextTypes

logger = structlog.get_logger(__name__)


def make_user_context_handler(user_client):  # type: ignore[no-untyped-def]
    """Factory that creates a user context handler bound to a UserClient.

    Args:
        user_client: bot_gateway.grpc_clients.user.UserClient instance.

    Returns:
        Async handler function to register as TypeHandler(Update, handler, group=-1).
    """

    async def load_user_context(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Load or create user record and store in context.user_data."""
        tg_user = None

        if update.message and update.message.from_user:
            tg_user = update.message.from_user
        elif update.callback_query and update.callback_query.from_user:
            tg_user = update.callback_query.from_user
        elif update.inline_query and update.inline_query.from_user:
            tg_user = update.inline_query.from_user

        if tg_user:
            try:
                user = await user_client.get_or_create(
                    telegram_id=str(tg_user.id),
                    telegram_username=tg_user.username or "",
                    first_name=tg_user.first_name or "",
                    last_name=tg_user.last_name or "",
                    language_code=tg_user.language_code or "en",
                )
                context.user_data["user"] = user  # type: ignore[index]
                logger.debug("user_context_loaded", user_id=user.user_id)
            except Exception as exc:
                logger.warning("user_context_load_failed", error=str(exc))
                context.user_data["user"] = None  # type: ignore[index]
        else:
            context.user_data["user"] = None  # type: ignore[index]

    return load_user_context
