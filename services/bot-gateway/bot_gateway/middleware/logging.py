"""Structured logging for PTB updates.

PTB doesn't have a middleware system like aiogram.
Instead we use a TypeHandler registered first (group=-1) that logs every update,
plus a post_init hook to configure structlog.
"""

import uuid

import structlog
from telegram import Update
from telegram.ext import ContextTypes

logger = structlog.get_logger(__name__)


async def log_update(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Log every incoming Telegram update.

    Register as:
        app.add_handler(TypeHandler(Update, log_update), group=-1)
    """
    request_id = str(uuid.uuid4())[:8]

    update_type = "unknown"
    user_id = None

    if update.message:
        update_type = "message"
        user_id = update.message.from_user.id if update.message.from_user else None
    elif update.callback_query:
        update_type = "callback_query"
        user_id = update.callback_query.from_user.id if update.callback_query.from_user else None
    elif update.inline_query:
        update_type = "inline_query"
        user_id = update.inline_query.from_user.id if update.inline_query.from_user else None

    logger.info(
        "update_received",
        request_id=request_id,
        update_type=update_type,
        tg_user_id=user_id,
        update_id=update.update_id,
    )

    # Store request_id in context for downstream handlers
    context.user_data["_request_id"] = request_id  # type: ignore[index]
