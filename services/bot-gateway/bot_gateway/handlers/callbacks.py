"""Catch-all callback query handler — handles unknown callback_data.

Registered last so domain handlers get first pick.
"""

import structlog
from telegram import Update
from telegram.ext import ContextTypes

logger = structlog.get_logger(__name__)


async def unknown_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle unknown callback queries — log and dismiss."""
    query = update.callback_query
    logger.warning(
        "unknown_callback_data",
        data=query.data if query else None,
        user_id=query.from_user.id if query and query.from_user else None,
    )
    if query:
        await query.answer(
            "This button is no longer active. Please use /menu.",
            show_alert=False,
        )
