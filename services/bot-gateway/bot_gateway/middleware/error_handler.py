"""Global error handler for PTB.

PTB has a built-in error handler mechanism via Application.add_error_handler().
This module provides the error handler function to register.
"""

import structlog
from telegram import Update
from telegram.ext import ContextTypes

logger = structlog.get_logger(__name__)

_ERROR_TEXT = (
    "😔 Something went wrong. Please try again in a moment.\n"
    "If the problem persists, use /start to reset."
)


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle uncaught exceptions from any handler.

    Register as:
        app.add_error_handler(error_handler)
    """
    logger.exception(
        "unhandled_handler_exception",
        error=str(context.error),
        update=str(update)[:200] if update else None,
    )

    # Try to notify the user
    if isinstance(update, Update):
        try:
            if update.message:
                await update.message.reply_text(_ERROR_TEXT)
            elif update.callback_query:
                await update.callback_query.answer(
                    "Something went wrong. Please try again.",
                    show_alert=True,
                )
                if update.callback_query.message:
                    await update.callback_query.message.reply_text(_ERROR_TEXT)
        except Exception:
            logger.warning("error_notification_failed")
