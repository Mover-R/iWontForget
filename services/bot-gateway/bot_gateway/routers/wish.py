"""Wish router — /wish, /wishes + ConversationHandler (Phase 1 stub).

Phase 0: Commands registered but return "coming soon" messages.
Phase 1: Full ConversationHandler with WishlistClient gRPC calls.
"""

import structlog
from telegram import Update
from telegram.ext import CommandHandler, ContextTypes, ConversationHandler

from bot_gateway.states.wish import WishStates

logger = structlog.get_logger(__name__)


async def cmd_wish(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Start wish creation (Phase 1 stub)."""
    await update.message.reply_text(  # type: ignore[union-attr]
        "🎁 <b>Add a Wish</b>\n\n"
        "Wish creation will be available in the next update.\n"
        "Stay tuned! 🚀",
        parse_mode="HTML",
    )


async def cmd_wishes(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """List user's wishes (Phase 1 stub)."""
    await update.message.reply_text(  # type: ignore[union-attr]
        "🎁 <b>My Wishes</b>\n\n"
        "Wish list will be available in the next update.\n"
        "Stay tuned! 🚀",
        parse_mode="HTML",
    )


def get_handlers() -> list:
    """Return PTB handler objects for wish commands."""
    return [
        CommandHandler("wish", cmd_wish),
        CommandHandler("wishes", cmd_wishes),
    ]


# ---------------------------------------------------------------------------
# Phase 1: ConversationHandler skeleton (not yet wired up)
# ---------------------------------------------------------------------------
# When Phase 1 is implemented, replace get_handlers() with get_conversation():
#
# def get_conversation() -> ConversationHandler:
#     from bot_gateway.routers.common import get_cancel_handler
#     from bot_gateway.keyboards.wish import category_keyboard
#     from bot_gateway.keyboards.common import skip_keyboard, confirm_keyboard
#
#     return ConversationHandler(
#         entry_points=[CommandHandler("wish", start_wish)],
#         states={
#             WishStates.NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, process_name)],
#             WishStates.CATEGORY: [CallbackQueryHandler(process_category, pattern=WishCB.CATEGORY_PATTERN)],
#             WishStates.PRICE: [MessageHandler(filters.TEXT & ~filters.COMMAND, process_price)],
#             WishStates.CONFIRM: [CallbackQueryHandler(process_confirm, pattern=ConfirmCB.PATTERN)],
#         },
#         fallbacks=[get_cancel_handler()],
#         persistent=True,
#         name="wish_conversation",
#     )
