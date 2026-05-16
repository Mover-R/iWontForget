"""Friend router — /friend, /friends, /birthdays (Phase 2 stub)."""

from telegram import Update
from telegram.ext import CommandHandler, ContextTypes


async def cmd_friends_stub(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Friend management (Phase 2 stub)."""
    await update.message.reply_text(  # type: ignore[union-attr]
        "👥 <b>Friends</b>\n\n"
        "Friend management is coming in Phase 2.\n"
        "Stay tuned! 🚀",
        parse_mode="HTML",
    )


def get_handlers() -> list:
    """Return PTB handler objects for friend commands."""
    return [
        CommandHandler("friend", cmd_friends_stub),
        CommandHandler("friends", cmd_friends_stub),
        CommandHandler("birthdays", cmd_friends_stub),
    ]
