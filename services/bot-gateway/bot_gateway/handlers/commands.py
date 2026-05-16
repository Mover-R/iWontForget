"""Catch-all unknown command handler.

Registered last — domain handlers get first pick.
"""

from telegram import Update
from telegram.ext import ContextTypes

from bot_gateway.keyboards.main_menu import main_menu_keyboard


async def unknown_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle unknown commands — suggest /help."""
    text = update.message.text if update.message else ""
    command = text.split()[0] if text else "that command"
    await update.message.reply_text(  # type: ignore[union-attr]
        f"❓ I don't know <code>{command}</code>.\n\n"
        "Use /help to see available commands.",
        parse_mode="HTML",
        reply_markup=main_menu_keyboard(),
    )
