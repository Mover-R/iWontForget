"""Reminder router — /remind, /reminders, /today + ConversationHandler (Phase 1 stub)."""

import structlog
from telegram import Update
from telegram.ext import CommandHandler, ContextTypes

logger = structlog.get_logger(__name__)


async def cmd_remind(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Start reminder creation (Phase 1 stub)."""
    await update.message.reply_text(  # type: ignore[union-attr]
        "⏰ <b>Set a Reminder</b>\n\n"
        "Reminder creation will be available in the next update.\n"
        "Stay tuned! 🚀",
        parse_mode="HTML",
    )


async def cmd_reminders(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """List upcoming reminders (Phase 1 stub)."""
    await update.message.reply_text(  # type: ignore[union-attr]
        "⏰ <b>Upcoming Reminders</b>\n\n"
        "Reminder list will be available in the next update.\n"
        "Stay tuned! 🚀",
        parse_mode="HTML",
    )


async def cmd_today(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show today's plan (Phase 1 stub)."""
    await update.message.reply_text(  # type: ignore[union-attr]
        "📅 <b>Today's Plan</b>\n\n"
        "Today's view will be available in the next update.\n"
        "Stay tuned! 🚀",
        parse_mode="HTML",
    )


def get_handlers() -> list:
    """Return PTB handler objects for reminder commands."""
    return [
        CommandHandler("remind", cmd_remind),
        CommandHandler("reminders", cmd_reminders),
        CommandHandler("today", cmd_today),
    ]
