"""Common handlers — /start, /help, /menu, /cancel, /settings + main menu callbacks.

This is the Phase 0 router: fully implemented.
Returns PTB handler objects to be registered in app.py.
"""

import structlog
from telegram import Update
from telegram.ext import (
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
)

from bot_gateway.keyboards.main_menu import main_menu_keyboard
from bot_gateway.utils.callback_data import MenuCB

logger = structlog.get_logger(__name__)


# ---------------------------------------------------------------------------
# /start
# ---------------------------------------------------------------------------

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /start — welcome message + main menu."""
    # Clear any active conversation state
    context.user_data.clear()  # type: ignore[union-attr]

    first_name = ""
    if update.effective_user:
        first_name = update.effective_user.first_name or "there"

    await update.message.reply_text(  # type: ignore[union-attr]
        f"👋 Hi, <b>{first_name}</b>!\n\n"
        "I'm <b>iWontForget</b> — your personal memory assistant.\n\n"
        "I can help you:\n"
        "🎁 Track your wishes\n"
        "⏰ Set reminders\n"
        "👥 Remember friends' birthdays\n\n"
        "What would you like to do?",
        parse_mode="HTML",
        reply_markup=main_menu_keyboard(),
    )


# ---------------------------------------------------------------------------
# /help
# ---------------------------------------------------------------------------

async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show available commands."""
    await update.message.reply_text(  # type: ignore[union-attr]
        "<b>Available commands:</b>\n\n"
        "🎁 <b>Wishes</b>\n"
        "  /wish — add a new wish\n"
        "  /wishes — view your wish list\n\n"
        "⏰ <b>Reminders</b>\n"
        "  /remind — set a reminder\n"
        "  /reminders — view upcoming reminders\n"
        "  /today — what's planned for today\n\n"
        "⚙️ <b>Other</b>\n"
        "  /menu — show main menu\n"
        "  /settings — notification settings\n"
        "  /cancel — cancel current action\n"
        "  /help — show this message",
        parse_mode="HTML",
    )


# ---------------------------------------------------------------------------
# /menu
# ---------------------------------------------------------------------------

async def cmd_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show the main menu keyboard."""
    await update.message.reply_text(  # type: ignore[union-attr]
        "What would you like to do?",
        reply_markup=main_menu_keyboard(),
    )


# ---------------------------------------------------------------------------
# /cancel — ends any active ConversationHandler
# ---------------------------------------------------------------------------

async def cmd_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Cancel the current multi-step dialog and return to idle.

    Returns ConversationHandler.END to terminate any active conversation.
    """
    context.user_data.clear()  # type: ignore[union-attr]
    await update.message.reply_text(  # type: ignore[union-attr]
        "❌ Cancelled. Use /menu to start over.",
        reply_markup=main_menu_keyboard(),
    )
    return ConversationHandler.END


# ---------------------------------------------------------------------------
# /settings
# ---------------------------------------------------------------------------

async def cmd_settings(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show settings (Phase 1 placeholder)."""
    await update.message.reply_text(  # type: ignore[union-attr]
        "⚙️ <b>Settings</b>\n\n"
        "Settings will be available in the next update.\n"
        "Stay tuned! 🚀",
        parse_mode="HTML",
    )


# ---------------------------------------------------------------------------
# Main menu callback
# ---------------------------------------------------------------------------

async def on_main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle main menu section selection."""
    query = update.callback_query
    await query.answer()  # type: ignore[union-attr]

    section = MenuCB.parse(query.data)  # type: ignore[union-attr]

    section_responses: dict[str, str] = {
        "wishes": (
            "🎁 <b>My Wishes</b>\n\n"
            "Use /wish to add a new wish\n"
            "Use /wishes to see your list"
        ),
        "reminders": (
            "⏰ <b>Reminders</b>\n\n"
            "Use /remind to set a reminder\n"
            "Use /reminders to see upcoming reminders\n"
            "Use /today to see today's plan"
        ),
        "friends": (
            "👥 <b>Friends</b>\n\n"
            "Friend management is coming in the next update! 🚀"
        ),
        "settings": (
            "⚙️ <b>Settings</b>\n\n"
            "Settings will be available in the next update. 🚀"
        ),
        "help": (
            "<b>Available commands:</b>\n\n"
            "/wish — add a wish\n"
            "/wishes — view wishes\n"
            "/remind — set a reminder\n"
            "/reminders — view reminders\n"
            "/today — today's plan\n"
            "/menu — main menu\n"
            "/cancel — cancel current action"
        ),
    }

    text = section_responses.get(section, "Unknown section.")
    await query.edit_message_text(  # type: ignore[union-attr]
        text,
        parse_mode="HTML",
        reply_markup=main_menu_keyboard(),
    )


# ---------------------------------------------------------------------------
# Handler objects to register in app.py
# ---------------------------------------------------------------------------

def get_handlers() -> list:
    """Return list of PTB handler objects for common commands."""
    return [
        CommandHandler("start", cmd_start),
        CommandHandler("help", cmd_help),
        CommandHandler("menu", cmd_menu),
        CommandHandler("settings", cmd_settings),
        CallbackQueryHandler(on_main_menu, pattern=MenuCB.PATTERN),
    ]


def get_cancel_handler() -> CommandHandler:
    """Return the /cancel CommandHandler (used inside ConversationHandlers too)."""
    return CommandHandler("cancel", cmd_cancel)
