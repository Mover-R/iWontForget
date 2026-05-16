"""Free-text message handler (catch-all for non-command, non-FSM messages).

Phase 0: Prompt user to use commands.
Phase 4: Will call AI Service to classify intent.
"""

from telegram import Update
from telegram.ext import ContextTypes

from bot_gateway.keyboards.main_menu import main_menu_keyboard


async def free_text_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle free-text messages when no ConversationHandler is active."""
    await update.message.reply_text(  # type: ignore[union-attr]
        "💬 I received your message!\n\n"
        "I'm still learning to understand free-form text. "
        "For now, please use the commands below:\n\n"
        "/wish — add a wish\n"
        "/remind — set a reminder\n"
        "/help — see all commands",
        parse_mode="HTML",
        reply_markup=main_menu_keyboard(),
    )
