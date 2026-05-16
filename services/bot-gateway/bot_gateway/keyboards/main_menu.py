"""Main menu inline keyboard."""

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from bot_gateway.utils.callback_data import MenuCB


def main_menu_keyboard() -> InlineKeyboardMarkup:
    """Return the main menu inline keyboard."""
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🎁 My Wishes", callback_data=MenuCB.section("wishes")),
            InlineKeyboardButton("⏰ Reminders", callback_data=MenuCB.section("reminders")),
        ],
        [
            InlineKeyboardButton("👥 Friends", callback_data=MenuCB.section("friends")),
            InlineKeyboardButton("⚙️ Settings", callback_data=MenuCB.section("settings")),
        ],
        [
            InlineKeyboardButton("❓ Help", callback_data=MenuCB.section("help")),
        ],
    ])
