"""Shared keyboard helpers used across multiple domains."""

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from bot_gateway.utils.callback_data import ConfirmCB, MenuCB


def confirm_keyboard() -> InlineKeyboardMarkup:
    """Generic ✅ Confirm / ❌ Cancel keyboard."""
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Confirm", callback_data=ConfirmCB.YES),
            InlineKeyboardButton("❌ Cancel", callback_data=ConfirmCB.NO),
        ]
    ])


def skip_keyboard() -> InlineKeyboardMarkup:
    """Single ⏭ Skip button."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("⏭ Skip", callback_data=ConfirmCB.SKIP)]
    ])


def back_keyboard(section: str) -> InlineKeyboardMarkup:
    """Single ← Back button navigating to a menu section."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("← Back", callback_data=MenuCB.section(section))]
    ])
