"""Keyboards for reminder-related dialogs."""

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from bot_gateway.utils.callback_data import ReminderCB


def reminder_notification_keyboard(reminder_id: str) -> InlineKeyboardMarkup:
    """Keyboard shown when a reminder notification fires."""
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton(
                "✅ Done",
                callback_data=ReminderCB.action("done", reminder_id),
            ),
            InlineKeyboardButton(
                "🔕 Snooze",
                callback_data=ReminderCB.action("snooze", reminder_id),
            ),
        ],
        [
            InlineKeyboardButton(
                "🗑 Delete",
                callback_data=ReminderCB.action("delete", reminder_id),
            ),
        ],
    ])


def snooze_duration_keyboard(reminder_id: str) -> InlineKeyboardMarkup:
    """Snooze duration selection keyboard."""
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("15 min", callback_data=ReminderCB.snooze(reminder_id, 15)),
            InlineKeyboardButton("1 hour", callback_data=ReminderCB.snooze(reminder_id, 60)),
            InlineKeyboardButton("Tomorrow", callback_data=ReminderCB.snooze(reminder_id, 1440)),
        ]
    ])


def recurrence_keyboard() -> InlineKeyboardMarkup:
    """Recurrence selection during reminder creation."""
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("Once", callback_data=ReminderCB.recurrence("once")),
            InlineKeyboardButton("Daily", callback_data=ReminderCB.recurrence("daily")),
        ],
        [
            InlineKeyboardButton("Weekly", callback_data=ReminderCB.recurrence("weekly")),
            InlineKeyboardButton("Monthly", callback_data=ReminderCB.recurrence("monthly")),
        ],
        [
            InlineKeyboardButton("⏭ Skip", callback_data=ReminderCB.recurrence("skip")),
        ],
    ])
