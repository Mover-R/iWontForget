"""Callback data string builders and pattern constants for PTB.

python-telegram-bot uses string patterns in CallbackQueryHandler.
Convention: "<prefix>:<value1>:<value2>"

Usage:
    # Building callback_data for a button:
    callback_data = WishCB.category("books")   # → "wish_cat:books"

    # Matching in handler registration:
    CallbackQueryHandler(on_category, pattern=WishCB.CATEGORY_PATTERN)

    # Parsing in handler:
    async def on_category(update, context):
        _, category = update.callback_query.data.split(":", 1)
"""

import re


# ---------------------------------------------------------------------------
# Wish callbacks
# ---------------------------------------------------------------------------

class WishCB:
    """Callback data helpers for wish-related actions."""

    CATEGORY_PATTERN = r"^wish_cat:"
    ACTION_PATTERN = r"^wish_action:"

    @staticmethod
    def category(slug: str) -> str:
        """wish_cat:<slug>  e.g. wish_cat:books"""
        return f"wish_cat:{slug}"

    @staticmethod
    def action(action: str, wish_id: str) -> str:
        """wish_action:<action>:<wish_id>  e.g. wish_action:delete:abc123"""
        return f"wish_action:{action}:{wish_id}"

    @staticmethod
    def parse_action(data: str) -> tuple[str, str]:
        """Parse wish_action:<action>:<wish_id> → (action, wish_id)."""
        parts = data.split(":", 2)
        return parts[1], parts[2]


# ---------------------------------------------------------------------------
# Reminder callbacks
# ---------------------------------------------------------------------------

class ReminderCB:
    """Callback data helpers for reminder-related actions."""

    ACTION_PATTERN = r"^rem_action:"
    SNOOZE_PATTERN = r"^rem_snooze:"
    RECURRENCE_PATTERN = r"^recur:"

    @staticmethod
    def action(action: str, reminder_id: str) -> str:
        """rem_action:<action>:<reminder_id>"""
        return f"rem_action:{action}:{reminder_id}"

    @staticmethod
    def snooze(reminder_id: str, minutes: int) -> str:
        """rem_snooze:<reminder_id>:<minutes>"""
        return f"rem_snooze:{reminder_id}:{minutes}"

    @staticmethod
    def recurrence(value: str) -> str:
        """recur:<value>  e.g. recur:daily"""
        return f"recur:{value}"

    @staticmethod
    def parse_action(data: str) -> tuple[str, str]:
        """Parse rem_action:<action>:<id> → (action, reminder_id)."""
        parts = data.split(":", 2)
        return parts[1], parts[2]

    @staticmethod
    def parse_snooze(data: str) -> tuple[str, int]:
        """Parse rem_snooze:<id>:<minutes> → (reminder_id, minutes)."""
        parts = data.split(":", 2)
        return parts[1], int(parts[2])


# ---------------------------------------------------------------------------
# Main menu callbacks
# ---------------------------------------------------------------------------

class MenuCB:
    """Callback data helpers for main menu navigation."""

    PATTERN = r"^menu:"

    @staticmethod
    def section(name: str) -> str:
        """menu:<section>  e.g. menu:wishes"""
        return f"menu:{name}"

    @staticmethod
    def parse(data: str) -> str:
        """Parse menu:<section> → section name."""
        return data.split(":", 1)[1]


# ---------------------------------------------------------------------------
# Generic confirm/cancel
# ---------------------------------------------------------------------------

class ConfirmCB:
    """Generic confirm/cancel callback data."""

    PATTERN = r"^confirm:"
    YES = "confirm:yes"
    NO = "confirm:no"
    SKIP = "confirm:skip"

    @staticmethod
    def parse(data: str) -> str:
        """Parse confirm:<action> → action ('yes', 'no', 'skip')."""
        return data.split(":", 1)[1]
