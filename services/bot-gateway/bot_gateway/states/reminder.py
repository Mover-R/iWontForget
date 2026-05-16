"""Conversation states for the reminder creation dialog.

Flow:
  /remind → REMINDER_TEXT → REMINDER_TIME → REMINDER_RECURRENCE → REMINDER_CONFIRM → [saved]
"""


class ReminderStates:
    """Integer state constants for the reminder creation ConversationHandler."""

    TEXT: int = 20        # Waiting for reminder text
    TIME: int = 21        # Waiting for date/time (natural language or ISO)
    RECURRENCE: int = 22  # Waiting for recurrence selection (optional)
    CONFIRM: int = 23     # Waiting for confirm/cancel
