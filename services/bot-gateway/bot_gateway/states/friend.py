"""Conversation states for friend management dialogs (Phase 2)."""


class FriendStates:
    """Integer state constants for the add-friend ConversationHandler."""

    USERNAME: int = 30   # Waiting for Telegram username
    CONFIRM: int = 31    # Waiting for confirm/cancel
