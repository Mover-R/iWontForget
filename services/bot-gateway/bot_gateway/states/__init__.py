"""Conversation state constants for python-telegram-bot ConversationHandler.

PTB uses integer constants to identify conversation states (unlike aiogram's StatesGroup).
Each module defines a set of int constants for a specific dialog flow.

States are persisted in Redis via PTB's RedisPersistence — survives pod restarts.
"""

from bot_gateway.states.wish import WishStates
from bot_gateway.states.reminder import ReminderStates
from bot_gateway.states.friend import FriendStates

__all__ = ["WishStates", "ReminderStates", "FriendStates"]
