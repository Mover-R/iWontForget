"""Unit tests for conversation state constants."""

from bot_gateway.states.wish import WishStates
from bot_gateway.states.reminder import ReminderStates
from bot_gateway.states.friend import FriendStates


def test_wish_states_are_integers():
    assert isinstance(WishStates.NAME, int)
    assert isinstance(WishStates.CATEGORY, int)
    assert isinstance(WishStates.PRICE, int)
    assert isinstance(WishStates.CONFIRM, int)


def test_reminder_states_are_integers():
    assert isinstance(ReminderStates.TEXT, int)
    assert isinstance(ReminderStates.TIME, int)
    assert isinstance(ReminderStates.RECURRENCE, int)
    assert isinstance(ReminderStates.CONFIRM, int)


def test_friend_states_are_integers():
    assert isinstance(FriendStates.USERNAME, int)
    assert isinstance(FriendStates.CONFIRM, int)


def test_states_are_unique_across_groups():
    """All state values across all groups must be unique to avoid routing conflicts."""
    wish_vals = {WishStates.NAME, WishStates.CATEGORY, WishStates.PRICE, WishStates.CONFIRM}
    reminder_vals = {ReminderStates.TEXT, ReminderStates.TIME, ReminderStates.RECURRENCE, ReminderStates.CONFIRM}
    friend_vals = {FriendStates.USERNAME, FriendStates.CONFIRM}

    assert wish_vals.isdisjoint(reminder_vals), "Wish and Reminder states overlap"
    assert wish_vals.isdisjoint(friend_vals), "Wish and Friend states overlap"
    assert reminder_vals.isdisjoint(friend_vals), "Reminder and Friend states overlap"
