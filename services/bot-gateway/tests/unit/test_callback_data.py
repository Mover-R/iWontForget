"""Unit tests for callback data helpers."""

from bot_gateway.utils.callback_data import ConfirmCB, MenuCB, ReminderCB, WishCB


def test_wish_category_format():
    assert WishCB.category("books") == "wish_cat:books"
    assert WishCB.category("skip") == "wish_cat:skip"


def test_wish_action_format():
    result = WishCB.action("delete", "abc123")
    assert result == "wish_action:delete:abc123"


def test_wish_action_parse():
    action, wish_id = WishCB.parse_action("wish_action:delete:abc123")
    assert action == "delete"
    assert wish_id == "abc123"


def test_reminder_action_format():
    result = ReminderCB.action("done", "rem456")
    assert result == "rem_action:done:rem456"


def test_reminder_snooze_format():
    result = ReminderCB.snooze("rem456", 60)
    assert result == "rem_snooze:rem456:60"


def test_reminder_snooze_parse():
    reminder_id, minutes = ReminderCB.parse_snooze("rem_snooze:rem456:60")
    assert reminder_id == "rem456"
    assert minutes == 60


def test_menu_section_format():
    assert MenuCB.section("wishes") == "menu:wishes"


def test_menu_section_parse():
    assert MenuCB.parse("menu:wishes") == "wishes"
    assert MenuCB.parse("menu:reminders") == "reminders"


def test_confirm_constants():
    assert ConfirmCB.YES == "confirm:yes"
    assert ConfirmCB.NO == "confirm:no"
    assert ConfirmCB.SKIP == "confirm:skip"


def test_confirm_parse():
    assert ConfirmCB.parse("confirm:yes") == "yes"
    assert ConfirmCB.parse("confirm:no") == "no"
