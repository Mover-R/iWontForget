"""Keyboards for wish-related dialogs."""

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from bot_gateway.utils.callback_data import WishCB

_CATEGORIES = [
    ("📚 Books", "books"),
    ("👗 Clothing", "clothing"),
    ("🎮 Games", "games"),
    ("🏠 Home", "home"),
    ("✈️ Travel", "travel"),
    ("🍽️ Food", "food"),
    ("💻 Tech", "tech"),
    ("🎨 Hobbies", "hobbies"),
    ("💆 Wellness", "wellness"),
    ("🎁 Other", "other"),
]


def category_keyboard() -> InlineKeyboardMarkup:
    """Category selection keyboard for wish creation."""
    rows = []
    for i in range(0, len(_CATEGORIES), 2):
        row = [
            InlineKeyboardButton(label, callback_data=WishCB.category(slug))
            for label, slug in _CATEGORIES[i : i + 2]
        ]
        rows.append(row)
    rows.append([InlineKeyboardButton("⏭ Skip", callback_data=WishCB.category("skip"))])
    return InlineKeyboardMarkup(rows)


def wish_actions_keyboard(wish_id: str) -> InlineKeyboardMarkup:
    """Action buttons for a single wish."""
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✏️ Edit", callback_data=WishCB.action("edit", wish_id)),
            InlineKeyboardButton("🗑 Delete", callback_data=WishCB.action("delete", wish_id)),
        ],
        [
            InlineKeyboardButton("🔗 Share", callback_data=WishCB.action("share", wish_id)),
        ],
    ])
