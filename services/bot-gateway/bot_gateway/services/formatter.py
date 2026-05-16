"""Message text formatting helpers.

All bot responses use HTML parse mode (safer than Markdown — no escaping surprises).
Use these helpers to build consistent, well-formatted messages.
"""

from datetime import datetime


def escape_html(text: str) -> str:
    """Escape special HTML characters for Telegram HTML parse mode."""
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def bold(text: str) -> str:
    return f"<b>{escape_html(text)}</b>"


def italic(text: str) -> str:
    return f"<i>{escape_html(text)}</i>"


def code(text: str) -> str:
    return f"<code>{escape_html(text)}</code>"


def link(text: str, url: str) -> str:
    return f'<a href="{url}">{escape_html(text)}</a>'


def format_wish(name: str, category: str | None, price_min: int, price_max: int) -> str:
    """Format a wish for display in a message."""
    lines = [f"🎁 {bold(name)}"]
    if category:
        lines.append(f"📂 Category: {escape_html(category)}")
    if price_min or price_max:
        price_str = _format_price_range(price_min, price_max)
        lines.append(f"💰 Price: {price_str}")
    return "\n".join(lines)


def format_reminder(title: str, due_at: str, description: str = "") -> str:
    """Format a reminder notification message."""
    lines = [f"⏰ {bold('Reminder')}", f"📝 {escape_html(title)}"]
    if description:
        lines.append(f"{italic(description)}")
    try:
        dt = datetime.fromisoformat(due_at)
        lines.append(f"🕐 {dt.strftime('%d %b %Y, %H:%M')}")
    except ValueError:
        lines.append(f"🕐 {escape_html(due_at)}")
    return "\n".join(lines)


def format_wish_list(wishes: list[dict]) -> str:
    """Format a list of wishes for display."""
    if not wishes:
        return "You have no wishes yet. Use /wish to add one! 🎁"

    lines = [bold("Your Wishes:"), ""]
    for i, wish in enumerate(wishes, 1):
        name = escape_html(wish.get("name", ""))
        category = wish.get("category", "")
        cat_str = f" ({escape_html(category)})" if category else ""
        lines.append(f"{i}. {name}{cat_str}")

    return "\n".join(lines)


def _format_price_range(price_min: int, price_max: int) -> str:
    """Format a price range in rubles (values are in kopecks)."""
    if price_min and price_max:
        return f"{price_min // 100}–{price_max // 100} ₽"
    if price_min:
        return f"from {price_min // 100} ₽"
    if price_max:
        return f"up to {price_max // 100} ₽"
    return ""
