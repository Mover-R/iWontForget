"""Unit tests for message formatter."""

import pytest

from bot_gateway.services.formatter import (
    bold,
    code,
    escape_html,
    format_reminder,
    format_wish,
    format_wish_list,
    italic,
    link,
)


def test_escape_html_ampersand():
    assert escape_html("a & b") == "a &amp; b"


def test_escape_html_angle_brackets():
    assert escape_html("<script>") == "&lt;script&gt;"


def test_escape_html_quotes():
    assert escape_html('"hello"') == "&quot;hello&quot;"


def test_bold():
    assert bold("Hello") == "<b>Hello</b>"


def test_italic():
    assert italic("World") == "<i>World</i>"


def test_code():
    assert code("print()") == "<code>print()</code>"


def test_link():
    result = link("Click here", "https://example.com")
    assert result == '<a href="https://example.com">Click here</a>'


def test_format_wish_basic():
    result = format_wish("New Laptop", None, 0, 0)
    assert "New Laptop" in result
    assert "🎁" in result


def test_format_wish_with_category_and_price():
    result = format_wish("Headphones", "tech", 500_00, 1000_00)
    assert "Headphones" in result
    assert "tech" in result
    assert "500" in result
    assert "1000" in result


def test_format_reminder_basic():
    result = format_reminder("Buy milk", "2024-01-15T10:00:00")
    assert "Buy milk" in result
    assert "⏰" in result


def test_format_wish_list_empty():
    result = format_wish_list([])
    assert "no wishes" in result.lower()


def test_format_wish_list_with_items():
    wishes = [
        {"name": "Book", "category": "books"},
        {"name": "Laptop", "category": "tech"},
    ]
    result = format_wish_list(wishes)
    assert "Book" in result
    assert "Laptop" in result
    assert "1." in result
    assert "2." in result
