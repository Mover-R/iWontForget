"""Telegram message sender abstraction for python-telegram-bot.

Wraps PTB's Bot to provide:
- Rate-limit-aware sending
- Structured logging per send
- Retry on Telegram flood control (RetryAfter)
"""

import asyncio

import structlog
from telegram import Bot, InlineKeyboardMarkup
from telegram.error import Forbidden, RetryAfter

logger = structlog.get_logger(__name__)

_MAX_RETRIES = 3


class NotificationSender:
    """Sends Telegram messages with retry logic.

    Args:
        bot: PTB Bot instance.
    """

    def __init__(self, bot: Bot) -> None:
        self._bot = bot

    async def send_text(
        self,
        chat_id: int | str,
        text: str,
        parse_mode: str | None = "HTML",
        reply_markup: InlineKeyboardMarkup | None = None,
    ) -> int | None:
        """Send a text message to a chat.

        Returns the Telegram message_id on success, None if the user blocked the bot.
        Raises on unexpected errors after retries.
        """
        for attempt in range(1, _MAX_RETRIES + 1):
            try:
                msg = await self._bot.send_message(
                    chat_id=chat_id,
                    text=text,
                    parse_mode=parse_mode,
                    reply_markup=reply_markup,
                )
                logger.info(
                    "notification_sent",
                    chat_id=chat_id,
                    message_id=msg.message_id,
                    attempt=attempt,
                )
                return msg.message_id

            except RetryAfter as e:
                wait = e.retry_after + 1
                logger.warning(
                    "telegram_flood_control",
                    chat_id=chat_id,
                    retry_after=wait,
                    attempt=attempt,
                )
                if attempt < _MAX_RETRIES:
                    await asyncio.sleep(wait)
                else:
                    raise

            except Forbidden:
                # User blocked the bot — not recoverable
                logger.info("telegram_bot_blocked", chat_id=chat_id)
                return None

        return None
