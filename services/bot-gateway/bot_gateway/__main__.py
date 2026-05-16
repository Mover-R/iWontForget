"""Entry point: python -m bot_gateway

Starts the PTB bot in either:
  - Long-polling mode (TELEGRAM__MODE=longpoll) — for local development
  - Webhook mode     (TELEGRAM__MODE=webhook)   — for production

Long-polling: PTB calls Telegram's getUpdates in a loop. Zero config needed.
Webhook:      PTB starts an HTTPS server; Telegram POSTs updates to it.
              Requires TELEGRAM__WEBHOOK_URL to be set.
"""

import asyncio
import logging

import structlog

from bot_gateway.app import create_app

logger = structlog.get_logger(__name__)


async def main() -> None:
    """Bootstrap and run the bot."""
    bot_app, settings = await create_app()

    # Connect infrastructure (gRPC clients, Kafka)
    await bot_app.start()

    ptb = bot_app.ptb_app
    assert ptb is not None

    mode = settings.telegram.mode.lower()

    try:
        if mode == "webhook":
            await _run_webhook(ptb, settings)
        else:
            await _run_longpoll(ptb)
    finally:
        await bot_app.stop()


async def _run_longpoll(ptb) -> None:  # type: ignore[no-untyped-def]
    """Run in long-polling mode — ideal for local development.

    PTB handles everything: getUpdates loop, graceful shutdown on Ctrl+C.
    No webhook URL or tunnel needed.
    """
    logger.info("bot_starting_longpoll")
    await ptb.run_polling(
        allowed_updates=["message", "callback_query", "inline_query"],
        drop_pending_updates=True,
    )


async def _run_webhook(ptb, settings) -> None:  # type: ignore[no-untyped-def]
    """Run in webhook mode — for production deployments.

    PTB starts an aiohttp server that Telegram POSTs updates to.
    Requires:
      TELEGRAM__WEBHOOK_URL  — public HTTPS URL (e.g. https://bot.example.com)
      TELEGRAM__WEBHOOK_PORT — port to listen on (default 8443)
      TELEGRAM__WEBHOOK_PATH — URL path (default /webhook)
    """
    webhook_url = settings.telegram.webhook_url
    if not webhook_url:
        raise ValueError(
            "TELEGRAM__WEBHOOK_URL must be set when TELEGRAM__MODE=webhook"
        )

    full_url = f"{webhook_url.rstrip('/')}{settings.telegram.webhook_path}"
    logger.info("bot_starting_webhook", url=full_url, port=settings.telegram.webhook_port)

    await ptb.run_webhook(
        listen="0.0.0.0",
        port=settings.telegram.webhook_port,
        url_path=settings.telegram.webhook_path,
        webhook_url=full_url,
        secret_token=settings.telegram.webhook_secret_token or None,
        allowed_updates=["message", "callback_query", "inline_query"],
        drop_pending_updates=True,
    )


if __name__ == "__main__":
    asyncio.run(main())
