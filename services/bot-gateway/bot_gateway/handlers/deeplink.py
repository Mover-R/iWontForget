"""Deep link handler — /start <payload> (Phase 3).

Deep links: t.me/bot?start=wishlist_<user_id>
Phase 0: Falls through to normal /start.
Phase 3: Parse payload and route to appropriate action.
"""

import structlog
from telegram import Update
from telegram.ext import ContextTypes

from bot_gateway.routers.common import cmd_start

logger = structlog.get_logger(__name__)


async def deep_link_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /start with a deep link payload."""
    payload = context.args[0] if context.args else ""
    logger.info("deep_link_received", payload=payload)
    # Phase 3: parse payload and route
    # For now, fall through to normal /start
    await cmd_start(update, context)
