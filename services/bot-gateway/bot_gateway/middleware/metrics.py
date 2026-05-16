"""Prometheus metrics for PTB updates.

Register as a TypeHandler in group=-1 (runs before domain handlers).
"""

import time

import structlog
from prometheus_client import Counter, Histogram
from telegram import Update
from telegram.ext import ContextTypes

logger = structlog.get_logger(__name__)

_updates_total = Counter(
    "bot_updates_total",
    "Total Telegram updates received",
    ["type"],
)

_handler_duration = Histogram(
    "bot_handler_duration_seconds",
    "Handler processing duration",
    ["type"],
    buckets=[0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0],
)


def _get_update_type(update: Update) -> str:
    if update.message:
        return "message"
    if update.callback_query:
        return "callback_query"
    if update.inline_query:
        return "inline_query"
    return "other"


async def record_metrics(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Record Prometheus metrics for every update.

    Register as:
        app.add_handler(TypeHandler(Update, record_metrics), group=-1)
    """
    update_type = _get_update_type(update)
    _updates_total.labels(type=update_type).inc()

    # Store start time for duration tracking
    context.user_data["_metrics_start"] = time.perf_counter()  # type: ignore[index]
