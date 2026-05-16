"""Conversation states for the wish creation dialog.

Flow:
  /wish → WISH_NAME → WISH_CATEGORY → WISH_PRICE → WISH_CONFIRM → [saved]
                                                                  ↘ [cancelled]

Usage in ConversationHandler:
    states={
        WishStates.NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, process_wish_name)],
        WishStates.CATEGORY: [CallbackQueryHandler(process_wish_category, pattern="^wish_cat:")],
        WishStates.PRICE: [MessageHandler(filters.TEXT & ~filters.COMMAND, process_wish_price)],
        WishStates.CONFIRM: [CallbackQueryHandler(process_wish_confirm, pattern="^confirm:")],
    }
"""


class WishStates:
    """Integer state constants for the wish creation ConversationHandler."""

    NAME: int = 10       # Waiting for wish name text
    CATEGORY: int = 11   # Waiting for category selection (inline keyboard)
    PRICE: int = 12      # Waiting for price range text or "skip"
    CONFIRM: int = 13    # Waiting for confirm/cancel
