"""Low-level update type handlers.

These are the catch-all handlers that sit AFTER all domain routers.
They handle updates that no domain router claimed:
  - commands.py: unknown commands → show help
  - messages.py: free-text → AI classification (Phase 4) or help
  - callbacks.py: unknown callback_data → log and answer
  - deeplink.py: deep link parameters from /start?start=...
"""
