"""Business logic services.

- ratelimit: Redis token bucket — enforces Telegram send rate limits
- dedup: Redis SET NX — prevents duplicate notification delivery
- notification: Telegram message sender abstraction
- formatter: Message text formatting helpers
"""
