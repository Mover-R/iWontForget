"""Kafka integration — publishes user activity events.

Events are fire-and-forget (no transactional outbox needed).
Losing one activity event is acceptable — it only affects analytics.
"""
