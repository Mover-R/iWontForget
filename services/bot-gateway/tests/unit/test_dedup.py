"""Unit tests for Deduplicator."""

import pytest


@pytest.mark.asyncio
async def test_first_delivery_returns_true(dedup):
    result = await dedup.is_first_delivery("reminder:abc:1")
    assert result is True


@pytest.mark.asyncio
async def test_second_delivery_returns_false(dedup):
    await dedup.is_first_delivery("reminder:abc:2")
    result = await dedup.is_first_delivery("reminder:abc:2")
    assert result is False


@pytest.mark.asyncio
async def test_different_keys_are_independent(dedup):
    assert await dedup.is_first_delivery("key:1") is True
    assert await dedup.is_first_delivery("key:2") is True
    assert await dedup.is_first_delivery("key:1") is False
    assert await dedup.is_first_delivery("key:2") is False


@pytest.mark.asyncio
async def test_revoke_allows_redelivery(dedup):
    await dedup.is_first_delivery("reminder:revoke:1")
    await dedup.revoke("reminder:revoke:1")
    result = await dedup.is_first_delivery("reminder:revoke:1")
    assert result is True


@pytest.mark.asyncio
async def test_mark_delivered_prevents_redelivery(dedup):
    await dedup.mark_delivered("reminder:mark:1")
    result = await dedup.is_first_delivery("reminder:mark:1")
    assert result is False
