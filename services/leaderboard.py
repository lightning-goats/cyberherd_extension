"""Leaderboard utilities for CyberHerd."""

from __future__ import annotations

import asyncio
from typing import Any, Tuple

from loguru import logger

from .. import crud
from .pubkey import resolve_effective_pubkey

DEFAULT_LEADERBOARD_LIMIT = 10

_watchers: dict[str, list[asyncio.Queue]] = {}
_watchers_lock: asyncio.Lock | None = None


def _get_watchers_lock() -> asyncio.Lock:
    global _watchers_lock
    if _watchers_lock is None:
        _watchers_lock = asyncio.Lock()
    return _watchers_lock


def _normalize_pubkey(value: str | None) -> str | None:
    if not value or not isinstance(value, str):
        return None
    candidate = value.strip().lower()
    if len(candidate) != 64:
        return None
    return candidate


async def resolve_user_by_pubkey(pubkey: str) -> Tuple[str | None, Any | None]:
    """Return (user_id, settings) for the provided effective pubkey."""
    target = _normalize_pubkey(pubkey)
    if not target:
        return None, None

    settings_list = await crud.get_all_settings()
    for settings in settings_list:
        try:
            eff = resolve_effective_pubkey(settings)
        except Exception:
            eff = None
        if eff and eff.strip().lower() == target:
            return settings.user_id, settings
    return None, None


async def get_leaderboard_entries(user_id: str, limit: int = DEFAULT_LEADERBOARD_LIMIT) -> list[dict[str, Any]]:
    if not user_id:
        return []

    members = await crud.get_all_cyberherd_members(user_id)
    entries: list[dict[str, Any]] = []

    for member in members or []:
        pubkey = member.get("pubkey")
        if not isinstance(pubkey, str):
            continue
        pubkey = pubkey.strip().lower()

        amount = 0
        try:
            amount = int(member.get("amount", 0) or 0)
        except Exception:
            amount = 0

        zap_totals = await crud.get_zap_totals_row(user_id, pubkey)
        historical_sats = 0
        historical_events = 0
        if zap_totals:
            try:
                historical_sats = int(zap_totals.get("total_sats") or 0)
            except Exception:
                historical_sats = 0
            try:
                historical_events = int(zap_totals.get("event_count") or 0)
            except Exception:
                historical_events = 0

        entries.append(
            {
                "pubkey": pubkey,
                "display_name": member.get("display_name") or member.get("alias") or "Anon",
                "picture": member.get("picture"),
                "amount": amount,
                "is_active": bool(member.get("is_active")),
                "historical_sats": historical_sats,
                "historical_events": historical_events,
            }
        )

    entries.sort(
        key=lambda item: (item.get("historical_sats", 0), item.get("amount", 0)),
        reverse=True,
    )
    if limit and limit > 0:
        entries = entries[:limit]
    return entries


async def register_leaderboard_watcher(user_id: str) -> asyncio.Queue:
    queue: asyncio.Queue = asyncio.Queue(maxsize=5)
    async with _get_watchers_lock():
        _watchers.setdefault(user_id, []).append(queue)
    return queue


async def unregister_leaderboard_watcher(user_id: str, queue: asyncio.Queue):
    async with _get_watchers_lock():
        watchers = _watchers.get(user_id)
        if not watchers:
            return
        if queue in watchers:
            watchers.remove(queue)
        if not watchers:
            _watchers.pop(user_id, None)


async def _broadcast_local(user_id: str, payload: dict):
    async with _get_watchers_lock():
        watchers = list(_watchers.get(user_id, []))
    for queue in watchers:
        try:
            queue.put_nowait(payload)
        except asyncio.QueueFull:
            try:
                queue.get_nowait()
            except Exception:
                pass
            try:
                queue.put_nowait(payload)
            except Exception:
                pass


async def broadcast_leaderboard_update(
    user_id: str,
    *,
    settings: Any | None = None,
    limit: int = DEFAULT_LEADERBOARD_LIMIT,
):
    if not user_id:
        return

    try:
        from . import messaging as ch_msg
    except Exception:
        ch_msg = None  # type: ignore

    if settings is None:
        try:
            settings = await crud.get_settings(user_id)
        except Exception as exc:
            logger.debug(f"cyberherd leaderboard: failed to load settings for {user_id}: {exc}")
            return

    leaderboard = await get_leaderboard_entries(user_id, limit=limit)
    eff_pub = resolve_effective_pubkey(settings)
    payload = {
        "type": "leaderboard_update",
        "pubkey": eff_pub,
        "leaderboard": leaderboard,
    }

    if ch_msg:
        herd_wallet_id = getattr(settings, "herd_wallet", None)
        if herd_wallet_id:
            try:
                from lnbits.core.crud import get_wallet

                wallet = await get_wallet(herd_wallet_id)
                websocket_topic = wallet.inkey if wallet else None
                if websocket_topic:
                    await ch_msg.send_to_websocket_clients(
                        "cyberherd_leaderboard",
                        payload,
                        websocket_topic=websocket_topic,
                    )
            except Exception as exc:
                logger.debug(f"cyberherd leaderboard: websocket broadcast failed: {exc}")

    await _broadcast_local(user_id, payload)


__all__ = [
    "get_leaderboard_entries",
    "broadcast_leaderboard_update",
    "register_leaderboard_watcher",
    "unregister_leaderboard_watcher",
    "resolve_user_by_pubkey",
]
