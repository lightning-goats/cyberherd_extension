from __future__ import annotations

import asyncio
import json
import time
from typing import Any, Dict, Iterable, List, Optional, Tuple

from loguru import logger

from lnbits.utils.nostr import validate_pub_key

from .. import crud
from .nostr_helpers import query_events
from .pubkey import resolve_effective_pubkey

try:  # Optional dependency used for bolt11 fallback parsing
    from bolt11 import decode as bolt11_decode  # type: ignore
except ImportError:  # pragma: no cover - optional dependency
    bolt11_decode = None  # type: ignore


CACHE_TTL_SECONDS = 60
MAX_BATCH_SIZE = 500
MAX_BATCH_LOOPS = 20

_lock_registry: Dict[Tuple[str, str], asyncio.Lock] = {}
_lock_registry_lock = asyncio.Lock()


class ZapTotalsError(Exception):
    """Raised when zap totals cannot be calculated."""


def _normalise_pubkey(value: str) -> str:
    if not isinstance(value, str):
        raise ZapTotalsError("pubkey must be a hex string")
    pk = value.strip().lower()
    if len(pk) != 64:
        raise ZapTotalsError("pubkey must be 64 hex characters")
    if not validate_pub_key(pk):
        raise ZapTotalsError("invalid nostr pubkey")
    return pk


async def _get_key_lock(key: Tuple[str, str]) -> asyncio.Lock:
    async with _lock_registry_lock:
        lock = _lock_registry.get(key)
        if lock is None:
            lock = asyncio.Lock()
            _lock_registry[key] = lock
        return lock


def _deserialize_last_event_ids(raw_ids: Any) -> Tuple[str, ...]:
    if not raw_ids:
        return ()
    try:
        parsed = json.loads(raw_ids)
        if isinstance(parsed, list):
            return tuple(str(item) for item in parsed if isinstance(item, str))
    except Exception:  # pragma: no cover - defensive parsing
        logger.debug("zap_totals: failed to parse last_event_ids", exc_info=True)
    return ()


def _extract_amount_msat(tags: Iterable[Any]) -> Optional[int]:
    """Extract amount (msat) from zap receipt tags."""
    try:
        for tag in tags:
            if isinstance(tag, list) and len(tag) >= 2 and tag[0] == "amount":
                raw = str(tag[1])
                digits = "".join(ch for ch in raw if ch.isdigit())
                if digits:
                    return int(digits)
    except Exception:  # pragma: no cover - defensive
        pass

    if bolt11_decode is None:
        return None

    try:
        for tag in tags:
            if isinstance(tag, list) and len(tag) >= 2 and tag[0] == "bolt11":
                invoice = str(tag[1])
                decoded = bolt11_decode(invoice)
                if decoded.amount_msat is not None:
                    return int(decoded.amount_msat)
    except Exception:
        logger.debug("zap_totals: failed to decode bolt11 tag", exc_info=True)
    return None


def _entry_to_result(
    *,
    user_id: str,
    zapper_pubkey: str,
    target_pubkey: str,
    total_sats: int,
    event_count: int,
    first_event_at: Optional[int],
    last_event_at: Optional[int],
    last_updated_at: float,
    cached: bool,
    updated: bool,
) -> dict:
    return {
        "user_id": user_id,
        "zapper_pubkey": zapper_pubkey,
        "target_pubkey": target_pubkey,
        "total_sats": total_sats,
        "event_count": event_count,
        "first_event_at": first_event_at,
        "last_event_at": last_event_at,
        "last_updated_at": last_updated_at,
        "cache_ttl_seconds": CACHE_TTL_SECONDS,
        "next_update_after": last_updated_at + CACHE_TTL_SECONDS,
        "cached": cached,
        "updated": updated,
    }


async def get_zap_totals_for_zapper(
    *,
    user_id: str,
    zapper_pubkey: str,
    force_refresh: bool = False,
) -> dict:
    """Return cumulative zap totals (in sats) sent by zapper_pubkey to user's watch pubkey."""
    if not user_id:
        raise ZapTotalsError("user_id required")

    zapper = _normalise_pubkey(zapper_pubkey)

    # Only track zaps for members (active or inactive)
    member = await crud.get_cyberherd_member_by_pubkey(zapper, user_id)
    if not member:
        raise ZapTotalsError("zapper is not a cyberherd member")

    settings = await crud.get_settings(user_id)
    if not settings:
        raise ZapTotalsError("Settings not found for user")

    watch_pubkey = resolve_effective_pubkey(settings)
    if not watch_pubkey:
        raise ZapTotalsError("Effective watch pubkey is not configured")
    watch_pubkey = _normalise_pubkey(watch_pubkey)

    key = (user_id, zapper)
    lock = await _get_key_lock(key)
    async with lock:
        row = await crud.get_zap_totals_row(user_id, zapper)
        existing_target = (row or {}).get("target_pubkey")
        if existing_target:
            existing_target = existing_target.lower()

        entry_total = int((row or {}).get("total_sats") or 0)
        entry_event_count = int((row or {}).get("event_count") or 0)
        entry_first_event = row.get("first_event_at") if row else None
        entry_last_event = row.get("last_event_at") if row else None
        entry_last_event_ids = _deserialize_last_event_ids((row or {}).get("last_event_ids"))
        entry_last_updated = float((row or {}).get("last_updated_at") or 0.0)

        now = time.time()
        if (
            row
            and existing_target == watch_pubkey
            and not force_refresh
            and now - entry_last_updated < CACHE_TTL_SECONDS
        ):
            return _entry_to_result(
                user_id=user_id,
                zapper_pubkey=zapper,
                target_pubkey=watch_pubkey,
                total_sats=entry_total,
                event_count=entry_event_count,
                first_event_at=entry_first_event,
                last_event_at=entry_last_event,
                last_updated_at=entry_last_updated,
                cached=True,
                updated=False,
            )

        if row and existing_target != watch_pubkey:
            # Watch pubkey changed: reset accumulators
            entry_total = 0
            entry_event_count = 0
            entry_first_event = None
            entry_last_event = None
            entry_last_event_ids = ()
            entry_last_updated = 0.0

        total_sats = entry_total
        event_count = entry_event_count
        first_event_at = entry_first_event
        last_event_at = entry_last_event
        last_event_ids_list: List[str] = list(entry_last_event_ids)
        prev_last_event_at = entry_last_event
        prev_last_event_ids = set(entry_last_event_ids)

        seen_ids: set[str] = set()
        updated = False

        cursor = last_event_at
        loops = 0

        while loops < MAX_BATCH_LOOPS:
            loops += 1
            filters: Dict[str, Any] = {
                "kinds": [9735],
                "authors": [zapper],
                "#p": [watch_pubkey],
            }
            if cursor is not None:
                filters["since"] = int(cursor)

            try:
                events = await query_events(filters, limit=MAX_BATCH_SIZE, timeout=8.0)
            except Exception as exc:  # pragma: no cover - defensive
                logger.warning(f"zap_totals: query_events failed: {exc}")
                break

            if not events:
                break

            clean_events: List[dict] = []
            for ev in events:
                ev_id = ev.get("id")
                if not isinstance(ev_id, str):
                    continue
                if ev_id in seen_ids:
                    continue
                seen_ids.add(ev_id)
                clean_events.append(ev)

            if not clean_events:
                break

            clean_events.sort(key=lambda ev: ev.get("created_at") or 0)

            batch_new = 0
            for ev in clean_events:
                created_at = ev.get("created_at")
                if created_at is None:
                    continue
                try:
                    created_at = int(created_at)
                except Exception:
                    continue

                ev_id = ev.get("id")
                if prev_last_event_at is not None:
                    if created_at < prev_last_event_at:
                        continue
                    if (
                        created_at == prev_last_event_at
                        and isinstance(ev_id, str)
                        and ev_id in prev_last_event_ids
                    ):
                        continue

                amount_msat = _extract_amount_msat(ev.get("tags", []))
                if amount_msat is None:
                    continue

                sats = amount_msat // 1000
                if sats <= 0:
                    continue

                total_sats += sats
                event_count += 1
                batch_new += 1

                if first_event_at is None or created_at < first_event_at:
                    first_event_at = created_at
                if last_event_at is None or created_at > last_event_at:
                    last_event_at = created_at
                    last_event_ids_list = [ev_id] if isinstance(ev_id, str) else []
                elif created_at == last_event_at and isinstance(ev_id, str):
                    if ev_id not in last_event_ids_list:
                        last_event_ids_list.append(ev_id)

            if batch_new == 0:
                break

            updated = True
            if last_event_at is None:
                break
            cursor = last_event_at
            prev_last_event_at = last_event_at
            prev_last_event_ids = set(last_event_ids_list)

            if len(clean_events) < MAX_BATCH_SIZE:
                break

        if not updated and row:
            # No new events but ensure last_event_ids_list reflects persisted state
            last_event_ids_list = list(entry_last_event_ids)
            last_event_at = entry_last_event
            first_event_at = entry_first_event
            total_sats = entry_total
            event_count = entry_event_count

        last_updated_at = time.time()

        await crud.upsert_zap_totals_row(
            user_id=user_id,
            zapper_pubkey=zapper,
            total_sats=total_sats,
            event_count=event_count,
            first_event_at=first_event_at,
            last_event_at=last_event_at,
            last_event_ids=last_event_ids_list,
            last_updated_at=last_updated_at,
            target_pubkey=watch_pubkey,
        )

        return _entry_to_result(
            user_id=user_id,
            zapper_pubkey=zapper,
            target_pubkey=watch_pubkey,
            total_sats=total_sats,
            event_count=event_count,
            first_event_at=first_event_at,
            last_event_at=last_event_at,
            last_updated_at=last_updated_at,
            cached=False,
            updated=updated,
        )


__all__ = ["get_zap_totals_for_zapper", "ZapTotalsError"]
