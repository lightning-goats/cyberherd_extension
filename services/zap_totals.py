from __future__ import annotations

import asyncio
import json
import time
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple

from loguru import logger

from lnbits.utils.nostr import validate_pub_key

from .. import crud
from .nostr_helpers import query_events
from .pubkey import resolve_effective_pubkey
from lnbits.db import Filters
from lnbits.core.models.payments import PaymentFilters
from lnbits.core.services.payments import get_payments

try:  # Optional dependency used for bolt11 fallback parsing
    from bolt11 import decode as bolt11_decode  # type: ignore
except ImportError:  # pragma: no cover - optional dependency
    bolt11_decode = None  # type: ignore


CACHE_TTL_SECONDS = 60
MAX_BATCH_SIZE = 500
MAX_BATCH_LOOPS = 20

_lock_registry: Dict[Tuple[str, str], asyncio.Lock] = {}
_lock_registry_lock: asyncio.Lock | None = None
_LOCK_REGISTRY_MAX_SIZE = 5000


def _get_lock_registry_lock() -> asyncio.Lock:
    global _lock_registry_lock
    if _lock_registry_lock is None:
        _lock_registry_lock = asyncio.Lock()
    return _lock_registry_lock


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
    async with _get_lock_registry_lock():
        lock = _lock_registry.get(key)
        if lock is None:
            # Evict unlocked entries when registry is too large
            if len(_lock_registry) >= _LOCK_REGISTRY_MAX_SIZE:
                to_remove = [
                    k for k, v in _lock_registry.items() if not v.locked()
                ][:len(_lock_registry) // 4]
                for k in to_remove:
                    _lock_registry.pop(k, None)
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


def _coerce_extra_dict(extra: Any) -> dict | None:
    if not extra:
        return None
    if isinstance(extra, str):
        try:
            extra = json.loads(extra)
        except Exception:
            return None
    if isinstance(extra, dict):
        return extra
    return None


def _extract_zap_from_payment(payment: Any) -> dict | None:
    """Return zap metadata (zapper, amount, timestamp, ids) from LNbits payment row.

    The LNbits payment.extra["nostr"] field is expected to contain a NIP-57
    zap request (kind 9734). We extract:
    - zapper pubkey
    - target pubkey (p-tag)
    - target note id (e-tag) when present
    - amount in sats (from payment.amount msat)
    - created_at (from payment.time)
    - event_id (payment_hash/checking_id)
    """
    try:
        amount_msat = int(getattr(payment, "amount", 0) or 0)
    except Exception:
        amount_msat = 0
    if amount_msat <= 0:
        return None

    extra = _coerce_extra_dict(getattr(payment, "extra", None))
    if not extra:
        return None

    nostr_payload = extra.get("nostr")
    if not nostr_payload:
        return None
    if isinstance(nostr_payload, str):
        try:
            nostr_payload = json.loads(nostr_payload)
        except Exception:
            return None
    if not isinstance(nostr_payload, dict):
        return None

    try:
        kind = int(nostr_payload.get("kind"))
    except Exception:
        kind = None
    if kind != 9734:
        return None

    zapper = nostr_payload.get("pubkey")
    if not isinstance(zapper, str):
        return None
    try:
        zapper = _normalise_pubkey(zapper)
    except ZapTotalsError:
        return None

    tags = nostr_payload.get("tags") or []
    target_pubkey = None
    target_note_id: Optional[str] = None
    for tag in tags:
        if not (isinstance(tag, list) and len(tag) >= 2):
            continue
        marker = tag[0]
        value = tag[1]
        if marker == "p" and isinstance(value, str) and target_pubkey is None:
            candidate = value
            try:
                target_pubkey = _normalise_pubkey(candidate)
            except ZapTotalsError:
                target_pubkey = None
        elif marker == "e" and isinstance(value, str) and target_note_id is None:
            try:
                target_note_id = value.strip().lower()
            except Exception:
                target_note_id = None

    created_at = None
    try:
        ts = getattr(payment, "time", None)
        if isinstance(ts, datetime):
            created_at = int(ts.timestamp())
    except Exception:
        created_at = None

    event_id = getattr(payment, "payment_hash", None)
    if not isinstance(event_id, str):
        event_id = getattr(payment, "checking_id", None)
        if not isinstance(event_id, str):
            event_id = None

    return {
        "zapper": zapper,
        "target_pubkey": target_pubkey,
        "target_note_id": target_note_id,
        "amount_sats": amount_msat // 1000,
        "created_at": created_at,
        "event_id": event_id,
    }


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


async def record_incremental_zap_total(
    *,
    user_id: str,
    zapper_pubkey: str,
    target_pubkey: str,
    amount_sats: int,
    event_timestamp: Optional[int] = None,
    event_id: Optional[str] = None,
    skip_member_check: bool = False,
) -> None:
    """Incrementally update zap totals when a zap is processed via payments."""
    if not user_id or amount_sats <= 0:
        return

    zapper = _normalise_pubkey(zapper_pubkey)
    target = _normalise_pubkey(target_pubkey)
    event_ts = int(event_timestamp or time.time())

    if not skip_member_check:
        try:
            member = await crud.get_cyberherd_member_by_pubkey(zapper, user_id)
        except Exception:
            member = None
        if not member:
            logger.debug(
                f"Zap totals: skipping incremental update — zapper {zapper[:8]}... not in cyberherd for user {user_id}"
            )
            return

    key = (user_id, zapper)
    lock = await _get_key_lock(key)
    async with lock:
        row = await crud.get_zap_totals_row(user_id, zapper)
        existing_target = (row or {}).get("target_pubkey")
        if existing_target:
            existing_target = existing_target.lower()

        total_sats = int((row or {}).get("total_sats") or 0)
        event_count = int((row or {}).get("event_count") or 0)
        first_event_at = row.get("first_event_at") if row else None
        last_event_at = row.get("last_event_at") if row else None
        last_event_ids = list(_deserialize_last_event_ids((row or {}).get("last_event_ids")))

        if row and existing_target != target:
            total_sats = 0
            event_count = 0
            first_event_at = None
            last_event_at = None
            last_event_ids = []

        total_sats += amount_sats
        event_count += 1
        if first_event_at is None or event_ts < first_event_at:
            first_event_at = event_ts

        if last_event_at is None or event_ts > last_event_at:
            last_event_at = event_ts
            last_event_ids = [event_id] if event_id else []
        elif event_ts == last_event_at:
            if event_id and event_id not in last_event_ids:
                last_event_ids.append(event_id)

        await crud.upsert_zap_totals_row(
            user_id=user_id,
            zapper_pubkey=zapper,
            total_sats=total_sats,
            event_count=event_count,
            first_event_at=first_event_at,
            last_event_at=last_event_at,
            last_event_ids=last_event_ids,
            last_updated_at=time.time(),
            target_pubkey=target,
        )


async def rebuild_zap_totals_from_payments(
    *,
    user_id: str,
    zapper_pubkey: str | None = None,
    batch_size: int = 250,
    all_zaps: bool = False,
) -> dict:
    """Scan LNbits payments for the herd wallet and rebuild zap totals.

    Args:
        user_id: CyberHerd owner ID.
        zapper_pubkey: Optional specific zapper to restrict processing.
        batch_size: Pagination size when querying payments.
    """
    if batch_size <= 0:
        batch_size = 250

    settings = await crud.get_settings(user_id)
    herd_wallet = getattr(settings, "herd_wallet", None)
    if not herd_wallet:
        raise ZapTotalsError("herd wallet not configured")
    # tracked_event_ids contains all notes that matched the configured tags
    # (manual + automatic). By default we only count zaps that target these
    # tracked notes. When all_zaps=True, this filter is disabled and *all*
    # LNURLp zaps to the herd wallet are included.
    tracked_notes = set(getattr(settings, "tracked_event_ids", []) or [])

    watch_pubkey = resolve_effective_pubkey(settings)
    if not watch_pubkey:
        raise ZapTotalsError("Effective watch pubkey is not configured")
    watch_pubkey = _normalise_pubkey(watch_pubkey)

    zapper_filter = None
    if zapper_pubkey:
        zapper_filter = _normalise_pubkey(zapper_pubkey)

    members = await crud.get_all_cyberherd_members(user_id)
    member_pks = {
        str(m.get("pubkey")).strip().lower()
        for m in members
        if isinstance(m.get("pubkey"), str)
    }
    # If a specific zapper is requested but not yet in the herd, create (or
    # refresh) an inactive member with up-to-date metadata so historical
    # contributors are tracked and fully enriched.
    if zapper_filter and zapper_filter not in member_pks:
        try:
            await crud.ensure_inactive_cyberherd_member(
                zapper_filter,
                user_id=user_id,
                refresh_metadata=True,
            )
            member_pks.add(zapper_filter)
        except Exception as exc:
            logger.debug(
                f"zap_totals: failed to ensure inactive member for {zapper_filter[:8]}...: {exc}"
            )

    aggregates: dict[str, dict[str, Any]] = {}
    payments_scanned = 0
    zap_candidates = 0
    offset = 0

    # Track which zappers we've already ensured/metadata-refreshed in this run
    # to avoid repeated metadata lookups when all_zaps is True.
    touched_zappers: set[str] = set()

    while True:
        filters = Filters(
            model=PaymentFilters,
            sortby="time",
            direction="asc",
            limit=batch_size,
            offset=offset,
        )
        batch = await get_payments(
            wallet_id=herd_wallet,
            incoming=True,
            filters=filters,
        )
        if not batch:
            break
        offset += len(batch)
        for payment in batch:
            payments_scanned += 1
            zap = _extract_zap_from_payment(payment)
            if not zap:
                continue
            zapper = zap["zapper"]
            target_note_id = zap.get("target_note_id")

            # When tracked notes exist, restrict to zaps that target those notes,
            # unless all_zaps=True (full rebuild over all zaps).
            if tracked_notes and not all_zaps:
                if not isinstance(target_note_id, str) or target_note_id not in tracked_notes:
                    continue

            if zapper_filter and zapper != zapper_filter:
                continue

            # Ensure zapper is represented as a cyberherd member. In normal
            # mode we only auto-enrol missing members with minimal metadata.
            # When all_zaps=True we also refresh metadata for existing members
            # so their profiles are up to date.
            if all_zaps:
                if zapper not in touched_zappers:
                    try:
                        await crud.ensure_inactive_cyberherd_member(
                            zapper,
                            user_id=user_id,
                            refresh_metadata=True,
                        )
                        member_pks.add(zapper)
                    except Exception as exc:
                        logger.debug(
                            f"zap_totals: failed to ensure/refresh member for {zapper[:8]}...: {exc}"
                        )
                        # If we cannot ensure the member row, still allow
                        # zap_totals aggregation to proceed.
                    touched_zappers.add(zapper)
            else:
                if zapper not in member_pks:
                    try:
                        await crud.ensure_inactive_cyberherd_member(zapper, user_id=user_id)
                        member_pks.add(zapper)
                    except Exception as exc:
                        logger.debug(
                            f"zap_totals: failed to ensure inactive member for {zapper[:8]}...: {exc}"
                        )
                        # If we cannot create the member row, skip this zapper to
                        # keep behaviour consistent with previous member-only logic.
                        continue
            amount_sats = int(zap.get("amount_sats") or 0)
            if amount_sats <= 0:
                continue
            zap_candidates += 1
            aggregate = aggregates.setdefault(
                zapper,
                {
                    "total_sats": 0,
                    "event_count": 0,
                    "first_event_at": None,
                    "last_event_at": None,
                    "last_event_ids": [],
                },
            )
            aggregate["total_sats"] += amount_sats
            aggregate["event_count"] += 1
            created_at = zap.get("created_at")
            if created_at is not None:
                if aggregate["first_event_at"] is None or created_at < aggregate["first_event_at"]:
                    aggregate["first_event_at"] = created_at
                if aggregate["last_event_at"] is None or created_at > aggregate["last_event_at"]:
                    aggregate["last_event_at"] = created_at
                    aggregate["last_event_ids"] = [
                        zap.get("event_id")
                    ] if zap.get("event_id") else []
                elif created_at == aggregate["last_event_at"]:
                    event_id = zap.get("event_id")
                    if event_id and event_id not in aggregate["last_event_ids"]:
                        aggregate["last_event_ids"].append(event_id)

    if zapper_filter and zapper_filter not in aggregates:
        raise ZapTotalsError("no matching zaps found in payment history")

    updated_rows: list[dict[str, Any]] = []
    for zapper, data in aggregates.items():
        await crud.upsert_zap_totals_row(
            user_id=user_id,
            zapper_pubkey=zapper,
            total_sats=int(data["total_sats"]),
            event_count=int(data["event_count"]),
            first_event_at=data["first_event_at"],
            last_event_at=data["last_event_at"],
            last_event_ids=[eid for eid in data["last_event_ids"] if eid],
            last_updated_at=time.time(),
            target_pubkey=watch_pubkey,
        )
        updated_rows.append(
            {
                "zapper_pubkey": zapper,
                "total_sats": int(data["total_sats"]),
                "event_count": int(data["event_count"]),
            }
        )

    return {
        "payments_scanned": payments_scanned,
        "zap_candidates": zap_candidates,
        "zappers_updated": len(updated_rows),
        "updated": updated_rows,
        "filtered_zapper": zapper_filter,
    }


__all__ = [
    "get_zap_totals_for_zapper",
    "record_incremental_zap_total",
    "rebuild_zap_totals_from_payments",
    "ZapTotalsError",
]
