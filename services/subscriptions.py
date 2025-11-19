"""Cyberherd tagged note subscription management.

This module delegates all Nostr relay connectivity to the shared
`nostr_helpers` wrapper around the nostrclient extension, avoiding any
direct coupling to adapter shims.

All Nostr interactions (queries, subscriptions) are routed through
`nostr_helpers` for stable API access. Legacy direct nostrclient access
has been removed in favor of the helper abstraction.

Key changes:
- All event queries via nostr_helpers.query_events()
- Repost/reaction recovery uses #e-only filters (no #t requirement)
- Multi-#e handling for batch queries
- Diagnostics report helper failures

Legacy per-relay websocket code has been removed in favour of pooled
subscriptions managed entirely via this module. In-memory cache handling and
status remain here for reuse by callers.
"""

from __future__ import annotations

import asyncio
import json
import re
from datetime import datetime, timezone
from typing import Any, Optional
import os

from loguru import logger

from . import nostr_helpers
from .note_metadata import apply_event_address
from .time_utils import (
    get_day_boundaries_utc,
    DayBoundaries,
)

# Import package-level CRUD helpers (lnbits/extensions/cyberherd/crud.py)
from .. import crud
from ..utils.common import parse_bool_env, parse_int_env, parse_float_env, utc_now
from .zap_totals import record_incremental_zap_total, bolt11_decode  # type: ignore
from .pubkey import resolve_effective_pubkey
from .headbutt import trigger_headbutt_from_zap

# Diagnostics for nostr_helpers interactions
_helper_diagnostics = {
    'queries_attempted': 0,
    'queries_succeeded': 0,
    'queries_failed': 0,
    'availability_checks': 0,
    'unavailable_count': 0,
    'last_failure': None,
    'last_failure_reason': None,
}

def get_helper_diagnostics() -> dict:
    """Get diagnostics for nostr_helpers interactions.
    
    Includes UTC/local time context for clarity.
    """
    diag = dict(_helper_diagnostics)
    
    # Add current time boundaries for context (UTC-first)
    try:
        boundaries = _get_today_boundaries_utc()
        diag['time_context'] = {
            'utc_day': boundaries.utc_day_str,
            'local_day': boundaries.local_day_str,
            'utc_since_ts': boundaries.utc_since_ts,
            'utc_until_ts': boundaries.utc_until_ts,
            'local_since_ts': boundaries.local_since_ts,
            'local_until_ts': boundaries.local_until_ts,
            'note': 'Cache keys use utc_day; event filtering uses local_since/until_ts'
        }
    except Exception as e:
        diag['time_context'] = {'error': str(e)}
    
    return diag

def _record_helper_query(success: bool, error: str | None = None):
    """Record a nostr_helpers query attempt (UTC timestamps)."""
    _helper_diagnostics['queries_attempted'] += 1
    if success:
        _helper_diagnostics['queries_succeeded'] += 1
        # Rolling last-success timestamp for diagnostics
        _helper_diagnostics['last_success'] = utc_now().isoformat()
    else:
        _helper_diagnostics['queries_failed'] += 1
        # UTC timestamp for consistency across timezones
        _helper_diagnostics['last_failure'] = utc_now().isoformat()
        _helper_diagnostics['last_failure_reason'] = error

def _record_availability_check(available: bool):
    """Record a nostr_helpers availability check."""
    _helper_diagnostics['availability_checks'] += 1
    if not available:
        _helper_diagnostics['unavailable_count'] += 1

def _dbg(msg: str, *args):  # debug helper with proper formatting
    try:
        logger.opt(lazy=True).debug("Cyberherd: " + msg.format(*args))
    except Exception:
        pass


def _hex_to_npub(pubkey: str) -> str | None:
    """Safely convert hex pubkey to npub; returns None on any failure."""
    try:
        from lnbits.utils.nostr import hex_to_npub
        return hex_to_npub(pubkey)
    except Exception:
        return None


def _would_event_be_tracked(event: dict, eff_pub: str | None, tags: list[str]) -> bool:
    """Return True if the provided event would be considered tracked for the
    given effective pubkey and tracked tags. This mirrors the matching logic
    in _append_today but without cache writes.
    """
    try:
        if not eff_pub:
            return False
        if event.get('pubkey') != eff_pub:
            return False
        try:
            created_at = int(event.get('created_at') or 0)
        except Exception:
            created_at = 0
        if created_at <= 0:
            return False
        boundaries = _get_today_boundaries_utc()
        if not boundaries.is_timestamp_in_local_day(created_at):
            return False

        tags_norm = [t.lstrip('#').lower() for t in tags if t]
        # If no tags configured, accept any note authored by eff_pub today
        if not tags_norm:
            return True

        ev_tags = []
        for t in event.get('tags', []) or []:
            if isinstance(t, list) and len(t) > 1 and t[0] == 't' and isinstance(t[1], str):
                ev_tags.append(t[1].lstrip('#').lower())
        if any(t in ev_tags for t in tags_norm):
            return True

        # content fallback
        content = event.get('content', '') or ''
        found = re.findall(r"#([\w\-]+)", content, flags=re.UNICODE)
        found_norm = [h.lstrip('#').lower() for h in found if h]
        if any(t in found_norm for t in tags_norm):
            return True

        return False
    except Exception:
        return False


def get_effective_pubkey(settings) -> Optional[str]:
    """Resolve an effective pubkey for settings using resolver then fallback.

    Returns hex pubkey string or None.
    """
    try:
        from .pubkey import resolve_effective_pubkey

        eff = resolve_effective_pubkey(settings)
        if eff:
            return eff
    except Exception:
        pass

    try:
        return getattr(settings, 'effective_pubkey', None)
    except Exception:
        return None


# Module-level mirror of status for callers that don't have app context
_subscription_status: dict[str, Any] = {}
# Use a dict to hold the event so both modules can share the reference
_refresh_event_holder: dict[str, asyncio.Event | None] = {'event': None}
_refresh_event: asyncio.Event | None = None  # deprecated, kept for compatibility
# Module-level cache used when no app/state is provided (tests or callers without app)
_module_note_cache: dict = {}


def get_refresh_event() -> asyncio.Event | None:
    """Get the shared refresh event, creating it if needed.
    
    Returns the event from the holder dict which is shared across modules.
    """
    global _refresh_event_holder
    if _refresh_event_holder['event'] is None:
        try:
            _refresh_event_holder['event'] = asyncio.Event()
        except Exception:
            pass
    return _refresh_event_holder['event']


def set_refresh_event():
    """Set/trigger the shared refresh event."""
    evt = get_refresh_event()
    if evt is not None:
        try:
            evt.set()
        except Exception:
            pass


def clear_refresh_event():
    """Clear the shared refresh event."""
    evt = get_refresh_event()
    if evt is not None:
        try:
            evt.clear()
        except Exception:
            pass



def get_subscription_status(app=None) -> dict:
    """Return current subscription/polling status.

    If an app is provided, read from app.state; else return the last known
    module-level snapshot. Exists for backward compatibility.
    """
    if app is not None:
        try:
            st = getattr(app, "state", app)
            status = getattr(st, "cyberherd_subscription_status", {}) or {}
            return status
        except Exception:
            pass
    return _subscription_status or {}


def _get_today_boundaries_utc() -> DayBoundaries:
    """Get today's day boundaries using UTC-first approach.
    
    This replaces the old _local_midnight_timestamp() function with a clearer
    UTC-first approach that returns structured boundary information.
    
    RATIONALE FOR UTC-FIRST:
    - Nostr events have created_at in UTC epoch seconds
    - Database stores UTC timestamps
    - Cache keys should use UTC to avoid DST/timezone issues
    - Local time is only for user-facing "today" filtering
    
    Returns:
        DayBoundaries object with both UTC and local time information
        
    Migration notes:
        Old: _local_midnight_timestamp() returned single UTC timestamp for local day
        New: Returns full boundaries object with UTC primary, local secondary
    """
    return get_day_boundaries_utc(days_ago=0)


def _local_midnight_timestamp() -> int:
    """Compatibility shim returning the UTC epoch seconds for the start of the
    current LOCAL day (what the older _local_midnight_timestamp provided).

    Internally the module now uses UTC-first structured boundaries. This shim
    provides the legacy single-integer API expected by other modules.
    """
    try:
        boundaries = _get_today_boundaries_utc()
        return int(boundaries.local_since_ts)
    except Exception:
        # Fallback: use UTC midnight if anything goes wrong
        try:
            # Use shared time utility to obtain configured local timezone
            from .time_utils import get_day_boundaries_utc
            boundaries = get_day_boundaries_utc(days_ago=0)
            return int(boundaries.local_since_ts)
        except Exception:
            return 0


# ---------------- Cache helpers ----------------
def _get_cache(app) -> dict:
    # If no app or app.state is provided, fall back to a module-level cache
    try:
        st = getattr(app, "state", app)
    except Exception:
        st = None

    if st is None:
        return _module_note_cache

    cache = getattr(st, "cyberherd_note_cache", None)
    if cache is None:
        cache = {}
        try:
            st.cyberherd_note_cache = cache
        except Exception:
            # Fall back to module-level cache if assignment fails
            return _module_note_cache
    return cache


def _get_cache_note_ids(cache: dict, key, create: bool = False) -> list[str]:
    """Return the mutable note-id list for a cache entry, handling dict/list storage."""
    entry = cache.get(key)

    if isinstance(entry, dict):
        # views_api stores {"note_ids": [...], "ts": ...}; maintain that shape
        note_ids_maybe = entry.get("note_ids")
        if isinstance(note_ids_maybe, list):
            return note_ids_maybe
        note_ids: list[str] = []
        entry["note_ids"] = note_ids
        return note_ids

    if isinstance(entry, list):
        if create:
            # Upgrade legacy list entry to dict form to stay compatible with views_api
            note_ids = entry
            cache[key] = {"note_ids": note_ids}
            return note_ids
        return entry

    if entry is None:
        if create:
            note_ids: list[str] = []
            cache[key] = {"note_ids": note_ids}
            return note_ids
        return []

    if create:
        note_ids: list[str] = []
        cache[key] = {"note_ids": note_ids}
        return note_ids
    return []


def _event_matches_tracked_tags(event: dict, tags_norm: list[str]) -> bool:
    """Check if event matches any of the tracked tags.
    
    Checks both:
    1. Explicit 't' tags in event.tags
    2. Hashtags in event.content (fallback for clients that don't add 't' tags)
    
    Args:
        event: Nostr event dict
        tags_norm: List of normalized (lowercase, no #) tags to match against
    
    Returns:
        True if event contains any tracked tag, False otherwise
        True if tags_norm is empty (no specific tags configured - accept all events)
    """
    # If no tags are configured, accept all events from the author
    if not tags_norm:
        return True
    
    # Check explicit 't' tags
    ev_tags = []
    for t in event.get("tags", []) or []:
        if isinstance(t, list) and len(t) > 1 and t[0] == "t" and isinstance(t[1], str):
            ev_tags.append(t[1].lstrip('#').lower())
    
    if any(t in ev_tags for t in tags_norm):
        return True
    
    # Content-hashtag fallback: check for #hashtags in content
    try:
        content = event.get('content', '') or ''
        found = re.findall(r"#([\w\-]+)", content, flags=re.UNICODE)
        found_norm = [h.lstrip('#').lower() for h in found if h]
        if any(t in found_norm for t in tags_norm):
            return True
    except Exception:
        pass
    
    return False


async def _append_today(cache: dict, user_id: str | None, eff_pub: str | None, tags: list[str], event: dict, app=None) -> bool:
    """Append event id into today's cache entries (user-specific + neutral) if matches:
    - Author equals eff_pub
    - Contains any tracked tag (t-tag equals or '#tag' in content)
    - created_at is within current LOCAL day (user's "today")

    Returns True if inserted (or already present), False if not matched.
    
    TIME HANDLING (UTC-FIRST):
    - Gets boundaries using get_day_boundaries_utc() for consistency
    - Checks event.created_at against LOCAL day boundaries (user's "today" concept)
    - Cache key uses UTC date for storage consistency
    """
    eid = event.get("id")
    if not eid:
        _dbg("_append_today: event has no id")
        return False
    
    tags_norm = [t.lstrip('#').lower() for t in tags if t]
    
    # Author check
    event_pubkey = event.get("pubkey")
    if not eff_pub:
        logger.warning(
            f"📝 Cannot validate event {eid[:16] if eid else 'unknown'}... - "
            f"no effective pubkey configured for user {user_id}. "
            f"Check nostr_pubkey_override or nostr_private_key settings."
        )
        return False
    if event_pubkey != eff_pub:
        logger.debug(
            f"📝 Skipping event {eid[:16] if eid else 'unknown'}... from pubkey {event_pubkey[:16] if event_pubkey else 'None'}... "
            f"(not matching effective pubkey {eff_pub[:16] if eff_pub else 'None'}...) "
            f"user_id={user_id}"
        )
        return False
    
    # UTC-FIRST: Get day boundaries (primary UTC, secondary local)
    boundaries = _get_today_boundaries_utc()
    
    # Same-day check: Use LOCAL day boundaries for user's "today" concept
    # This ensures users see notes from their local calendar day, not UTC day
    try:
        created_at = int(event.get("created_at") or 0)
    except Exception:
        created_at = 0
    if created_at <= 0:
        return False
    
    # Check if event is from user's local "today"
    if not boundaries.is_timestamp_in_local_day(created_at):
        from datetime import datetime
        logger.warning(
            f"📅 Skipping event {eid[:16]}... from {datetime.fromtimestamp(created_at).isoformat()} "
            f"(outside today's local window: {datetime.fromtimestamp(boundaries.local_since_ts).isoformat()} "
            f"to {datetime.fromtimestamp(boundaries.local_until_ts).isoformat()}) user_id={user_id}"
        )
        return False
    
    # Use shared tag matching logic unless tags are empty (in which case accept all notes
    # from the effective pubkey for today's window)
    if tags_norm and not _event_matches_tracked_tags(event, tags_norm):
        # Extract event's tags for logging
        event_t_tags = [t[1] for t in event.get('tags', []) if isinstance(t, list) and len(t) >= 2 and t[0] == 't']
        content_preview = (event.get('content', '') or '')[:100]
        logger.debug(
            f"🏷️ Skipping event {eid[:16]}... with t-tags {event_t_tags} content_preview='{content_preview}' "
            f"(not matching tracked tags {tags_norm}) user_id={user_id}"
        )
        return False

    # Auto-add detected note event IDs to tracked_event_ids for repost/reaction tracking
    if app is not None and user_id is not None:
        try:
            from .. import crud

            settings = await crud.get_settings(user_id)
            if settings:
                current_tracked = getattr(settings, 'tracked_event_ids', []) or []
                updated_tracked = current_tracked if eid in current_tracked else current_tracked + [eid]
                is_long_form = False
                try:
                    is_long_form = int(event.get("kind") or 0) == 30311
                except Exception:
                    is_long_form = event.get("kind") == 30311

                pruned_addresses = None
                addresses_changed = False
                if is_long_form:
                    addresses = dict(getattr(settings, 'tracked_event_addresses', {}) or {})
                    apply_event_address(addresses, event)
                    pruned_addresses = {
                        nid: addr for nid, addr in addresses.items() if nid in updated_tracked
                    }
                    addresses_changed = pruned_addresses != (
                        getattr(settings, 'tracked_event_addresses', {}) or {}
                    )

                if eid not in current_tracked or addresses_changed:
                    settings.tracked_event_ids = updated_tracked
                    if pruned_addresses is not None:
                        settings.tracked_event_addresses = pruned_addresses
                    await crud.upsert_settings(settings, user_id)
                    _dbg(f"Auto-added/updated event {eid} metadata for user {user_id}")
                    if eid not in current_tracked:
                        try:
                            logger.info(
                                f"🎯 Auto-added tracked event id={eid} for user={user_id} (note author={eff_pub}). "
                                f"Total tracked events: {len(updated_tracked)}. Triggering immediate subscription refresh..."
                            )
                        except Exception:
                            pass
                        
                        # Trigger immediate WebSocket subscription refresh for this user
                        try:
                            from .nostr_websocket_monitor import trigger_immediate_refresh
                            await trigger_immediate_refresh(user_id)
                            logger.info(
                                f"✅ WebSocket subscriptions refreshed immediately for user={user_id} "
                                f"after adding tracked event {eid}. Kind 6/7 subscriptions updated."
                            )
                        except Exception as e:
                            logger.warning(
                                f"Failed to trigger immediate WebSocket refresh for user {user_id}: {e}. "
                                f"Will be picked up by periodic refresh loop within 60 seconds."
                            )
        except Exception as e:
            logger.warning(f"Failed to auto-add event {eid} to tracked_event_ids: {e}")

    # UTC-FIRST: Cache key uses UTC date for storage consistency
    # This ensures cache keys don't change during DST transitions
    # and are consistent across different user timezones
    day = boundaries.utc_day_str  # Use UTC date string (e.g., "2025-10-04")
    tagset = tuple(sorted(tags_norm))
    
    # Cache keys format: (utc_date, user_id, eff_pub, tagset)
    # The UTC date ensures storage consistency while the local day check
    # (above) ensures users see their "today" notes correctly
    keys = [
        (day, user_id, eff_pub, tagset),
        (day, None, eff_pub, tagset),  # neutral key for unauthenticated lookups
    ]
    for k in keys:
        note_ids = _get_cache_note_ids(cache, k, create=True)
        if eid not in note_ids:
            note_ids.append(eid)
    return True


async def force_requery_for_user(app, user_id: str) -> list[str]:
    """Force recovery of missed events from today for a specific user.
    
    This function:
    1. Queries notes (kind 1/30311) from midnight today matching user's tracked tags
    2. Processes those notes to populate tracked_event_ids
    3. Queries engagement events (kind 6/7) for tracked notes
    4. Processes those engagement events
    
    Args:
        app: FastAPI app instance (can be None)
        user_id: User ID to recover events for
        
    Returns:
        List of event IDs that were processed
    """
    logger.info(f"🔄 Starting forced recovery for user {user_id}")
    processed_ids = []
    
    try:
        # Get user settings
        settings = await crud.get_settings(user_id)
        if not settings:
            logger.warning(f"No settings found for user {user_id}, cannot recover")
            return []
        
        # Check if nostrclient is available
        if not nostr_helpers.check_availability():
            logger.warning("nostrclient not available, cannot recover events")
            _record_availability_check(False)
            return []
        
        _record_availability_check(True)
        
        # Get time boundaries for today (UTC-first approach)
        boundaries = _get_today_boundaries_utc()
        since_ts = int(boundaries.local_since_ts)  # Midnight today local time
        
        # Get tracked tags and effective pubkey
        tracked_tags = getattr(settings, 'tracked_tags', []) or []
        effective_pubkey = get_effective_pubkey(settings)
        
        if not tracked_tags:
            logger.info(f"No tracked tags for user {user_id}, skipping note recovery")
        elif not effective_pubkey:
            logger.warning(f"No effective pubkey for user {user_id}, cannot recover notes")
        else:
            # Normalize tags for Nostr query (remove # prefix)
            # Nostr "#t" filter expects tag values without the # prefix
            # Note: Tag matching is case-sensitive in Nostr, so preserve case
            query_tags = [t.lstrip('#') for t in tracked_tags if t]
            
            # Query notes (kind 1 and 30311) matching tracked tags from midnight
            # IMPORTANT: Must filter by author (effective_pubkey) to only get user's own notes
            logger.debug(
                f"Querying notes since {since_ts} for author {effective_pubkey[:16]}... "
                f"with tags: {tracked_tags} (normalized to {query_tags})"
            )
            
            filters = {
                "kinds": [1, 30311],
                "authors": [effective_pubkey],  # Only notes from this user
                "#t": query_tags,
                "since": since_ts,
                "limit": 500
            }
            
            try:
                notes = await nostr_helpers.query_events(filters, limit=500, timeout=10.0)
                _record_helper_query(True)
                
                logger.info(f"Found {len(notes)} notes for user {user_id} recovery")
                
                # Process each note
                for note in notes:
                    try:
                        await process_note_for_tracked_tags(
                            user_id=user_id,
                            event=note,
                            app=app
                        )
                        note_id = note.get("id")
                        if note_id:
                            processed_ids.append(note_id)
                    except Exception as e:
                        logger.warning(f"Failed to process note {note.get('id')} during recovery: {e}")
                        
            except Exception as e:
                logger.error(f"Failed to query notes for recovery: {e}")
                _record_helper_query(False, str(e))
        
        # Query engagement events (reposts and reactions) for tracked notes
        tracked_event_ids = getattr(settings, 'tracked_event_ids', []) or []
        if not tracked_event_ids:
            logger.info(f"No tracked events for user {user_id}, skipping engagement recovery")
        else:
            logger.debug(f"Querying engagement for {len(tracked_event_ids)} tracked notes")
            
            # Build filters for reposts and reactions
            filters = {
                "kinds": [6, 7],  # reposts and reactions
                "#e": tracked_event_ids,
                "since": since_ts,
                "limit": 1000
            }
            
            try:
                engagement_events = await nostr_helpers.query_events(filters, limit=1000, timeout=10.0)
                _record_helper_query(True)
                
                logger.info(f"Found {len(engagement_events)} engagement events for user {user_id} recovery")
                
                # Process each engagement event
                for event in engagement_events:
                    try:
                        kind = event.get("kind")
                        if kind == 6:
                            await process_repost_for_tracked_notes(
                                user_id=user_id,
                                event=event,
                                app=app
                            )
                        elif kind == 7:
                            await process_reaction_for_tracked_notes(
                                user_id=user_id,
                                event=event,
                                app=app
                            )
                        
                        event_id = event.get("id")
                        if event_id:
                            processed_ids.append(event_id)
                    except Exception as e:
                        logger.warning(f"Failed to process engagement event {event.get('id')} during recovery: {e}")
                        
            except Exception as e:
                logger.error(f"Failed to query engagement events for recovery: {e}")
                _record_helper_query(False, str(e))
        
        # Query zap receipts (kind 9735) for effective pubkey
        # Note: Zaps are addressed to the pubkey, not necessarily the note (though they tag the note)
        # We query all zaps to the user's pubkey since midnight and let process_zap_receipt_for_tracked_notes filter them
        if effective_pubkey:
            logger.debug(f"Querying zap receipts for pubkey {effective_pubkey[:8]}...")
            
            filters = {
                "kinds": [9735],
                "#p": [effective_pubkey],
                "since": since_ts,
                "limit": 1000
            }
            
            try:
                zap_events = await nostr_helpers.query_events(filters, limit=1000, timeout=10.0)
                _record_helper_query(True)
                
                logger.info(f"Found {len(zap_events)} zap receipts for user {user_id} recovery")
                
                # Process each zap receipt
                for event in zap_events:
                    try:
                        await process_zap_receipt_for_tracked_notes(
                            user_id=user_id,
                            event=event,
                            app=app
                        )
                        event_id = event.get("id")
                        if event_id:
                            processed_ids.append(event_id)
                    except Exception as e:
                        logger.warning(f"Failed to process zap receipt {event.get('id')} during recovery: {e}")
                        
            except Exception as e:
                logger.error(f"Failed to query zap receipts for recovery: {e}")
                _record_helper_query(False, str(e))
        
        # Payment-based recovery as fallback for missed zaps
        # This catches zaps that have payment.extra["nostr"] data but weren't
        # captured via kind 9735 WebSocket subscriptions
        try:
            from ..services.payment_coordinator import get_payment_coordinator
            monitor = get_payment_coordinator(app=app, db=crud, user_id=user_id)
            logger.info(f"Triggering payment-based zap recovery for user {user_id}")
            payment_result = await monitor._recover_missed_payment_zaps(settings)
            if payment_result:
                scanned = payment_result.get("scanned", 0)
                processed = payment_result.get("processed", 0)
                logger.info(f"Payment recovery: scanned {scanned}, processed {processed} payments")
        except Exception as e:
            logger.warning(f"Payment-based recovery failed for user {user_id}: {e}")
        
        logger.info(f"✅ Recovery completed for user {user_id}: {len(processed_ids)} events processed")
        return processed_ids
        
    except Exception as e:
        logger.error(f"Recovery failed for user {user_id}: {e}")
        return processed_ids


async def _trigger_recovery_for_all_users(app):
    """Trigger zap recovery for all users with tracking enabled after tracked_event_ids are detected.
    
    This is called automatically after startup when tracked_event_ids are first populated.
    It runs recovery in the background to avoid blocking startup.
    """
    try:
        from .. import crud
        from ..utils.common import get_lnbits_users_function, is_extension_enabled_for_user
        
        # Get all users
        get_users = get_lnbits_users_function()

        if get_users and asyncio.iscoroutinefunction(get_users):
            users = await get_users()
        else:
            users = []
        
        recovery_count = 0
        for user in users:
            try:
                # Check if user has cyberherd extension enabled
                if not await is_extension_enabled_for_user(user.id):
                    continue
                
                settings = await crud.get_settings(user.id)
                if not settings:
                    continue
                
                # Only recover for users with zap tracking enabled
                if not getattr(settings, 'zap_tracking_enabled', False):
                    continue
                
                # Check if they have tracked_event_ids (recovery requires this)
                tracked_notes = getattr(settings, 'tracked_event_ids', []) or []
                if not tracked_notes:
                    continue
                
                # Get zap monitor and trigger recovery
                try:
                    from ..views_api import get_zap_monitor
                    zap_monitor = get_zap_monitor(app=app, db=crud, user_id=user.id)
                    
                    if zap_monitor and hasattr(zap_monitor, "_recover_missed_payment_zaps"):
                        # Run recovery in background (don't await to avoid blocking startup)
                        asyncio.create_task(
                            zap_monitor._recover_missed_payment_zaps(settings)
                        )
                        recovery_count += 1
                        logger.info(f"Triggered automatic zap recovery for user {user.id}")
                except Exception as e:
                    logger.warning(f"Failed to trigger zap recovery for user {user.id}: {e}")
                    
            except Exception as e:
                logger.warning(f"Error processing recovery for user {user.id}: {e}")
        
        if recovery_count > 0:
            logger.info(f"Automatic zap recovery: {recovery_count} users triggered")
        else:
            logger.info("No users required automatic zap recovery")
            
    except Exception as e:
        logger.error(f"Error in _trigger_recovery_for_all_users: {e}")


async def _initialize_tracked_event_ids_on_startup(app):
    """DEPRECATED: Replaced by force_requery_for_user in startup flow.
    
    This function is no longer needed because force_requery_for_user does everything
    this function did PLUS more, eliminating duplicate queries during startup.
    
    Original purpose: Initialize tracked_event_ids with recent notes on startup.
    New approach: force_requery_for_user queries notes and auto-populates tracked_event_ids
    via _append_today, while also handling engagement event recovery in one pass.
    """
    logger.warning(
        "DEPRECATED: _initialize_tracked_event_ids_on_startup called. "
        "This is now handled by force_requery_for_user in the startup flow."
    )
    # Function body removed - use force_requery_for_user instead


async def _any_tracked_event_ids_exist(app) -> bool:
    """Return True if any user with tracking enabled already has tracked_event_ids.

    This is used to decide whether it's safe to start realtime engagement
    subscriptions that depend on tracked_event_ids.
    """
    try:
        from .. import crud
        from ..utils.common import get_lnbits_users_function, is_extension_enabled_for_user
        
        get_users = get_lnbits_users_function()

        users = await get_users() if get_users and asyncio.iscoroutinefunction(get_users) else []
        for user in users:
            try:
                # Check if user has cyberherd extension enabled
                if not await is_extension_enabled_for_user(user.id):
                    continue
                
                settings = await crud.get_settings(user.id)
                if not settings:
                    continue
                if not (getattr(settings, 'repost_tracking_enabled', False) or getattr(settings, 'likes_tracking_enabled', False)):
                    continue
                tracked = getattr(settings, 'tracked_event_ids', []) or []
                if tracked:
                    return True
            except Exception:
                continue
    except Exception:
        return False
    return False


async def _wait_for_tracked_event_ids(app, timeout_seconds: float = 30.0) -> bool:
    """DEPRECATED: No longer needed with optimized startup flow.
    
    This function is no longer needed because force_requery_for_user runs first
    during startup and immediately populates tracked_event_ids, eliminating the
    need to wait/poll for them to be ready.
    
    Original purpose: Wait for any user to have tracked_event_ids populated.
    New approach: force_requery_for_user populates them synchronously during startup.
    """
    logger.warning(
        "DEPRECATED: _wait_for_tracked_event_ids called. "
        "No longer needed - force_requery_for_user populates tracked_event_ids synchronously."
    )
    return True  # Return True to avoid breaking existing logic


async def cyberherd_subscription_handler():
    """Main subscription handler that runs as a permanent background task.
    
    This is a minimal handler that just keeps the task alive.
    The actual monitoring is handled by:
    - NostrWebSocketMonitor (via handle_websocket_monitors in __init__.py)
    - ZapMonitorService (payment listener started in __init__.py)
    
    This is called by create_permanent_unique_task() in __init__.py during extension startup.
    """
    import asyncio
    
    logger.info("🚀 cyberherd_subscription_handler: Starting (monitoring via NostrWebSocketMonitor)...")
    
    try:
        # Wait for nostrclient relays to be ready before proceeding
        from .relay_readiness import wait_for_relays_ready
        relay_status = await wait_for_relays_ready(max_wait_seconds=30.0, check_interval=2.0, min_connected_relays=1)
        if not relay_status['ready']:
            logger.warning(
                f"⚠️ Proceeding even though nostrclient relays not confirmed ready after {relay_status['waited_seconds']}s. "
                f"Status: {relay_status['relay_count']} relay(s) configured, {relay_status['connected_count']} connected. "
                f"Reason: {relay_status['reason']}"
            )
        
        logger.info("✅ Cyberherd subscription handler initialized (NostrWebSocketMonitor handles subscriptions)")
        
        # Keep the task alive forever (like nwcprovider's handle_nwc)
        try:
            while True:
                await asyncio.sleep(3600)  # Sleep for 1 hour, wake up periodically to check if cancelled
        except asyncio.CancelledError:
            logger.info("Cyberherd subscription handler cancelled")
            raise
            
    except asyncio.CancelledError:
        logger.info("Cyberherd subscription handler cancelled during startup")
        raise
    except Exception as e:
        logger.error(f"❌ Cyberherd subscription handler failed: {e}", exc_info=True)
        raise


def start_subscriptions(app):
    """DEPRECATED: Legacy function kept for backward compatibility.
    
    The new approach uses cyberherd_subscription_handler() called by
    create_permanent_unique_task() in __init__.py, following the nwcprovider pattern.
    
    This function is no longer used but kept to avoid breaking imports.
    """
    logger.warning(
        "start_subscriptions() called but is deprecated. "
        "Subscriptions are now started via cyberherd_subscription_handler() "
        "using create_permanent_unique_task() in __init__.py"
    )
    return None


async def poll_now(*args, **kwargs):  # legacy public API kept as no-op
    """Deprecated: previously triggered an immediate poll.

    Now returns current subscription status only; retained for backward
    compatibility with admin tooling.
    """
    try:
        app = args[0] if args else None
        if app is not None:
            st = getattr(app, "state", app)
            status = getattr(st, "cyberherd_subscription_status", {}) or {}
            return {"status": status, "note": "poll deprecated"}
    except Exception:
        pass

 


async def process_event_for_user(user_id: str, event: dict, settings, app, recovery_mode: bool = False):
    try:
        eid = event.get("id")
        pubkey = event.get("pubkey")
        try:
            kind = int(event.get("kind") or 0)
        except Exception:
            kind = 0
        if not eid or not pubkey:
            return

        eff_pub = get_effective_pubkey(settings)
        tags = getattr(settings, "tracked_tags", [])

        # kind 1: notes, kind 30311: long-form content
        # For notes, we WANT events from the effective pubkey (user's own notes)
        if kind in (1, 30311):
            logger.info(
                f"📝 Processing kind {kind} note event {eid[:16]}... from pubkey {pubkey[:16]}... "
                f"(user's effective_pubkey: {eff_pub[:16] if eff_pub else 'None'}...) "
                f"tracked_tags: {tags} user_id={user_id}"
            )
            cache = _get_cache(app)
            result = await _append_today(cache, user_id, eff_pub, tags, event, app)
            if result:
                logger.info(f"✅ New tracked note detected: {eid[:16]}... for user {user_id}")
            else:
                logger.debug(
                    f"⚠️ Event {eid[:16]}... was not added to tracked_event_ids (filtered out) user_id={user_id}"
                )

        # kind 6: reposts
        elif kind == 6 and getattr(settings, "repost_tracking_enabled", False):
            # Reject reposts from the effective pubkey (operator's own actions)
            # This prevents self-reposts from counting
            if eff_pub and pubkey == eff_pub:
                logger.debug(
                    f"Skipping repost event {eid[:16]}... from effective pubkey "
                    f"(operator's own repost) for user {user_id}"
                )
                return
            
            logger.info(f"🔄 Processing kind 6 repost event {eid[:16] if eid else 'unknown'}... for user {user_id} from pubkey {pubkey[:16]}...")
            
            # No timestamp check for kind 6 - we only care if it references a tracked event ID.
            # The tracked event IDs themselves are already filtered to today's window when detected.
            
            cache = _get_cache(app)
            
            # Get current tracked event IDs for debugging
            tracked_ids = getattr(settings, 'tracked_event_ids', []) or []
            logger.info(f"📌 Current tracked_event_ids for user {user_id} ({len(tracked_ids)}): {[t[:16]+'...' for t in tracked_ids[:5]]}")
            
            # Collect all identifiers from the repost event
            identifiers = _collect_reference_identifiers(event, include_content=True)
            logger.info(f"📋 Found {len(identifiers)} reference identifiers in repost event: {[i[:16]+'...' for i in identifiers]}")
            
            # Find ALL tracked notes (not just the first one)
            tracked_targets = []
            for identifier in identifiers:
                logger.debug(f"🔎 Checking identifier: {identifier[:16]}...")
                resolved_id, _metadata = await _resolve_tracked_event(settings, identifier, cache, app)
                if not resolved_id:
                    logger.debug(f"❌ Identifier {identifier[:16]}... did not resolve")
                    continue
                logger.debug(f"✅ Resolved to: {resolved_id[:16]}...")
                
                is_tracked = await _is_tracked_event(user_id, resolved_id, settings, app, cache)
                logger.info(f"🎯 Is {resolved_id[:16]}... tracked? {is_tracked}")
                
                if is_tracked:
                    tracked_targets.append(resolved_id)
                    logger.info(f"✅ Found tracked event: {resolved_id[:16]}... for repost!")

            if tracked_targets:
                logger.info(f"🎬 Triggering headbutt for {len(tracked_targets)} tracked note(s) in repost event {eid[:16]}...")
                
                # Process each tracked note referenced in the repost
                for target_id in tracked_targets:
                    logger.info(f"🎬 Processing repost for tracked note {target_id[:16]}...")
                    
                    # Persistent dedupe: check if this specific (event, note) pair was already processed
                    # Note: We check per-note because the same repost event can reference multiple tracked notes,
                    # and we want to process each one independently.
                    already_processed = False
                    try:
                        from .. import crud
                        try:
                            # Check if we have a record for this specific (event_id, note_id) pair
                            # The database stores tuples of (user_id, event_hash, note_id, ...)
                            row = await crud.db.fetchone(
                                f"SELECT 1 FROM {crud.db.references_schema}processed_events "
                                f"WHERE user_id = :user_id AND event_hash = :event_hash AND note_id = :note_id",
                                {"user_id": user_id, "event_hash": eid, "note_id": target_id},
                            )
                            already_processed = row is not None
                            if already_processed:
                                logger.debug(
                                    f"Skipping repost event {eid[:16]}... for note {target_id[:16]}... "
                                    f"because this (event, note) pair was already processed"
                                )
                                continue  # Skip this target, try next one
                        except Exception:
                            # If check fails, continue to attempt processing
                            pass
                    except Exception:
                        # Import failed; this is non-fatal — continue processing
                        pass

                    result = await _trigger_repost_headbutt(user_id, pubkey, target_id, eid, app, recovery_mode=recovery_mode)
                    logger.info(f"🎬 Repost headbutt result for {target_id[:16]}...: {result is not None}")
                    
                    # Persist processed status when subscription-driven processing succeeds
                    try:
                        if result and eid:
                            from .. import crud
                            try:
                                res = await crud.register_processed_event(
                                    user_id,
                                    eid,
                                    note_id=target_id,
                                    pubkey=pubkey,
                                    event_type="repost",
                                )
                                try:
                                    st = getattr(app, 'state', app)
                                    metrics = getattr(st, 'cyberherd_metrics', None)
                                    if metrics is None:
                                        metrics = {}
                                        try:
                                            setattr(st, 'cyberherd_metrics', metrics)
                                        except Exception:
                                            pass
                                    if res:
                                        metrics['repost_persist_success'] = metrics.get('repost_persist_success', 0) + 1
                                    else:
                                        metrics['repost_persist_failure'] = metrics.get('repost_persist_failure', 0) + 1
                                except Exception:
                                    pass
                                if res:
                                    logger.debug("Persisted processed repost event %s for user %s", eid, user_id)
                                else:
                                    logger.warning(f"Failed to persist processed repost event {eid} for user {user_id}")
                            except Exception:
                                # Non-fatal: processing succeeded but persistence failed
                                logger.warning(f"Failed to persist processed repost event {eid} for user {user_id}")
                    except Exception:
                        pass
            else:
                logger.warning(f"⚠️ Repost event {eid[:16]}... does NOT reference any tracked event for user {user_id}")

        # kind 7: reactions
        elif kind == 7 and getattr(settings, "likes_tracking_enabled", False):
            # Reject reactions from the effective pubkey (operator's own actions)
            # This prevents self-reactions from counting
            if eff_pub and pubkey == eff_pub:
                logger.debug(
                    f"Skipping reaction event {eid[:16]}... from effective pubkey "
                    f"(operator's own reaction) for user {user_id}"
                )
                return
            
            logger.info(f"🔍 Processing kind 7 reaction event {eid[:16] if eid else 'unknown'}... for user {user_id}")
            
            # No timestamp check for kind 7 - we only care if it references a tracked event ID.
            # The tracked event IDs themselves are already filtered to today's window when detected.
            
            cache = _get_cache(app)
            identifiers = _collect_reference_identifiers(event, include_content=False)
            logger.info(f"📋 Found {len(identifiers)} reference identifiers in kind 7 event: {identifiers}")
            
            tracked_ids = getattr(settings, 'tracked_event_ids', []) or []
            logger.info(f"📌 Current tracked_event_ids ({len(tracked_ids)}): {tracked_ids}")
            
            # Find ALL tracked notes (not just the first one)
            tracked_targets = []
            for identifier in identifiers:
                logger.debug(f"🔎 Checking identifier: {identifier[:16]}...")
                resolved_id, _metadata = await _resolve_tracked_event(settings, identifier, cache, app)
                if not resolved_id:
                    logger.debug(f"❌ Identifier {identifier[:16]}... did not resolve")
                    continue
                logger.debug(f"✅ Resolved to: {resolved_id[:16]}...")
                
                is_tracked = await _is_tracked_event(user_id, resolved_id, settings, app, cache)
                logger.info(f"🎯 Is {resolved_id[:16]}... tracked? {is_tracked}")
                
                if is_tracked:
                    tracked_targets.append(resolved_id)

            if tracked_targets:
                logger.info(f"✅ Kind 7 reaction matched {len(tracked_targets)} tracked event(s)! Processing headbutt(s)...")
                
                # Process each tracked note referenced in the reaction
                for reacted_id in tracked_targets:
                    logger.info(f"🎬 Processing reaction for tracked note {reacted_id[:16]}...")
                    
                    # Persistent dedupe: check if this specific (event, note) pair was already processed
                    # Note: We check per-note because the same reaction event can reference multiple tracked notes,
                    # and we want to process each one independently.
                    already_processed = False
                    try:
                        from .. import crud
                        try:
                            # Check if we have a record for this specific (event_id, note_id) pair
                            row = await crud.db.fetchone(
                                f"SELECT 1 FROM {crud.db.references_schema}processed_events "
                                f"WHERE user_id = :user_id AND event_hash = :event_hash AND note_id = :note_id",
                                {"user_id": user_id, "event_hash": eid, "note_id": reacted_id},
                            )
                            already_processed = row is not None
                            if already_processed:
                                logger.info(
                                    f"⏭️ Skipping reaction event {eid[:16]}... for note {reacted_id[:16]}... "
                                    f"because this (event, note) pair was already processed"
                                )
                                continue  # Skip this target, try next one
                        except Exception:
                            pass
                    except Exception:
                        pass

                    result = await _trigger_reaction_headbutt(user_id, pubkey, reacted_id, eid, app, recovery_mode=recovery_mode)
                    logger.info(f"🎬 Reaction headbutt result for {reacted_id[:16]}...: {result is not None}")
                    
                    # Persist processed status when subscription-driven processing succeeds
                    try:
                        if result and eid:
                            from .. import crud
                            try:
                                res = await crud.register_processed_event(
                                    user_id,
                                    eid,
                                    note_id=reacted_id,
                                    pubkey=pubkey,
                                    event_type="reaction",
                                )
                                try:
                                    st = getattr(app, 'state', app)
                                    metrics = getattr(st, 'cyberherd_metrics', None)
                                    if metrics is None:
                                        metrics = {}
                                        try:
                                            setattr(st, 'cyberherd_metrics', metrics)
                                        except Exception:
                                            pass
                                    if res:
                                        metrics['reaction_persist_success'] = metrics.get('reaction_persist_success', 0) + 1
                                    else:
                                        metrics['reaction_persist_failure'] = metrics.get('reaction_persist_failure', 0) + 1
                                except Exception:
                                    pass
                                if res:
                                    logger.debug("Persisted processed reaction event %s for user %s", eid, user_id)
                                else:
                                    logger.warning(f"Failed to persist processed reaction event {eid} for user {user_id}")
                            except Exception:
                                logger.warning(f"Failed to persist processed reaction event {eid} for user {user_id}")
                    except Exception:
                        pass
            else:
                logger.warning(f"⚠️ Kind 7 reaction event {eid[:16] if eid else 'unknown'}... did NOT match any tracked events. Identifiers checked: {identifiers}")
                
    except Exception as e:
        logger.error(f"Error processing event for user {user_id}: {e}")
    finally:
        # monotonic watermark
        try:
            st = getattr(app, "state", app)
            last_seen = int(getattr(st, "cyberherd_social_last_seen_ts", 0) or 0)
            ca = int(event.get("created_at") or 0)
            if ca and ca > last_seen:
                setattr(st, "cyberherd_social_last_seen_ts", ca)
        except Exception:
            pass
    


def _collect_reference_identifiers(event: dict, include_content: bool = False) -> list[str]:
    """Collect candidate identifiers (event ids or addresses) from a nostr event."""
    identifiers: list[str] = []
    seen: set[str] = set()

    for tag in event.get("tags", []) or []:
        if not (isinstance(tag, list) and len(tag) >= 2):
            continue
        if tag[0] not in ("e", "a"):
            continue
        value = tag[1]
        if isinstance(value, str) and value and value not in seen:
            seen.add(value)
            identifiers.append(value)

    if include_content:
        content = event.get("content") or ""
        # raw event JSON inside content (reposts often embed original event)
        try:
            obj = json.loads(content)
            if isinstance(obj, dict):
                candidate = obj.get("id")
                if isinstance(candidate, str) and candidate and candidate not in seen:
                    seen.add(candidate)
                    identifiers.append(candidate)
        except Exception:
            pass

        # NIP-19 note/nevent tokens embedded in content
        from ..utils.common import get_nip19_decoder
        nip19 = get_nip19_decoder()

        if nip19:
            for token in re.findall(r'(nevent1[0-9a-z]+|note1[0-9a-z]+)', content):
                if token in seen:
                    continue
                try:
                    typ, data = nip19.decode(token)
                    if typ == "nevent" and isinstance(data, dict):
                        candidate = data.get("id")
                    elif typ == "note" and isinstance(data, str):
                        candidate = data
                    else:
                        candidate = None
                    if isinstance(candidate, str) and candidate:
                        seen.add(candidate)
                        identifiers.append(candidate)
                except Exception:
                    continue

    return identifiers


def _normalize_tracked_event_id(candidate: str | None) -> str | None:
    """Normalize tracked event identifiers (kind 1 or 30311) to hex event ids.

    Supports plain hex ids, NIP-19 note/nevent tokens, and simple 'kind:pubkey:identifier'
    address strings (returns None for addresses because they require lookup).
    """
    if not isinstance(candidate, str):
        return None
    value = candidate.strip()
    if not value:
        return None

    # Allow prefixed forms like 'nostr:<token>'
    if value.startswith("nostr:"):
        value = value.split(":", 1)[1] or ""

    lower = value.lower()
    if re.fullmatch(r"[0-9a-f]{64}", lower):
        return lower

    if lower.startswith("note1") or lower.startswith("nevent1"):
        from ..utils.common import get_nip19_decoder
        nip19 = get_nip19_decoder()
        
        if nip19:
            try:
                kind, data = nip19.decode(lower)
                if kind == "note" and isinstance(data, str) and re.fullmatch(r"[0-9a-f]{64}", data.lower()):
                    return data.lower()
                if kind == "nevent" and isinstance(data, dict):
                    ev_id = data.get("id")
                    if isinstance(ev_id, str) and re.fullmatch(r"[0-9a-f]{64}", ev_id.lower()):
                        return ev_id.lower()
            except Exception:
                return None
    return None


async def _resolve_tracked_event(settings, identifier: str, cache: dict | None, app) -> tuple[str | None, dict[str, Any] | None]:
    """Resolve an identifier used in repost/reaction tags to a tracked event ID and metadata.

    Returns (event_id, event_metadata_dict or None).
    """
    event_id = _normalize_tracked_event_id(identifier)
    if event_id:
        return event_id, None

    # Attempt to resolve via tracked_event_addresses (note_id -> address mapping)
    addresses = dict(getattr(settings, 'tracked_event_addresses', {}) or {})
    for note_id, addr in addresses.items():
        if not isinstance(addr, str) or not addr:
            continue
        if addr == identifier:
            resolved_hex = _normalize_tracked_event_id(note_id)
            if resolved_hex:
                return resolved_hex, {'address': addr, 'note_id': note_id}

    # Fallback: try to look up the address via nostr_helpers
    try:
        if identifier and ':' in identifier:
            if not nostr_helpers.check_availability():
                return None, None
            # identifier expected form: kind:pubkey:dTag
            parts = identifier.split(':')
            if len(parts) >= 3:
                kind = None
                try:
                    kind = int(parts[0])
                except Exception:
                    pass
                author = parts[1]
                d_tag = ':'.join(parts[2:])
                if kind in (1, 30311) and author and d_tag:
                    filters = {"kinds": [kind], "authors": [author], "#d": [d_tag]}
                    events = await nostr_helpers.query_events(filters, limit=1, timeout=5.0)
                    if events:
                        ev = events[0]
                        ev_id = _normalize_tracked_event_id(ev.get('id'))
                        if ev_id:
                            return ev_id, ev
    except Exception:
        pass

    return None, None


async def _is_tracked_event(user_id: str, note_event_id: str, settings, app, cache: dict | None = None) -> bool:
    """Check if the supplied event ID corresponds to a tracked note (kind 1 or 30311).

    UTC-FIRST: Cache keys use UTC date for consistency across timezones.
    """
    try:
        logger.debug(f"🔍 _is_tracked_event: Checking if {note_event_id[:16]}... is tracked for user {user_id}")
        
        # First check if the event is in today's cache
        cache = cache or _get_cache(app)
        
        # UTC-FIRST: Use UTC date for cache key (consistent across timezones)
        boundaries = _get_today_boundaries_utc()
        day = boundaries.utc_day_str

        eff_pub = get_effective_pubkey(settings)
        tags = getattr(settings, 'tracked_tags', [])
        tagset = tuple(sorted([t.lstrip('#').lower() for t in tags if t]))

        logger.debug(f"  Cache day: {day}, eff_pub: {eff_pub[:16] if eff_pub else 'None'}..., tags: {tags}")

        # Check both user-specific and neutral cache keys
        cache_keys = [
            (day, None, eff_pub, tagset),  # neutral key
            (day, user_id, eff_pub, tagset),  # user-specific using caller user_id
        ]

        # If the event ID is explicitly listed in settings.tracked_event_ids,
        # treat it as tracked immediately. This covers manual IDs and
        # automatically-detected IDs stored in settings so that zaps/reactions
        # referencing those event IDs are processed even if the note itself
        # wasn't seen in the in-memory cache (e.g. added manually via UI).
        try:
            explicit_tracked = getattr(settings, 'tracked_event_ids', []) or []
            logger.debug(f"  Explicit tracked_event_ids ({len(explicit_tracked)}): {[e[:16]+'...' for e in explicit_tracked[:5]]}")
            
            if note_event_id in explicit_tracked:
                logger.debug(f"✅ Event {note_event_id[:16]}... found in explicit tracked_event_ids!")
                # Ensure it's present in the runtime cache for faster future checks
                for key in cache_keys:
                    note_ids = _get_cache_note_ids(cache, key, create=True)
                    if note_event_id not in note_ids:
                        note_ids.append(note_event_id)
                return True
            else:
                logger.debug(f"  Event {note_event_id[:16]}... NOT in explicit tracked_event_ids")
        except Exception as e:
            # Fall back to cache lookup below on any error
            logger.debug(f"  Error checking explicit tracked_event_ids: {e}")
            pass

        # Check cache
        for key in cache_keys:
            event_ids = _get_cache_note_ids(cache, key)
            if note_event_id in event_ids:
                logger.debug(f"✅ Event {note_event_id[:16]}... found in cache!")
                return True
        
        logger.debug(f"  Event {note_event_id[:16]}... NOT in cache, checking via nostr_helpers...")
        
        # If not in cache, query for the event via nostr_helpers and check if it would be tracked
        try:
            available = nostr_helpers.check_availability()
            _record_availability_check(available)
            
            if not available:
                logger.warning("Nostr helpers not available for repost tracking query")
                return False
            
            events = await nostr_helpers.query_events(
                {"ids": [note_event_id]},
                limit=1,
                timeout=5.0
            )
            _record_helper_query(True)
            
            if events:
                event = events[0]
                logger.debug(f"  Found event via nostr_helpers, checking if it would be tracked...")
                # Check if this event would be tracked
                if _would_event_be_tracked(event, eff_pub, tags):
                    logger.debug(f"✅ Event {note_event_id[:16]}... WOULD be tracked, adding to cache")
                    # Add to cache so future checks are faster
                    for key in cache_keys:
                        note_ids = _get_cache_note_ids(cache, key, create=True)
                        if note_event_id not in note_ids:
                            note_ids.append(note_event_id)
                    return True
                else:
                    logger.debug(f"❌ Event {note_event_id[:16]}... would NOT be tracked (doesn't match criteria)")
            else:
                logger.debug(f"❌ Event {note_event_id[:16]}... NOT found via nostr_helpers query")
        except Exception as e:
            _record_helper_query(False, str(e))
            logger.warning(f"Error querying for tracked event {note_event_id} via nostr_helpers: {e}")
        
        logger.debug(f"❌ Event {note_event_id[:16]}... is NOT tracked")
        return False
    except Exception as e:
        logger.error(f"Error checking if event is tracked: {e}")
        return False


async def _trigger_repost_headbutt(user_id: str, reposter_pubkey: str, reposted_event_id: str, repost_event_id: str, app, recovery_mode: bool = False):
    """Trigger headbutt processing for a repost event.
    
    Args:
        user_id: User ID for settings and service
        reposter_pubkey: Hex pubkey of the person who reposted
        reposted_event_id: Event ID of the original note that was reposted
        repost_event_id: Event ID of the repost event itself
        app: FastAPI app object
        recovery_mode: If True, skip publishing celebratory notes (for startup recovery)
        
    Returns:
        Result dict from attempt_headbutt or None
    """
    try:
        from .headbutt import trigger_headbutt_from_repost
        
        # Use the standardized trigger function from headbutt.py
        # It handles metadata lookup, admission logic, and messaging
        result = await trigger_headbutt_from_repost(
            user_id=user_id,
            pubkey=reposter_pubkey,
            note_id=reposted_event_id,
            event_id=repost_event_id,
            app=app,
            recovery_mode=recovery_mode,
        )
        
        if result:
            _dbg("Repost headbutt successful for {} on event {}", reposter_pubkey, reposted_event_id)
        else:
            _dbg("Repost headbutt failed for {} on event {}", reposter_pubkey, reposted_event_id)
        
        return result
            
    except Exception as e:
        logger.error(f"Error triggering repost headbutt: {e}")
        return None


async def _trigger_reaction_headbutt(user_id: str, reactor_pubkey: str, reacted_event_id: str, reaction_event_id: str, app, recovery_mode: bool = False):
    """Trigger headbutt processing for a reaction event.
    
    Args:
        user_id: User ID for settings and service
        reactor_pubkey: Hex pubkey of the person who reacted
        reacted_event_id: Event ID of the original note that was reacted to
        reaction_event_id: Event ID of the reaction event itself
        app: FastAPI app object
        recovery_mode: If True, skip publishing celebratory notes (for startup recovery)
        
    Returns:
        Result dict from attempt_headbutt or None
    """
    try:
        from .headbutt import trigger_headbutt_from_reaction
        
        # Use the standardized trigger function from headbutt.py
        # It handles metadata lookup, admission logic, and messaging
        result = await trigger_headbutt_from_reaction(
            user_id=user_id,
            pubkey=reactor_pubkey,
            note_id=reacted_event_id,
            event_id=reaction_event_id,
            app=app,
            recovery_mode=recovery_mode,
        )
        
        if result:
            _dbg("Reaction headbutt successful for {} on event {}", reactor_pubkey, reacted_event_id)
        else:
            _dbg("Reaction headbutt failed for {} on event {}", reactor_pubkey, reacted_event_id)
        
        return result
            
    except Exception as e:
        logger.error(f"Error triggering reaction headbutt: {e}")
        return None

async def process_note_for_tracked_tags(user_id: str, event: dict, app=None):
    """Process a kind 1 or kind 30311 note event for tracked tags.
    
    This is a wrapper function called by the WebSocket monitor when a note
    event is received. It fetches the necessary context (settings, app) and
    delegates to the main processing logic.
    
    Args:
        user_id: User ID to process the note for
        event: The kind 1 or kind 30311 note event from Nostr
        app: Optional FastAPI app instance (required for processing)
    """
    try:
        # Get settings for this user
        settings = await crud.get_settings(user_id)
        if not settings:
            logger.warning(f"No settings found for user {user_id}, cannot process note")
            return
        
        # Track notes even if no tracked_tags are configured.
        # When tags are empty, any note authored by the effective pubkey today
        # will be considered tracked (to support repost/reaction handling).
        tracked_tags = getattr(settings, 'tracked_tags', []) or []
        
        # Debug: Check if effective pubkey is available
        eff_pub = get_effective_pubkey(settings)
        if not eff_pub:
            logger.warning(
                f"No effective pubkey available for user {user_id}. "
                f"Cannot track notes. Check nostr_pubkey_override or nostr_private_key settings."
            )
            return
        
        logger.info(
            f"Processing note for user {user_id}: "
            f"event_id={event.get('id', 'unknown')[:16]}..., "
            f"event_pubkey={event.get('pubkey', 'unknown')[:16]}..., "
            f"effective_pubkey={eff_pub[:16]}..., "
            f"tracked_tags={tracked_tags}"
        )
        
        # Process the event using the main logic
        # This will:
        # 1. Check if note matches tracked tags (or any author note if tags empty)
        # 2. Add to tracked_event_ids via _append_today
        # 3. Trigger immediate subscription refresh (for engagement tracking)
        await process_event_for_user(user_id, event, settings, app, recovery_mode=False)
        
    except Exception as e:
        logger.error(f"Error processing note for user {user_id}: {e}")


async def process_repost_for_tracked_notes(user_id: str, event: dict, app=None):
    """Process a kind 6 repost event for tracked notes.
    
    This is a wrapper function called by the WebSocket monitor when a repost
    event is received. It fetches the necessary context (settings, app) and
    delegates to the main processing logic.
    
    Args:
        user_id: User ID to process the repost for
        event: The kind 6 repost event from Nostr
        app: Optional FastAPI app instance (required for processing)
    """
    try:
        # Get settings for this user
        settings = await crud.get_settings(user_id)
        if not settings:
            logger.warning(f"No settings found for user {user_id}, cannot process repost")
            return
        
        # Check if repost tracking is enabled
        if not getattr(settings, "repost_tracking_enabled", False):
            logger.debug(f"Repost tracking not enabled for user {user_id}")
            return
        
        # Deduplication: skip if already processed
        event_id = (event.get("id") or "").strip().lower()
        if event_id:
            try:
                if await crud.is_event_processed(user_id, event_id):
                    logger.debug(f"Repost {event_id[:8]}... already processed for user {user_id}")
                    return
            except Exception:
                pass
        
        # Process the event using the main logic
        await process_event_for_user(user_id, event, settings, app, recovery_mode=False)
        
        # Mark as processed after successful processing
        if event_id:
            try:
                await crud.register_processed_event(
                    user_id,
                    event_id,
                    event_type="repost"
                )
            except Exception as e:
                logger.debug(f"Failed to register processed repost {event_id[:8]}...: {e}")
        
    except Exception as e:
        logger.error(f"Error processing repost for user {user_id}: {e}")


async def process_reaction_for_tracked_notes(user_id: str, event: dict, app=None):
    """Process a kind 7 reaction event for tracked notes.
    
    This is a wrapper function called by the WebSocket monitor when a reaction
    event is received. It fetches the necessary context (settings, app) and
    delegates to the main processing logic.
    
    Args:
        user_id: User ID to process the reaction for
        event: The kind 7 reaction event from Nostr
        app: Optional FastAPI app instance (required for processing)
    """
    try:
        # Get settings for this user
        settings = await crud.get_settings(user_id)
        if not settings:
            logger.warning(f"No settings found for user {user_id}, cannot process reaction")
            return
        
        # Check if reaction tracking is enabled
        if not getattr(settings, "likes_tracking_enabled", False):
            logger.debug(f"Reaction tracking not enabled for user {user_id}")
            return
        
        # Deduplication: skip if already processed
        event_id = (event.get("id") or "").strip().lower()
        if event_id:
            try:
                if await crud.is_event_processed(user_id, event_id):
                    logger.debug(f"Reaction {event_id[:8]}... already processed for user {user_id}")
                    return
            except Exception:
                pass
        
        # Process the event using the main logic
        await process_event_for_user(user_id, event, settings, app, recovery_mode=False)
        
        # Mark as processed after successful processing
        if event_id:
            try:
                await crud.register_processed_event(
                    user_id,
                    event_id,
                    event_type="reaction"
                )
            except Exception as e:
                logger.debug(f"Failed to register processed reaction {event_id[:8]}...: {e}")
        
    except Exception as e:
        logger.error(f"Error processing reaction for user {user_id}: {e}")


async def process_zap_receipt_for_tracked_notes(user_id: str, event: dict, app=None):
    """Process a kind 9735 zap receipt event for tracked notes.

    This validates the receipt (NIP-57), extracts the zapper pubkey and amount
    from the tags/bolt11, de-duplicates by event id, and then routes the zap
    to headbutt admission logic. On success, it records the processed zap and
    updates zap totals for leaderboard consumers.

    Args:
        user_id: User ID to process the zap for
        event: The kind 9735 zap receipt event from Nostr
        app: Optional FastAPI app instance (required for processing)
    """
    try:
        # Only handle proper kind
        kind = event.get("kind")
        if kind != 9735:
            return

        settings = await crud.get_settings(user_id)
        if not settings:
            logger.warning(f"No settings found for user {user_id}, cannot process zap receipt")
            return

        if not getattr(settings, "zap_tracking_enabled", False):
            logger.debug(f"Zap tracking not enabled for user {user_id}")
            return

        # Validate receipt per NIP-57
        ok, reason = nostr_helpers.validate_nip57_zap_receipt(event)
        if not ok:
            logger.debug(f"NIP-57 receipt validation failed for user {user_id}: {reason}")
            return

        event_id = (event.get("id") or "").strip().lower()
        if not event_id:
            return

        # Idempotency: skip if already processed
        try:
            if await crud.is_event_processed(user_id, event_id):
                logger.debug(f"Zap receipt {event_id[:8]}... already processed for user {user_id}")
                return
        except Exception:
            pass

        # Extract zapped note id and zapper pubkey
        tags = event.get("tags") or []
        zapped_note_id = None
        recipient_pubkey = None
        bolt11_invoice = None
        amount_msat_tag = None
        description_json = None
        for tag in tags:
            if not (isinstance(tag, list) and len(tag) >= 2):
                continue
            marker, value = tag[0], tag[1]
            if marker == "e" and isinstance(value, str) and not zapped_note_id:
                zapped_note_id = value.strip().lower()
            elif marker == "p" and isinstance(value, str) and not recipient_pubkey:
                recipient_pubkey = value.strip().lower()
            elif marker == "bolt11" and isinstance(value, str) and value:
                bolt11_invoice = value.strip()
            elif marker == "amount" and isinstance(value, str):
                # Keep raw amount (msat)
                amount_msat_tag = value
            elif marker == "description" and isinstance(value, str):
                description_json = value

        # Parse zap request to get zapper pubkey
        zapper_pubkey = None
        try:
            if description_json:
                req = json.loads(description_json)
                if isinstance(req, dict):
                    zp = req.get("pubkey")
                    if isinstance(zp, str):
                        zapper_pubkey = zp.strip().lower()
        except Exception:
            zapper_pubkey = None

        if not zapper_pubkey:
            logger.debug(f"Zap receipt {event_id[:8]}... missing zapper pubkey in description for user {user_id}")
            return

        # Reject self-zaps (effective pubkey == zapper)
        try:
            eff_pub = resolve_effective_pubkey(settings)
            if eff_pub and zapper_pubkey == eff_pub.strip().lower():
                logger.info(
                    f"Zap rejected: zapper is the effective pubkey (operator's own zap) zapper={zapper_pubkey[:16]}..."
                )
                return
        except Exception:
            pass

        # Amount: prefer explicit amount tag (msat); fallback to bolt11 decode
        amount_msat = None
        if isinstance(amount_msat_tag, str):
            try:
                digits = "".join(ch for ch in amount_msat_tag if ch.isdigit())
                amount_msat = int(digits) if digits else None
            except Exception:
                amount_msat = None

        if amount_msat is None and bolt11_invoice and bolt11_decode is not None:
            try:
                decoded = bolt11_decode(bolt11_invoice)
                if getattr(decoded, "amount_msat", None) is not None:
                    amount_msat = int(decoded.amount_msat)
            except Exception:
                logger.debug("Failed to decode bolt11 in zap receipt", exc_info=True)

        if amount_msat is None:
            logger.debug(f"Zap receipt {event_id[:8]}... missing amount information for user {user_id}")
            return

        amount_sats = max(0, amount_msat // 1000)
        if amount_sats <= 0:
            logger.debug(f"Zap receipt {event_id[:8]}... zero/negative amount for user {user_id}")
            return

        # Register payment hash FIRST to prevent duplicate via payment recovery
        # This is done before the zap receipt duplicate check so that even if this
        # zap was already processed via kind 9735, we still register the payment hash
        # to prevent payment-based recovery from reprocessing it
        payment_hash_registered = False
        if bolt11_invoice and bolt11_decode is not None:
            try:
                decoded = bolt11_decode(bolt11_invoice)
                payment_hash = getattr(decoded, "payment_hash", None)
                if payment_hash:
                    # Normalize to lowercase hex string
                    if isinstance(payment_hash, bytes):
                        payment_hash = payment_hash.hex().lower()
                    elif isinstance(payment_hash, str):
                        payment_hash = payment_hash.strip().lower()
                    
                    # Register payment hash to prevent payment-based recovery from re-processing
                    # IMPORTANT: Check return value! If False, it means payment was already processed
                    # (e.g. by payment coordinator), so we MUST stop here to avoid double counting.
                    ph_inserted = await crud.register_processed_event(
                        user_id,
                        payment_hash,
                        note_id=zapped_note_id,
                        pubkey=zapper_pubkey,
                        event_type="payment_zap",
                    )
                    payment_hash_registered = True
                    
                    if not ph_inserted:
                        logger.info(
                            f"Zap receipt {event_id[:8]}... skipped: payment hash {payment_hash[:16]}... already processed"
                        )
                        return
                    
                    logger.debug(
                        f"Registered payment hash {payment_hash[:16]}... for zap receipt {event_id[:8]}..."
                    )
            except Exception as e:
                logger.debug(f"Failed to extract/register payment hash from bolt11: {e}")

        # Register processed receipt (idempotency) - check AFTER payment hash registration
        inserted = False
        try:
            inserted = await crud.register_processed_event(
                user_id,
                event_id,
                note_id=zapped_note_id,
                pubkey=zapper_pubkey,
                event_type="zap_receipt",
            )
        except Exception:
            # Best-effort
            pass
        if not inserted:
            # Already processed - but we still registered the payment hash above (if available)
            # so payment recovery won't reprocess this zap
            logger.debug(
                f"Zap receipt {event_id[:8]}... already registered for user {user_id} "
                f"(payment hash {'registered' if payment_hash_registered else 'N/A'})"
            )
            return


        # Trigger headbutt admission for zap
        try:
            result = await trigger_headbutt_from_zap(
                user_id=user_id,
                pubkey=zapper_pubkey,
                amount_sats=amount_sats,
                note_id=zapped_note_id or "",
                event_id=event_id,
                app=app,
            )
        except Exception as e:
            logger.error(f"Error triggering headbutt from zap receipt: {e}")
            result = None

        if result:
            # Persist processed zap record (diagnostic/history)
            try:
                await crud.create_processed_zap(
                    {
                        "user_id": user_id,
                        "event_id": event_id,
                        "note_id": zapped_note_id or "",
                        "zapper_pubkey": zapper_pubkey,
                        "amount": int(amount_sats),
                        "processed_at": int(utc_now().timestamp()),
                    }
                )
            except Exception:
                logger.debug("Failed to persist processed zap row", exc_info=True)

            # Update zap totals and notify leaderboard watchers
            try:
                target_pubkey = resolve_effective_pubkey(settings)
            except Exception:
                target_pubkey = None

            if target_pubkey:
                try:
                    created_at = int(event.get("created_at") or 0) or int(utc_now().timestamp())
                except Exception:
                    created_at = int(utc_now().timestamp())
                try:
                    await record_incremental_zap_total(
                        user_id=user_id,
                        zapper_pubkey=zapper_pubkey,
                        target_pubkey=target_pubkey,
                        amount_sats=amount_sats,
                        event_timestamp=created_at,
                        event_id=event_id,
                    )
                    from .leaderboard import broadcast_leaderboard_update

                    await broadcast_leaderboard_update(user_id, settings=settings)
                except Exception:
                    logger.debug("Failed to update zap totals/broadcast", exc_info=True)

            logger.info(
                f"✅ Processed zap receipt: {amount_sats} sats from {zapper_pubkey[:8]}... on note {str(zapped_note_id)[:8]}..."
            )
        else:
            logger.debug(
                f"Zap receipt headbutt did not admit/update member for user {user_id} (event {event_id[:8]}...)"
            )
    except Exception as e:
        logger.error(f"Error processing zap receipt for user {user_id}: {e}")
