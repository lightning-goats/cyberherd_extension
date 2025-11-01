"""Cyberherd tagged note subscription management.

This module now delegates real Nostr relay connectivity to the shared
`nostrclient` extension via the adapter in `nostr_adapter.py`.

All Nostr interactions (queries, subscriptions) are routed through
`nostr_helpers` for stable API access. Legacy direct nostrclient access
has been removed in favor of the helper abstraction.

Key changes:
- All event queries via nostr_helpers.query_events()
- Repost/reaction recovery uses #e-only filters (no #t requirement)
- Multi-#e handling for batch queries
- Diagnostics report helper failures

Legacy per-relay websocket code has been removed in favour of pooled
subscriptions managed via nostr_adapter. In-memory cache handling and
status remain here for reuse by the adapter.
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

# Enable verbose logging with: export CYBERHERD_DEBUG=true
CYBERHERD_DEBUG = os.getenv("CYBERHERD_DEBUG", "false").lower() in ("1", "true", "yes", "y")

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
        _helper_diagnostics['last_success'] = datetime.now(timezone.utc).isoformat()
    else:
        _helper_diagnostics['queries_failed'] += 1
        # UTC timestamp for consistency across timezones
        _helper_diagnostics['last_failure'] = datetime.now(timezone.utc).isoformat()
        _helper_diagnostics['last_failure_reason'] = error

def _record_availability_check(available: bool):
    """Record a nostr_helpers availability check."""
    _helper_diagnostics['availability_checks'] += 1
    if not available:
        _helper_diagnostics['unavailable_count'] += 1

def _dbg(msg: str, *args):  # conditional debug helper with proper formatting
    if CYBERHERD_DEBUG:
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
        if not tags_norm:
            return False

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
_refresh_event: asyncio.Event | None = None  # set by API when settings change
# Module-level cache used when no app/state is provided (tests or callers without app)
_module_note_cache: dict = {}


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
        note_ids = entry.get("note_ids")
        if isinstance(note_ids, list):
            return note_ids
        note_ids = []
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
    global _refresh_event

    eid = event.get("id")
    if not eid:
        return False
    tags_norm = [t.lstrip('#').lower() for t in tags if t]
    if not tags_norm:
        return False
    # Author check
    event_pubkey = event.get("pubkey")
    if event_pubkey != eff_pub:
        _dbg(f"_append_today: author mismatch event_pubkey={event_pubkey} eff_pub={eff_pub}")
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
        _dbg(f"_append_today: time mismatch created_at={created_at} "
             f"local_day=[{boundaries.local_since_ts}, {boundaries.local_until_ts}) "
             f"utc_date={boundaries.utc_day_str} eid={eid}")
        return False
    # Robust: Nostr tags are arrays like ["t", "tag", ...]; tolerate mixed shapes
    # Normalize by stripping a leading '#', then lowercase for comparison
    ev_tags = []
    for t in event.get("tags", []) or []:
        if isinstance(t, list) and len(t) > 1 and t[0] == "t" and isinstance(t[1], str):
            ev_tags.append(t[1].lstrip('#').lower())
    matched = any(t in ev_tags for t in tags_norm)

    # Content-hashtag fallback: if no explicit 't' tags matched, try to
    # find #hashtags in the content body and match those. This is enabled
    # by default so clients that don't add 't' tags still get detected.
    found_norm = []
    if not matched:
        try:
            content = event.get('content', '') or ''
            # simple hashtag capture (unicode-friendly would be a future enhancement)
            found = re.findall(r"#([\w\-]+)", content, flags=re.UNICODE)
            found_norm = [h.lstrip('#').lower() for h in found if h]
            matched = any(t in found_norm for t in tags_norm)
        except Exception:
            matched = matched

    if not matched:
        _dbg(f"_append_today: tag mismatch ev_tags={ev_tags} tags_norm={tags_norm} content_hashtags={found_norm} eid={eid}")
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
                                f"Total tracked events: {len(updated_tracked)}. Triggering kind 6/7 subscription refresh..."
                            )
                        except Exception:
                            pass
                        # Refresh signaling unified via app.state flag
                        # This will cause kind 6/7 subscriptions to be recreated with the new event ID
                        try:
                            st = getattr(app, "state", app)
                            setattr(st, "cyberherd_force_subscription_refresh", True)
                            if _refresh_event is None:
                                try:
                                    _refresh_event = asyncio.Event()
                                except Exception:
                                    _refresh_event = None
                            if _refresh_event is not None:
                                try:
                                    _refresh_event.set()
                                except Exception:
                                    pass
                            logger.info(
                                f"✅ Subscription refresh triggered for user={user_id} after adding tracked event {eid}. "
                                f"Kind 6/7 subscriptions will be updated automatically."
                            )
                            _dbg("Set force refresh flag after adding event {} for user {}", eid, user_id)
                        except Exception as e:
                            logger.warning(f"Failed to set subscription refresh flag: {e}")
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


async def _initialize_tracked_event_ids_on_startup(app):
    """Initialize tracked_event_ids with recent notes on startup.
    
    Queries for recent kind 1 notes by each user's effective pubkey and
    populates tracked_event_ids. This ensures subscriptions for reposts/reactions
    can be created immediately, even if no notes have been detected yet.
    
    UTC-FIRST: Uses UTC midnight as query "since" timestamp.
    """
    try:
        # Check if nostr_helpers is available
        available = nostr_helpers.check_availability()
        _record_availability_check(available)
        
        if not available:
            logger.warning("Nostr helpers not available for tracked_event_ids initialization")
            return
        
        # Get all users with tracking enabled
        from .. import crud
        # Dynamic import for get_users to avoid static-analysis import errors
        import importlib
        get_users = None
        try:
            core_mod = importlib.import_module('lnbits.core.crud')
            get_users = getattr(core_mod, 'get_users', None)
        except Exception:
            try:
                from lnbits.core.crud import get_users as _gu  # type: ignore
                get_users = _gu
            except Exception:
                get_users = None

        # Only await if we obtained an async function
        if get_users and asyncio.iscoroutinefunction(get_users):
            users = await get_users()
        else:
            users = []
        # By default initialize tracked_event_ids using events from a recent lookback window.
        # Configure lookback via CYBERHERD_TRACKED_EVENT_LOOKBACK_DAYS (default 3 days).
        try:
            lookback_days = int(os.getenv("CYBERHERD_TRACKED_EVENT_LOOKBACK_DAYS", "3") or 3)
            boundaries = get_day_boundaries_utc(days_ago=lookback_days)
            since_ts = boundaries.local_since_ts
            logger.info(f"Initializing tracked_event_ids on startup with lookback_days={lookback_days} since_ts={since_ts} local_day={boundaries.local_day_str}")
        except Exception as e:
            # Fallback to UTC now if helper fails
            from datetime import datetime, timezone
            since_ts = int(datetime.now(timezone.utc).timestamp())
            logger.warning(f"Failed to compute day boundaries for tracked_event_ids init: {e}; falling back to now since_ts={since_ts}")
        
        for user in users:
            try:
                settings = await crud.get_settings(user.id)
                if not settings:
                    continue
                
                # Skip users without tracking enabled
                if not (getattr(settings, 'repost_tracking_enabled', False) or getattr(settings, 'likes_tracking_enabled', False)):
                    continue
                
                eff_pub = get_effective_pubkey(settings)
                tags = getattr(settings, 'tracked_tags', [])
                
                if not eff_pub or not tags:
                    continue
                
                # Query for recent kind 1 and 30311 notes by this author
                filter_dict = {
                    "kinds": [1, 30311],
                    "authors": [eff_pub],
                    "since": since_ts
                }
                
                events = await nostr_helpers.query_events(
                    filter_dict,
                    limit=100,  # Get last 100 notes from today
                    timeout=5.0
                )
                _record_helper_query(True)
                
                if events:
                    # Filter events to only those matching tracked tags
                    tracked_ids = []
                    tags_norm = [t.lstrip('#').lower() for t in tags if t]
                    
                    for event in events:
                        # Check if event has any tracked tags
                        ev_tags = []
                        for t in event.get("tags", []) or []:
                            if isinstance(t, list) and len(t) > 1 and t[0] == "t" and isinstance(t[1], str):
                                ev_tags.append(t[1].lstrip('#').lower())
                        
                        if any(t in ev_tags for t in tags_norm):
                            event_id = event.get("id")
                            if event_id:
                                tracked_ids.append(event_id)
                    
                    if tracked_ids:
                        # Update tracked_event_ids in settings
                        current_tracked = getattr(settings, 'tracked_event_ids', []) or []
                        updated_tracked = list(set(current_tracked + tracked_ids))
                        settings.tracked_event_ids = updated_tracked
                        await crud.upsert_settings(settings, user.id)
                        
                        logger.info(f"Initialized tracked_event_ids for user {user.id}: added {len(tracked_ids)} recent notes (total: {len(updated_tracked)})")
                        _dbg(f"Initialized tracked_event_ids for user {user.id} with {len(tracked_ids)} notes")
                        
            except Exception as e:
                logger.warning(f"Error initializing tracked_event_ids for user {user.id}: {e}")
                
    except Exception as e:
        logger.warning(f"Error in tracked_event_ids initialization: {e}")


async def _any_tracked_event_ids_exist(app) -> bool:
    """Return True if any user with tracking enabled already has tracked_event_ids.

    This is used to decide whether it's safe to start realtime engagement
    subscriptions that depend on tracked_event_ids.
    """
    try:
        from .. import crud
        import importlib
        get_users = None
        try:
            core_mod = importlib.import_module('lnbits.core.crud')
            get_users = getattr(core_mod, 'get_users', None)
        except Exception:
            try:
                from lnbits.core.crud import get_users as _gu  # type: ignore
                get_users = _gu
            except Exception:
                get_users = None

        users = await get_users() if get_users and asyncio.iscoroutinefunction(get_users) else []
        for user in users:
            try:
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
    """Wait up to timeout_seconds for any tracked_event_ids to appear.

    Returns True if tracked_event_ids were detected, False on timeout.
    """
    global _refresh_event
    try:
        # Fast path: already present
        if await _any_tracked_event_ids_exist(app):
            return True

        # Ensure event exists so _append_today can set it when it auto-adds IDs
        if _refresh_event is None:
            try:
                _refresh_event = asyncio.Event()
            except Exception:
                _refresh_event = None

        if _refresh_event is None:
            # Can't rely on event waking, just sleep a bit and re-check
            await asyncio.sleep(timeout_seconds)
            return await _any_tracked_event_ids_exist(app)

        try:
            await asyncio.wait_for(_refresh_event.wait(), timeout=timeout_seconds)
        except asyncio.TimeoutError:
            return False
        finally:
            try:
                _refresh_event.clear()
            except Exception:
                pass

        # Give a small moment for DB writes to complete
        await asyncio.sleep(0.1)
        return await _any_tracked_event_ids_exist(app)
    except Exception:
        return False


def trigger_subscription_refresh(app, reason: str | None = None):
    """Signal the adapter manager to refresh subscriptions immediately.

    This sets a flag on app.state and fires the module-level _refresh_event if present.
    Can be called by other modules when tracked_event_ids are added/updated.
    """
    try:
        st = getattr(app, 'state', app)
        setattr(st, 'cyberherd_force_subscription_refresh', True)
    except Exception:
        pass

    global _refresh_event
    try:
        if _refresh_event is None:
            try:
                _refresh_event = asyncio.Event()
            except Exception:
                _refresh_event = None
        if _refresh_event is not None:
            try:
                _refresh_event.set()
            except Exception:
                pass
    except Exception:
        pass

    # Try to sync the event to the nostr_adapter module so the adapter's
    # manager loop (which may be waiting on its own _refresh_event) is
    # reliably woken up immediately instead of waiting for the loop timeout.
    try:
        from . import nostr_adapter
        try:
            # If adapter has its own _refresh_event, set it or replace it with ours
            a_ev = getattr(nostr_adapter, '_refresh_event', None)
            if a_ev is None:
                try:
                    setattr(nostr_adapter, '_refresh_event', _refresh_event)
                except Exception:
                    pass
            else:
                try:
                    # If adapter event exists, prefer to set it so its wait wakes
                    a_ev.set()
                except Exception:
                    # If setting fails, try to replace it
                    try:
                        setattr(nostr_adapter, '_refresh_event', _refresh_event)
                    except Exception:
                        pass
        except Exception:
            pass
    except Exception:
        # Adapter may not be importable in some contexts; ignore
        pass

    try:
        logger.info(f"Triggered subscription refresh{f' - {reason}' if reason else ''}")
    except Exception:
        pass


async def _tracked_ids_monitor(app, check_interval: float = 10.0):
    """Background monitor that polls for the appearance of tracked_event_ids.

    When a change from no tracked ids to some tracked ids is detected, this
    triggers a subscription refresh. It runs indefinitely to pick up future
    additions as well.
    """
    try:
        prev = False
        while True:
            try:
                now_has = await _any_tracked_event_ids_exist(app)
                if not prev and now_has:
                    trigger_subscription_refresh(app, reason="tracked_event_ids_detected")
                prev = now_has
            except Exception:
                # Ignore transient errors and continue polling
                pass
            await asyncio.sleep(check_interval)
    except asyncio.CancelledError:
        return
    except Exception:
        return


async def _authoritative_tag_subscription(app):  # kept for backward import paths
    # Delegate start to adapter (idempotent)
    from . import nostr_adapter
    await nostr_adapter.start_adapter(app)

def start_subscriptions(app):
    """Start cyberherd subscriptions.

    Returns the created background task so caller (or tests) can track it.
    Keeps a reference on app.state to avoid GC collecting the task which can
    lead to 'coroutine was never awaited' runtime warnings if exceptions occur
    before the event loop cycles.
    """
    try:
        import asyncio

        async def _kick():
            global _refresh_event
            try:
                # Wait for nostrclient relays to be ready before starting subscriptions
                from .relay_readiness import wait_for_relays_ready
                relay_status = await wait_for_relays_ready(max_wait_seconds=30.0, check_interval=2.0, min_connected_relays=1)
                if not relay_status['ready']:
                    logger.warning(
                        f"⚠️ Starting subscriptions even though nostrclient relays not confirmed ready after {relay_status['waited_seconds']}s. "
                        f"Status: {relay_status['relay_count']} relay(s) configured, {relay_status['connected_count']} connected. "
                        f"Reason: {relay_status['reason']}"
                    )
                
                # Initialize tracked_event_ids with recent notes before starting subscriptions
                await _initialize_tracked_event_ids_on_startup(app)
                
                # Wait for tracked_event_ids to be initialized before starting the
                # adapter so engagement subscriptions (kinds 6/7) can be created
                # immediately. This avoids starting realtime subscriptions too
                # early when there are no tracked ids.
                try:
                    wait_secs = float(os.getenv('CYBERHERD_WAIT_FOR_TRACKED_IDS_SECONDS', '30') or 30)
                except Exception:
                    wait_secs = 30.0

                got_tracked = False
                try:
                    got_tracked = await _wait_for_tracked_event_ids(app, timeout_seconds=wait_secs)
                except Exception:
                    got_tracked = False

                if not got_tracked:
                    logger.warning(
                        f"Timed out waiting {wait_secs}s for tracked_event_ids; starting adapter without guaranteed engagement filters"
                    )

                # Start the adapter (which creates initial subscriptions)
                await _authoritative_tag_subscription(app)
                # Register a simple callback with nostr_adapter so it can notify
                # this module when it considers updating filters. The callback
                # will trigger a subscription refresh which the adapter already
                # responds to via its manager loop.
                try:
                    from . import nostr_adapter

                    def _adapter_notify_cb(a, reason=None):
                        try:
                            trigger_subscription_refresh(a, reason=f"adapter_notify:{reason}")
                        except Exception:
                            pass

                    try:
                        nostr_adapter.register_filter_update_callback(_adapter_notify_cb)
                    except Exception:
                        pass
                    try:
                        # Register a richer provider so the adapter can request
                        # per-user prepared filters (Option A). The provider will
                        # delegate to the adapter's internal prepare helper if
                        # available to avoid duplicating logic.
                        async def _adapter_filter_provider(a, ctx):
                            try:
                                # Attempt to use adapter's prepare helper if present
                                prep = getattr(nostr_adapter, '_prepare_websocket_subscription', None)
                                if callable(prep):
                                    # prep expects (user_id, settings, app)
                                    uid = ctx.get('user_id')
                                    settings = ctx.get('settings')
                                    try:
                                        res = prep(uid, settings, a)
                                        if asyncio.iscoroutine(res):
                                            res = await res
                                        return res
                                    except Exception:
                                        return None
                                return None
                            except Exception:
                                return None

                        try:
                            nostr_adapter.register_filter_provider(_adapter_filter_provider)
                        except Exception:
                            pass
                    except Exception:
                        pass
                except Exception:
                    pass
                logger.info("Cyberherd nostr adapter started")
                
                # CRITICAL: Wait a moment for initial subscriptions to be created,
                # then trigger a refresh so engagement subscriptions (kinds 6/7) are updated
                # are created now that tracked_event_ids has been populated
                await asyncio.sleep(2)
                logger.info("Cyberherd: Triggering subscription refresh after tracked_event_ids initialization")
                
                # Trigger subscription refresh by touching the _update_config_flag in app.state
                # The manager loop checks this flag and recreates subscriptions when it's set
                try:
                    st = getattr(app, "state", app)
                    setattr(st, "cyberherd_force_subscription_refresh", True)
                    if _refresh_event is None:
                        try:
                            _refresh_event = asyncio.Event()
                        except Exception:
                            _refresh_event = None
                    if _refresh_event is not None:
                        try:
                            _refresh_event.set()
                        except Exception:
                            pass
                except Exception as e:
                    logger.warning(f"Could not set subscription refresh flag: {e}")
                
                # Recover missed reposts and reactions on startup
                await _recover_missed_reposts_and_reactions_on_startup(app)
                # Start background monitor to watch for tracked_event_ids added later
                try:
                    st = getattr(app, 'state', app)
                    monitor_task = asyncio.create_task(_tracked_ids_monitor(app))
                    bg = getattr(st, 'cyberherd_bg_tasks', None)
                    if bg is None:
                        bg = []
                        try:
                            setattr(st, 'cyberherd_bg_tasks', bg)
                        except Exception:
                            pass
                    bg.append(monitor_task)
                except Exception:
                    pass
            except Exception as e:  # pragma: no cover
                logger.warning(f"Cyberherd adapter start failed: {e}")

        task = asyncio.create_task(_kick())
        try:  # store reference
            st = getattr(app, "state", app)
            bg = getattr(st, "cyberherd_bg_tasks", None)
            if bg is None:
                bg = []
                setattr(st, "cyberherd_bg_tasks", bg)
            bg.append(task)
        except Exception:  # pragma: no cover
            pass
        return task
    except Exception as e:
        logger.error(f"Failed to start cyberherd subscriptions: {e}")
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

        # kind 1: notes
        if kind == 1:
            cache = _get_cache(app)
            await _append_today(cache, user_id, eff_pub, tags, event, app)

        # kind 6: reposts
        elif kind == 6 and getattr(settings, "repost_tracking_enabled", False):
            # Respect local "today" window: ignore historic reposts unless recovery_mode is requested
            try:
                if not recovery_mode:
                    boundaries = _get_today_boundaries_utc()
                    created_at = int(event.get('created_at') or 0)
                    if not boundaries.is_timestamp_in_local_day(created_at):
                        _dbg("Ignoring repost outside today's window: created_at=%s eid=%s", created_at, eid)
                        return
            except Exception:
                # On any error, fall back to normal behavior
                pass

            cache = _get_cache(app)
            target_id = None
            for identifier in _collect_reference_identifiers(event, include_content=True):
                resolved_id, _metadata = await _resolve_tracked_event(settings, identifier, cache, app)
                if not resolved_id:
                    continue
                if await _is_tracked_event(user_id, resolved_id, settings, app, cache):
                    target_id = resolved_id
                    break

            if target_id:
                # Persistent dedupe: if this repost event was already processed
                # and recorded in processed_events, skip to avoid cross-restart
                # reprocessing. Fall back to processing if the persistence check
                # fails for any reason.
                try:
                    from .. import crud
                    try:
                        if eid and await crud.is_event_processed(user_id, eid):
                            logger.debug("Skipping repost event %s because it's marked processed persistently", eid)
                            return
                    except Exception:
                        # If check fails, continue to attempt processing
                        pass
                except Exception:
                    # Import failed; this is non-fatal — continue processing
                    pass

                result = await _trigger_repost_headbutt(user_id, pubkey, target_id, eid, app, recovery_mode=recovery_mode)
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

        # kind 7: reactions
        elif kind == 7 and getattr(settings, "likes_tracking_enabled", False):
            # Respect local "today" window: ignore historic reactions unless recovery_mode is requested
            try:
                if not recovery_mode:
                    boundaries = _get_today_boundaries_utc()
                    created_at = int(event.get('created_at') or 0)
                    if not boundaries.is_timestamp_in_local_day(created_at):
                        _dbg("Ignoring reaction outside today's window: created_at=%s eid=%s", created_at, eid)
                        return
            except Exception:
                pass

            cache = _get_cache(app)
            reacted_id = None
            for identifier in _collect_reference_identifiers(event, include_content=False):
                resolved_id, _metadata = await _resolve_tracked_event(settings, identifier, cache, app)
                if not resolved_id:
                    continue
                if await _is_tracked_event(user_id, resolved_id, settings, app, cache):
                    reacted_id = resolved_id
                    break

            if reacted_id:
                # Persistent dedupe: skip if reaction event already processed
                try:
                    from .. import crud
                    try:
                        if eid and await crud.is_event_processed(user_id, eid):
                            logger.debug("Skipping reaction event %s because it's marked processed persistently", eid)
                            return
                    except Exception:
                        pass
                except Exception:
                    pass

                result = await _trigger_reaction_headbutt(user_id, pubkey, reacted_id, eid, app, recovery_mode=recovery_mode)
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
        try:
            import importlib
            nostr_mod = importlib.import_module('lnbits.utils.nostr')
            nip19 = getattr(nostr_mod, 'nip19', None)
        except Exception:
            nip19 = None

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
        try:
            import importlib
            nostr_mod = importlib.import_module('lnbits.utils.nostr')
            nip19 = getattr(nostr_mod, 'nip19', None)
        except Exception:
            nip19 = None
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
        # First check if the event is in today's cache
        cache = cache or _get_cache(app)
        
        # UTC-FIRST: Use UTC date for cache key (consistent across timezones)
        boundaries = _get_today_boundaries_utc()
        day = boundaries.utc_day_str

        eff_pub = get_effective_pubkey(settings)
        tags = getattr(settings, 'tracked_tags', [])
        tagset = tuple(sorted([t.lstrip('#').lower() for t in tags if t]))

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
            if note_event_id in explicit_tracked:
                # Ensure it's present in the runtime cache for faster future checks
                for key in cache_keys:
                    note_ids = _get_cache_note_ids(cache, key, create=True)
                    if note_event_id not in note_ids:
                        note_ids.append(note_event_id)
                return True
        except Exception:
            # Fall back to cache lookup below on any error
            pass

        for key in cache_keys:
            event_ids = _get_cache_note_ids(cache, key)
            if note_event_id in event_ids:
                return True
        
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
                # Check if this event would be tracked
                if _would_event_be_tracked(event, eff_pub, tags):
                    # Add to cache so future checks are faster
                    for key in cache_keys:
                        note_ids = _get_cache_note_ids(cache, key, create=True)
                        if note_event_id not in note_ids:
                            note_ids.append(note_event_id)
                    return True
        except Exception as e:
            _record_helper_query(False, str(e))
            logger.warning(f"Error querying for tracked event {note_event_id} via nostr_helpers: {e}")
        
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


# DEPRECATED: _trigger_zap_headbutt has been removed
# Zaps are now processed exclusively via the payment listener (zap_monitor.py)
# which handles LNURLp invoice.extra["nostr"] zap requests. This ensures
# accurate tracking via actual payment settlement rather than Nostr events.
# 
# Historical context: Kind 9735 (zap receipt) events were previously processed
# here, but this created duplicate processing issues and timing problems.
# The payment listener path is more reliable and accurate.


async def _recover_missed_reposts_and_reactions_on_startup(app):
    """Recover missed reposts and reactions on startup by querying recent kind 6 and 7 events.

    Note: zap receipts (kind 9735) are intentionally excluded here — zaps are processed
    via the invoice/listener path only.
    """
    try:
        if os.getenv('DISABLE_MISSED_REPOST_RECOVERY', 'false').lower() == 'true':
            return
        
        # Get all users with repost tracking enabled
        from .. import crud
        import importlib
        get_users = None
        try:
            core_mod = importlib.import_module('lnbits.core.crud')
            get_users = getattr(core_mod, 'get_users', None)
        except Exception:
            try:
                from lnbits.core.crud import get_users as _gu  # type: ignore
                get_users = _gu
            except Exception:
                get_users = None

        users = await get_users() if asyncio.iscoroutinefunction(get_users) else []
        for user in users:
            try:
                settings = await crud.get_settings(user.id)
                if not (getattr(settings, 'repost_tracking_enabled', False) or getattr(settings, 'likes_tracking_enabled', False)):
                    continue
                
                await _recover_missed_reposts_and_reactions_for_user(user.id, settings, app)
            except Exception as e:
                logger.warning(f"Error recovering reposts and reactions for user {user.id}: {e}")
                
    except Exception as e:
        logger.warning(f"Error in repost/reaction recovery: {e}")


async def _recover_missed_reposts_and_reactions_for_user(user_id: str, settings, app):
    """Recover missed reposts and reactions for a specific user.
    
    Uses #e-only filters (no #t requirement) to catch ALL reposts/reactions
    on tracked events. Supports multi-#e batching for efficient queries.
    
    Note: Zap receipts (kind 9735) are intentionally excluded — zaps are
    processed via the invoice/listener path only.
    
    UTC-FIRST: Uses UTC midnight for consistent "since" timestamp across timezones.
    Events are filtered from the start of the current UTC day, ensuring reliable
    recovery regardless of user timezone or DST transitions.
    """
    try:
        # Check if nostr_helpers is available
        available = nostr_helpers.check_availability()
        _record_availability_check(available)
        
        if not available:
            logger.warning(f"Nostr helpers not available for repost/reaction recovery (user {user_id})")
            return
        
        # UTC-FIRST: Get day boundaries for consistent time filtering
        # Use UTC midnight as the "since" timestamp to ensure we query from
        # the start of the current UTC day (matches Nostr event timestamps)
        boundaries = _get_today_boundaries_utc()
        since_ts = boundaries.utc_since_ts
        
        # Get tracked event IDs (these are the events we want to find reactions/reposts for)
        tracked_event_ids = getattr(settings, 'tracked_event_ids', []) or []
        
        if not tracked_event_ids:
            _dbg(f"No tracked event IDs for user {user_id}, skipping recovery")
            return

        cache = _get_cache(app)
        
        # Determine which kinds to query
        kinds_to_query = []
        if getattr(settings, 'repost_tracking_enabled', False):
            kinds_to_query.append(6)
        if getattr(settings, 'likes_tracking_enabled', False):
            kinds_to_query.append(7)
        
        if not kinds_to_query:
            _dbg(f"No repost/reaction tracking enabled for user {user_id}")
            return
        
        # Build filter using #e tags only (no #t requirement for reposts/reactions)
        # Split into batches if we have many tracked event IDs (Nostr relays may limit filter size)
        batch_size = 100  # Conservative batch size
        all_events = []
        
        for i in range(0, len(tracked_event_ids), batch_size):
            batch_ids = tracked_event_ids[i:i + batch_size]
            
            # Query via nostr_helpers with #e-only filter
            filter_dict = {
                "kinds": kinds_to_query,
                "#e": batch_ids,
                "since": since_ts
            }
            
            try:
                events = await nostr_helpers.query_events(
                    filter_dict,
                    limit=500,
                    timeout=10.0
                )
                _record_helper_query(True)
                
                if events:
                    all_events.extend(events)
                    _dbg(f"Recovered {len(events)} events in batch {i // batch_size + 1} for user {user_id}")
                    
            except Exception as e:
                _record_helper_query(False, str(e))
                logger.warning(f"Nostr helper query failed for batch {i // batch_size + 1} (user {user_id}): {e}")
        
        # Remove duplicates (events might appear in multiple batches if IDs overlap)
        seen_ids: set[str] = set()
        unique_events = []
        for event in all_events:
            event_id = event.get('id')
            if not event_id or event_id in seen_ids:
                continue
            seen_ids.add(event_id)
            try:
                if await crud.is_event_processed(user_id, event_id):
                    _dbg(f"Skipping already processed event {event_id[:16]} for user {user_id}")
                    continue
            except Exception:
                pass
            unique_events.append(event)
        
        logger.info(f"Recovered {len(unique_events)} repost/reaction events for user {user_id} via #e filters")
        
        # Process each event recovered via #e filters
        for event in unique_events:
            try:
                # Use recovery_mode=True to prevent publishing notes for historical events
                await process_event_for_user(user_id, event, settings, app, recovery_mode=True)
            except Exception as e:
                logger.warning(f"Error processing recovered event {event.get('id')} for user {user_id}: {e}")

        # --- Additional broad sweep for content-only reposts (no #e tag) ---
        try:
            # Look back 24 hours from local midnight (fallback to UTC if necessary)
            try:
                lookback_since = max(_local_midnight_timestamp() - 24 * 3600, 0)
            except Exception:
                from datetime import datetime, timezone
                lookback_since = int(datetime.now(timezone.utc).timestamp()) - 24 * 3600

            # Broad query for kind 6 reposts in recent window
            broad_events = []
            try:
                broad_events = await nostr_helpers.query_events(
                    {"kinds": [6], "since": lookback_since},
                    limit=2000,
                    timeout=8.0,
                )
                _record_helper_query(True)
            except Exception as e:
                _record_helper_query(False, str(e))
                logger.warning(f"Broad kind=6 sweep failed for user {user_id}: {e}")

            if broad_events:
                candidate_events = []
                for ev in broad_events:
                    try:
                        identifiers = _collect_reference_identifiers(ev, include_content=True)
                    except Exception:
                        identifiers = []
                    matched = False
                    for ident in identifiers:
                        try:
                            resolved_id, _meta = await _resolve_tracked_event(settings, ident, cache, app)
                        except Exception:
                            resolved_id = None
                        if resolved_id and resolved_id in tracked_event_ids:
                            matched = True
                            break
                    if matched:
                        candidate_events.append(ev)

                # Only process events we have not already seen
                new_events = []
                for ev in candidate_events:
                    eid = ev.get('id')
                    if not eid or eid in seen_ids:
                        continue
                    seen_ids.add(eid)
                    try:
                        if await crud.is_event_processed(user_id, eid):
                            _dbg(f"Skipping already processed content-only repost {eid[:16]} for user {user_id}")
                            continue
                    except Exception:
                        pass
                    new_events.append(ev)

                logger.info(f"Recovered {len(new_events)} content-only reposts for user {user_id}")

                # Process only the newly discovered content-only reposts
                for event in new_events:
                    try:
                        await process_event_for_user(user_id, event, settings, app, recovery_mode=True)
                    except Exception as e:
                        logger.warning(f"Error processing recovered event {event.get('id')} for user {user_id}: {e}")
        except Exception as e:
            logger.warning(f"Error during broad kind=6 recovery sweep for user {user_id}: {e}")
                
    except Exception as e:
        logger.warning(f"Error recovering reposts/reactions via nostr_helpers for user {user_id}: {e}")
