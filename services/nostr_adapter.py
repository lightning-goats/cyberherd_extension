"""Cyberherd Nostr subscription adapter with improved WebSocket connectivity.

Enhanced version that can use direct WebSocket connection to nostrclient
for better reliability, similar to nwcprovider pattern.

Features:
- Direct WebSocket connection option to /nostrclient/api/v1/relay
- Robust reconnection logic with rate limiting
- Proper EOSE/CLOSED message handling
- Fallback to relay_manager API if WebSocket fails
- Maintains backward compatibility with existing implementation

Filter Strategy (Updated October 2025):
- Kind 1 (Notes): TWO filters for comprehensive coverage:
  * Filter 1a: author + #t tags (catches tagged notes)
  * Filter 1b: author only (catches ALL author's notes, even without tags)
- Kind 6 (Reposts): ONLY #e tags (no #t requirement) - catches all reposts of tracked events
- Kind 7 (Reactions): ONLY #e tags (no #t requirement) - catches all reactions to tracked events

All relay_manager access is routed through nostr_helpers for:
- add_subscription()
- close_subscription()
- create_message_pool_poller()
- get_relay_info()
- check_availability()
- query_events()
"""
from __future__ import annotations

import asyncio
import json
import os
import secrets
from datetime import datetime, timezone
from typing import Any, Dict, Optional, cast

from loguru import logger

from .subscriptions import (
    _append_today,
    _get_cache,
    _local_midnight_timestamp,
    _refresh_event,
    get_subscription_status,
    get_effective_pubkey,
    _get_today_boundaries_utc,
)  # reuse helpers
from .. import crud
from . import nostr_helpers

# Debug flag (shared with subscriptions module if set)
CYBERHERD_DEBUG = os.getenv("CYBERHERD_DEBUG", "false").lower() in ("1","true","yes","y")
CYBERHERD_DIAG = os.getenv("CYBERHERD_DIAG", "false").lower() in ("1","true","yes","y")

# New configuration options
CYBERHERD_USE_WEBSOCKET = os.getenv("CYBERHERD_USE_WEBSOCKET", "true").lower() in ("1", "true", "yes", "y")
CYBERHERD_WS_RECONNECT_DELAY = int(os.getenv("CYBERHERD_WS_RECONNECT_DELAY", "5") or 5)
CYBERHERD_WS_MAX_RECONNECT_ATTEMPTS = int(os.getenv("CYBERHERD_WS_MAX_RECONNECT_ATTEMPTS", "10") or 10)
CYBERHERD_BROAD_REPOST_LIMIT = int(os.getenv("CYBERHERD_BROAD_REPOST_LIMIT", "800") or 800)


def _dbg(msg: str, *args):
    if CYBERHERD_DEBUG:
        try:
            logger.opt(lazy=True).debug("Cyberherd(nostr_adapter): " + msg.format(*args))
        except Exception:
            pass

# Globals for adapter
_adapter_started = False
_subscriptions: Dict[str, Dict[str, Any]] = {}  # sub_id -> metadata
_websocket_connections: Dict[str, Dict[str, Any]] = {}  # user_id -> ws connection info
_last_seen: Dict[str, int] = {}  # key: user_id or 'None' -> last seen created_at
_first_cycle: Dict[str, bool] = {}  # user key -> first cycle pending

# Diagnostics counters (aggregate)
_diag_counts = {
    'events_total': 0,
    'events_matched': 0,
    'events_filtered_author': 0,
    'events_filtered_tag': 0,
    'events_filtered_window': 0,
    'eose_total': 0,
    'fallback_resubs': 0,
}

def get_diagnostics_snapshot():
    if not CYBERHERD_DIAG:
        return { 'enabled': False }
    # Use a flexible-typed snapshot so we can add heterogeneous diagnostic fields
    snap: Dict[str, Any] = dict(_diag_counts)
    extra: Dict[str, Any] = {
        'subscription_count': len(_subscriptions),
        'user_subscriptions': sorted({m['user_id'] for m in _subscriptions.values()}),
        'websocket_connections': len(_websocket_connections),
        'websocket_users': sorted(_websocket_connections.keys()),
        'websocket_enabled': CYBERHERD_USE_WEBSOCKET,
        'reconnect_delay': CYBERHERD_WS_RECONNECT_DELAY,
        'max_reconnect_attempts': CYBERHERD_WS_MAX_RECONNECT_ATTEMPTS,
    }
    snap.update(extra)
    return snap

async def diagnose_subscription_issues(app):
    """Comprehensive diagnostic function for subscription issues.

    This function checks all components of the subscription system and provides
    specific recommendations for fixing issues.

    Usage:
        from lnbits.extensions.cyberherd.services.nostr_adapter import diagnose_subscription_issues
        diag = await diagnose_subscription_issues(app)
        print(diag)
    """
    diag = {
        'timestamp': datetime.now().isoformat(),
        'nostrclient_available': False,
        'relay_manager_available': False,
        'message_pool_available': False,
        'active_subscriptions': len(_subscriptions),
        'subscription_details': list_active_subscriptions(),
        'relay_status': {},
        'websocket_connections': len(_websocket_connections),
        'websocket_enabled': CYBERHERD_USE_WEBSOCKET,
        'import_errors': [],
        'recommendations': []
    }

    # Test nostrclient import
    try:
        from lnbits.extensions.nostrclient.router import nostr_client
        diag['nostrclient_available'] = True

        if hasattr(nostr_client, 'relay_manager'):
            diag['relay_manager_available'] = True

            # Check relays
            try:
                relays = getattr(nostr_client.relay_manager, 'relays', {})
                diag['relay_status'] = {
                    'count': len(relays),
                    'urls': list(relays.keys()),
                    'connected': [url for url, relay in relays.items() if getattr(relay, 'connected', False)]
                }
            except Exception as e:
                diag['relay_status'] = {'error': str(e)}

            # Check message pool
            if hasattr(nostr_client.relay_manager, 'message_pool'):
                diag['message_pool_available'] = True
            else:
                diag['import_errors'].append("message_pool not available")

        else:
            diag['import_errors'].append("relay_manager not available")

    except Exception as e:
        diag['import_errors'].append(f"nostrclient import failed: {str(e)}")

    # Generate recommendations
    if not diag['nostrclient_available']:
        diag['recommendations'].append("CRITICAL: Nostrclient extension not available - check if it's installed and enabled")
        diag['recommendations'].append("  - Verify nostrclient extension is in your extensions list")
        diag['recommendations'].append("  - Check LNbits logs for nostrclient initialization errors")
    elif not diag['relay_manager_available']:
        diag['recommendations'].append("CRITICAL: Relay manager not available - nostrclient may not be properly initialized")
        diag['recommendations'].append("  - Restart LNbits to ensure proper extension loading order")
    elif diag['relay_status'].get('count', 0) == 0:
        diag['recommendations'].append("WARNING: No relays configured - add relays in nostrclient settings")
        diag['recommendations'].append("  - Go to LNbits admin panel -> Extensions -> Nostrclient")
        diag['recommendations'].append("  - Add relay URLs like: wss://relay.damus.io, wss://nos.lol")
    elif len(diag['relay_status'].get('connected', [])) == 0:
        diag['recommendations'].append("WARNING: No relays connected - check relay URLs and network connectivity")
        diag['recommendations'].append("  - Verify relay URLs are accessible from your server")
        diag['recommendations'].append("  - Check firewall settings for WebSocket connections (port 443)")
    elif diag['active_subscriptions'] == 0:
        diag['recommendations'].append("INFO: No active subscriptions - check user settings and zap tracking configuration")
        diag['recommendations'].append("  - Verify cyberherd settings have zap_tracking_enabled = true")
        diag['recommendations'].append("  - Check that tracked_tags and effective pubkey are configured")
        diag['recommendations'].append("  - Ensure source_wallet and herd_wallet are set")

    # WebSocket-specific recommendations
    if CYBERHERD_USE_WEBSOCKET:
        if diag['websocket_connections'] == 0 and diag['active_subscriptions'] > 0:
            diag['recommendations'].append("WARNING: WebSocket enabled but no WebSocket connections active")
            diag['recommendations'].append("  - Check if websockets library is installed: pip install websockets")
            diag['recommendations'].append("  - Verify LNbits port configuration for WebSocket URL")
            diag['recommendations'].append("  - Check firewall settings for localhost WebSocket connections")
        elif diag['websocket_connections'] > 0:
            diag['recommendations'].append("INFO: WebSocket connections active - this should provide better reliability")
    else:
        diag['recommendations'].append("INFO: WebSocket connections disabled - using relay_manager API")
        diag['recommendations'].append("  - Consider enabling WebSocket with CYBERHERD_USE_WEBSOCKET=true for better reliability")

    # Add polling fallback status
    use_polling = os.getenv("CYBERHERD_POLLING_FALLBACK", "true").lower() in ("1", "true", "yes", "y")
    diag['polling_fallback_enabled'] = use_polling
    if use_polling:
        diag['recommendations'].append("INFO: Polling fallback is enabled - realtime detection should work via polling if subscriptions fail")

    return diag


async def auto_fix_subscription_issues(app):
    """Automatically attempt to fix common subscription issues.

    This function tries to resolve issues found by diagnose_subscription_issues().
    It will:
    1. Restart subscriptions if they exist but aren't working
    2. Reinitialize relay connections if needed
    3. Enable polling fallback if subscriptions fail
    4. Provide status of fixes attempted

    Returns:
        dict: Status of fixes attempted and their results
    """
    logger.info("Starting subscription fix process")

    fixes_attempted = {
        'timestamp': datetime.now().isoformat(),
        'fixes_attempted': [],
        'fixes_successful': [],
        'fixes_failed': [],
        'current_status': {}
    }

    try:
        # First diagnose the issues
        diag = await diagnose_subscription_issues(app)
        fixes_attempted['initial_diagnosis'] = diag

        # Fix 1: Try to restart subscriptions
        if diag['active_subscriptions'] > 0:
            logger.debug("Attempting to restart existing subscriptions")
            fixes_attempted['fixes_attempted'].append("restart_subscriptions")

            try:
                # Stop existing subscriptions
                await stop_all_subscriptions()
                await asyncio.sleep(1)  # Brief pause

                # Restart them
                await start_subscriptions_for_all_users(app)
                fixes_attempted['fixes_successful'].append("restart_subscriptions")
                logger.info("Successfully restarted subscriptions")
            except Exception as e:
                fixes_attempted['fixes_failed'].append(f"restart_subscriptions: {str(e)}")
                logger.error(f"Failed to restart subscriptions: {e}")

        # Fix 2: Try to reinitialize relay connections
        if diag['nostrclient_available'] and diag['relay_manager_available']:
            logger.debug("Attempting to reinitialize relay connections")
            fixes_attempted['fixes_attempted'].append("reinitialize_relays")

            try:
                from lnbits.extensions.nostrclient.router import nostr_client

                # Try to reconnect relays
                # Use getattr + callable check to keep static type checkers happy
                conn_fn = getattr(nostr_client.relay_manager, 'connect_relays', None)
                if callable(conn_fn):
                    try:
                        result = conn_fn()
                        if asyncio.iscoroutine(result):
                            await result
                    except Exception as e:
                        logger.warning(f"Error calling relay_manager.connect_relays(): {e}")
                    fixes_attempted['fixes_successful'].append("reinitialize_relays")
                    logger.info("Successfully reinitialized relay connections")
                else:
                    fixes_attempted['fixes_failed'].append("reinitialize_relays: connect_relays method not available")
            except Exception as e:
                fixes_attempted['fixes_failed'].append(f"reinitialize_relays: {str(e)}")
                logger.error(f"Failed to reinitialize relays: {e}")

        # Fix 3: Ensure polling fallback is enabled
        if not diag.get('polling_fallback_enabled', False):
            logger.info("Enabling polling fallback for reliability")
            fixes_attempted['fixes_attempted'].append("enable_polling_fallback")

            try:
                os.environ["CYBERHERD_POLLING_FALLBACK"] = "true"
                fixes_attempted['fixes_successful'].append("enable_polling_fallback")
                logger.info("Successfully enabled polling fallback")
            except Exception as e:
                fixes_attempted['fixes_failed'].append(f"enable_polling_fallback: {str(e)}")

        # Fix 4: Start subscriptions if none exist but should
        if diag['active_subscriptions'] == 0 and diag['nostrclient_available']:
            logger.debug("Attempting to start subscriptions for all users")
            fixes_attempted['fixes_attempted'].append("start_missing_subscriptions")

            try:
                await start_subscriptions_for_all_users(app)
                fixes_attempted['fixes_successful'].append("start_missing_subscriptions")
                logger.debug("Successfully started subscriptions for users")
            except Exception as e:
                fixes_attempted['fixes_failed'].append(f"start_missing_subscriptions: {str(e)}")
                logger.error(f"Failed to start subscriptions: {e}")

        # Get final status
        final_diag = await diagnose_subscription_issues(app)
        fixes_attempted['final_diagnosis'] = final_diag
        fixes_attempted['current_status'] = {
            'active_subscriptions': final_diag['active_subscriptions'],
            'relays_connected': len(final_diag['relay_status'].get('connected', [])),
            'polling_enabled': final_diag.get('polling_fallback_enabled', False)
        }

        logger.info(f"Auto-fix process completed. Successful: {len(fixes_attempted['fixes_successful'])}, Failed: {len(fixes_attempted['fixes_failed'])}")

    except Exception as e:
        fixes_attempted['fixes_failed'].append(f"general_error: {str(e)}")
        logger.error(f"Auto-fix process failed with error: {e}")

    return fixes_attempted


# WebSocket Connection Methods (Enhanced reliability like nwcprovider)

async def _connect_websocket_for_user(user_id: str, settings, app) -> bool:
    """Establish direct WebSocket connection to nostrclient relay endpoint.

    This method mimics nwcprovider's robust connection pattern for better reliability.
    """
    try:
        import websockets
        from websockets.exceptions import ConnectionClosedError, WebSocketException
    except ImportError:
        logger.warning("websockets library not available, falling back to relay_manager")
        return False

    # Get the port from app settings or environment
    port = getattr(app.state, 'port', None) or os.getenv('LNBITS_PORT', '5000')
    ws_url = f"ws://localhost:{port}/nostrclient/api/v1/relay"

    sub_id = secrets.token_hex(6)
    reconnect_attempts = 0

    while reconnect_attempts < CYBERHERD_WS_MAX_RECONNECT_ATTEMPTS:
        try:
            _dbg(f"Attempting WebSocket connection for user {user_id} to {ws_url}")
            websocket = await websockets.connect(ws_url, ping_interval=30, ping_timeout=10)

            # Store connection info
            _websocket_connections[user_id] = {
                'websocket': websocket,
                'sub_id': sub_id,
                'connected_at': datetime.now(timezone.utc),
                'reconnect_attempts': reconnect_attempts
            }

            # Start message handling task
            asyncio.create_task(_handle_websocket_messages(user_id, websocket, settings, app))

            # Send subscription request
            await _send_websocket_subscription(user_id, websocket, settings, app)

            logger.info(f"WebSocket connection established for user {user_id} with sub_id {sub_id}")
            return True

        except (ConnectionClosedError, WebSocketException, OSError) as e:
            reconnect_attempts += 1
            delay = min(CYBERHERD_WS_RECONNECT_DELAY * (2 ** reconnect_attempts), 300)  # Exponential backoff, max 5min
            logger.warning(f"WebSocket connection failed for user {user_id} (attempt {reconnect_attempts}/{CYBERHERD_WS_MAX_RECONNECT_ATTEMPTS}): {e}")
            if reconnect_attempts < CYBERHERD_WS_MAX_RECONNECT_ATTEMPTS:
                await asyncio.sleep(delay)
            else:
                logger.error(f"Max reconnection attempts reached for user {user_id}")
                return False
        except Exception as e:
            logger.error(f"Unexpected error connecting WebSocket for user {user_id}: {e}")
            return False

    return False


async def _handle_websocket_messages(user_id: str, websocket, settings, app):
    """Handle incoming WebSocket messages with proper EOSE/CLOSED processing."""
    try:
        async for message in websocket:
            try:
                data = json.loads(message)
                await _process_websocket_message(user_id, data, settings, app)
            except json.JSONDecodeError:
                logger.warning(f"Invalid JSON message received for user {user_id}: {message}")
            except Exception as e:
                logger.error(f"Error processing WebSocket message for user {user_id}: {e}")

    except Exception as e:
        logger.error(f"WebSocket message handling error for user {user_id}: {e}")
        # Trigger reconnection
        if user_id in _websocket_connections:
            del _websocket_connections[user_id]
        # Attempt to reconnect after a delay
        await asyncio.sleep(CYBERHERD_WS_RECONNECT_DELAY)
        await _connect_websocket_for_user(user_id, settings, app)


async def _process_websocket_message(user_id: str, data: list, settings, app):
    """Process WebSocket messages (EVENT, EOSE, CLOSED, NOTICE)."""
    if not isinstance(data, list) or len(data) < 2:
        return

    msg_type = data[0]

    if msg_type == "EVENT":
        if len(data) >= 3:
            event = data[2]
            await _process_event_message(user_id, event, settings, app)

    elif msg_type == "EOSE":
        if len(data) >= 2:
            sub_id = data[1]
            await _process_eose_message(user_id, sub_id, settings, app)

    elif msg_type == "CLOSED":
        if len(data) >= 3:
            sub_id = data[1]
            reason = data[2] if len(data) > 2 else "Unknown"
            logger.info(f"Subscription {sub_id} closed for user {user_id}: {reason}")
            # Handle subscription closure (could trigger resubscription)

    elif msg_type == "NOTICE":
        if len(data) >= 2:
            notice = data[1]
            logger.info(f"NOTICE for user {user_id}: {notice}")

    else:
        _dbg(f"Unhandled message type {msg_type} for user {user_id}")


async def _process_event_message(user_id: str, event: dict, settings, app):
    """Process EVENT messages and filter for relevant zaps."""
    try:
        # Ignore zap receipts (NIP-57) here — they are handled
        # via the lnurlp invoice-listener preferred path.
        try:
            if int(event.get('kind') or 0) == 9735:
                return
        except Exception:
            pass
        # Reuse existing event processing logic from subscriptions module
        # But re-fetch latest settings for the user to ensure toggles
        # (repost_tracking_enabled / likes_tracking_enabled) are honored
        # in real-time instead of relying on the snapshot captured when
        # the subscription was created.
        from .subscriptions import process_event_for_user
        try:
            # Lazy import crud to avoid circulars at module import time
            from .. import crud as _crud
            latest_settings = await _crud.get_settings(user_id)
        except Exception:
            # Fall back to provided settings if DB fetch fails
            latest_settings = settings

        await process_event_for_user(user_id, event, latest_settings, app)
    except Exception as e:
        logger.error(f"Error processing event for user {user_id}: {e}")


async def _process_eose_message(user_id: str, sub_id: str, settings, app):
    """Process EOSE (End of Stored Events) messages."""
    ukey = user_id if user_id is not None else 'None'

    # Mark initial cycle as done
    _first_cycle[ukey] = False

    # Update subscription metadata
    if sub_id in _subscriptions:
        _subscriptions[sub_id]['initial_done'] = True

    # Check if we need fallback resubscription (no events received)
    if sub_id in _subscriptions and _subscriptions[sub_id]['appended'] == 0:
        logger.info(f"No events received for subscription {sub_id}, considering fallback")
        # Could implement fallback logic here similar to original

    logger.debug(f"EOSE received for subscription {sub_id} (user {user_id})")


async def _send_websocket_subscription(user_id: str, websocket, settings, app):
    """Send subscription request over WebSocket."""
    sub_id = _websocket_connections[user_id]['sub_id']

    overlap_seconds = int(os.getenv("CYBERHERD_SINCE_OVERLAP_SECONDS", "300") or 300)
    initial_limit = int(os.getenv("CYBERHERD_INITIAL_LIMIT", "500") or 500)
    broad_repost_limit = CYBERHERD_BROAD_REPOST_LIMIT

    base_since = _local_midnight_timestamp()
    # Prefer shared monotonic watermark from app.state when available
    st = getattr(app, 'state', app)
    shared = int(getattr(st, 'cyberherd_social_last_seen_ts', 0) or 0)
    local = int(_last_seen.get(user_id or 'None', 0) or 0)
    watermark = max(shared, local, base_since)
    since = max(base_since, watermark - overlap_seconds)

    # Build filters list
    filters = []

    # Strip # from tracked tags for Nostr filter (kept for cache normalization)
    stripped_tags = [t.lstrip('#') for t in (getattr(settings, 'tracked_tags', []) or [])]

    # Filter 1a: Notes (kind 1) authored by the effective pubkey with #t tag filter
    eff_pub = None
    try:
        eff_pub = get_effective_pubkey(settings)
    except Exception:
        eff_pub = getattr(settings, 'effective_pubkey', None)

    if eff_pub:
        if stripped_tags:
            notes_tagged_filter = {"kinds": [1], "#t": stripped_tags, "authors": [eff_pub], "since": since}
            if _first_cycle.get(user_id or 'None', True) and initial_limit > 0:
                notes_tagged_filter['limit'] = initial_limit
            filters.append(notes_tagged_filter)
        # Filter 1b: Notes (kind 1) authored by the effective pubkey WITHOUT #t filter (catches all author's notes)
        notes_author_filter = {"kinds": [1], "authors": [eff_pub], "since": since}
        if _first_cycle.get(user_id or 'None', True) and initial_limit > 0:
            notes_author_filter['limit'] = initial_limit
        filters.append(notes_author_filter)

    # Get tracked event IDs
    # Prefer today's detected note ids from the in-memory cache. Only open
    # engagement subscriptions when there are today note ids (filter by #e tags).
    try:
        cache = _get_cache(app)
        try:
            boundaries = _get_today_boundaries_utc()
            day = boundaries.utc_day_str
        except Exception:
            day = None
        eff = get_effective_pubkey(settings)
        tags_norm = tuple(sorted([t.lstrip('#').lower() for t in (getattr(settings, 'tracked_tags', []) or []) if t]))
        tracked_event_ids = []
        if day is not None:
            keys = [(day, getattr(settings, 'user_id', None), eff, tags_norm), (day, None, eff, tags_norm)]
            seen = set()
            for k in keys:
                try:
                    lst = cache.get(k) or []
                    for e in lst:
                        if isinstance(e, str) and e not in seen:
                            seen.add(e)
                except Exception:
                    continue
            tracked_event_ids = list(seen)
        # Fallback: if no today ids in cache, also consider persisted tracked_event_ids
        if not tracked_event_ids:
            tracked_event_ids = getattr(settings, 'tracked_event_ids', []) or []
    except Exception as _e:
        # On any error reading the runtime cache, fall back to persisted configured IDs
        logger.debug(f"Could not read today cache for engagement filters: {_e}")
        tracked_event_ids = getattr(settings, 'tracked_event_ids', []) or []
    
    # Filter 2: Reposts/reactions (kinds 6/7) - ONLY #e filter, NO #t restriction
    react_kinds = []
    if getattr(settings, 'repost_tracking_enabled', False):
        react_kinds.append(6)
    if getattr(settings, 'likes_tracking_enabled', False):
        react_kinds.append(7)
    
    # Only create engagement filter when we have event ids to track (prefer today ids)
    if react_kinds and tracked_event_ids:
        # Only filter by #e (tracked event IDs) - no #t tag requirement for reposts/reactions
        react_filter = {"kinds": react_kinds, "#e": tracked_event_ids, "since": since}
        filters.append(react_filter)
        # Safety-net: also subscribe to broad kind=6 (reposts) without #e so we can
        # detect content-only reposts client-side. Use a larger limit for the broad
        # filter and rely on client-side filtering to reduce false positives.
        try:
            if 6 in react_kinds and getattr(settings, 'repost_tracking_enabled', False):
                # Only add broad repost safety-net when we have at least one tracked id
                broad_repost_filter = {"kinds": [6], "since": since, "limit": CYBERHERD_BROAD_REPOST_LIMIT}
                filters.append(broad_repost_filter)
        except Exception:
            pass

    # Send REQ message with multiple filters
    req_message = ["REQ", sub_id] + filters
    await websocket.send(json.dumps(req_message))

    # Store subscription metadata
    _subscriptions[sub_id] = {
        'user_id': user_id,
        'tags': [t.lower() for t in stripped_tags],
        'eff_pub': settings.effective_pubkey,
        'fallback': False,
        'appended': 0,
        'initial_done': False,
        'since': since,
        'websocket': True
    }

    logger.info(f"Sent WebSocket subscription {sub_id} for user {user_id}")


async def _disconnect_websocket_for_user(user_id: str):
    """Cleanly disconnect WebSocket for a user."""
    if user_id in _websocket_connections:
        try:
            ws_info = _websocket_connections[user_id]
            websocket = ws_info['websocket']
            await websocket.close()
            logger.info(f"WebSocket disconnected for user {user_id}")
        except Exception as e:
            logger.error(f"Error disconnecting WebSocket for user {user_id}: {e}")
        finally:
            del _websocket_connections[user_id]


async def start_subscription_for_user(user_id: str, settings, app):
    """Start a subscription for a specific user with their settings.

    Uses WebSocket connection by default for better reliability (like nwcprovider),
    with fallback to relay_manager API if WebSocket fails.
    """
    # Check if user already has a subscription
    existing = [(sid, meta) for sid, meta in _subscriptions.items() if meta['user_id'] == user_id]
    if existing:
        logger.info(f"User {user_id} already has subscription {existing[0][0]}")
        return True

    # Try WebSocket connection first if enabled
    if CYBERHERD_USE_WEBSOCKET:
        logger.debug("Attempting WebSocket connection for user %s", user_id)
        if await _connect_websocket_for_user(user_id, settings, app):
            return True
        else:
            logger.warning(f"WebSocket connection failed for user {user_id}, falling back to relay_manager")

    # Fallback to relay_manager API via nostr_helpers
    if not nostr_helpers.check_availability():
        logger.error(f"Cannot start subscription for user {user_id}: nostrclient not available")
        return False

    # Create subscription for user using nostr_helpers
    sub_id = secrets.token_hex(6)
    ukey = user_id if user_id is not None else 'None'

    overlap_seconds = int(os.getenv("CYBERHERD_SINCE_OVERLAP_SECONDS", "300") or 300)
    initial_limit = int(os.getenv("CYBERHERD_INITIAL_LIMIT", "500") or 500)

    base_since = _local_midnight_timestamp()
    st = getattr(app, 'state', app)
    shared = int(getattr(st, 'cyberherd_social_last_seen_ts', 0) or 0)
    local = int(_last_seen.get(ukey) or 0 or 0)
    watermark = max(shared, local, base_since)
    since = max(base_since, watermark - overlap_seconds)

    # Build filters
    filters = []
    
    # Strip # from tracked tags for Nostr filter
    stripped_tags = [t.lstrip('#') for t in (getattr(settings, 'tracked_tags', []) or [])]
    
    # Filter 1a: Notes (kind 1) with #t tags by effective pubkey
    if stripped_tags:
        notes_tagged_filter = {"kinds": [1], "#t": stripped_tags, "authors": [settings.effective_pubkey], "since": since}
        if _first_cycle.get(ukey, True) and initial_limit > 0:
            notes_tagged_filter['limit'] = initial_limit
        filters.append(notes_tagged_filter)
    
    # Filter 1b: Notes (kind 1) by effective pubkey WITHOUT #t filter (catches all author's notes)
    notes_author_filter = {"kinds": [1], "authors": [settings.effective_pubkey], "since": since}
    if _first_cycle.get(ukey, True) and initial_limit > 0:
        notes_author_filter['limit'] = initial_limit
    filters.append(notes_author_filter)

    # Get tracked event IDs: prefer today's detected note ids from the runtime cache.
    try:
        cache = _get_cache(app)
        try:
            boundaries = _get_today_boundaries_utc()
            day = boundaries.utc_day_str
        except Exception:
            day = None
        eff = get_effective_pubkey(settings)
        tags_norm = tuple(sorted([t.lstrip('#').lower() for t in (getattr(settings, 'tracked_tags', []) or []) if t]))
        tracked_event_ids = []
        if day is not None:
            keys = [(day, getattr(settings, 'user_id', None), eff, tags_norm), (day, None, eff, tags_norm)]
            seen = set()
            for k in keys:
                try:
                    lst = cache.get(k) or []
                    for e in lst:
                        if isinstance(e, str) and e not in seen:
                            seen.add(e)
                except Exception:
                    continue
            tracked_event_ids = list(seen)
        if not tracked_event_ids:
            tracked_event_ids = getattr(settings, 'tracked_event_ids', []) or []
    except Exception as _e:
        logger.debug(f"Could not read today cache for engagement filters (websocket path): {_e}")
        tracked_event_ids = getattr(settings, 'tracked_event_ids', []) or []
    
    # Filter 2: Reposts/reactions (kinds 6/7) - ONLY #e filter, NO #t restriction
    react_kinds = []
    if getattr(settings, 'repost_tracking_enabled', False):
        react_kinds.append(6)
    if getattr(settings, 'likes_tracking_enabled', False):
        react_kinds.append(7)
    
    if react_kinds and tracked_event_ids:
        # Only filter by #e (tracked event IDs) - no #t tag requirement for reposts/reactions
        react_filter = {"kinds": react_kinds, "#e": tracked_event_ids, "since": since}
        filters.append(react_filter)
        # Add broad kind=6 safety-net subscription for content-only repost detection
        try:
            if 6 in react_kinds and getattr(settings, 'repost_tracking_enabled', False):
                broad_repost_filter = {"kinds": [6], "since": since, "limit": CYBERHERD_BROAD_REPOST_LIMIT}
                filters.append(broad_repost_filter)
        except Exception:
            pass

    # Add subscription via nostr_helpers
    if not nostr_helpers.add_subscription(sub_id, filters):
        logger.error(f"Failed to add subscription {sub_id} for user {user_id}")
        return False

    logger.info(f"Created relay_manager subscription {sub_id} for user {user_id}")

    _subscriptions[sub_id] = {
        'user_id': user_id,
        'tags': [t.lower() for t in stripped_tags],
        'eff_pub': settings.effective_pubkey,
        'fallback': False,
        'appended': 0,
        'initial_done': False,
        'since': since,
        'websocket': False,
        # keep quick flags for event_pump
        'repost_enabled': getattr(settings, 'repost_tracking_enabled', False),
        'likes_enabled': getattr(settings, 'likes_tracking_enabled', False),
    }

    return True


async def stop_all_subscriptions():
    """Stop all active subscriptions (both WebSocket and relay_manager)."""
    logger.info(f"Stopping {len(_subscriptions)} active subscriptions and {len(_websocket_connections)} WebSocket connections")

    # Stop WebSocket connections
    for user_id in list(_websocket_connections.keys()):
        await _disconnect_websocket_for_user(user_id)

    # Stop relay_manager subscriptions via nostr_helpers
    for sub_id, sub_info in _subscriptions.items():
        try:
            if not sub_info.get('websocket', False):
                # Close relay_manager subscriptions using nostr_helpers
                nostr_helpers.close_subscription(sub_id)
            logger.debug(f"Stopped subscription {sub_id}")
        except Exception as e:
            logger.error(f"Error stopping subscription {sub_id}: {e}")

    _subscriptions.clear()
    logger.info("All subscriptions stopped")


async def start_subscriptions_for_all_users(app):
    """Start subscriptions for all users with zap tracking enabled."""
    from lnbits.extensions.cyberherd.crud import get_cyberherd_settings_for_all_users

    logger.info("Starting subscriptions for all users with zap tracking enabled")

    try:
        # Get all users with cyberherd settings
        all_settings = await get_cyberherd_settings_for_all_users()

        started_count = 0
        for settings in all_settings:
            eff = get_effective_pubkey(settings)
            # Start if there is an effective pubkey and any tracking is enabled
            if eff and (
                getattr(settings, 'tracked_tags', None)
                or getattr(settings, 'repost_tracking_enabled', False)
                or getattr(settings, 'likes_tracking_enabled', False)
                or getattr(settings, 'zap_tracking_enabled', False)
            ):
                try:
                    # Only start subscription when a user_id is present
                    if getattr(settings, 'user_id', None):
                        await start_subscription_for_user(cast(str, settings.user_id), settings, app)
                        started_count += 1
                        logger.debug(f"Started subscription for user {settings.user_id}")
                    else:
                        logger.debug("Skipping subscription start: missing user_id in settings")
                except Exception as e:
                    logger.error(f"Failed to start subscription for user {settings.user_id}: {e}")

        logger.info(f"Started subscriptions for {started_count} users")

    except Exception as e:
        logger.error(f"Error starting subscriptions for all users: {e}")
        raise

def list_active_subscriptions():
    """Return lightweight list of active subscription filters (diagnostics)."""
    data = []
    for sid, meta in _subscriptions.items():
        data.append({
            'subscription_id': sid,
            'user_id': meta.get('user_id'),
            'tags': meta.get('tags'),
            'appended': meta.get('appended'),
            'since': meta.get('since'),
            'fallback': meta.get('fallback'),
        })
    return data

async def force_requery_for_user(app, user_id: str | None):
    """Manually re-query current day's notes for a specific user (or global) and backfill cache.

    Uses the same filter logic as subscription (authors + #t) via nostr_helpers.query_events.
    Returns list of matching event ids appended.
    """
    try:
        from ..views_api import _get_cached_effective_pubkey as _eff
        from .. import crud
        from .subscriptions import _append_today, _get_cache, _local_midnight_timestamp
    except Exception as e:
        logger.warning(f"force_requery import error: {e}")
        return []
    try:
        settings = await crud.get_settings(user_id)
        eff = _eff(settings)
        raw_tags = [t.lstrip('#') for t in (getattr(settings, 'tracked_tags', []) or [])]
        tags_norm = sorted({t.lower() for t in raw_tags if t})
        if not eff or not tags_norm:
            return []
        since = _local_midnight_timestamp()
        # Build query filters
        filter_tags = list(dict.fromkeys([rt for rt in raw_tags if rt] + [rt.lower() for rt in raw_tags if rt]))
        
        # Filter 1: Kind 1 notes with author + #t tags
        kinds_1_filter = {"kinds": [1], "authors": [eff], "#t": filter_tags, "since": since}
        
        # Filter 2: Reposts/reactions (kinds 6/7) - ONLY #e filter, NO #t
        react_kinds = []
        if getattr(settings, 'repost_tracking_enabled', False):
            react_kinds.append(6)
        if getattr(settings, 'likes_tracking_enabled', False):
            react_kinds.append(7)
        
        # Query kind 1 notes first
        logger.info(f"Cyberherd force_requery: user={user_id} eff_pub={eff[:8]}... tags={tags_norm} since={since} query={kinds_1_filter}")
        events = await nostr_helpers.query_events(kinds_1_filter, limit=500, timeout=10.0)
        
        # Query reposts/reactions if enabled
        if react_kinds:
            tracked_event_ids = getattr(settings, 'tracked_event_ids', []) or []
            if tracked_event_ids:
                react_filter = {"kinds": react_kinds, "#e": tracked_event_ids, "since": since}
                logger.info(f"Cyberherd force_requery: user={user_id} querying reactions query={react_filter}")
                react_events = await nostr_helpers.query_events(react_filter, limit=500, timeout=10.0)
                events = (events or []) + (react_events or [])
        
        logger.info(f"Cyberherd force_requery: Found {len(events or [])} events for user {user_id}")
        cache = _get_cache(app)
        appended_ids = []
        for ev in events or []:
            try:
                if _append_today(cache, user_id, eff, tags_norm, ev):
                    if isinstance(ev.get('id'), str):
                        appended_ids.append(ev['id'])
            except Exception:
                pass
        if CYBERHERD_DIAG:
            _dbg("DIAG force_requery user=%s matched=%s", user_id, len(appended_ids))
        return appended_ids
    except Exception as e:
        logger.warning(f"force_requery error: {e}")
        return []

async def debug_check_event(app, event_id: str):
    """Fetch a specific event id via nostr_helpers and evaluate why it might be filtered.
    Returns diagnostic dict."""
    info = {"event_id": event_id, "found": False}
    try:
        # Query via nostr_helpers instead of nostr_lookup
        evs = await nostr_helpers.query_events({"ids": [event_id]}, limit=1, timeout=6.0)
        
        if not evs:
            return info
        ev = evs[0]
        info['found'] = True
        info['pubkey'] = ev.get('pubkey')
        info['tags'] = [t for t in ev.get('tags', []) if isinstance(t, list) and t[:1] == ['t']]
        info['created_at'] = ev.get('created_at')
        # Evaluate against each subscription
        sub_results = []
        for sid, meta in _subscriptions.items():
            verdict = {"subscription_id": sid, "user_id": meta.get('user_id'), "author_match": False, "tag_match": False, "window": False}
            try:
                eff_pub = meta.get('eff_pub')
                verdict['author_match'] = (ev.get('pubkey') == eff_pub)
                # time window
                base_since = meta.get('since') or 0
                ca = int(ev.get('created_at') or 0)
                verdict['window'] = (base_since <= ca < base_since + 86400)
                # tags
                ev_tags = []
                for t in ev.get('tags', []) or []:
                    if isinstance(t, list) and len(t) > 1 and t[0] == 't':
                        ev_tags.append(t[1].lstrip('#').lower())
                verdict['tag_match'] = any(t in ev_tags for t in (meta.get('tags') or []))
            except Exception:
                pass
            sub_results.append(verdict)
        info['subscription_evaluation'] = sub_results
    except Exception as e:
        info['error'] = str(e)
    return info

async def _polling_fallback_loop(app):
    """Polling-based fallback for realtime detection when subscriptions fail.

    This function provides a reliable alternative to websocket subscriptions by:
    1. Periodically querying relays for new notes using the same logic as recovery
    2. Processing any new notes found through the existing cache system
    3. Running alongside subscriptions (doesn't interfere if they work)
    4. Being more resilient to network issues and relay connectivity problems

    Environment variables:
    - CYBERHERD_POLLING_FALLBACK: Enable/disable polling (default: true)
    - CYBERHERD_POLLING_INTERVAL: Polling interval in seconds (default: 30)
    """
    polling_interval = int(os.getenv("CYBERHERD_POLLING_INTERVAL", "30"))  # seconds
    logger.info(f"Cyberherd: Starting polling fallback (interval: {polling_interval}s)")

    while True:
        try:
            # Get all users with zap tracking enabled
            contexts = await _load_contexts()
            users_to_check = {ctx['user_id'] for ctx in contexts}

            for user_id in users_to_check:
                try:
                    # Use the existing force_requery_for_user function
                    new_notes = await force_requery_for_user(app, user_id)
                    if new_notes:
                        logger.info(f"Cyberherd polling: Found {len(new_notes)} new notes for user {user_id}")
                        # The force_requery_for_user function already handles caching and processing
                except Exception as e:
                    logger.debug(f"Cyberherd polling: Error checking user {user_id}: {e}")

            # Wait before next polling cycle
            await asyncio.sleep(polling_interval)

        except Exception as e:
            logger.warning(f"Cyberherd polling: Error in polling loop: {e}")
            await asyncio.sleep(polling_interval)

async def _load_contexts():
    contexts: list[dict[str, Any]] = []
    try:
        rows = await crud.db.fetchall("SELECT * FROM settings")
    except Exception:
        rows = []
    from ..views_api import _get_cached_effective_pubkey as _eff
    if not rows:
        s = await crud.get_settings(None)
        eff = _eff(s)
        # Preserve original tag case for filter while keeping normalized list for cache/meta
        raw_tags = [t.lstrip('#') for t in (getattr(s, 'tracked_tags', []) or [])]
        tags_norm = sorted({t.lower() for t in raw_tags if t})
        # Expand filter tags with original + lowercase variants (deduplicated order stable)
        filter_tags = list(dict.fromkeys([rt for rt in raw_tags if rt] + [rt.lower() for rt in raw_tags if rt]))
        if filter_tags and eff:
            contexts.append({
                'user_id': None, 
                'settings': s, 
                'tags': tags_norm, 
                'filter_tags': filter_tags, 
                'eff_pub': eff, 
                'repost_tracking_enabled': getattr(s, 'repost_tracking_enabled', False),
                'likes_tracking_enabled': getattr(s, 'likes_tracking_enabled', False),
                'zap_tracking_enabled': getattr(s, 'zap_tracking_enabled', False)
            })
    else:
        for r in rows:
            uid = r.get('user_id')
            try:
                s = await crud.get_settings(uid)
            except Exception:
                continue
            eff = _eff(s)
            raw_tags = [t.lstrip('#') for t in (getattr(s, 'tracked_tags', []) or [])]
            tags_norm = sorted({t.lower() for t in raw_tags if t})
            filter_tags = list(dict.fromkeys([rt for rt in raw_tags if rt] + [rt.lower() for rt in raw_tags if rt]))
            if not filter_tags or not eff:
                continue
            contexts.append({
                'user_id': uid, 
                'settings': s, 
                'tags': tags_norm, 
                'filter_tags': filter_tags, 
                'eff_pub': eff, 
                'repost_tracking_enabled': getattr(s, 'repost_tracking_enabled', False),
                'likes_tracking_enabled': getattr(s, 'likes_tracking_enabled', False),
                'zap_tracking_enabled': getattr(s, 'zap_tracking_enabled', False)
            })
    return contexts

async def start_adapter(app):
    global _adapter_started
    if _adapter_started:
        return
    _adapter_started = True

    # Check if polling fallback is enabled (default: enabled)
    use_polling_fallback = os.getenv("CYBERHERD_POLLING_FALLBACK", "true").lower() in ("1", "true", "yes", "y")

    # Try subscription-based approach first
    try:
        asyncio.create_task(_manager_loop(app))
        asyncio.create_task(_event_pump(app))
        logger.info("Cyberherd: Started subscription-based realtime detection")
    except Exception as e:
        logger.warning(f"Cyberherd: Subscription system failed, falling back to polling: {e}")

    # Start polling fallback if enabled (runs alongside subscriptions if they work)
    if use_polling_fallback:
        asyncio.create_task(_polling_fallback_loop(app))

    # Start invoice-listener based zap detection (preferred path)
    try:
        use_invoice_listener = os.getenv("CYBERHERD_USE_INVOICE_LISTENER", "true").lower() in ("1","true","yes","y")
        if use_invoice_listener:
            try:
                # Import via importlib and getattr to avoid static import-time resolution issues
                import importlib

                mod = importlib.import_module('lnbits.extensions.cyberherd.tasks')
                start_invoice_listener = getattr(mod, 'start_invoice_listener', None)
                if callable(start_invoice_listener):
                    try:
                        coro = start_invoice_listener(app)
                        if asyncio.iscoroutine(coro):
                            asyncio.create_task(coro)
                            logger.info("Cyberherd: started invoice-listener zap detection (preferred)")
                        else:
                            logger.warning("Cyberherd: start_invoice_listener did not return a coroutine")
                    except Exception as e:
                        logger.warning(f"Error starting invoice listener: {e}")
                else:
                    logger.warning("Cyberherd: start_invoice_listener not found in tasks module")
            except Exception as e:
                logger.warning(f"Cyberherd: failed to start invoice listener: {e}")
    except Exception:
        pass

async def _manager_loop(app):
    """Reconcile desired user subscriptions with active ones.
    Each user -> one subscription; handles initial/fallback cycles.
    """
    refresh_seconds = int(os.getenv("CYBERHERD_USER_SETTINGS_REFRESH_SECONDS", "60") or 60)
    overlap_seconds = int(os.getenv("CYBERHERD_SINCE_OVERLAP_SECONDS", "300") or 300)
    initial_limit = int(os.getenv("CYBERHERD_INITIAL_LIMIT", "500") or 500)

    # Check nostrclient availability via nostr_helpers
    if not nostr_helpers.check_availability():
        logger.warning("Cyberherd: Nostrclient extension not available - subscriptions disabled")
        return

    st = getattr(app, 'state', app)
    status = getattr(st, 'cyberherd_subscription_status', {}) or {}
    
    # Get relay info via nostr_helpers
    relay_info = nostr_helpers.get_relay_info()
    current_relays = relay_info.get('relay_urls', [])
    logger.info(f"Cyberherd: Found {len(current_relays)} configured relays: {current_relays}")
    
    status.update({
        'relays': current_relays, 
        'mode': 'pooled', 
        'relay_workers': {}, 
        'users': [], 
        'user_count': 0
    })
    st.cyberherd_subscription_status = status

    global _refresh_event
    if _refresh_event is None:
        from .subscriptions import _refresh_event as legacy_event  # ensure event created
        _refresh_event = legacy_event or asyncio.Event()

    while True:
        try:
            # Check if forced refresh was requested (e.g., after tracked_event_ids initialization)
            force_refresh = False
            try:
                st = getattr(app, "state", app)
                if getattr(st, "cyberherd_force_subscription_refresh", False):
                    force_refresh = True
                    setattr(st, "cyberherd_force_subscription_refresh", False)
                    logger.info("Cyberherd: Processing forced subscription refresh")
            except Exception:
                pass
            
            contexts = await _load_contexts()
            desired_keys = {ctx['user_id'] for ctx in contexts}
            
            # Remove subscriptions for users no longer present
            for sub_id, meta in list(_subscriptions.items()):
                if meta['user_id'] not in desired_keys:
                    _dbg("Removing subscription %s user=%s", sub_id, meta['user_id'])
                    nostr_helpers.close_subscription(sub_id)
                    del _subscriptions[sub_id]
            
            # Ensure subscription per user and resubscribe if tags/pubkey changed OR force_refresh
            for ctx in contexts:
                ukey = ctx['user_id'] if ctx['user_id'] is not None else 'None'
                # Find existing sub for this user
                existing = [(sid, meta) for sid, meta in _subscriptions.items() if meta['user_id'] == ctx['user_id']]
                
                # Force recreation if force_refresh is True
                if force_refresh and existing:
                    sid, meta = existing[0]
                    _dbg("Force refresh: Recreating subscription for user=%s", ctx['user_id'])
                    nostr_helpers.close_subscription(sid)
                    del _subscriptions[sid]
                    existing = []
                
                if not existing:
                    # Create new subscription
                    sub_id = secrets.token_hex(6)
                    base_since = _local_midnight_timestamp()
                    st = getattr(app, 'state', app)
                    shared = int(getattr(st, 'cyberherd_social_last_seen_ts', 0) or 0)
                    local = int(_last_seen.get(ukey) or 0 or 0)
                    watermark = max(shared, local, base_since)
                    since = max(base_since, watermark - overlap_seconds)
                    
                    # Build filters
                    filters = []
                    
                    # Filter 1a: Notes (kind 1) with #t tags by effective pubkey
                    if ctx.get('filter_tags') or ctx['tags']:
                        notes_tagged_filter = {
                            "kinds": [1], 
                            "#t": ctx.get('filter_tags', ctx['tags']), 
                            "authors": [ctx['eff_pub']], 
                            "since": since
                        }
                        if _first_cycle.get(ukey, True) and initial_limit > 0:
                            notes_tagged_filter['limit'] = initial_limit
                        filters.append(notes_tagged_filter)
                    
                    # Filter 1b: Notes (kind 1) by effective pubkey WITHOUT #t filter (catches all author's notes)
                    notes_author_filter = {
                        "kinds": [1], 
                        "authors": [ctx['eff_pub']], 
                        "since": since
                    }
                    if _first_cycle.get(ukey, True) and initial_limit > 0:
                        notes_author_filter['limit'] = initial_limit
                    filters.append(notes_author_filter)
                    
                    # Filter 2: Engagement events (kinds 6/7) - ONLY #e filter, NO #t restriction
                    # Zaps are handled via invoice listener and are not subscribed here
                    engagement_kinds = []
                    if ctx.get('repost_tracking_enabled', False):
                        engagement_kinds.append(6)
                    if ctx.get('likes_tracking_enabled', False):
                        engagement_kinds.append(7)
                    
                    # Log engagement tracking status for debugging
                    logger.info(
                        f"Engagement tracking for user {ctx['user_id']}: "
                        f"repost={ctx.get('repost_tracking_enabled', False)}, "
                        f"likes={ctx.get('likes_tracking_enabled', False)}, "
                        f"kinds={engagement_kinds}"
                    )
                    
                    if engagement_kinds:
                        # Get tracked event IDs from settings
                        try:
                            settings = ctx.get('settings', {})
                            tracked_event_ids = getattr(settings, 'tracked_event_ids', []) or []
                            
                            logger.info(
                                f"📝 Tracked event IDs for user {ctx['user_id']}: "
                                f"count={len(tracked_event_ids)}, "
                                f"sample={tracked_event_ids[:3] if tracked_event_ids else 'none'}"
                            )
                            
                            if tracked_event_ids:
                                # Only create subscription if we have specific event IDs to track
                                # This prevents unnecessary relay queries and ensures efficient filtering
                                engagement_filter = {"kinds": engagement_kinds, "#e": tracked_event_ids, "since": since}
                                filters.append(engagement_filter)
                                # Safety-net: also add a broad kind=6 only filter (no #e) so content-only
                                # reposts can be recovered client-side. Use a higher limit controlled
                                # by CYBERHERD_BROAD_REPOST_LIMIT.
                                try:
                                    if 6 in engagement_kinds:
                                        broad_repost_filter = {"kinds": [6], "since": since, "limit": CYBERHERD_BROAD_REPOST_LIMIT}
                                        filters.append(broad_repost_filter)
                                except Exception:
                                    pass
                                logger.info(
                                    f"✅ Created engagement subscription for user {ctx['user_id']}: "
                                    f"kinds={engagement_kinds}, tracking {len(tracked_event_ids)} event(s)"
                                )
                                _dbg("Created engagement subscription for user=%s kinds=%s event_count=%d", ctx['user_id'], engagement_kinds, len(tracked_event_ids))
                            else:
                                # Log why no subscription was created
                                logger.warning(
                                    f"⚠️ No engagement subscription created for user {ctx['user_id']}: "
                                    f"tracked_event_ids is empty (kinds would be {engagement_kinds})"
                                )
                            # Note: No subscription created if tracked_event_ids is empty
                            # Subscriptions will be created automatically via refresh when:
                            # 1. Startup initialization populates tracked_event_ids with recent notes
                            # 2. New notes are detected and auto-added to tracked_event_ids
                        except Exception as e:
                            logger.warning(f"Error creating engagement filter: {e}")
                    
                    if CYBERHERD_DIAG:
                        _dbg("DIAG add_sub user=%s sub=%s filter=%s", ctx['user_id'], sub_id, filters)
                    
                    _dbg("Add sub user=%s pubkey=%s tags=%s since=%s filters=%s", ctx['user_id'], ctx['eff_pub'], ctx['tags'], since, len(filters))
                    
                    # Add subscription via nostr_helpers
                    if not nostr_helpers.add_subscription(sub_id, filters):
                        logger.error(f"Cyberherd: Failed to create subscription for user {ctx['user_id']}")
                        continue
                    
                    logger.info(f"✅ Cyberherd: Created subscription {sub_id} for user {ctx['user_id']}")
                    
                    _subscriptions[sub_id] = {
                        'user_id': ctx['user_id'],
                        'tags': ctx['tags'],
                        'eff_pub': ctx['eff_pub'],
                        'fallback': False,
                        'appended': 0,
                        'initial_done': False,
                        'since': since,
                    }
                    _dbg("Added subscription sub=%s user=%s pubkey=%s tags=%s since=%s", sub_id, ctx['user_id'], ctx['eff_pub'], ctx['tags'], since)
                    
                    # Mark subscription system as connected after first successful subscription
                    try:
                        st = getattr(app, "state", app)
                        status = getattr(st, "cyberherd_subscription_status", {}) or {}
                        if not status.get("connected", False):
                            status["connected"] = True
                            st.cyberherd_subscription_status = status
                            logger.info("🔗 Cyberherd subscriptions connected and ready")
                    except Exception as e:
                        logger.debug(f"Could not update connection status: {e}")
                    
                else:
                    # Update existing subscription if config changed
                    sid, meta = existing[0]
                    if meta.get('tags') != ctx['tags'] or meta.get('eff_pub') != ctx['eff_pub']:
                        # Close old subscription
                        nostr_helpers.close_subscription(sid)
                        
                        # Create new subscription
                        new_id = secrets.token_hex(6)
                        base_since = _local_midnight_timestamp()
                        st = getattr(app, 'state', app)
                        shared = int(getattr(st, 'cyberherd_social_last_seen_ts', 0) or 0)
                        local = int(_last_seen.get(ukey) or 0 or 0)
                        watermark = max(shared, local, base_since)
                        since = max(base_since, watermark - overlap_seconds)
                        
                        # Build filters
                        filters = []
                        
                        # Filter 1a: Notes (kind 1) with #t tags
                        if ctx.get('filter_tags') or ctx['tags']:
                            notes_tagged_filter = {
                                "kinds": [1], 
                                "#t": ctx.get('filter_tags', ctx['tags']), 
                                "authors": [ctx['eff_pub']], 
                                "since": since
                            }
                            if _first_cycle.get(ukey, True) and initial_limit > 0:
                                notes_tagged_filter['limit'] = initial_limit
                            filters.append(notes_tagged_filter)
                        
                        # Filter 1b: Notes (kind 1) WITHOUT #t filter (all author's notes)
                        notes_author_filter = {
                            "kinds": [1], 
                            "authors": [ctx['eff_pub']], 
                            "since": since
                        }
                        if _first_cycle.get(ukey, True) and initial_limit > 0:
                            notes_author_filter['limit'] = initial_limit
                        filters.append(notes_author_filter)
                        
                        # Filter 2: Engagement events (reposts/reactions) - ONLY #e filter, NO #t restriction
                        engagement_kinds = []
                        if ctx.get('repost_tracking_enabled', False):
                            engagement_kinds.append(6)
                        if ctx.get('likes_tracking_enabled', False):
                            engagement_kinds.append(7)
                        
                        if engagement_kinds:
                            try:
                                settings = ctx.get('settings', {})
                                tracked_event_ids = getattr(settings, 'tracked_event_ids', []) or []
                                if tracked_event_ids:
                                    # Only create subscription if we have specific event IDs to track
                                    engagement_filter = {"kinds": engagement_kinds, "#e": tracked_event_ids, "since": since}
                                    filters.append(engagement_filter)
                                    _dbg("Created engagement subscription (resub) for user=%s kinds=%s event_ids=%d", ctx.get('user_id'), engagement_kinds, len(tracked_event_ids))
                                # Note: No subscription created if tracked_event_ids is empty
                            except Exception as e:
                                logger.warning(f"Error creating engagement filter (resub): {e}")
                        
                        if CYBERHERD_DIAG:
                            _dbg("DIAG resub user=%s old=%s new=%s filter=%s", ctx['user_id'], sid, new_id, filters)
                        
                        _dbg("Resub (config change) user=%s pubkey=%s tags=%s since=%s filters=%s", ctx['user_id'], ctx['eff_pub'], ctx['tags'], since, len(filters))
                        
                        # Add new subscription via nostr_helpers
                        nostr_helpers.add_subscription(new_id, [*filters])
                        
                        _subscriptions[new_id] = meta
                        _subscriptions[new_id].update({
                            'tags': ctx['tags'], 
                            'eff_pub': ctx['eff_pub'], 
                            'appended': 0, 
                            'initial_done': False, 
                            'since': since, 
                            'fallback': False
                        })
                        del _subscriptions[sid]
                        _dbg("Resubscribed (config change) user=%s old=%s new=%s pubkey=%s tags=%s", ctx['user_id'], sid, new_id, ctx['eff_pub'], ctx['tags'])
            
            # Update status
            st_status = getattr(st, 'cyberherd_subscription_status', {}) or {}
            st_status['users'] = sorted({m['user_id'] for m in _subscriptions.values()})
            st_status['user_count'] = len(st_status['users'])
            st.cyberherd_subscription_status = st_status
            
        except Exception as e:
            _dbg("manager error %s", e)
        
        # Wait for refresh or timeout
        try:
            if _refresh_event is None:
                await asyncio.sleep(refresh_seconds)
            else:
                try:
                    await asyncio.wait_for(_refresh_event.wait(), timeout=refresh_seconds)
                    _refresh_event.clear()
                except asyncio.TimeoutError:
                    pass
        except Exception:
            await asyncio.sleep(refresh_seconds)

async def _event_pump(app):
    overlap_seconds = int(os.getenv("CYBERHERD_SINCE_OVERLAP_SECONDS", "300") or 300)
    # Default to strict 't' tag matching only; no hashtag content fallback unless explicitly enabled
    content_fallback = os.getenv("CYBERHERD_CONTENT_FALLBACK", "false").lower() in ("1","true","yes","y")
    initial_limit = int(os.getenv("CYBERHERD_INITIAL_LIMIT", "500") or 500)

    # Check nostrclient availability via nostr_helpers
    if not nostr_helpers.check_availability():
        logger.warning("Cyberherd adapter: nostrclient not available for event pump")
        return

    # Create message pool poller via nostr_helpers
    poller = nostr_helpers.create_message_pool_poller()
    if not poller.message_pool:
        logger.error("Cyberherd: message_pool not available for event pump")
        return

    cache = _get_cache(app)
    logger.info("Cyberherd: Event pump started successfully")

    while True:
        try:
            # Process events via nostr_helpers poller
            while poller.has_events():
                ev_msg = poller.get_event()
                if not ev_msg:
                    continue
                    
                sub_id = getattr(ev_msg, 'subscription_id', None)
                # Ensure sub_id is a string before using as dict key
                sub_key: Optional[str] = sub_id if isinstance(sub_id, str) else None
                if sub_key is None:
                    poller.put_event_back(ev_msg)
                    continue
                meta = _subscriptions.get(sub_key)
                if not meta:
                    # Put back events for other subscriptions
                    poller.put_event_back(ev_msg)
                    continue
                    
                try:
                    ev = json.loads(getattr(ev_msg, 'event', '{}'))
                except Exception:
                    continue
                    
                # Determine user and settings up-front
                user_id = meta.get('user_id')
                try:
                    from .. import crud as _crud
                    # Only fetch settings when user_id looks valid
                    settings = await _crud.get_settings(user_id) if isinstance(user_id, str) else None
                except Exception:
                    settings = None
                    
                # Forward reposts/reactions (kinds 6 and 7) into the per-user processor
                try:
                    kind = int(ev.get('kind') or 0)
                except Exception:
                    kind = 0

                # Only forward reposts (6) and reactions (7). Zap receipts (9735)
                # are handled via the invoice listener path and should not be forwarded
                if kind in (6, 7):
                    try:
                        from .subscriptions import process_event_for_user
                        # let existing processor handle validation; it will check settings flags
                        # Only call the processor when user_id is a valid string
                        if isinstance(user_id, str):
                            await process_event_for_user(user_id, ev, settings, app)
                    except Exception as e:
                        logger.debug(f"Error forwarding kind {kind} event to processor for user {user_id}: {e}")
                    # continue processing (do not early-continue) so last_seen and status updates still run
                    
                if CYBERHERD_DIAG:
                    _diag_counts['events_total'] += 1
                    
                tags = meta['tags']  # normalized lowercase tags
                user_id = meta['user_id']
                eff_pub = meta['eff_pub']
                eid = ev.get('id')
                created_at = 0
                try:
                    created_at = int(ev.get('created_at') or 0)
                except Exception:
                    pass
                    
                _dbg("Event recv user=%s id=%s created_at=%s", user_id, (eid[:12] + '…') if isinstance(eid, str) else eid, created_at)
                
                # For kind 1 notes, delegate matching logic to _append_today which
                # now supports t-tags and content-hashtag fallback. Always call it
                # instead of gating on explicit 't' tags here.
                if int(ev.get('kind') or 0) == 1:
                    try:
                        if _append_today(cache, user_id, eff_pub, tags, ev):
                            meta['appended'] += 1
                            if CYBERHERD_DIAG:
                                _diag_counts['events_matched'] += 1
                            _dbg("Appended to cache user=%s total_appended=%s", user_id, meta['appended'])
                    except Exception:
                        pass
                        
                if created_at:
                    ukey = str(user_id) if user_id is not None else 'None'
                    prev = _last_seen.get(ukey)
                    if prev is None or created_at > prev:
                        _last_seen[ukey] = created_at
                        
                # Status update (batched minimal)
                if meta['appended'] % 10 == 1:
                    _update_status(app, sub_key, meta)
                    
            # Process EOSE notices via nostr_helpers poller
            while poller.has_eose_notices():
                eose = poller.get_eose_notice()
                if not eose:
                    continue
                    
                sub_id = getattr(eose, 'subscription_id', None)
                sub_key = sub_id if isinstance(sub_id, str) else None
                if sub_key is None:
                    poller.put_eose_back(eose)
                    continue
                meta = _subscriptions.get(sub_key)
                if not meta:
                    # Put back EOSE for other subscriptions
                    poller.put_eose_back(eose)
                    continue
                    
                if CYBERHERD_DIAG:
                    _diag_counts['eose_total'] += 1
                    
                _dbg("EOSE for sub=%s user=%s appended=%s initial_done=%s fallback=%s", sub_id, meta.get('user_id'), meta.get('appended'), meta.get('initial_done'), meta.get('fallback'))
                
                if not meta['initial_done']:
                    meta['initial_done'] = True
                    
                # Mark first cycle complete for this user (handled after first EOSE)
                ukey = str(meta.get('user_id')) if meta.get('user_id') is not None else 'None'
                _first_cycle[ukey] = False
                
                # Fallback if zero appended events
                if meta['appended'] == 0 and content_fallback and not meta['fallback']:
                    meta['fallback'] = True
                    
                    # Close old subscription via nostr_helpers
                    nostr_helpers.close_subscription(sub_key)
                    
                    new_id = secrets.token_hex(6)
                    user_id = meta['user_id']
                    ukey = user_id if user_id is not None else 'None'
                    base_since = _local_midnight_timestamp()
                    st = getattr(app, 'state', app)
                    shared = int(getattr(st, 'cyberherd_social_last_seen_ts', 0) or 0)
                    local = int(_last_seen.get(ukey) or 0 or 0)
                    watermark = max(shared, local, base_since)
                    since = max(base_since, watermark - overlap_seconds)
                    
                    # Fallback filter: author-only for kind 1 (no #t filter)
                    filt = {"kinds": [1], "authors": [meta['eff_pub']], "since": since}
                    if _first_cycle.get(ukey, True) and initial_limit > 0:
                        filt['limit'] = initial_limit
                        
                    _dbg("Fallback resub user=%s pubkey=%s since=%s limit=%s", user_id, meta['eff_pub'], since, filt.get('limit'))
                    
                    # Add new subscription via nostr_helpers
                    nostr_helpers.add_subscription(new_id, [filt])
                    
                    _subscriptions[new_id] = meta
                    if sub_key in _subscriptions:
                        del _subscriptions[sub_key]
                    _dbg("Fallback resubscribe user=%s pubkey=%s old=%s new=%s since=%s", user_id, meta['eff_pub'], sub_id, new_id, since)
                    
                    if CYBERHERD_DIAG:
                        _diag_counts['fallback_resubs'] += 1
                        
                    _update_status(app, new_id, meta)
                    continue
                    
                _update_status(app, sub_key, meta)
                
            await asyncio.sleep(0.2)
            
        except Exception as e:
            _dbg("event pump error %s", e)
            await asyncio.sleep(1)


def _update_status(app, sub_id: str, meta: Dict[str, Any]):
    try:
        st = getattr(app, 'state', app)
        st_status = getattr(st, 'cyberherd_subscription_status', {}) or {}
        rw = st_status.setdefault('relay_workers', {})  # reuse structure
        u_entry = rw.setdefault(meta['user_id'] if meta['user_id'] is not None else 'None', {})
        # aggregated info
        u_entry['pooled'] = {
            'subscription_id': sub_id,
            'appended': meta['appended'],
            'initial_sync_done': meta['initial_done'],
            'fallback': meta['fallback'],
            'tags': meta['tags'],
        }
        st_status['users'] = list({(k if k != 'None' else None) for k in rw.keys()})
        st_status['user_count'] = len(st_status['users'])
        st.cyberherd_subscription_status = st_status
    except Exception:
        pass
