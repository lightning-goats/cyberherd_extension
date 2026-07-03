"""Nostr Client Helper Module for CyberHerd.

This module provides a stable, public API wrapper around nostrclient's relay manager.
All Nostr interactions should go through these helpers to avoid touching private internals.

Public API Functions:
- query_events(): Query events from relays
- add_subscription(): Add a subscription to relay manager
- close_subscription(): Close a subscription
- poll_message_pool(): Poll events and EOSE notices from message pool
- get_relay_info(): Get current relay connection information
- check_availability(): Check if nostrclient is available
"""

from __future__ import annotations

import asyncio
import json
import secrets
from typing import Any, Iterable

import httpx

from loguru import logger
from datetime import datetime, timezone

# Nostrclient availability check
_NOSTRCLIENT_AVAILABLE = False
_nostr_client = None

try:
    from lnbits.extensions.nostrclient.router import nostr_client as _nostr_client
    _NOSTRCLIENT_AVAILABLE = True
except Exception as e:
    logger.debug(f"Nostrclient not available: {e}")


# ============================================================================
# Diagnostics and Monitoring
# ============================================================================

_helper_diagnostics = {
    'queries_total': 0,
    'queries_successful': 0,
    'queries_failed': 0,
    'queries_empty': 0,
    'last_query_timestamp': None,
    'last_query_duration_ms': None,
    'last_failure_timestamp': None,
    'last_failure_reason': None,
    'subscriptions_added': 0,
    'subscriptions_closed': 0,
    'subscriptions_failed': 0,
    'relay_checks': 0,
    'relay_unavailable_count': 0,
}


def get_diagnostics() -> dict[str, Any]:
    """Get comprehensive diagnostics for nostr_helpers.
    
    Returns diagnostic information including:
    - Query statistics (total, successful, failed, empty)
    - Subscription statistics
    - Relay connection status
    - Recent failures and timing
    - Availability checks
    
    Returns:
        Dict with diagnostic information
    """
    diag = dict(_helper_diagnostics)
    
    # Add current relay status
    relay_info = get_relay_info()
    diag['relay_info'] = relay_info
    
    # Calculate success rate
    if diag['queries_total'] > 0:
        success_rate = (diag['queries_successful'] / diag['queries_total']) * 100
        diag['query_success_rate'] = round(success_rate, 2)
    else:
        diag['query_success_rate'] = None
    
    # Add availability status
    diag['nostrclient_available'] = check_availability()
    
    return diag


def reset_diagnostics():
    """Reset all diagnostic counters.
    
    Useful for testing or starting fresh monitoring period.
    """
    global _helper_diagnostics
    _helper_diagnostics = {
        'queries_total': 0,
        'queries_successful': 0,
        'queries_failed': 0,
        'queries_empty': 0,
        'last_query_timestamp': None,
        'last_query_duration_ms': None,
        'last_failure_timestamp': None,
        'last_failure_reason': None,
        'subscriptions_added': 0,
        'subscriptions_closed': 0,
        'subscriptions_failed': 0,
        'relay_checks': 0,
        'relay_unavailable_count': 0,
    }


def _record_query_start():
    """Record the start of a query operation."""
    return datetime.now(timezone.utc)


def _record_query_success(start_time: datetime, event_count: int):
    """Record a successful query operation.
    
    Args:
        start_time: Query start timestamp
        event_count: Number of events returned
    """
    _helper_diagnostics['queries_total'] += 1
    _helper_diagnostics['queries_successful'] += 1
    
    if event_count == 0:
        _helper_diagnostics['queries_empty'] += 1
    
    # Calculate duration
    end_time = datetime.now(timezone.utc)
    duration_ms = (end_time - start_time).total_seconds() * 1000
    
    _helper_diagnostics['last_query_timestamp'] = end_time.isoformat()
    _helper_diagnostics['last_query_duration_ms'] = round(duration_ms, 2)


def _record_query_failure(error: str):
    """Record a failed query operation.
    
    Args:
        error: Error message/reason for failure
    """
    _helper_diagnostics['queries_total'] += 1
    _helper_diagnostics['queries_failed'] += 1
    _helper_diagnostics['last_failure_timestamp'] = datetime.now(timezone.utc).isoformat()
    _helper_diagnostics['last_failure_reason'] = str(error)


def _record_subscription_added():
    """Record a subscription addition."""
    _helper_diagnostics['subscriptions_added'] += 1


def _record_subscription_closed():
    """Record a subscription closure."""
    _helper_diagnostics['subscriptions_closed'] += 1


def _record_subscription_failed():
    """Record a failed subscription operation."""
    _helper_diagnostics['subscriptions_failed'] += 1


def _record_relay_check(available: bool):
    """Record a relay availability check.
    
    Args:
        available: Whether relay was available
    """
    _helper_diagnostics['relay_checks'] += 1
    if not available:
        _helper_diagnostics['relay_unavailable_count'] += 1


# ============================================================================
# Availability Check
# ============================================================================

def check_availability() -> bool:
    """Check if nostrclient is available.
    
    Returns:
        True if nostrclient is available, False otherwise
    """
    return _NOSTRCLIENT_AVAILABLE and _nostr_client is not None


def get_nostr_client():
    """Get the nostr_client instance.
    
    Returns:
        nostr_client instance or None if not available
    """
    if not check_availability():
        return None
    return _nostr_client


async def reconnect_nostrclient_ws(
    app=None,
    relays: Iterable[str] | None = None,
    ensure_restart: bool = True,
) -> bool:
    """Best-effort helper to (re)establish nostrclient relay connectivity.

    This maintains compatibility with legacy code paths that expect this helper
    to exist. It does not raise on failure; callers should treat a False return
    value as a signal that nostrclient is currently unavailable.
    """
    if not check_availability():
        logger.debug("reconnect_nostrclient_ws: nostrclient unavailable")
        return False

    client = get_nostr_client()
    if client is None:
        logger.debug("reconnect_nostrclient_ws: no client instance")
        return False

    relay_urls: list[str] = []

    if relays:
        try:
            relay_urls = [str(r).strip() for r in relays if str(r).strip()]
        except Exception:
            relay_urls = []

    if not relay_urls and app is not None:
        try:
            state_relays = getattr(getattr(app, "state", app), "cyberherd_relays", None)
            if isinstance(state_relays, (list, tuple)):
                relay_urls = [str(r).strip() for r in state_relays if str(r).strip()]
        except Exception:
            relay_urls = []

    if relay_urls:
        try:
            client.reconnect(relay_urls)
            logger.debug(
                f"reconnect_nostrclient_ws: reconnected to relays {relay_urls}"
            )
            return True
        except Exception as e:
            logger.debug(f"reconnect_nostrclient_ws: relay reconnect failed: {e}")

    if ensure_restart:
        try:
            client.relay_manager.check_and_restart_relays()
            return True
        except Exception as e:
            logger.debug(f"reconnect_nostrclient_ws: restart check failed: {e}")

    return False


# ============================================================================
# Relay Information
# ============================================================================

def get_relay_info() -> dict[str, Any]:
    """Get current relay connection information.
    
    Returns:
        Dict with relay statistics: {
            'available': bool,
            'relay_count': int,
            'connected_count': int,
            'relay_urls': list[str]
        }
    """
    if not check_availability():
        return {
            'available': False,
            'relay_count': 0,
            'connected_count': 0,
            'relay_urls': []
        }
    
    try:
        relays = getattr(_nostr_client.relay_manager, "relays", {})
        relay_urls = list(relays.keys()) if hasattr(relays, "keys") else []
        
        # Count connected relays
        connected = 0
        try:
            vals = list(relays.values()) if hasattr(relays, "values") else []
            connected = sum(1 for r in vals if getattr(r, "is_connected", False))
        except Exception:
            pass
        
        return {
            'available': True,
            'relay_count': len(relay_urls),
            'connected_count': connected,
            'relay_urls': relay_urls
        }
    except Exception as e:
        logger.debug(f"Error getting relay info: {e}")
        return {
            'available': True,
            'relay_count': 0,
            'connected_count': 0,
            'relay_urls': []
        }


def add_relay(relay_url: str) -> bool:
    """Add a relay to the relay manager.
    
    Args:
        relay_url: WebSocket URL of the relay (wss://...)
        
    Returns:
        True if relay was added successfully, False otherwise
    """
    if not check_availability():
        logger.warning("Cannot add relay: nostrclient not available")
        return False
    
    try:
        relays = getattr(_nostr_client.relay_manager, "relays", {})
        if relay_url in relays:
            logger.debug(f"Relay already exists: {relay_url}")
            return True
        
        _nostr_client.relay_manager.add_relay(relay_url)
        logger.debug(f"Added relay: {relay_url}")
        return True
    except Exception as e:
        logger.warning(f"Failed to add relay {relay_url}: {e}")
        return False


# ============================================================================
# Subscriptions
# ============================================================================

def add_subscription(subscription_id: str, filters: list[dict[str, Any]]) -> bool:
    """Add a subscription to the relay manager.
    
    Args:
        subscription_id: Unique identifier for this subscription
        filters: List of Nostr filter dicts (e.g., [{"kinds": [1], "limit": 10}])
        
    Returns:
        True if subscription was added, False otherwise
    """
    if not check_availability():
        logger.warning(f"Cannot add subscription {subscription_id}: nostrclient not available")
        _record_relay_check(False)
        _record_subscription_failed()
        return False
    
    _record_relay_check(True)
    
    try:
        _nostr_client.relay_manager.add_subscription(subscription_id, filters)
        logger.debug(f"Added subscription: {subscription_id} with {len(filters)} filter(s)")
        _record_subscription_added()
        return True
    except Exception as e:
        logger.error(f"Failed to add subscription {subscription_id}: {e}")
        _record_subscription_failed()
        return False


def close_subscription(subscription_id: str) -> bool:
    """Close a subscription in the relay manager.
    
    Args:
        subscription_id: Subscription ID to close
        
    Returns:
        True if subscription was closed, False otherwise
    """
    if not check_availability():
        return False
    
    try:
        _nostr_client.relay_manager.close_subscription(subscription_id)
        logger.debug(f"Closed subscription: {subscription_id}")
        _record_subscription_closed()
        return True
    except Exception as e:
        logger.debug(f"Failed to close subscription {subscription_id}: {e}")
        return False


# ============================================================================
# Message Pool Polling
# ============================================================================

class MessagePoolPoller:
    """Helper class for polling the nostrclient message pool.
    
    This provides a clean interface to poll events and EOSE notices
    without directly accessing message_pool internals.
    """
    
    def __init__(self):
        self.message_pool = None
        if check_availability():
            self.message_pool = _nostr_client.relay_manager.message_pool
    
    def has_events(self) -> bool:
        """Check if there are events in the pool."""
        if not self.message_pool:
            return False
        try:
            return getattr(self.message_pool, "has_events")()
        except Exception:
            return False
    
    def get_event(self) -> Any | None:
        """Get an event from the pool.
        
        Returns:
            Event message object or None
        """
        if not self.message_pool:
            return None
        try:
            return self.message_pool.get_event()
        except Exception:
            return None
    
    def put_event_back(self, event_msg: Any) -> bool:
        """Put an event back into the pool.
        
        Args:
            event_msg: Event message to put back
            
        Returns:
            True if successful, False otherwise
        """
        if not self.message_pool:
            return False
        try:
            self.message_pool.events.put(event_msg)
            return True
        except Exception:
            return False
    
    def has_eose_notices(self) -> bool:
        """Check if there are EOSE notices in the pool."""
        if not self.message_pool:
            return False
        try:
            return getattr(self.message_pool, "has_eose_notices")()
        except Exception:
            return False
    
    def get_eose_notice(self) -> Any | None:
        """Get an EOSE notice from the pool.
        
        Returns:
            EOSE notice object or None
        """
        if not self.message_pool:
            return None
        try:
            return self.message_pool.get_eose_notice()
        except Exception:
            return None
    
    def put_eose_back(self, eose_msg: Any) -> bool:
        """Put an EOSE notice back into the pool.
        
        Args:
            eose_msg: EOSE message to put back
            
        Returns:
            True if successful, False otherwise
        """
        if not self.message_pool:
            return False
        try:
            self.message_pool.eose_notices.put(eose_msg)
            return True
        except Exception:
            return False


def create_message_pool_poller() -> MessagePoolPoller:
    """Create a message pool poller instance.
    
    Returns:
        MessagePoolPoller instance
    """
    return MessagePoolPoller()


# ============================================================================
# Event Queries
# ============================================================================

async def query_events(
    filters: dict[str, Any] | list[dict[str, Any]],
    limit: int = 50,
    timeout: float = 6.0,
    extra_relays: list[str] | None = None,
    max_retries: int = 3
) -> list[dict]:
    """Query events from Nostr relays using nostrclient relay manager.
    
    This is the primary method for querying events. It:
    - Uses the in-process relay manager (no direct WebSocket access)
    - Adds temporary subscription
    - Collects events until EOSE/timeout/limit
    - Cleans up subscription automatically
    - Retries on failure with exponential backoff
    
    Args:
        filters: Nostr filter dict or list of filter dicts
        limit: Maximum number of events to return
        timeout: Timeout in seconds for the query
        extra_relays: Optional list of additional relay URLs to use
        max_retries: Maximum number of retry attempts
        
    Returns:
        List of event dicts (may be empty if no events found)
    """
    if not check_availability():
        logger.warning("Cannot query events: nostrclient not available")
        _record_relay_check(False)
        return []
    
    _record_relay_check(True)
    start_time = _record_query_start()
    
    # Log current relay state
    relay_info = get_relay_info()
    logger.debug(
        f"Query starting: {relay_info['connected_count']}/{relay_info['relay_count']} "
        f"relays connected"
    )
    
    # Add extra relays if provided
    if extra_relays:
        logger.debug(f"Adding {len(extra_relays)} extra relay(s) for query")
        added = []
        for url in extra_relays:
            if not isinstance(url, str) or not url:
                continue
            if add_relay(url):
                added.append(url)
        
        if added:
            logger.debug(f"Added extra relays: {added}")
            # Give relays time to connect
            await asyncio.sleep(1.0)
    
    # Ensure we have relays
    relay_info = get_relay_info()
    if relay_info['relay_count'] == 0:
        logger.warning("No relays configured, cannot query events")
        return []
    
    async def _run_once() -> list[dict]:
        """Run a single query attempt."""
        sub_id = secrets.token_hex(8)
        events: list[dict] = []
        poller = create_message_pool_poller()
        
        try:
            # Normalize filters to list format
            if isinstance(filters, list):
                filter_list = filters
            else:
                filter_list = [filters]
            
            # Add subscription
            if not add_subscription(sub_id, filter_list):
                return []
            
            # Poll for events until timeout/limit/EOSE
            deadline = asyncio.get_event_loop().time() + max(0.5, float(timeout))
            saw_eose = False
            
            while asyncio.get_event_loop().time() < deadline and len(events) < limit:
                progressed = False
                drain_count = 0
                
                # Drain events from pool
                while poller.has_events() and len(events) < limit and drain_count < 300:
                    event_msg = poller.get_event()
                    if not event_msg:
                        break
                    
                    try:
                        # Check if this event is for our subscription
                        msg_sub_id = getattr(event_msg, "subscription_id", None)
                        if msg_sub_id == sub_id:
                            # Parse event JSON
                            event_json = getattr(event_msg, "event", "{}")
                            try:
                                ev = json.loads(event_json)
                                if isinstance(ev, dict):
                                    events.append(ev)
                                    progressed = True
                            except (json.JSONDecodeError, TypeError) as e:
                                logger.debug(f"Failed to parse event JSON: {e}")
                        else:
                            # Not our event, put it back
                            poller.put_event_back(event_msg)
                    except Exception as e:
                        logger.debug(f"Error processing event message: {e}")
                    finally:
                        drain_count += 1
                
                # Drain EOSE notices
                drain_eose = 0
                while poller.has_eose_notices() and drain_eose < 100:
                    eose_msg = poller.get_eose_notice()
                    if not eose_msg:
                        break
                    
                    try:
                        msg_sub_id = getattr(eose_msg, "subscription_id", None)
                        if msg_sub_id == sub_id:
                            saw_eose = True
                            progressed = True
                        else:
                            # Not our EOSE, put it back
                            poller.put_eose_back(eose_msg)
                    except Exception as e:
                        logger.debug(f"Error processing EOSE notice: {e}")
                    finally:
                        drain_eose += 1
                
                # Stop if we saw EOSE and made no progress
                if saw_eose and not progressed:
                    break
                
                await asyncio.sleep(0.05)
                
        finally:
            # Always close subscription
            close_subscription(sub_id)
        
        return events
    
    # Retry logic with exponential backoff
    for attempt in range(max_retries):
        try:
            result = await _run_once()
            if result:
                logger.debug(
                    f"Query successful on attempt {attempt + 1}: "
                    f"{len(result)} event(s) found"
                )
                _record_query_success(start_time, len(result))
                return result
            else:
                logger.debug(f"Query returned empty on attempt {attempt + 1}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(0.5 * (attempt + 1))  # Exponential backoff
        except Exception as e:
            logger.warning(f"Query failed on attempt {attempt + 1}: {e}")
            if attempt < max_retries - 1:
                await asyncio.sleep(0.5 * (attempt + 1))  # Exponential backoff
            else:
                # Final attempt failed
                _record_query_failure(str(e))
    
    logger.debug(f"Query failed after {max_retries} attempts")
    _record_query_failure(f"Failed after {max_retries} retry attempts")
    return []


# ============================================================================
# Convenience Functions
# ============================================================================

async def query_kind_0_metadata(pubkey: str, extra_relays: list[str] | None = None) -> dict | None:
    """Query kind 0 metadata for a pubkey.
    
    Args:
        pubkey: Nostr pubkey (hex)
        extra_relays: Optional additional relays to query
        
    Returns:
        Metadata dict or None if not found
    """
    filters = {
        "kinds": [0],
        "authors": [pubkey],
        "limit": 1
    }
    
    events = await query_events(filters, limit=1, timeout=5.0, extra_relays=extra_relays)
    if not events:
        return None
    
    try:
        content = events[0].get("content", "{}")
        return json.loads(content)
    except (json.JSONDecodeError, KeyError):
        return None


async def query_user_relays(pubkey: str) -> list[str]:
    """Query kind 10002 relay list for a pubkey.
    
    Args:
        pubkey: Nostr pubkey (hex)
        
    Returns:
        List of relay URLs
    """
    filters = {
        "kinds": [10002],
        "authors": [pubkey],
        "limit": 1
    }
    
    events = await query_events(filters, limit=1, timeout=5.0)
    if not events:
        return []
    
    relays = []
    for tag in events[0].get("tags", []):
        if isinstance(tag, list) and len(tag) >= 2 and tag[0] == "r":
            relays.append(tag[1])
    
    return relays


async def lookup_metadata(pubkey: str, api_key: str | None = None) -> dict[str, str | None] | None:
    """Return display metadata for a pubkey (backward-compatible signature)."""

    def _extract_best(ev_list: list[dict]) -> dict[str, Any] | None:
        best = None
        best_created = 0
        for ev in ev_list or []:
            try:
                created = int(ev.get("created_at") or 0)
                if created >= best_created:
                    content = ev.get("content")
                    if isinstance(content, str):
                        content = json.loads(content)
                    if isinstance(content, dict) and (
                        content.get("lud16")
                        or content.get("name")
                        or content.get("display_name")
                        or content.get("nip05")
                    ):
                        best = content
                        best_created = created
            except Exception as exc:
                logger.warning(exc)
                continue
        return best

    try:
        rels = await query_user_relays(pubkey)
    except Exception as exc:
        logger.debug(f"lookup_metadata: relays fetch failed: {exc}")
        rels = []

    events = await query_events(
        {"kinds": [0], "authors": [pubkey]},
        limit=3,
        timeout=8.0,
        extra_relays=rels or None,
    )

    best = _extract_best(events)
    if best:
        dn = best.get("display_name") or best.get("name")
        if not dn:
            nip = best.get("nip05")
            if isinstance(nip, str) and "@" in nip:
                dn = nip.split("@", 1)[0]
        logger.info(
            "cyberherd: metadata for %s display_name=%s lud16=%s nip05=%s picture=%s",
            pubkey,
            dn or "",
            best.get("lud16") or "",
            best.get("nip05") or "",
            "yes" if best.get("picture") else "no",
        )
        return {
            "display_name": dn or "Anon",
            "lud16": best.get("lud16"),
            "picture": best.get("picture"),
            "nip05": best.get("nip05"),
        }

    logger.info("cyberherd: no metadata found for %s", pubkey[:8] + "...")
    return None


async def lookup_relays(pubkey: str, api_key: str | None = None) -> list[str]:
    """Backward-compatible wrapper around query_user_relays."""
    try:
        return await query_user_relays(pubkey)
    except Exception as exc:
        logger.warning(f"lookup_relays: error via nostr_helpers: {exc}")
        return []


async def verify_nip05(pubkey_hex: str, nip05: str) -> bool:
    """Validate that nip05 identifier resolves to the given pubkey."""

    def _normalize_candidate(value: str | None) -> str | None:
        if not value or not isinstance(value, str):
            return None
        candidate = value.strip()
        if not candidate:
            return None
        try:
            from lnbits.utils.nostr import normalize_public_key  # type: ignore

            normalized = normalize_public_key(candidate)  # type: ignore[assignment]
        except Exception:
            normalized = candidate.strip().lower()
        else:
            normalized = normalized.strip().lower()
        return normalized

    pubkey = (pubkey_hex or "").strip().lower()
    identity = (nip05 or "").strip()
    if not pubkey or len(pubkey) != 64:
        logger.debug("verify_nip05: invalid pubkey '%s'", pubkey_hex)
        return False
    if "@" not in identity:
        logger.debug("verify_nip05: invalid nip05 '%s'", nip05)
        return False

    try:
        username, domain = identity.split("@", 1)
        username = username.strip().lower()
        domain = domain.strip()
        if not username or not domain:
            return False

        url = f"https://{domain}/.well-known/nostr.json?name={username}"
        async with httpx.AsyncClient() as client:
            resp = await client.get(url, timeout=10)
            resp.raise_for_status()
            payload = resp.json()

        mapped = None
        try:
            mapped = payload.get("names", {}).get(username)
        except Exception:
            mapped = None

        mapped_norm = _normalize_candidate(mapped)
        if mapped_norm and mapped_norm == pubkey:
            return True
        logger.info(
            "verify_nip05: mapping mismatch for %s -> %s (expected %s)",
            identity,
            mapped,
            pubkey,
        )
    except Exception as exc:
        logger.debug("verify_nip05 direct fetch failed: %s", exc)

    try:
        from lnbits.core.services.nostr import fetch_nip5_details  # type: ignore

        resolved_pubkey, _ = await fetch_nip5_details(identity)
        resolved_norm = _normalize_candidate(resolved_pubkey)
        if resolved_norm:
            return resolved_norm == pubkey
        return False
    except Exception as exc:
        logger.debug("verify_nip05 core fallback failed: %s", exc)
        return False


# ============================================================================
# Subscription Management Helpers
# ============================================================================

class SubscriptionManager:
    """Helper for managing multiple subscriptions.
    
    Tracks active subscriptions and provides cleanup methods.
    """
    
    def __init__(self):
        self.active_subscriptions: set[str] = set()
    
    def add(self, subscription_id: str, filters: list[dict[str, Any]]) -> bool:
        """Add and track a subscription.
        
        Returns:
            True if successful, False otherwise
        """
        if add_subscription(subscription_id, filters):
            self.active_subscriptions.add(subscription_id)
            return True
        return False
    
    def close(self, subscription_id: str) -> bool:
        """Close and untrack a subscription.
        
        Returns:
            True if successful, False otherwise
        """
        if subscription_id in self.active_subscriptions:
            self.active_subscriptions.discard(subscription_id)
            return close_subscription(subscription_id)
        return False
    
    def close_all(self) -> int:
        """Close all tracked subscriptions.
        
        Returns:
            Number of subscriptions closed
        """
        closed = 0
        for sub_id in list(self.active_subscriptions):
            if close_subscription(sub_id):
                closed += 1
            self.active_subscriptions.discard(sub_id)
        return closed
    
    def get_active_count(self) -> int:
        """Get count of active subscriptions."""
        return len(self.active_subscriptions)
    
    def get_active_ids(self) -> list[str]:
        """Get list of active subscription IDs."""
        return list(self.active_subscriptions)


def create_subscription_manager() -> SubscriptionManager:
    """Create a subscription manager instance.
    
    Returns:
        SubscriptionManager instance
    """
    return SubscriptionManager()


# ============================================================================
# NIP-57 Validation Helpers
# ============================================================================

def validate_nip57_zap_receipt(event: dict[str, Any], *, require_preimage: bool = False) -> tuple[bool, str]:
    """Validate a NIP-57 zap receipt (kind 9735)."""
    if not isinstance(event, dict):
        return False, "Event is not a dictionary"

    kind = event.get("kind")
    if kind != 9735:
        return False, f"Invalid kind: {kind} (expected 9735 for zap receipt)"

    tags = event.get("tags", [])
    if not isinstance(tags, list):
        return False, "Tags field is not a list"

    tags_dict: dict[str, list[str]] = {}
    for tag in tags:
        if isinstance(tag, list) and len(tag) >= 2:
            tags_dict.setdefault(tag[0], []).append(tag[1])

    missing = [name for name in ("bolt11", "description", "p") if name not in tags_dict]
    if missing:
        return False, f"Missing required tags: {', '.join(missing)}"

    bolt11_values = tags_dict.get("bolt11", [])
    if not bolt11_values or not isinstance(bolt11_values[0], str) or not bolt11_values[0].lower().startswith("ln"):
        return False, "Invalid bolt11 format"

    description_values = tags_dict.get("description", [])
    if not description_values or not isinstance(description_values[0], str):
        return False, "description tag is not a string"

    try:
        zap_request = json.loads(description_values[0])
    except json.JSONDecodeError as exc:
        return False, f"description tag is not valid JSON: {exc}"

    if not isinstance(zap_request, dict) or zap_request.get("kind") != 9734:
        return False, f"Invalid zap request kind: {zap_request.get('kind')} (expected 9734)"

    p_values = tags_dict.get("p", [])
    recipient = p_values[0] if p_values else ""
    if not isinstance(recipient, str) or len(recipient) != 64:
        return False, "Invalid recipient pubkey length (expected 64 hex chars)"

    preimage_values = tags_dict.get("preimage", [])
    if require_preimage:
        if not preimage_values or not isinstance(preimage_values[0], str) or len(preimage_values[0]) != 64:
            return False, "preimage tag is required but missing/invalid"
    elif preimage_values:
        if not isinstance(preimage_values[0], str) or len(preimage_values[0]) != 64:
            return False, "Invalid preimage format"

    return True, ""


def validate_nip57_payment_zap(payment_data: dict[str, Any], *, require_bolt11: bool = True) -> tuple[bool, str]:
    """Validate zap metadata extracted from invoice payment payloads."""
    if not isinstance(payment_data, dict):
        return False, "Payment data is not a dictionary"

    nostr_data = payment_data.get("nostr")
    if not nostr_data:
        return False, "Missing 'nostr' field in payment data"

    if isinstance(nostr_data, str):
        try:
            nostr_data = json.loads(nostr_data)
        except json.JSONDecodeError as exc:
            return False, f"nostr field is not valid JSON: {exc}"

    if not isinstance(nostr_data, dict):
        return False, "nostr field is not a dict"

    tags = nostr_data.get("tags")
    if not isinstance(tags, list):
        return False, "nostr tags missing or invalid"

    tags_dict: dict[str, list[str]] = {}
    for tag in tags:
        if isinstance(tag, list) and len(tag) >= 2:
            tags_dict.setdefault(tag[0], []).append(tag[1])

    if require_bolt11 and "bolt11" not in tags_dict:
        return False, "Missing bolt11 tag"

    if "description" not in tags_dict:
        return False, "Missing description tag"

    description_values = tags_dict.get("description", [])
    if not description_values or not isinstance(description_values[0], str):
        return False, "Description tag invalid"

    try:
        zap_request = json.loads(description_values[0])
    except json.JSONDecodeError as exc:
        return False, f"Description tag invalid JSON: {exc}"

    if zap_request.get("kind") != 9734:
        return False, "Zap request kind is not 9734"

    if "p" not in tags_dict:
        return False, "Missing recipient pubkey tag"

    recipient = tags_dict["p"][0]
    if not isinstance(recipient, str) or len(recipient) != 64:
        return False, "Recipient pubkey invalid"

    return True, ""
