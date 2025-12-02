"""WebSocket-based Nostr event monitor for CyberHerd.

This module follows the same architecture as nwcprovider:
- Direct WebSocket connection to nostrclient's relay endpoint
- Manual REQ/EVENT message handling
- No dependency on nostrclient's Python API or message_pool
- Per-user monitor with automatic reconnection

References:
- https://github.com/lnbits/nwcprovider/blob/main/nwcp.py
"""

from __future__ import annotations

import asyncio
import json
import time
from typing import Any

from lnbits.settings import settings
from loguru import logger
from websockets.legacy.client import connect

from ..crud import get_settings
from .subscriptions import (
    process_note_for_tracked_tags,
    process_reaction_for_tracked_notes,
    process_repost_for_tracked_notes,
    # New: process zap receipts (kind 9735)
    process_zap_receipt_for_tracked_notes,
)

# Module-level app reference (set when first monitor is created)
_app_instance = None

# Subscription lookback window in seconds for engagement events (kind 6/7)
# Engagement events use short lookback since we only care about recent interactions
SUBSCRIPTION_LOOKBACK_SECONDS = 30





class NostrWebSocketMonitor:
    """Per-user Nostr event monitor using WebSocket connection.
    
    This follows the nwcprovider architecture:
    1. Opens direct WebSocket to nostrclient relay endpoint
    2. Sends REQ messages manually with filters
    3. Receives EVENT messages directly
    4. No interaction with nostrclient's message_pool
    
    Each user gets their own monitor instance with independent WebSocket connection.
    """
    
    def __init__(self, user_id: str, app=None):
        """Initialize monitor for a specific user.
        
        Args:
            user_id: LNbits user ID to monitor events for
            app: FastAPI app instance (required for event processing)
        """
        self.user_id = user_id
        self.app = app
        
        # WebSocket connection to nostrclient's relay endpoint
        self.relay = f"ws://localhost:{settings.port}/nostrclient/api/v1/relay"
        
        # WebSocket connection
        self.ws = None
        self.connected = False
        self.shutdown = False
        
        # Subscriptions tracking (per-subscription metadata)
        # Keys are sub_ids, values include type/filter plus lifecycle info
        self.subscriptions: dict[str, dict[str, Any]] = {}
        self.subscription_counter = 0
        # Timeout (in seconds) for initial subscription handshake (EVENT/EOSE/CLOSED).
        # If a subscription never receives any message within this window, we treat
        # it as stale and refresh all subscriptions, mirroring the NWC pattern.
        self.subscription_timeout = 10
        self.subscription_timeout_task = None
        
        # Reconnection task
        self.reconnect_task = None
        
        # Event deduplication: Track recently processed event IDs to prevent
        # duplicate processing when lookback window overlaps with real-time events
        # Keys are event IDs, values are timestamps when they were processed
        self.processed_event_ids: dict[str, float] = {}
        # How long to keep processed event IDs in memory (5 minutes)
        self.dedup_ttl_seconds = 300
        # Last cleanup time for processed event IDs
        self.last_dedup_cleanup = time.time()
        
        logger.debug(f"🔌 NostrWebSocketMonitor created for user {user_id}")
    
    def clear_processed_cache(self):
        """Clear the in-memory processed event deduplication cache.
        
        This is required during midnight reset to ensure that notes from the new day
        are re-processed and added to tracked_event_ids even if they were just seen.
        """
        self.processed_event_ids.clear()
        logger.debug(f"User {self.user_id}: Cleared processed event deduplication cache")

    def _is_shutting_down(self) -> bool:
        """Return True if the monitor (or LNbits) is shutting down."""
        return self.shutdown or not settings.lnbits_running
    
    async def start(self):
        """Start the monitor (WebSocket connection only).
        
        Following nwcprovider pattern: subscriptions are created on connection
        and only updated when settings change or new tracked events are found.
        No periodic refresh needed.
        """
        if self.reconnect_task and not self.reconnect_task.done():
            logger.debug(f"User {self.user_id}: Monitor already running")
            return
        
        if self.reconnect_task and self.reconnect_task.done():
            exc = self.reconnect_task.exception()
            if exc:
                logger.warning(
                    f"User {self.user_id}: Previous monitor task ended with error: {exc}"
                )
            self.reconnect_task = None

        self.shutdown = False
        logger.debug(f"▶️  NostrWebSocketMonitor starting for user {self.user_id}")
        
        # Start WebSocket reconnection task
        self.reconnect_task = asyncio.create_task(self._connect_to_relay())

        # Start subscription-timeout monitor task (if not already running)
        if not self.subscription_timeout_task or self.subscription_timeout_task.done():
            self.subscription_timeout_task = asyncio.create_task(
                self._handle_subscription_timeouts()
            )
        
        logger.debug(f"✅ NostrWebSocketMonitor started for user {self.user_id}")
    
    async def cleanup(self):
        """Cleanup resources and close connections."""
        logger.debug(f"🛑 NostrWebSocketMonitor stopping for user {self.user_id}")
        
        self.shutdown = True
        
        # Cancel tasks
        if self.subscription_timeout_task:
            self.subscription_timeout_task.cancel()
            try:
                await self.subscription_timeout_task
            except asyncio.CancelledError:
                pass
            except Exception as e:
                logger.warning(
                    f"Error waiting for subscription-timeout task to stop: {e}"
                )
            self.subscription_timeout_task = None

        if self.reconnect_task:
            self.reconnect_task.cancel()
            try:
                await self.reconnect_task
            except asyncio.CancelledError:
                pass
            except Exception as e:
                logger.warning(f"Error waiting for reconnect task to stop: {e}")
            self.reconnect_task = None
        
        # Close WebSocket
        if self.ws:
            try:
                await self.ws.close()
            except Exception as e:
                logger.warning(f"Error closing WebSocket: {e}")
        
        logger.debug(f"✅ NostrWebSocketMonitor stopped for user {self.user_id}")
    
    def is_running(self) -> bool:
        """Return True when the reconnect loop is still active."""
        return self.reconnect_task is not None and not self.reconnect_task.done()
    
    def _get_new_subid(self) -> str:
        """Generate unique subscription ID.
        
        Returns:
            Unique subscription ID like "cyberherd_user123_0"
        """
        sub_id = f"cyberherd_{self.user_id}_{self.subscription_counter}"
        self.subscription_counter += 1
        return sub_id
    
    async def _wait_for_connection(self, timeout: int = 60 * 2):
        """Wait until WebSocket is connected or timeout/shutdown occurs."""
        start = time.time()
        while not self.connected:
            if self._is_shutting_down():
                raise Exception("Monitor is shutting down")
            if time.time() - start > timeout:
                raise Exception("Connection timeout, cannot connect to nostr relay")
            await asyncio.sleep(0.5)
    
    async def _send(self, data: list[Any]):
        """Send data to relay via WebSocket.
        
        Args:
            data: Nostr message as list (e.g., ["REQ", sub_id, filter])
        """
        if not self.ws:
            raise Exception("WebSocket not connected")
        if self._is_shutting_down():
            logger.warning(f"User {self.user_id}: Trying to send while shutting down")
            return
        
        await self._wait_for_connection()
        
        message = json.dumps(data, separators=(",", ":"), ensure_ascii=False)
        await self.ws.send(message)

    async def _close_subscription(self, sub_id: str, send_event: bool = True):
        """Close a subscription locally and optionally send CLOSE to the relay.
        
        Mirrors the NWC pattern: track a closed flag and ensure we don't try
        to CLOSE the same subscription multiple times.
        """
        meta = self.subscriptions.get(sub_id)
        if not meta:
            return

        if not meta.get("closed"):
            meta["closed"] = True
            if send_event:
                try:
                    await self._send(["CLOSE", sub_id])
                except Exception as e:
                    logger.warning(
                        f"User {self.user_id}: Error sending CLOSE for "
                        f"subscription {sub_id}: {e}"
                    )

        # Remove from tracking regardless of whether sending CLOSE succeeded
        self.subscriptions.pop(sub_id, None)

    async def _handle_subscription_timeouts(self):
        """Monitor new subscriptions and refresh if handshakes never complete.
        
        For each subscription we expect at least one message (EVENT, EOSE or
        CLOSED) shortly after sending REQ. If nothing arrives within
        subscription_timeout, we treat subscriptions as stale and refresh them,
        following the spirit of the NWC subscription handling pattern.
        """
        try:
            while not self._is_shutting_down():
                await asyncio.sleep(int(self.subscription_timeout * 0.5))

                # Skip checks while disconnected
                if not self.connected:
                    continue

                now = time.time()

                # If we're connected but have no active subscriptions, refresh.
                if not self.subscriptions:
                    try:
                        logger.warning(
                            f"User {self.user_id}: No active subscriptions while "
                            "connected; refreshing subscriptions"
                        )
                        await self._subscribe_to_tracked_notes()
                    except Exception as e:
                        logger.error(
                            f"User {self.user_id}: Error refreshing subscriptions "
                            f"when none active: {e}"
                        )
                    continue

                # Detect any subscription whose handshake never completed.
                stale_found = False
                for sub_id, meta in list(self.subscriptions.items()):
                    if meta.get("closed"):
                        continue
                    created_at = meta.get("created_at") or 0
                    handshake_completed = meta.get("handshake_completed", False)
                    if not created_at or handshake_completed:
                        continue
                    elapsed = now - created_at
                    if elapsed > self.subscription_timeout:
                        logger.warning(
                            f"User {self.user_id}: Subscription {sub_id} handshake "
                            f"timed out after {int(elapsed)}s; refreshing "
                            "subscriptions"
                        )
                        stale_found = True
                        break

                if stale_found:
                    try:
                        await self._subscribe_to_tracked_notes()
                    except Exception as e:
                        logger.error(
                            f"User {self.user_id}: Error refreshing subscriptions "
                            f"after timeout: {e}"
                        )
        except asyncio.CancelledError:
            logger.debug(
                f"User {self.user_id}: Subscription-timeout monitor cancelled"
            )
        except Exception as e:
            logger.error(
                f"User {self.user_id}: Error in subscription-timeout monitor: {e}"
            )
    
    async def _subscribe_to_tracked_notes(self):
        """Subscribe to reactions and reposts for all tracked notes.
        
        This loads the user's settings and creates TWO types of subscriptions:
        
        1. For kind 6/7 (reposts/reactions):
           - Filter by tracked_event_ids (the notes we're monitoring)
           - Uses "#e" tag to match events referencing tracked notes
        
        2. For kind 1/30311 (new notes from user):
           - Filter by author pubkey (user's effective pubkey)
           - Filter by tracked_tags (hashtags user is tracking)
           - These become NEW tracked notes when detected
        """
        try:
            # Load user settings
            user_settings = await get_settings(self.user_id)
            if not user_settings:
                logger.debug(f"User {self.user_id}: No settings found")
                return
            
            tracked_note_ids = user_settings.tracked_event_ids or []
            tracked_tags = getattr(user_settings, 'tracked_tags', []) or []
            
            # Use proper effective pubkey resolution
            from .subscriptions import get_effective_pubkey
            effective_pubkey = get_effective_pubkey(user_settings)
            
            # Clear old subscriptions (we'll recreate them)
            old_subs = list(self.subscriptions.keys())
            for old_sub_id in old_subs:
                try:
                    await self._close_subscription(old_sub_id, send_event=True)
                except Exception as e:
                    logger.warning(
                        f"Error closing old subscription {old_sub_id}: {e}"
                    )
            
            logger.debug(
                f"User {self.user_id}: Subscribing - "
                f"{len(tracked_note_ids)} tracked notes, "
                f"{len(tracked_tags)} tracked tags, "
                f"pubkey: {effective_pubkey[:8] if effective_pubkey else 'None'}..."
            )
            
            # SUBSCRIPTION 1: Reposts and reactions for tracked notes (kind 6, 7)
            # Only create if we have tracked notes AND at least one engagement type is enabled
            if tracked_note_ids:
                # Check which engagement types are enabled
                repost_enabled = getattr(user_settings, 'repost_tracking_enabled', False)
                reaction_enabled = getattr(user_settings, 'likes_tracking_enabled', False)
                
                # Build kinds list based on enabled tracking
                engagement_kinds = []
                if repost_enabled:
                    engagement_kinds.append(6)
                if reaction_enabled:
                    engagement_kinds.append(7)
                
                # Only create subscription if at least one engagement type is enabled
                if engagement_kinds:
                    sub_id = self._get_new_subid()
                    
                    filter_dict = {
                        "kinds": engagement_kinds,  # Only enabled types
                        "#e": tracked_note_ids,  # Match events referencing ANY tracked note
                        # Use lookback window to catch events published during connection/subscription setup
                        "since": int(time.time()) - SUBSCRIPTION_LOOKBACK_SECONDS,
                    }
                    
                    # Send REQ message
                    await self._send(["REQ", sub_id, filter_dict])
                    
                    # Track subscription
                    self.subscriptions[sub_id] = {
                        "type": "engagement",  # reposts/reactions
                        "filter": filter_dict,
                        "created_at": time.time(),
                        "handshake_completed": False,
                        "last_event_at": None,
                        "closed": False,
                    }
                    
                    engagement_types = []
                    if repost_enabled:
                        engagement_types.append("reposts")
                    if reaction_enabled:
                        engagement_types.append("reactions")
                    
                    logger.debug(
                        f"✅ User {self.user_id}: Created engagement subscription "
                        f"({'/'.join(engagement_types)}) for {len(tracked_note_ids)} tracked note(s) "
                        f"with {SUBSCRIPTION_LOOKBACK_SECONDS}s lookback window "
                        f"(sub_id: {sub_id})"
                    )
                else:
                    logger.debug(
                        f"⏭️  User {self.user_id}: Has {len(tracked_note_ids)} tracked notes but "
                        f"both repost and reaction tracking are disabled - skipping engagement subscription"
                    )
            else:
                logger.debug(f"⏭️  User {self.user_id}: No tracked notes yet - skipping engagement subscription (will be created when notes are detected)")
            
            # SUBSCRIPTION 2: New notes from user (kind 1, 30311)
            # Only create if we have effective pubkey
            if effective_pubkey:
                sub_id = self._get_new_subid()
                
                # Build filter for user's new notes
                # We use a short lookback window (real-time) similar to engagement events.
                # Historical events (from earlier today) are handled by:
                # 1. force_requery_for_user() called on connection
                # 2. periodic_event_recovery() called hourly
                # This avoids issues with "since" filters being too far in the past.
                
                filter_dict = {
                    "kinds": [1, 30311],  # Regular notes and long-form content
                    "authors": [effective_pubkey],  # Only from this user
                    # Real-time subscription with short lookback
                    "since": int(time.time()) - SUBSCRIPTION_LOOKBACK_SECONDS,
                }
                
                # Add tag filter if user is tracking specific hashtags
                if tracked_tags:
                    # Normalize tags (remove # prefix)
                    # Note: Tag matching is case-sensitive in Nostr, so preserve case
                    normalized_tags = [tag.lstrip('#') for tag in tracked_tags]
                    filter_dict["#t"] = normalized_tags
                    
                    logger.debug(
                        f"User {self.user_id}: Adding tag filter for: {normalized_tags}"
                    )
                
                # Send REQ message
                await self._send(["REQ", sub_id, filter_dict])
                
                # Track subscription
                self.subscriptions[sub_id] = {
                    "type": "notes",  # new notes from user
                    "filter": filter_dict,
                    "created_at": time.time(),
                    "handshake_completed": False,
                    "last_event_at": None,
                    "closed": False,
                }

                logger.debug(
                    f"✅ User {self.user_id}: Created notes subscription (kind 1/30311) "
                    f"for pubkey {effective_pubkey[:8]}... "
                    f"{'with ' + str(len(tracked_tags)) + ' tag filter(s)' if tracked_tags else 'all tags'} "
                    f"since now-{SUBSCRIPTION_LOOKBACK_SECONDS}s "
                    f"(sub_id: {sub_id})"
                )
            else:
                logger.debug(f"User {self.user_id}: No effective pubkey, skipping note subscription")

            # 3) Zap receipts (kind 9735) subscription intentionally DISABLED
            # Previously we created a broad subscription for kind 9735 addressed to
            # the user's effective pubkey. To avoid relying on nostr zap receipts
            # over WebSocket (and to prefer payment-based/fallback recovery paths),
            # we no longer create real-time 9735 subscriptions here. Zap receipts
            # are still processed when discovered via recovery queries or payment
            # coordinator flows.
            if effective_pubkey:
                logger.debug(
                    f"User {self.user_id}: Zap receipts (kind 9735) subscriptions are disabled; "
                    f"skipping creation for effective pubkey {effective_pubkey[:8]}..."
                )

            logger.debug(f"✅ User {self.user_id}: Created {len(self.subscriptions)} subscriptions")
        
        except Exception as e:
            logger.error(f"User {self.user_id}: Error in _subscribe_to_tracked_notes: {e}")
    
    async def refresh_subscriptions_now(self):
        """Immediately refresh subscriptions (called when settings change or new tracked events found).
        
        This is called when:
        1. Settings are updated (pubkey, tags, etc.)
        2. New tracked events are added via _append_today
        
        Following nwcprovider pattern: subscriptions are only updated when needed,
        not on a periodic timer.
        """
        try:
            if not self.connected:
                logger.debug(f"User {self.user_id}: Skipping refresh (not connected)")
                return
            
            logger.debug(f"🔄 User {self.user_id}: Subscription refresh triggered")
            await self._subscribe_to_tracked_notes()
            
        except Exception as e:
            logger.error(f"User {self.user_id}: Error in subscription refresh: {e}")
    
    async def _process_event(self, event: dict):
        """Process a Nostr event (note, repost, or reaction).
        
        Args:
            event: Nostr event dict from EVENT message
        """
        try:
            event_id = event.get("id", "unknown")
            kind = event.get("kind")
            
            # Deduplication: Check if we've already processed this event recently
            # This prevents duplicate processing when lookback window overlaps with real-time
            current_time = time.time()
            if event_id in self.processed_event_ids:
                logger.debug(
                    f"User {self.user_id}: Skipping duplicate event {event_id[:8]}... "
                    f"(already processed {int(current_time - self.processed_event_ids[event_id])}s ago)"
                )
                return
            
            # Mark event as processed
            self.processed_event_ids[event_id] = current_time
            
            # Periodic cleanup of old processed event IDs (every 60 seconds)
            if current_time - self.last_dedup_cleanup > 60:
                cutoff = current_time - self.dedup_ttl_seconds
                old_ids = [eid for eid, ts in self.processed_event_ids.items() if ts < cutoff]
                for eid in old_ids:
                    del self.processed_event_ids[eid]
                if old_ids:
                    logger.debug(
                        f"User {self.user_id}: Cleaned up {len(old_ids)} old processed event IDs"
                    )
                self.last_dedup_cleanup = current_time
            
            if kind in (1, 30311):
                # Kind 1: Regular note
                # Kind 30311: Parameterized replaceable event (long-form content)
                # These are NEW notes from the tracked user with tracked tags
                logger.info(
                    f"User {self.user_id}: New kind {kind} note {event_id[:8]}... detected"
                )
                
                # Process the note: check tags, add to tracked_event_ids, trigger subscription refresh
                await process_note_for_tracked_tags(self.user_id, event, self.app)
            
            elif kind == 6:
                # Repost
                logger.debug(f"User {self.user_id}: Processing repost {event_id[:8]}...")
                await process_repost_for_tracked_notes(self.user_id, event, self.app)
            
            elif kind == 7:
                # Reaction (like)
                logger.debug(f"User {self.user_id}: Processing reaction {event_id[:8]}...")
                await process_reaction_for_tracked_notes(self.user_id, event, self.app)
            
            elif kind == 9735:
                # Zap receipts are ignored in realtime: zap subscriptions were
                # intentionally disabled. Payments are the single authoritative
                # detection path. Log and ignore any incoming 9735 events to
                # avoid dual-processing if a relay or external component still
                # sends them.
                logger.debug(
                    f"User {self.user_id}: Ignoring incoming kind 9735 event {event_id[:8]}... (zap processing disabled in WS)"
                )
            
            else:
                logger.debug(
                    f"User {self.user_id}: Ignoring event kind {kind} "
                    f"(processing 1/30311=notes, 6=repost, 7=reaction, 9735=zap)"
                )
        
        except Exception as e:
            logger.error(f"User {self.user_id}: Error processing event: {e}")
    
    async def _on_message(self, message: str):
        """Handle incoming WebSocket message.
        
        Args:
            message: Raw WebSocket message (JSON string)
        """
        try:
            msg = json.loads(message)
            msg_type = msg[0]
            
            if msg_type == "EVENT":
                # ["EVENT", sub_id, event]
                sub_id = msg[1]
                event = msg[2]
                
                # Only process events from our subscriptions
                if sub_id in self.subscriptions:
                    meta = self.subscriptions.get(sub_id) or {}
                    meta["handshake_completed"] = True
                    meta["last_event_at"] = time.time()
                    await self._process_event(event)
            
            elif msg_type == "EOSE":
                # End of stored events
                sub_id = msg[1]
                logger.debug(f"User {self.user_id}: EOSE for subscription {sub_id}")
                meta = self.subscriptions.get(sub_id)
                if meta is not None:
                    meta["handshake_completed"] = True
            
            elif msg_type == "CLOSED":
                # Subscription closed by relay
                sub_id = msg[1]
                reason = msg[2] if len(msg) > 2 else ""
                logger.warning(
                    f"User {self.user_id}: Subscription {sub_id} closed by relay: {reason}"
                )
                # Remove from tracking (do not send CLOSE back, relay initiated)
                if sub_id in self.subscriptions:
                    await self._close_subscription(sub_id, send_event=False)
            
            elif msg_type == "NOTICE":
                # Relay notice
                notice = msg[1] if len(msg) > 1 else ""
                logger.debug(f"User {self.user_id}: Relay notice: {notice}")
            
            elif msg_type == "OK":
                # Event publication response (we don't publish, so ignore)
                pass
            
            else:
                logger.debug(f"User {self.user_id}: Unknown message type: {msg_type}")
        
        except Exception as e:
            logger.error(f"User {self.user_id}: Error parsing message: {e}")
    
    async def _on_connection(self):
        """Called when WebSocket connection is established.
        
        Subscribes to tracked notes.
        """
        logger.debug(f"User {self.user_id}: WebSocket connected, subscribing...")
        await self._subscribe_to_tracked_notes()
        
        # Trigger recovery of missed events (notes, zaps, engagements)
        # This ensures that if we were disconnected, we catch up on what we missed
        try:
            from .subscriptions import force_requery_for_user
            logger.info(f"User {self.user_id}: Triggering forced recovery after connection established")
            # Run in background to not block the websocket loop
            asyncio.create_task(force_requery_for_user(self.app, self.user_id))
        except Exception as e:
            logger.warning(f"User {self.user_id}: Failed to trigger recovery on connect: {e}")
    
    async def _connect_to_relay(self):
        """Connect to nostrclient relay and handle messages.
        
        This runs in a loop with automatic reconnection.
        Follows the nwcprovider pattern.
        """
        await asyncio.sleep(1)  # Initial delay
        
        logger.debug(f"User {self.user_id}: Connecting to relay {self.relay}")
        
        while not self._is_shutting_down():
            try:
                logger.debug(f"User {self.user_id}: Creating WebSocket connection...")
                
                async with connect(self.relay) as ws:
                    self.ws = ws
                    self.connected = True
                    
                    logger.debug(f"✅ User {self.user_id}: WebSocket connected")
                    
                    # Subscribe to tracked notes
                    await self._on_connection()
                    
                    # Message loop
                    while not self._is_shutting_down():
                        try:
                            message = await ws.recv()
                            if isinstance(message, bytes):
                                message = message.decode("utf-8")
                            await self._on_message(message)
                        
                        except Exception as e:
                            logger.debug(f"User {self.user_id}: Error receiving message: {e}")
                            break
                
                logger.debug(f"User {self.user_id}: WebSocket connection closed")
            
            except Exception as e:
                logger.error(f"User {self.user_id}: Error connecting to relay: {e}")
            
            # Mark as disconnected
            self.connected = False
            
            if not self._is_shutting_down():
                # Wait before reconnecting
                logger.debug(f"User {self.user_id}: Reconnecting in 5 seconds...")
                await asyncio.sleep(5)


# Global registry of active monitors
_active_monitors: dict[str, NostrWebSocketMonitor] = {}


async def start_monitor_for_user(user_id: str, app=None) -> NostrWebSocketMonitor:
    """Start a WebSocket monitor for a user.
    
    Args:
        user_id: LNbits user ID
        app: FastAPI app instance (required for event processing)
        
    Returns:
        The created monitor instance
    """
    global _app_instance
    
    # Store app instance globally if not already set
    if app and not _app_instance:
        _app_instance = app
        logger.debug("Stored app instance for WebSocket monitors")
    
    # Use stored app instance if not provided
    if not app:
        app = _app_instance
    
    if user_id in _active_monitors:
        monitor = _active_monitors[user_id]
        await monitor.start()
        return monitor
    
    monitor = NostrWebSocketMonitor(user_id, app)
    await monitor.start()
    
    _active_monitors[user_id] = monitor
    
    return monitor


async def stop_monitor_for_user(user_id: str):
    """Stop a WebSocket monitor for a user.
    
    Args:
        user_id: LNbits user ID
    """
    monitor = _active_monitors.get(user_id)
    if monitor:
        await monitor.cleanup()
        del _active_monitors[user_id]
        logger.info(f"✅ Stopped monitor for user {user_id}")
    else:
        logger.debug(f"No monitor found for user {user_id}")


def get_active_monitors() -> dict[str, NostrWebSocketMonitor]:
    """Get all active monitors.
    
    Returns:
        Dict mapping user_id to monitor instance
    """
    return _active_monitors.copy()


async def trigger_immediate_refresh(user_id: str):
    """Trigger immediate subscription refresh for a specific user.
    
    This is called when:
    1. Settings are updated (pubkey, tags, etc.)
    2. New tracked events are added
    
    Args:
        user_id: LNbits user ID
    """
    monitor = _active_monitors.get(user_id)
    if monitor:
        await monitor.refresh_subscriptions_now()
    else:
        logger.debug(f"No active monitor for user {user_id}, skipping refresh")


async def clear_monitor_cache_for_user(user_id: str):
    """Clear the in-memory deduplication cache for a user's monitor.
    
    Args:
        user_id: LNbits user ID
    """
    monitor = _active_monitors.get(user_id)
    if monitor:
        monitor.clear_processed_cache()

