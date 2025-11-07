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
)

# Module-level app reference (set when first monitor is created)
_app_instance = None


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
        
        # Subscriptions tracking
        self.subscriptions: dict[str, dict[str, Any]] = {}  # sub_id -> filter
        self.subscription_counter = 0
        
        # Reconnection task
        self.reconnect_task = None
        
        logger.info(f"🔌 NostrWebSocketMonitor created for user {user_id}")
    
    async def start(self):
        """Start the monitor (WebSocket connection only).
        
        Following nwcprovider pattern: subscriptions are created on connection
        and only updated when settings change or new tracked events are found.
        No periodic refresh needed.
        """
        logger.info(f"▶️  NostrWebSocketMonitor starting for user {self.user_id}")
        
        # Start WebSocket reconnection task
        self.reconnect_task = asyncio.create_task(self._connect_to_relay())
        
        logger.info(f"✅ NostrWebSocketMonitor started for user {self.user_id}")
    
    async def cleanup(self):
        """Cleanup resources and close connections."""
        logger.info(f"🛑 NostrWebSocketMonitor stopping for user {self.user_id}")
        
        self.shutdown = True
        
        # Cancel tasks
        if self.reconnect_task:
            self.reconnect_task.cancel()
        
        # Close WebSocket
        if self.ws:
            try:
                await self.ws.close()
            except Exception as e:
                logger.warning(f"Error closing WebSocket: {e}")
        
        logger.info(f"✅ NostrWebSocketMonitor stopped for user {self.user_id}")
    
    def _get_new_subid(self) -> str:
        """Generate unique subscription ID.
        
        Returns:
            Unique subscription ID like "cyberherd_user123_0"
        """
        sub_id = f"cyberherd_{self.user_id}_{self.subscription_counter}"
        self.subscription_counter += 1
        return sub_id
    
    async def _wait_for_connection(self):
        """Wait until WebSocket is connected."""
        while not self.connected:
            if self.shutdown:
                raise Exception("Monitor is shutting down")
            await asyncio.sleep(0.5)
    
    async def _send(self, data: list[Any]):
        """Send data to relay via WebSocket.
        
        Args:
            data: Nostr message as list (e.g., ["REQ", sub_id, filter])
        """
        if not self.ws:
            raise Exception("WebSocket not connected")
        if self.shutdown:
            logger.warning(f"User {self.user_id}: Trying to send while shutting down")
            return
        
        await self._wait_for_connection()
        
        message = json.dumps(data, separators=(",", ":"), ensure_ascii=False)
        await self.ws.send(message)
    
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
            effective_pubkey = getattr(user_settings, 'nostr_pubkey_override', None) or \
                              getattr(user_settings, 'nostr_private_key', None)
            
            # Convert private key to pubkey if needed
            if effective_pubkey and len(effective_pubkey) == 64:
                try:
                    # Check if it's a private key (try to derive pubkey)
                    import secp256k1
                    try:
                        priv = secp256k1.PrivateKey(bytes.fromhex(effective_pubkey))
                        pub = priv.pubkey
                        effective_pubkey = pub.serialize().hex()[2:]  # Remove '02' prefix
                    except:
                        pass  # Already a pubkey
                except ImportError:
                    pass
            
            # Clear old subscriptions (we'll recreate them)
            old_subs = list(self.subscriptions.keys())
            for old_sub_id in old_subs:
                try:
                    await self._send(["CLOSE", old_sub_id])
                except Exception as e:
                    logger.warning(f"Error closing old subscription {old_sub_id}: {e}")
            self.subscriptions.clear()
            
            logger.info(
                f"User {self.user_id}: Subscribing - "
                f"{len(tracked_note_ids)} tracked notes, "
                f"{len(tracked_tags)} tracked tags, "
                f"pubkey: {effective_pubkey[:8] if effective_pubkey else 'None'}..."
            )
            
            # SUBSCRIPTION 1: Reposts and reactions for tracked notes (kind 6, 7)
            # Only create if we have tracked notes
            if tracked_note_ids:
                sub_id = self._get_new_subid()
                
                filter_dict = {
                    "kinds": [6, 7],  # Reposts and reactions
                    "#e": tracked_note_ids,  # Match events referencing ANY tracked note
                    "since": int(time.time()),  # Only new events (real-time)
                }
                
                # Send REQ message
                await self._send(["REQ", sub_id, filter_dict])
                
                # Track subscription
                self.subscriptions[sub_id] = {
                    "type": "engagement",  # reposts/reactions
                    "filter": filter_dict,
                }
                
                logger.info(
                    f"✅ User {self.user_id}: Created engagement subscription (kind 6/7) for "
                    f"{len(tracked_note_ids)} tracked note(s) (sub_id: {sub_id})"
                )
            else:
                logger.info(f"⏭️  User {self.user_id}: No tracked notes yet - skipping engagement subscription (will be created when notes are detected)")
            
            # SUBSCRIPTION 2: New notes from user (kind 1, 30311)
            # Only create if we have effective pubkey
            if effective_pubkey:
                sub_id = self._get_new_subid()
                
                # Build filter for user's new notes
                filter_dict = {
                    "kinds": [1, 30311],  # Regular notes and long-form content
                    "authors": [effective_pubkey],  # Only from this user
                    "since": int(time.time()),  # Only new events (real-time)
                }
                
                # Add tag filter if user is tracking specific hashtags
                if tracked_tags:
                    # Normalize tags (remove # prefix, lowercase)
                    normalized_tags = [tag.lstrip('#').lower() for tag in tracked_tags]
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
                }
                
                logger.info(
                    f"✅ User {self.user_id}: Created notes subscription (kind 1/30311) "
                    f"for pubkey {effective_pubkey[:8]}... "
                    f"{'with ' + str(len(tracked_tags)) + ' tag filter(s)' if tracked_tags else 'all tags'} "
                    f"(sub_id: {sub_id})"
                )
            else:
                logger.debug(f"User {self.user_id}: No effective pubkey, skipping note subscription")
            
            logger.info(
                f"✅ User {self.user_id}: Created {len(self.subscriptions)} subscriptions"
            )
        
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
            
            logger.info(f"🔄 User {self.user_id}: Subscription refresh triggered")
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
            
            else:
                logger.debug(
                    f"User {self.user_id}: Ignoring event kind {kind} "
                    f"(only processing 1/30311=notes, 6=repost, 7=reaction)"
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
                    await self._process_event(event)
            
            elif msg_type == "EOSE":
                # End of stored events
                sub_id = msg[1]
                logger.debug(f"User {self.user_id}: EOSE for subscription {sub_id}")
            
            elif msg_type == "CLOSED":
                # Subscription closed by relay
                sub_id = msg[1]
                reason = msg[2] if len(msg) > 2 else ""
                logger.warning(
                    f"User {self.user_id}: Subscription {sub_id} closed by relay: {reason}"
                )
                # Remove from tracking
                if sub_id in self.subscriptions:
                    del self.subscriptions[sub_id]
            
            elif msg_type == "NOTICE":
                # Relay notice
                notice = msg[1] if len(msg) > 1 else ""
                logger.info(f"User {self.user_id}: Relay notice: {notice}")
            
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
        logger.info(f"User {self.user_id}: WebSocket connected, subscribing...")
        await self._subscribe_to_tracked_notes()
    
    async def _connect_to_relay(self):
        """Connect to nostrclient relay and handle messages.
        
        This runs in a loop with automatic reconnection.
        Follows the nwcprovider pattern.
        """
        await asyncio.sleep(1)  # Initial delay
        
        logger.info(f"User {self.user_id}: Connecting to relay {self.relay}")
        
        while not self.shutdown:
            try:
                logger.debug(f"User {self.user_id}: Creating WebSocket connection...")
                
                async with connect(self.relay) as ws:
                    self.ws = ws
                    self.connected = True
                    
                    logger.info(f"✅ User {self.user_id}: WebSocket connected")
                    
                    # Subscribe to tracked notes
                    await self._on_connection()
                    
                    # Message loop
                    while not self.shutdown:
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
            
            if not self.shutdown:
                # Wait before reconnecting
                logger.info(f"User {self.user_id}: Reconnecting in 5 seconds...")
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
        logger.info(f"Monitor already running for user {user_id}")
        return _active_monitors[user_id]
    
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
