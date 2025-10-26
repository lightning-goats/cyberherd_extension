"""Nostr event monitor using nostrclient relay_manager.

This module provides real-time monitoring of Nostr events (zap receipts,
reposts, reactions) for tracked notes using the nostrclient extension's
internal API.

Key features:
- Subscribe to kind 9735 (zap receipts), 6 (reposts), 7 (reactions)
- Poll nostrclient's message_pool for events
- Parse and extract relevant data from events
- Callback-based event handling
- Dynamic subscription updates
"""

from __future__ import annotations

import asyncio
import json
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

from loguru import logger

from . import nostr_helpers


def _get_midnight_timestamp() -> int:
    """Get Unix timestamp for midnight today in UTC.
    
    Nostr events use UTC timestamps, so all time comparisons should be in UTC.
    """
    now = datetime.now(timezone.utc)
    midnight = datetime.combine(now.date(), datetime.min.time()).replace(tzinfo=timezone.utc)
    return int(midnight.timestamp())


def _get_earliest_timestamp_from_dict(note_ids: list[str], timestamps: dict[str, int], default_days_ago: int = 30) -> int:
    """Get the earliest timestamp from the stored timestamps dict.
    
    Args:
        note_ids: List of note IDs to check
        timestamps: Dict mapping note_id -> created_at timestamp
        default_days_ago: Fallback if no timestamps found (default: 30 days)
        
    Returns:
        Unix timestamp of the earliest note, or (now - default_days_ago) as fallback
    """
    if not note_ids or not timestamps:
        # Fallback: default_days_ago days ago
        return int((datetime.now() - timedelta(days=default_days_ago)).timestamp())  # Local time
    
    # Get timestamps for our tracked notes
    relevant_timestamps = [timestamps.get(note_id) for note_id in note_ids if note_id in timestamps]
    relevant_timestamps = [ts for ts in relevant_timestamps if ts is not None]
    
    if relevant_timestamps:
        earliest = min(relevant_timestamps)
        logger.info(f"Found earliest tracked note timestamp from stored data: {earliest} ({datetime.fromtimestamp(earliest).astimezone().isoformat()})")  # Show in local time
        return earliest
    else:
        logger.debug(f"No stored timestamps for tracked notes, using fallback: {default_days_ago} days ago")
        return int((datetime.now() - timedelta(days=default_days_ago)).timestamp())  # Local time


class NostrEventMonitor:
    """Monitor Nostr events for CyberHerd using nostrclient.
    
    This class manages subscriptions to Nostr relays via the nostrclient
    extension and processes incoming events (zap receipts, reposts, reactions).
    
    Usage:
        monitor = NostrEventMonitor(user_id="user123")
        monitor.on_zap_receipt = handle_zap_callback
        monitor.on_repost = handle_repost_callback
        monitor.on_reaction = handle_reaction_callback
        await monitor.start(tracked_note_ids=["note1", "note2"])
        
        # Later...
        await monitor.stop()
    """
    
    def __init__(self, user_id: str):
        """Initialize the Nostr event monitor.
        
        Args:
            user_id: User ID this monitor belongs to
        """
        self.user_id = user_id
        self.subscription_ids: list[str] = []
        self.running = False
        self._event_processing_task: asyncio.Task | None = None
        self._poll_task: asyncio.Task | None = None
        
        # AsyncIO Queue for event-driven processing (replaces polling)
        self._event_queue: asyncio.Queue = asyncio.Queue(maxsize=1000)
        
        # Event callbacks (set these before starting)
        self.on_zap_receipt: Callable | None = None
        self.on_repost: Callable | None = None
        self.on_reaction: Callable | None = None
        
        # Stats
        self.events_processed = 0
        self.events_filtered = 0
        self.zaps_detected = 0
        self.reposts_detected = 0
        self.reactions_detected = 0
        self.last_event_at: int | None = None
        
        # Deduplication tracking
        self._processed_event_ids: set[str] = set()
        self._max_tracked_events = 1000  # Prevent memory bloat
    
    async def start(self, tracked_note_ids: list[str], author_pubkey: str | None = None, 
                    enable_zaps: bool = True, enable_reposts: bool = False, 
                    enable_reactions: bool = False, since_timestamp: int | None = None,
                    note_timestamps: dict[str, int] | None = None) -> bool:
        """Start monitoring Nostr events for tracked notes.
        
        Args:
            tracked_note_ids: List of note IDs to monitor
            author_pubkey: Optional author pubkey for additional filtering
            enable_zaps: Subscribe to kind 9735 zap receipts (default: True)
            enable_reposts: Subscribe to kind 6 reposts (default: False)
            enable_reactions: Subscribe to kind 7 reactions (default: False)
            since_timestamp: Optional epoch timestamp to filter events from
            note_timestamps: Dict mapping note_id -> created_at timestamp (for event recovery)
            
        Returns:
            True if monitoring started successfully, False otherwise
        """
        if not NOSTRCLIENT_AVAILABLE or not nostr_client:
            logger.warning("Nostrclient not available, cannot start Nostr monitoring")
            return False
        
        if self.running:
            logger.warning(f"Nostr monitor already running for user {self.user_id}")
            return True
        
        if not tracked_note_ids:
            logger.warning(f"No tracked notes for user {self.user_id}, nothing to monitor")
            return False
        
        # Wait for relays to be connected before subscribing
        if not await self._wait_for_relays(timeout=30):
            logger.error(f"Timeout waiting for Nostr relays for user {self.user_id}")
            return False
        
        # Determine 'since' timestamp:
        # 1. Use provided timestamp if given
        # 2. Otherwise, use earliest stored note timestamp (for recovery)
        # 3. Fallback to 30 days ago if no timestamps available
        if since_timestamp is not None:
            since = since_timestamp
        else:
            since = _get_earliest_timestamp_from_dict(tracked_note_ids, note_timestamps or {}, default_days_ago=30)
        
        logger.info(f"Starting Nostr monitor for user {self.user_id}, {len(tracked_note_ids)} notes, since={since} ({datetime.fromtimestamp(since).astimezone().isoformat()})")  # Show in local time
        
        logger.info(f"📋 Tracked notes for user {self.user_id}: {[n[:16]+'...' for n in tracked_note_ids[:10]]}")
        if len(tracked_note_ids) > 10:
            logger.info(f"   ... and {len(tracked_note_ids) - 10} more notes")
        
        try:
            # Subscribe to enabled event types
            if enable_zaps:
                await self._subscribe_zap_receipts(tracked_note_ids, since)
            else:
                logger.info(f"⚠️  Zaps disabled for user {self.user_id}, skipping zap subscription")
            
            if enable_reposts or enable_reactions:
                # Don't use 'since' for reposts/reactions - they can happen at any time
                # Using the note's timestamp would miss reposts that happened before monitoring started
                await self._subscribe_social_events(tracked_note_ids, since=None, enable_reposts=enable_reposts, enable_reactions=enable_reactions)
            else:
                logger.info(f"⚠️  Reposts and reactions disabled for user {self.user_id}, skipping social subscription")
            
            if not (enable_zaps or enable_reposts or enable_reactions):
                logger.warning(f"No event types enabled for user {self.user_id}, monitoring will be idle")
            
            # Start background tasks
            self.running = True
            # Task 1: Poll message_pool and push events to queue
            self._poll_task = asyncio.create_task(self._poll_message_pool())
            # Task 2: Process events from queue (event-driven, blocks when empty)
            self._event_processing_task = asyncio.create_task(self._process_events())
            
            logger.info(f"Nostr monitor started for user {self.user_id} (event-driven mode)")
            return True
            
        except Exception as e:
            logger.error(f"Failed to start Nostr monitor for user {self.user_id}: {e}")
            self.running = False
            return False
    
    async def _subscribe_zap_receipts(self, note_ids: list[str], since: int | None = None):
        """Subscribe to kind 9735 zap receipts for tracked notes.
        
        CRITICAL: This filters by #e (event reference) NOT #t (hashtag tags).
        Zap receipts reference the zapped note's ID via the 'e' tag.
        
        Args:
            note_ids: List of note IDs to monitor for zaps
            since: Timestamp to filter from (for event recovery on restart/toggle)
        """
        sub_id = f"cyberherd_zaps_{self.user_id}"
        
        filters = [{
            "kinds": [9735],  # Zap receipts
            "#e": note_ids,   # Zapped note IDs
            "limit": 100,     # Request up to 100 historical zaps
        }]
        
        # Add since filter if provided (for event recovery)
        if since is not None:
            filters[0]["since"] = since
        
        nostr_helpers.add_subscription(sub_id, filters)
        self.subscription_ids.append(sub_id)
        
        since_str = f", since={since} ({datetime.fromtimestamp(since).astimezone().isoformat()})" if since else ""  # Show in local time
        logger.info(
            f"✅ Subscribed to zap receipts: {sub_id}, "
            f"tracking {len(note_ids)} notes: {[n[:16] + '...' for n in note_ids[:3]]}{'...' if len(note_ids) > 3 else ''}{since_str}"
        )
    
    async def _subscribe_social_events(self, note_ids: list[str], since: int | None = None,
                                        enable_reposts: bool = True, enable_reactions: bool = True):
        """Subscribe to kind 6 (reposts) and/or kind 7 (reactions) for tracked notes.
        
        CRITICAL: This filters by #e (event reference) NOT #t (hashtag tags).
        Reposts and reactions reference the original note's ID via the 'e' tag,
        regardless of whether the person reposting/reacting includes the hashtag.
        
        This ensures we catch ALL engagements with our tracked notes, even if:
        - The reposter doesn't add #CyberHerd hashtag
        - The reactor doesn't include t-tags
        - The engagement is from someone who never uses hashtags
        
        Args:
            note_ids: List of note IDs to monitor for social interactions
            since: Timestamp to filter from (for event recovery on restart/toggle)
            enable_reposts: Include kind 6 reposts (default: True)
            enable_reactions: Include kind 7 reactions (default: True)
        """
        # Build kinds list based on what's enabled
        kinds = []
        if enable_reposts:
            kinds.append(6)
        if enable_reactions:
            kinds.append(7)
        
        if not kinds:
            logger.debug(f"No social event kinds enabled for user {self.user_id}, skipping subscription")
            return
        
        sub_id = f"cyberherd_social_{self.user_id}"
        
        filters = [{
            "kinds": kinds,
            "#e": note_ids,   # Referenced note IDs
            "limit": 100,     # Request up to 100 historical events per note
        }]
        
        # Add since filter if provided (for event recovery)
        if since is not None:
            filters[0]["since"] = since
        
        nostr_helpers.add_subscription(sub_id, filters)
        self.subscription_ids.append(sub_id)
        
        kinds_str = ", ".join([f"kind {k}" for k in kinds])
        since_str = f", since={since} ({datetime.fromtimestamp(since).astimezone().isoformat()})" if since else ""  # Show in local time
        logger.info(
            f"✅ Subscribed to social events: {sub_id}, "
            f"tracking {len(note_ids)} notes: {[n[:16] + '...' for n in note_ids[:3]]}{'...' if len(note_ids) > 3 else ''}, "
            f"{kinds_str}{since_str}"
        )
    
    async def _poll_message_pool(self):
        """Poll nostrclient's message_pool and push events to our queue.
        
        This lightweight poller checks the shared message_pool and transfers
        events to our AsyncIO queue for event-driven processing. Only events
        matching our subscription IDs are transferred.
        """
        logger.info(f"Starting message pool poller for user {self.user_id}, subscription IDs: {self.subscription_ids}")
        
        # Create message pool poller
        poller = nostr_helpers.create_message_pool_poller()
        
        # Log EOSE (End Of Stored Events) messages to understand relay responses
        eose_count = 0
        while poller.has_eose_notices():
            eose_msg = poller.get_eose_notice()
            if eose_msg and eose_msg.subscription_id in self.subscription_ids:
                eose_count += 1
                logger.info(f"📭 EOSE received for subscription {eose_msg.subscription_id} from {eose_msg.url} (relay finished sending stored events)")
        
        if eose_count > 0:
            logger.info(f"📭 Received {eose_count} EOSE messages - relays have finished sending historical events")
        
        poll_count = 0
        while self.running:
            try:
                # Check if there are new events in the shared message pool
                events_found = 0
                events_checked = 0
                events_for_us = 0
                
                # Limit how many events we check per cycle to prevent infinite loops
                # when putting non-matching events back into the pool
                max_checks_per_cycle = 100
                
                while poller.has_events() and events_checked < max_checks_per_cycle:
                    event_msg = poller.get_event()
                    events_checked += 1
                    
                    # Only queue events from our subscriptions
                    if event_msg and event_msg.subscription_id in self.subscription_ids:
                        events_for_us += 1
                        try:
                            # Non-blocking put with timeout to prevent blocking on full queue
                            await asyncio.wait_for(
                                self._event_queue.put(event_msg),
                                timeout=0.1
                            )
                            events_found += 1
                        except asyncio.TimeoutError:
                            logger.warning(f"Event queue full for user {self.user_id}, dropping event")
                            self.events_filtered += 1
                    elif event_msg:
                        # PUT IT BACK! This event is for another subscription
                        # (e.g., NWC, other users, etc.) and needs to stay in the pool
                        poller.put_event_back(event_msg)
                
                if events_for_us > 0:
                    logger.info(f"✅ Found {events_for_us} cyberherd events (zaps/reposts/reactions) for user {self.user_id}")
                
                # Periodically log that we're still polling
                poll_count += 1
                if poll_count % 120 == 0:  # Every ~60 seconds (assuming 0.5s sleep)
                    logger.debug(f"Poll active for user {self.user_id}, checked {events_checked} events this cycle, "
                                f"subscriptions: {self.subscription_ids}, running: {self.running}")
                
                # Log every 10 seconds if we're checking events but none are for us
                if poll_count % 20 == 0 and events_checked > 0 and events_for_us == 0:
                    logger.info(f"🔍 Polling: checked {events_checked} events in message pool, "
                               f"none matched our subscriptions {self.subscription_ids} (user: {self.user_id})")
                
                # Adaptive sleep: short when processing events, longer when idle
                if events_found > 0:
                    await asyncio.sleep(0.05)  # 50ms - stay responsive when active
                else:
                    await asyncio.sleep(0.5)   # 500ms - save CPU when idle
                
            except Exception as e:
                logger.error(f"Error polling message pool for user {self.user_id}: {e}")
                await asyncio.sleep(1)  # Back off on error
    
    async def _process_events(self):
        """Process events from the queue in an event-driven manner.
        
        This method blocks waiting for events to arrive in the queue, consuming
        zero CPU when idle. Events are processed immediately upon arrival with
        no polling delay.
        """
        logger.info(f"Starting event-driven processor for user {self.user_id}")
        
        while self.running:
            try:
                # Block until an event arrives (zero CPU when idle!)
                event_msg = await self._event_queue.get()
                
                # Process the event
                await self._handle_event(event_msg)
                
                # Mark task as done for queue management
                self._event_queue.task_done()
                
            except asyncio.CancelledError:
                logger.info(f"Event processor cancelled for user {self.user_id}")
                break
            except Exception as e:
                logger.error(f"Error processing event for user {self.user_id}: {e}")
                await asyncio.sleep(0.1)  # Brief backoff on error
    
    async def _handle_event(self, event_msg):
        """Handle a received event message.
        
        Args:
            event_msg: Event message from nostrclient message pool
        """
        try:
            # Parse event JSON
            event = json.loads(event_msg.event)
            event_id = event.get("id")
            kind = event.get("kind")
            
            # Log ALL incoming events of interest (kinds 6, 7, 9735) BEFORE any filtering
            if kind in [6, 7, 9735]:
                tags = event.get("tags", [])
                e_tags = [tag[1] for tag in tags if isinstance(tag, list) and len(tag) >= 2 and tag[0] == "e"]
                pubkey = event.get("pubkey", "")[:16]
                
                kind_names = {6: "repost", 7: "reaction", 9735: "zap"}
                kind_name = kind_names.get(kind, f"kind{kind}")
                
                logger.info(
                    f"📥 Received {kind_name} event {event_id[:16]}... "
                    f"from {pubkey}... with e_tags: {[e[:16] + '...' for e in e_tags]} "
                    f"(user: {self.user_id})"
                )
            
            # Deduplication check
            if event_id in self._processed_event_ids:
                logger.debug(f"Skipping duplicate event {event_id}")
                return
            
            # Add to processed set
            self._processed_event_ids.add(event_id)
            
            # Prevent memory bloat
            if len(self._processed_event_ids) > self._max_tracked_events:
                # Remove oldest 100 entries (FIFO)
                oldest = list(self._processed_event_ids)[:100]
                for old_id in oldest:
                    self._processed_event_ids.discard(old_id)
            
            # Update stats
            self.events_processed += 1
            self.last_event_at = int(datetime.now().timestamp())  # Local time
            
            # Route to appropriate handler
            if kind == 9735 and self.on_zap_receipt:
                await self._handle_zap_receipt(event)
            elif kind == 9735 and not self.on_zap_receipt:
                logger.warning(f"⚠️  Zap receipt handler not set for user {self.user_id}")
            elif kind == 6 and self.on_repost:
                await self._handle_repost(event)
            elif kind == 6 and not self.on_repost:
                logger.warning(f"⚠️  Repost handler not set for user {self.user_id}")
            elif kind == 7 and self.on_reaction:
                await self._handle_reaction(event)
            elif kind == 7 and not self.on_reaction:
                logger.warning(f"⚠️  Reaction handler not set for user {self.user_id}")
            else:
                self.events_filtered += 1
                
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse event JSON for user {self.user_id}: {e}")
        except Exception as e:
            logger.error(f"Error handling event for user {self.user_id}: {e}")
    
    async def _handle_zap_receipt(self, event: dict):
        """Process kind 9735 zap receipt.
        
        Extracts zapper pubkey, zapped note ID, and amount from the zap receipt.
        According to NIP-57, zap receipts should include:
        - 'e' tag(s): References to zapped note(s) - prefer the last if multiple
        - 'p' tag: Pubkey of the recipient
        - 'description' tag: JSON-encoded zap request (kind 9734)
        - 'amount' tag: Amount in millisats
        
        Args:
            event: Zap receipt event (kind 9735)
        """
        try:
            event_id = event.get("id")
            tags = event.get("tags", [])
            
            logger.debug(f"🔍 Processing zap receipt {event_id[:16]}... (user: {self.user_id})")
            
            # Extract ALL 'e' tags (zapped notes) - NIP-57 prefers the last one
            e_tags = []
            for tag in tags:
                if isinstance(tag, list) and len(tag) >= 2 and tag[0] == "e":
                    e_tags.append(tag[1])
            
            # Use the last 'e' tag if multiple exist (per NIP-57 convention)
            zapped_note_id = e_tags[-1] if e_tags else None
            
            # Extract 'p' tag (recipient pubkey) for validation
            recipient_pubkey = None
            for tag in tags:
                if isinstance(tag, list) and len(tag) >= 2 and tag[0] == "p":
                    recipient_pubkey = tag[1]
                    break
            
            # Validate compliance: must have both 'e' and 'p' tags
            if not zapped_note_id:
                logger.warning(
                    f"⚠️  Non-compliant zap receipt {event_id[:16]}... - missing 'e' tag. "
                    f"Has p-tag: {bool(recipient_pubkey)}. Ignoring. (user: {self.user_id})"
                )
                return
            
            if not recipient_pubkey:
                logger.warning(
                    f"⚠️  Non-compliant zap receipt {event_id[:16]}... - missing 'p' tag. "
                    f"e-tag count: {len(e_tags)}. Ignoring. (user: {self.user_id})"
                )
                return
            
            # Log if multiple 'e' tags found (using last one per convention)
            if len(e_tags) > 1:
                logger.info(
                    f"📋 Zap receipt {event_id[:16]}... has {len(e_tags)} 'e' tags: {[e[:8]+'...' for e in e_tags]}. "
                    f"Using last: {zapped_note_id[:8]}... (user: {self.user_id})"
                )
            
            # Extract zap request (kind 9734) from 'description' tag
            # The description tag contains the JSON-encoded zap request
            zap_request = None
            for tag in tags:
                if isinstance(tag, list) and len(tag) >= 2 and tag[0] == "description":
                    try:
                        zap_request = json.loads(tag[1])
                    except json.JSONDecodeError as e:
                        logger.warning(
                            f"⚠️  Non-compliant zap receipt {event_id[:16]}... - "
                            f"invalid JSON in 'description' tag: {e}. Ignoring. (user: {self.user_id})"
                        )
                        return
                    break
            
            if not zap_request:
                logger.warning(
                    f"⚠️  Non-compliant zap receipt {event_id[:16]}... - missing 'description' tag "
                    f"(should contain zap request). Ignoring. (user: {self.user_id})"
                )
                return
            
            # Validate zap request structure
            if not isinstance(zap_request, dict):
                logger.warning(
                    f"⚠️  Non-compliant zap receipt {event_id[:16]}... - "
                    f"'description' is not a dict: {type(zap_request)}. Ignoring. (user: {self.user_id})"
                )
                return
            
            # Get zapper pubkey from zap request
            zapper_pubkey = zap_request.get("pubkey")
            if not zapper_pubkey:
                logger.warning(
                    f"⚠️  Non-compliant zap receipt {event_id[:16]}... - "
                    f"zap request missing 'pubkey' field. Ignoring. (user: {self.user_id})"
                )
                return
            
            # Get zap amount from 'amount' tag (in millisats)
            amount_msats = 0
            for tag in tags:
                if isinstance(tag, list) and len(tag) >= 2 and tag[0] == "amount":
                    try:
                        amount_msats = int(tag[1])
                    except (ValueError, TypeError) as e:
                        logger.warning(
                            f"⚠️  Invalid 'amount' tag in zap receipt {event_id[:16]}...: {e}. "
                            f"Defaulting to 0. (user: {self.user_id})"
                        )
                    break
            
            if amount_msats == 0:
                logger.debug(
                    f"Zap receipt {event_id[:16]}... has zero amount or missing 'amount' tag. "
                    f"Proceeding anyway. (user: {self.user_id})"
                )
                    break
            
            amount_sats = amount_msats // 1000
            
            self.zaps_detected += 1
            
            logger.info(
                f"✅ Valid zap receipt: {amount_sats} sats from {zapper_pubkey[:8]}... "
                f"to note {zapped_note_id[:8]}..., recipient {recipient_pubkey[:8]}... "
                f"(event: {event_id[:16]}..., user: {self.user_id})"
            )
            
            # Call callback if set
            if self.on_zap_receipt:
                await self.on_zap_receipt(
                    zapper_pubkey=zapper_pubkey,
                    zapped_note_id=zapped_note_id,
                    amount_sats=amount_sats,
                    event_id=event_id,
                    event=event
                )
                
        except Exception as e:
            logger.error(f"Error processing zap receipt: {e}")
    
    async def _handle_repost(self, event: dict):
        """Process kind 6 repost.
        
        According to NIP-18, reposts reference the original note via 'e' tag(s).
        May have multiple 'e' tags - we'll check all of them for tracked notes.
        
        Args:
            event: Repost event (kind 6)
        """
        try:
            event_id = event.get("id")
            reposter_pubkey = event.get("pubkey")
            
            logger.debug(f"🔍 Processing repost {event_id[:16]}... from {reposter_pubkey[:16]}... (user: {self.user_id})")
            
            # Extract ALL 'e' tags (reposted notes) - may reference multiple notes
            e_tags = []
            for tag in event.get("tags", []):
                if isinstance(tag, list) and len(tag) >= 2 and tag[0] == "e":
                    e_tags.append(tag[1])
            
            # Remove duplicates while preserving order
            seen = set()
            unique_e_tags = []
            for note_id in e_tags:
                if note_id not in seen:
                    seen.add(note_id)
                    unique_e_tags.append(note_id)
            
            if not unique_e_tags:
                logger.warning(f"⚠️  Repost {event_id[:16]}... missing 'e' tag (user: {self.user_id})")
                return
            
            # Log if multiple 'e' tags found (will process all of them)
            if len(unique_e_tags) > 1:
                logger.info(
                    f"📋 Repost {event_id[:16]}... has {len(e_tags)} 'e' tags ({len(unique_e_tags)} unique): "
                    f"{[e[:8]+'...' for e in unique_e_tags[:5]]}. Processing all. (user: {self.user_id})"
                )
            
            # Process each reposted note (callback can filter by tracked notes)
            for reposted_note_id in unique_e_tags:
                self.reposts_detected += 1
                
                logger.info(
                    f"Repost detected: {reposter_pubkey[:8]}... reposted {reposted_note_id[:8]}... "
                    f"(event: {event_id[:16]}..., user: {self.user_id})"
                )
                
                # Call callback if set
                if self.on_repost:
                    await self.on_repost(
                        reposter_pubkey=reposter_pubkey,
                        reposted_note_id=reposted_note_id,
                        event_id=event_id,
                        event=event
                    )
                
        except Exception as e:
            logger.error(f"Error processing repost: {e}")
    
    async def _handle_reaction(self, event: dict):
        """Process kind 7 reaction.
        
        According to NIP-25, reactions reference notes via 'e' tag(s) and authors via 'p' tag(s).
        May have multiple 'e' tags - we'll check all of them for tracked notes.
        
        Args:
            event: Reaction event (kind 7)
        """
        try:
            event_id = event.get("id")
            reactor_pubkey = event.get("pubkey")
            reaction_content = event.get("content", "+")  # Usually "+", "❤️", etc.
            
            logger.debug(f"🔍 Processing reaction {event_id[:16]}... from {reactor_pubkey[:16]}... content='{reaction_content}' (user: {self.user_id})")
            
            # Extract ALL 'e' tags (reacted notes) - may react to multiple notes
            e_tags = []
            for tag in event.get("tags", []):
                if isinstance(tag, list) and len(tag) >= 2 and tag[0] == "e":
                    e_tags.append(tag[1])
            
            # Remove duplicates while preserving order
            seen = set()
            unique_e_tags = []
            for note_id in e_tags:
                if note_id not in seen:
                    seen.add(note_id)
                    unique_e_tags.append(note_id)
            
            if not unique_e_tags:
                logger.warning(f"⚠️  Reaction {event_id[:16]}... missing 'e' tag (user: {self.user_id})")
                return
            
            # Log if multiple 'e' tags found (will process all of them)
            if len(unique_e_tags) > 1:
                logger.info(
                    f"📋 Reaction {event_id[:16]}... has {len(e_tags)} 'e' tags ({len(unique_e_tags)} unique): "
                    f"{[e[:8]+'...' for e in unique_e_tags[:5]]}. Processing all. (user: {self.user_id})"
                )
            
            # Process each reacted note (callback can filter by tracked notes)
            for reacted_note_id in unique_e_tags:
                self.reactions_detected += 1
                
                logger.info(
                    f"Reaction detected: {reactor_pubkey[:8]}... reacted '{reaction_content}' "
                    f"to {reacted_note_id[:8]}... (event: {event_id[:16]}..., user: {self.user_id})"
                )
                
                # Call callback if set
                if self.on_reaction:
                    await self.on_reaction(
                        reactor_pubkey=reactor_pubkey,
                        reacted_note_id=reacted_note_id,
                        reaction_content=reaction_content,
                        event_id=event_id,
                        event=event
                    )
                
        except Exception as e:
            logger.error(f"Error processing reaction: {e}")
    
    async def update_subscriptions(self, tracked_note_ids: list[str],
                                   enable_zaps: bool = True, enable_reposts: bool = False,
                                   enable_reactions: bool = False, since_timestamp: int | None = None,
                                   note_timestamps: dict[str, int] | None = None):
        """Update subscriptions with new tracked note IDs and/or event type preferences.
        
        This closes old subscriptions and creates new ones with the
        updated note list and event type settings. Useful when:
        - tracked_event_ids change
        - user enables/disables event types (zaps, reposts, reactions)
        - recovery mode is triggered
        
        Args:
            tracked_note_ids: New list of note IDs to monitor
            enable_zaps: Subscribe to kind 9735 zap receipts (default: True)
            enable_reposts: Subscribe to kind 6 reposts (default: False)
            enable_reactions: Subscribe to kind 7 reactions (default: False)
            since_timestamp: Optional epoch timestamp to filter events from
            note_timestamps: Dict mapping note_id -> created_at timestamp (for event recovery)
        """
        if not self.running:
            logger.warning("Cannot update subscriptions, monitor not running")
            return
        
        # Use provided timestamp or stored note timestamps for recovery
        if since_timestamp is not None:
            since = since_timestamp
        else:
            since = _get_earliest_timestamp_from_dict(tracked_note_ids, note_timestamps or {}, default_days_ago=30)
        
        logger.info(f"Updating subscriptions for user {self.user_id}, {len(tracked_note_ids)} notes, "
                   f"zaps={enable_zaps}, reposts={enable_reposts}, reactions={enable_reactions}, since={since} ({datetime.fromtimestamp(since).astimezone().isoformat()})")  # Show in local time
        
        # Close old subscriptions
        for sub_id in self.subscription_ids:
            try:
                nostr_helpers.close_subscription(sub_id)
                logger.debug(f"Closed subscription {sub_id}")
            except Exception as e:
                logger.warning(f"Failed to close subscription {sub_id}: {e}")
        
        self.subscription_ids.clear()
        
        # Create new subscriptions
        if tracked_note_ids:
            if enable_zaps:
                await self._subscribe_zap_receipts(tracked_note_ids, since)
            
            if enable_reposts or enable_reactions:
                # Don't use 'since' for reposts/reactions - they can happen at any time
                await self._subscribe_social_events(tracked_note_ids, since=None, enable_reposts=enable_reposts, enable_reactions=enable_reactions)
            
            logger.info(f"Updated subscriptions for user {self.user_id}")
        else:
            logger.warning(f"No tracked notes provided for user {self.user_id}, subscriptions cleared")
    
    async def stop(self):
        """Stop monitoring and clean up resources."""
        if not self.running:
            return
        
        logger.info(f"Stopping Nostr monitor for user {self.user_id}")
        
        self.running = False
        
        # Cancel both tasks
        if self._poll_task:
            self._poll_task.cancel()
            try:
                await self._poll_task
            except asyncio.CancelledError:
                pass
            self._poll_task = None
        
        if self._event_processing_task:
            self._event_processing_task.cancel()
            try:
                await self._event_processing_task
            except asyncio.CancelledError:
                pass
            self._event_processing_task = None
        
        # Clear the queue
        while not self._event_queue.empty():
            try:
                self._event_queue.get_nowait()
                self._event_queue.task_done()
            except asyncio.QueueEmpty:
                break
        
        # Close subscriptions
        for sub_id in self.subscription_ids:
            try:
                nostr_helpers.close_subscription(sub_id)
                logger.debug(f"Closed subscription {sub_id}")
            except Exception as e:
                logger.warning(f"Failed to close subscription {sub_id}: {e}")
        
        self.subscription_ids.clear()
        
        logger.info(f"Nostr monitor stopped for user {self.user_id}")
    
    def status(self) -> dict[str, Any]:
        """Return monitoring status and statistics.
        
        Returns:
            Dictionary with monitoring status and stats
        """
        return {
            "running": self.running,
            "user_id": self.user_id,
            "subscription_ids": self.subscription_ids,
            "events_processed": self.events_processed,
            "events_filtered": self.events_filtered,
            "zaps_detected": self.zaps_detected,
            "reposts_detected": self.reposts_detected,
            "reactions_detected": self.reactions_detected,
            "last_event_at": self.last_event_at,
            "queue_size": self._event_queue.qsize(),
            "queue_maxsize": self._event_queue.maxsize,
        }

    async def _wait_for_relays(self, timeout: int = 30) -> bool:
        """Wait for at least one relay to be connected.
        
        This is critical for startup - we must wait for nostrclient to initialize
        its relays before trying to subscribe, otherwise subscriptions fail silently.
        
        Args:
            timeout: Maximum seconds to wait
            
        Returns:
            True if relays connected, False if timeout
        """
        import time
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            relay_info = nostr_helpers.get_relay_info()
            
            if not relay_info.get('available'):
                logger.debug(f"Nostrclient not available for user {self.user_id}, waiting...")
                await asyncio.sleep(1)
                continue
            
            if relay_info.get('relay_count', 0) == 0:
                logger.debug(f"No relays configured yet for user {self.user_id}, waiting...")
                await asyncio.sleep(1)
                continue
            
            if relay_info.get('connected_count', 0) > 0:
                logger.info(f"Found {relay_info['connected_count']}/{relay_info['relay_count']} connected relays for user {self.user_id}")
                return True
            
            logger.debug(f"Relays exist but not connected yet for user {self.user_id}, waiting...")
            await asyncio.sleep(1)
        
        logger.warning(f"Timeout waiting for relays for user {self.user_id} after {timeout}s")
        return False
    
    async def check_relay_status(self) -> dict:
        """Check relay connection status for diagnostics.
        
        Returns:
            Dict with relay status information
        """
        return nostr_helpers.get_relay_info()


# Module-level instance tracking for management
_monitors: dict[str, NostrEventMonitor] = {}


def get_monitor(user_id: str) -> NostrEventMonitor | None:
    """Get existing monitor for user.
    
    Args:
        user_id: User ID
        
    Returns:
        NostrEventMonitor instance or None if not found
    """
    return _monitors.get(user_id)


def create_monitor(user_id: str) -> NostrEventMonitor:
    """Create or get existing monitor for user.
    
    Args:
        user_id: User ID
        
    Returns:
        NostrEventMonitor instance
    """
    if user_id not in _monitors:
        _monitors[user_id] = NostrEventMonitor(user_id)
    return _monitors[user_id]


async def cleanup_monitor(user_id: str):
    """Stop and remove monitor for user.
    
    Args:
        user_id: User ID
    """
    if user_id in _monitors:
        await _monitors[user_id].stop()
        del _monitors[user_id]


def get_all_monitors() -> dict[str, NostrEventMonitor]:
    """Get all active monitors.
    
    Returns:
        Dictionary of user_id -> NostrEventMonitor
    """
    return _monitors.copy()
