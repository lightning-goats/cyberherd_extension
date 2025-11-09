"""Nostr-based CyberHerd event monitor.

This service monitors Nostr events (reposts, reactions) for tracked notes
using the subscription system. Event detection happens via Nostr relays through
the subscriptions module (subscriptions.py), which publishes kind 6 reposts
and kind 7 reactions.

ZAP DETECTION: Zaps are detected ONLY via the payment listener system, which
processes invoice payments containing zap requests in payment.extra["nostr"].
Nostr-based zap detection (kinds 9734/9735) has been intentionally removed to
avoid duplicate processing and ensure accurate tracking via invoice settlements.

The payment listener parses LNURLp zap requests following NIP-57 format:
- Zap request JSON from payment.extra["nostr"]
- Target note ID extracted from tags: ["e", "<note_id>", ...]
- Amount taken from payment.amount (in millisats)
- Zapper pubkey from zap_request.get("pubkey")

Note: The NostrEventMonitor class was planned but not implemented. Event monitoring
is handled by the subscriptions system instead.
"""

from __future__ import annotations

import asyncio
import time
from datetime import datetime, timezone
from typing import Any, Optional, cast
import re

from loguru import logger

from .. import crud
from .subscriptions import get_subscription_status
from .pubkey import resolve_effective_pubkey
from .note_metadata import apply_event_address
from ..utils.common import utc_now_timestamp  # Centralized UTC timestamp function
from ..utils.zap_requests import extract_zap_request

# NostrEventMonitor was planned but not implemented - monitoring happens via subscriptions.py
_monitoring_available = False
_register_invoice_listener = None  # type: ignore

try:  # pragma: no cover
    from lnbits.tasks import register_invoice_listener as _register_invoice_listener  # type: ignore
except Exception:  # pragma: no cover
    _register_invoice_listener = None  # type: ignore
else:  # pragma: no cover
    _monitoring_available = True

try:  # pragma: no cover
    from . import nostr_helpers  # type: ignore
except Exception:  # pragma: no cover
    nostr_helpers = None  # type: ignore

_instances: dict[str, 'ZapMonitorService'] = {}
_last_access: dict[str, float] = {}
_TTL = 1800

# Aliases for backward compatibility with views_api.py imports
_zap_monitor_instances = _instances
_ZAP_MONITOR_LAST_ACCESS = _last_access


async def cleanup_stale_monitors(max_age: int | None = None) -> dict[str, int]:
    """Clean up monitors that haven't been accessed in a while."""
    age = max_age or _TTL
    now = time.time()
    removed = 0
    for uid, ts in list(_last_access.items()):
        if (now - ts) > age and uid in _instances:
            inst = _instances[uid]
            try:
                if inst._running:
                    await inst.stop_monitoring()
            except Exception:
                pass
            _instances.pop(uid, None)
            _last_access.pop(uid, None)
            removed += 1
    return {"removed": removed, "age_seconds": age}


class ZapMonitorService:
    """Service for monitoring Nostr events (reposts, reactions) and payment-based zaps.
    
    This service uses two detection paths:
    
    1. **Payment Listener (Zaps)**: Monitors invoice payments for LNURLp zaps via
       payment.extra["nostr"]. This is the ONLY way zaps are detected.
       - Kind 9734 (zap request) and 9735 (zap receipt) are NOT monitored via Nostr
       - Prevents duplicate processing and ensures accurate invoice-based tracking
    
    2. **Nostr Subscriptions (Engagement)**: Monitors Nostr relays for:
       - Kind 6: Reposts (shares/boosts)
       - Kind 7: Reactions (likes/emoji reactions)
       - Kind 1: Regular notes (for today's notes cache)
       - Kind 30311: Long-form content (for today's notes cache)
    
    Zaps are processed when:
    - Invoice payment received to herd_wallet
    - payment.extra["nostr"] contains valid NIP-57 zap request
    - Target note ID extracted from tags: ["e", "<note_id>"]
    - Amount from payment.amount (in millisats)
    """
    
    def __init__(self, app=None, db=None, messaging=None, user_id: str | None = None):
        # Core references
        self.app = app
        self.db = db
        self.messaging = messaging
        self.user_id = user_id

        # Runtime flags
        self._running = False

        # State / timestamps
        self.last_message_at = None  # epoch seconds
        self.last_zap_at = None      # epoch seconds of last processed zap
        self.last_error = None
        self.mode = 'nostr'

        # Metrics / counters
        self.total_zaps_processed = 0
        self.total_reposts_processed = 0
        self.total_reactions_processed = 0
        # Recovery scans performed (payment-based recovery routine)
        self.total_recovery_scans = 0
        
        # Deduplication cache: track processed events to prevent duplicate handling
        # Key: (event_type, event_id, note_id, pubkey)
        # Value: timestamp when processed
        self._processed_events: dict[tuple[str, str, str, str], int] = {}
        self._processed_events_max_size = 1000  # Prevent unbounded growth
        
        # Payment listener task and registration state
        self._payment_listener_task: Optional[asyncio.Task] = None
        self._invoice_queue: Optional[asyncio.Queue] = None  # Queue for invoice payments
        
        # NostrEventMonitor not implemented - monitoring handled by subscriptions.py
        self.nostr_monitor: Optional[Any] = None  # Reserved for future implementation

    @staticmethod
    def _normalize_payment_hash(value: Any) -> str | None:
        """Return lowercase 64-hex hash when possible."""
        if isinstance(value, bytes):
            try:
                value = value.hex()
            except Exception:
                value = None
        if isinstance(value, str):
            candidate = value.strip().lower()
            if len(candidate) >= 64:
                candidate = candidate[:64]
            if re.fullmatch(r"[0-9a-f]{64}", candidate):
                return candidate
        return None

    @staticmethod
    def _parse_a_tag_address(address: str) -> tuple[int | None, str | None, str | None]:
        """Parse an 'a' tag address string into (kind, pubkey, identifier)."""
        if not isinstance(address, str):
            return None, None, None
        parts = address.split(":")
        if len(parts) < 3:
            return None, None, None
        kind_part, pubkey_part, *rest = parts
        try:
            kind = int(kind_part)
        except Exception:
            return None, None, None
        pubkey = pubkey_part.strip().lower() if pubkey_part else None
        identifier = ":".join(rest).strip() if rest else None
        if identifier == "":
            identifier = None
        return kind, pubkey, identifier

    async def _resolve_note_from_addresses(
        self,
        settings,
        address_candidates: list[str],
    ) -> tuple[str | None, str | None]:
        """Resolve note id and author from provided NIP-33 address candidates."""
        if not address_candidates:
            return None, None

        try:
            address_map = dict(getattr(settings, "tracked_event_addresses", {}) or {})
        except Exception:
            address_map = {}

        # Prefer address matches already cached in settings
        inverse: dict[str, str] = {}
        for note_id, addr in address_map.items():
            if isinstance(note_id, str) and isinstance(addr, str):
                inverse[addr] = note_id.strip().lower()

        for address in address_candidates:
            note_id = inverse.get(address)
            if note_id and re.fullmatch(r"[0-9a-f]{64}", note_id):
                _, author, _ = self._parse_a_tag_address(address)
                author_norm = author.strip().lower() if isinstance(author, str) else None
                return note_id, author_norm

        # Fallback to querying relays when cache is missing
        if not nostr_helpers or not hasattr(nostr_helpers, "check_availability"):
            return None, None
        try:
            available = nostr_helpers.check_availability()
        except Exception:
            available = False
        if not available:
            return None, None

        for address in address_candidates:
            kind, author, identifier = self._parse_a_tag_address(address)
            if kind is None or not author:
                continue
            filters = {"kinds": [kind], "authors": [author]}
            if identifier:
                filters["#d"] = [identifier]
            try:
                events = await nostr_helpers.query_events(filters, limit=1, timeout=6.0)
            except Exception as exc:
                logger.debug(
                    f"Zap monitor: failed to resolve address {address} for user {self.user_id}: {exc}"
                )
                continue
            if not events:
                continue
            event = events[0]
            note_id_raw = event.get("id")
            if not isinstance(note_id_raw, str):
                continue
            note_id_norm = note_id_raw.strip().lower()
            if not re.fullmatch(r"[0-9a-f]{64}", note_id_norm):
                continue

            # Cache the resolved address for future lookups
            address_map[note_id_norm] = address
            try:
                apply_event_address(address_map, event)
            except Exception:
                pass
            try:
                settings.tracked_event_addresses = address_map
            except Exception:
                pass
            if self.user_id:
                try:
                    await crud.upsert_settings(settings, self.user_id)
                except Exception as exc:
                    logger.debug(
                        f"Zap monitor: failed to persist address mapping for user {self.user_id}: {exc}"
                    )
            return note_id_norm, author

        return None, None

    async def start_monitoring(self):
        """Start monitoring for zaps via payment listener.
        
        The payment listener is started when:
        - zap_tracking_enabled is true (for processing zaps), OR
        - send_splits_enabled is true (for automatic splits)
        
        Nostr event monitoring (reposts/reactions) is handled separately by subscriptions.py.
        """
        # Get user settings
        settings = await crud.get_settings(self.user_id)
        
        # Start payment listener if either zap tracking OR automatic splits are enabled
        zap_tracking = getattr(settings, 'zap_tracking_enabled', False)
        auto_splits = getattr(settings, 'send_splits_enabled', False)
        
        if not zap_tracking and not auto_splits:
            logger.info(
                f"Payment listener not needed for user {self.user_id}: "
                f"zap_tracking_enabled={zap_tracking}, send_splits_enabled={auto_splits}"
            )
            if self._running:
                await self.stop_monitoring()
            return
        
        if self._running:
            return
            
        self._running = True
        
        # Start payment listener for LNURLp zaps AND automatic splits
        self._payment_listener_task = asyncio.create_task(self.payment_listener())
        logger.info(
            f"Started payment listener for user {self.user_id} "
            f"(zap_tracking={zap_tracking}, auto_splits={auto_splits})"
        )

    async def stop_monitoring(self):
        """Stop monitoring Nostr events."""
        if not self._running:
            return
            
        self._running = False
        
        # Stop payment listener task
        if self._payment_listener_task:
            try:
                self._payment_listener_task.cancel()
                await asyncio.wait([self._payment_listener_task], timeout=2.0)
                logger.info(f"Payment listener stopped for user {self.user_id}")
            except Exception as e:
                logger.error(f"Error stopping payment listener: {e}")
            self._payment_listener_task = None
        
        # Stop Nostr monitoring if active
        if self.nostr_monitor:
            try:
                await self.nostr_monitor.stop()
                logger.info(f"Nostr monitor stopped for user {self.user_id}")
            except Exception as e:
                logger.error(f"Error stopping Nostr monitor: {e}")
            self.nostr_monitor = None
    
    async def _process_payment_for_zap(self, payment):
        """Process a payment notification to detect LNURLp zaps.
        
        This method is called by the invoice listener and parses zap requests from
        payment.extra["nostr"] (NIP-57 zap request payload).
        
        IMPORTANT: Only processes zaps for TODAY's tracked notes. This enforces
        the rule that zaps are only valid for the current day's CyberHerd notes.
        
        Args:
            payment: Payment object with extra data containing zap request
        """
        try:
            # Only process payments to the configured herd wallet
            settings = await crud.get_settings(self.user_id)
            if not settings:
                logger.debug(f"No settings found for user {self.user_id}")
                return
            
            if not getattr(settings, "zap_tracking_enabled", False):
                logger.info(
                    f"Zap processing skipped for user {self.user_id}: zap_tracking_enabled is False"
                )
                self.last_error = "zap_tracking_disabled"
                return False
            
            herd_wallet_id = getattr(settings, 'herd_wallet', None)
            # Coerce herd_wallet_id to a string only if it's a real value. Tests use Mock objects
            # which will return a Mock for missing attributes; treat non-str values as unset.
            if not isinstance(herd_wallet_id, str):
                herd_wallet_id = None

            # If herd_wallet_id is configured, require the payment to be to that wallet.
            if herd_wallet_id:
                if payment.wallet_id != herd_wallet_id:
                    # Payment is not to the herd wallet, ignore
                    return
            
            zap_info = extract_zap_request(payment)
            if not zap_info:
                payment_hash_preview = None
                try:
                    payment_hash = getattr(payment, 'payment_hash', None) or getattr(payment, 'checking_id', None)
                    payment_hash_preview = payment_hash[:16] if isinstance(payment_hash, str) else None
                except Exception:
                    payment_hash_preview = None
                logger.debug(f"No zap request found in payment {payment_hash_preview}...")
                self.last_error = "payment_missing_nostr_field"
                return False

            zap_request = zap_info.payload
            zap_request_source = zap_info.source or "nostr"

            # Extract zapper pubkey
            zapper_pubkey = zap_request.get("pubkey")
            if not zapper_pubkey:
                logger.warning(f"Zap request missing pubkey field (source: {zap_request_source})")
                self.last_error = "missing_zapper_pubkey"
                return False
            
            # Reject zaps from the effective pubkey (operator's own zaps)
            try:
                eff_pub = resolve_effective_pubkey(settings)
                if eff_pub and zapper_pubkey.lower() == eff_pub.lower():
                    logger.info(
                        f"Zap rejected: zapper is the effective pubkey (operator's own zap) "
                        f"zapper={zapper_pubkey[:16]}..."
                    )
                    self.last_error = "zapper_is_effective_pubkey"
                    return False
            except Exception as e:
                logger.debug(f"Could not check if zapper is effective pubkey: {e}")
            
            # Extract target note references from tags (NIP-57 format)
            target_note_id_raw: Optional[str] = None
            target_author_raw: Optional[str] = None
            address_candidates: list[str] = []

            tags = zap_request.get("tags", [])
            for tag in tags:
                if not (isinstance(tag, list) and len(tag) >= 2):
                    continue
                marker = tag[0]
                value = tag[1]
                if marker == "e" and value and target_note_id_raw is None:
                    target_note_id_raw = value
                elif marker == "p" and value and target_author_raw is None:
                    target_author_raw = value
                elif marker == "a" and isinstance(value, str) and value:
                    address_candidates.append(value)

            # Fallbacks for non-standard fields
            if not target_note_id_raw:
                target_note_id_raw = zap_request.get("e")
            if not target_author_raw:
                target_author_raw = zap_request.get("p")

            extra_address = zap_request.get("a")
            if isinstance(extra_address, str) and extra_address:
                address_candidates.append(extra_address)

            if not target_note_id_raw and address_candidates:
                resolved_id, resolved_author = await self._resolve_note_from_addresses(settings, address_candidates)
                if resolved_id:
                    target_note_id_raw = resolved_id
                    if not target_author_raw and resolved_author:
                        target_author_raw = resolved_author

            if not target_note_id_raw:
                logger.warning(
                    f"Zap request missing target note ID (no resolvable 'e' or 'a' tag). "
                    f"Tags: {tags[:3] if tags else []}"
                )
                self.last_error = "profile_zap_no_e_tag"
                return False

            # Normalize target note id to a lowercase 64-hex string for reliable
            # comparison against settings.tracked_event_ids (which are stored
            # as lower-case hex). Reject non-64-hex values early.
            target_note_id = None
            try:
                if isinstance(target_note_id_raw, str):
                    cand = target_note_id_raw.strip().lower()
                    if re.fullmatch(r"[0-9a-f]{64}", cand):
                        target_note_id = cand
            except Exception:
                target_note_id = None

            if not target_note_id:
                logger.warning(
                    f"Zap request target note id invalid after normalization: {repr(target_note_id_raw)}"
                )
                self.last_error = "invalid_target_note_id"
                return False

            target_author = None
            try:
                if isinstance(target_author_raw, str):
                    cand = target_author_raw.strip().lower()
                    if re.fullmatch(r"[0-9a-f]{64}", cand):
                        target_author = cand
            except Exception:
                target_author = None
            
            # Use payment amount, not zap request amount (zap request amount is in millisats string)
            amount_msats = payment.amount or 0
            amount_sats = max(amount_msats // 1000, 0)
            
            try:
                payment_hash = getattr(payment, 'payment_hash', None) or getattr(payment, 'checking_id', None)
                payment_hash_preview = payment_hash[:16] if isinstance(payment_hash, str) else None
            except Exception:
                payment_hash_preview = None
            logger.info(
                f"💰 LNURLp zap detected: {amount_sats} sats from {zapper_pubkey[:8]}... "
                f"to note {target_note_id[:8]}... (payment: {payment_hash_preview}..., "
                f"source: {zap_request_source})"
            )
            
            # Check if zapper is already an existing active member
            # Existing members can zap ANY Lightning Goats note to increase their amount
            # New members must zap today's #CyberHerd tagged note
            is_existing_member = False
            
            try:
                existing_member = await crud.get_cyberherd_member_by_pubkey(zapper_pubkey, user_id=self.user_id)
                is_existing_member = bool(existing_member and existing_member.get('is_active'))
                if is_existing_member:
                    logger.info(
                        f"✅ Zapper {zapper_pubkey[:8]}... is an existing active member - "
                        f"allowing zap to ANY Lightning Goats note"
                    )
            except Exception as e:
                logger.debug(f"Error checking existing membership for {zapper_pubkey[:8]}...: {e}")
                is_existing_member = False
            
            # For existing members, only verify the note is authored by Lightning Goats (effective pubkey)
            # For new members, enforce strict tracking requirements
            if is_existing_member:
                # Verify note author matches effective pubkey (Lightning Goats)
                if target_author:
                    try:
                        eff_pub = resolve_effective_pubkey(settings)
                        if eff_pub and target_author.lower() != eff_pub.lower():
                            logger.info(
                                f"Zap target {target_note_id[:8]}... by author {target_author[:8]}... "
                                f"does not match Lightning Goats pubkey {eff_pub[:8]}... - rejecting"
                            )
                            self.last_error = "note_author_mismatch"
                            return False
                    except Exception as e:
                        logger.debug(f"Could not verify note author: {e}")
                        # Continue optimistically if verification fails
                else:
                    logger.debug(
                        f"No author information for note {target_note_id[:8]}... - "
                        f"allowing zap for existing member {zapper_pubkey[:8]}..."
                    )
            else:
                # New member: enforce strict tracking requirements
                # Simple rule: If the zapped note is in tracked_event_ids, it's valid
                # tracked_event_ids only contains notes from TODAY with the required tags
                tracked_notes = getattr(settings, 'tracked_event_ids', []) or []
                
                # Check if target note is in tracked events
                is_tracked = target_note_id in tracked_notes
                
                if not is_tracked:
                    logger.info(
                        f"❌ Zap target {target_note_id[:8]}... is NOT in tracked_event_ids "
                        f"({len(tracked_notes)} tracked events). Ignoring zap."
                    )
                    self.last_error = "note_not_tracked"
                    return False
                
                # Note is validated as being in tracked_event_ids
                logger.debug(
                    f"✅ Zap target {target_note_id[:16]}... IS in tracked_event_ids. "
                    f"Proceeding with headbutt processing..."
                )
            
            # Check for duplicate processing using payment hash as event ID
            event_id_raw = getattr(payment, "payment_hash", None) or getattr(payment, "checking_id", None)
            event_id = self._normalize_payment_hash(event_id_raw)

            # First check persisted processed_events table (guards against duplicate posts across restarts)
            try:
                # Only call persistence API if we have an event_id string
                raw_processed = (
                    await crud.is_payment_processed(cast(str, self.user_id), event_id)
                    if event_id
                    else False
                )
            except Exception:
                # If persistence layer unavailable in tests/mocks, assume not processed
                raw_processed = False

            # Only treat a True bool as already processed; mocks returning MagicMock should not short-circuit
            is_already_processed = True if isinstance(raw_processed, bool) and raw_processed else False
            if is_already_processed:
                pid_preview = event_id[:16] + "..." if event_id else "unknown"
                nid_preview = target_note_id[:8] + "..." if isinstance(target_note_id, str) else "unknown"
                ppk_preview = zapper_pubkey[:8] + "..." if isinstance(zapper_pubkey, str) else "unknown"
                logger.debug(
                    f"🔄 Duplicate payment detected (persisted): payment_hash={pid_preview}, note={nid_preview}, pubkey={ppk_preview} Skipping."
                )
                self.last_error = None
                return True
            
            # Fast path in-memory check (still useful for within-session deduplication)
            if event_id and self._mark_event_processed('zap_payment', event_id, target_note_id, zapper_pubkey):
                self.last_error = None
                return True  # Already processed in this session
            
            # Register payment in persisted store before processing to prevent races
            inserted = True
            try:
                if event_id:
                    inserted = await crud.register_processed_payment(
                        user_id=cast(str, self.user_id),
                        payment_hash=event_id,
                        note_id=target_note_id,
                        pubkey=zapper_pubkey,
                        event_type="zap",
                    )
            except Exception:
                # Best-effort: ignore persistence failures in tests/mocks
                inserted = False

            if event_id and not inserted:
                logger.debug(
                    f"Zap dedupe: payment hash {event_id[:16]}... already persisted for user {self.user_id}, skipping."
                )
                self.last_error = None
                return True
            
            # Trigger headbutt processing
            from .headbutt import trigger_headbutt_from_zap
            
            # Call trigger_headbutt_from_zap; it may be patched in tests as a non-async Mock.
            try:
                call_res = trigger_headbutt_from_zap(
                    user_id=cast(str, self.user_id),
                    pubkey=zapper_pubkey,
                    amount_sats=amount_sats,
                    note_id=target_note_id,
                    event_id=event_id or "",
                    app=self.app,
                )
                if asyncio.iscoroutine(call_res):
                    result = await call_res
                else:
                    result = call_res
                logger.debug(f"trigger_headbutt_from_zap returned: {result!r}")
            except Exception as e:
                logger.error(f"Error calling trigger_headbutt_from_zap: {e}")
                result = None
            
            if result:
                self.total_zaps_processed += 1
                self.last_zap_at = utc_now_timestamp()
                self.last_message_at = self.last_zap_at
                self.last_error = None
                logger.info(
                    f"✅ Successfully processed LNURLp zap: {amount_sats} sats from {zapper_pubkey[:8]}..."
                )

                # Persist zap record for diagnostics and historical counters
                try:
                    if self.user_id:
                        await crud.create_processed_zap(
                            {
                                "user_id": cast(str, self.user_id),
                                "event_id": event_id or "",
                                "note_id": target_note_id,
                                "zapper_pubkey": zapper_pubkey,
                                "amount": int(amount_sats),
                                "processed_at": utc_now_timestamp(),
                            }
                        )
                except Exception as exc:
                    logger.debug(
                        f"Zap monitor: failed to persist processed zap for user {self.user_id}: {exc}"
                    )
                
                # Note: Opportunistic registry update removed - we now enforce that notes
                # must be in today's tracked_event_ids before processing zaps
                # Note: Automatic splits trigger moved to _check_and_trigger_splits which is
                # called for ALL payments to the herd wallet (not just zaps)
                return True
            else:
                logger.warning(f"Failed to process LNURLp zap from {zapper_pubkey[:8]}...")
                # Record failure for diagnostics and signal failure to caller
                self.last_error = "headbutt_processing_failed"
                return False
                
        except Exception as e:
            logger.error(f"Error processing payment for zap: {e}")
            import traceback
            logger.debug(f"Traceback: {traceback.format_exc()}")
            # Prefix with 'exception:' to match test expectations
            try:
                self.last_error = f"exception:{e}"
            except Exception:
                self.last_error = "exception:unknown"
            return False
        # Default to False for any non-successful processing (explicit)
        return False

    async def _check_and_trigger_splits(self, payment):
        """Check if automatic splits should be triggered for any payment to the herd wallet.
        
        This method is called for EVERY payment received to the herd wallet, regardless of
        whether it's a zap or not. It checks if:
        1. send_splits_enabled is true
        2. feeder_trigger_sats is configured
        3. herd wallet balance >= trigger amount
        
        If all conditions are met, it triggers an automatic transfer of the entire herd
        wallet balance to the CyberHerd (splits) wallet.
        
        Args:
            payment: Payment object received from the invoice listener
        """
        try:
            # Get user settings
            settings = await crud.get_settings(self.user_id)
            if not settings:
                return
            
            # Check if automatic splits are enabled
            if not getattr(settings, "send_splits_enabled", False):
                return
            
            # Only process payments to the configured herd wallet
            herd_wallet_id = getattr(settings, 'herd_wallet', None)
            if not isinstance(herd_wallet_id, str):
                herd_wallet_id = None
            
            if not herd_wallet_id:
                return
            
            # Check if this payment is to the herd wallet
            if payment.wallet_id != herd_wallet_id:
                return
            
            # Trigger automatic splits check
            from .send_splits import maybe_send_splits_for_user
            
            split_result = await maybe_send_splits_for_user(
                user_id=cast(str, self.user_id),
                settings=settings
            )
            
            if split_result and split_result.get("ok"):
                logger.info(
                    f"💸 Automatic splits triggered by payment: transferred {split_result.get('sats')} sats "
                    f"from herd wallet to splits wallet for user {self.user_id}"
                )
            elif split_result and not split_result.get("ok"):
                # Log the error but don't fail the payment processing
                logger.debug(
                    f"Automatic splits check failed for user {self.user_id}: {split_result.get('error')}"
                )
                
        except Exception as exc:
            # Don't let splits check errors affect payment processing
            logger.debug(
                f"Error checking/triggering automatic splits for user {self.user_id}: {exc}"
            )

    async def _ensure_note_tracked(
        self,
        settings,
        note_id: str,
        created_at: int | None = None,
        author_hint: str | None = None,
    ) -> bool:
        """Best-effort helper to persist zap target note IDs into tracked_event_ids.

        Situations where this is useful:
        - Startup races where invoice settlements arrive before subscriptions
          finish populating tracked_event_ids.
        - Recovery scenarios where LNURLp callbacks hit the server before
          the registry refresh completes.
        """
        if not self.user_id or not settings or not note_id or not author_hint:
            return False

        try:
            # If we know the expected author, verify it matches the zap target
            if author_hint:
                try:
                    eff = resolve_effective_pubkey(settings)
                    eff_norm = str(eff).strip().lower() if eff else ""
                    if eff_norm and author_hint != eff_norm:
                        logger.debug(
                            "Zap monitor: refusing to auto-track note %s for user %s "
                            "because author %s != effective pubkey %s",
                            note_id[:8],
                            self.user_id,
                            author_hint[:8],
                            eff_norm[:8],
                        )
                        return False
                except Exception:
                    # If effective pubkey resolution fails, continue optimistically
                    pass

            tagged_event = await self._fetch_tagged_note(settings, note_id, author_hint)
            if not tagged_event:
                logger.debug(
                    "Zap monitor: refusing to auto-track note %s for user %s because it "
                    "does not contain required tracked tags",
                    note_id[:8],
                    self.user_id,
                )
                return False

            tracked = list(getattr(settings, "tracked_event_ids", []) or [])
            if note_id in tracked:
                return True

            tracked.append(note_id)
            # Preserve insertion order while ensuring normalized lowercase IDs
            normalized = []
            seen = set()
            for nid in tracked:
                if not isinstance(nid, str):
                    continue
                candidate = nid.strip().lower()
                if not candidate or candidate in seen:
                    continue
                seen.add(candidate)
                normalized.append(candidate)
            settings.tracked_event_ids = normalized

            # Maintain timestamps map if available so recovery routines have context
            timestamps = dict(getattr(settings, "tracked_event_timestamps", {}) or {})
            if note_id not in timestamps:
                created_at = None
                try:
                    created_at = int(tagged_event.get("created_at") or 0)
                except Exception:
                    created_at = None
                if created_at and created_at > 0:
                    timestamps[note_id] = created_at
                else:
                    timestamps[note_id] = utc_now_timestamp()
                settings.tracked_event_timestamps = timestamps

            # Update tracked event addresses with any metadata derived from the event
            try:
                addresses = dict(getattr(settings, "tracked_event_addresses", {}) or {})
                apply_event_address(addresses, tagged_event)
                settings.tracked_event_addresses = addresses
            except Exception:
                pass

            await crud.upsert_settings(settings, self.user_id)
            logger.info(
                f"ℹ️  Zap monitor: opportunistically recorded note {note_id[:8]}... "
                f"in tracked_event_ids for user {self.user_id}"
            )
            return True
        except Exception as e:
            logger.debug(
                f"Zap monitor: failed to append note {note_id[:8]}... to tracked_event_ids "
                f"for user {self.user_id}: {e}"
            )
            return False

    async def _fetch_tagged_note(self, settings, note_id: str, author_hint: str) -> dict | None:
        """Return the event dict when *note_id* carries one of the tracked tags."""
        tags = getattr(settings, "tracked_tags", []) or []
        tag_norm = [
            t.lstrip("#").lower()
            for t in tags
            if isinstance(t, str) and t.strip()
        ]
        if not tag_norm:
            return None

        if nostr_helpers is None:
            return None

        try:
            events = await nostr_helpers.query_events(
                {"ids": [note_id], "kinds": [1, 30311]},
                limit=1,
                timeout=6.0,
            )
        except Exception as exc:
            logger.debug(
                "Zap monitor: failed to query note %s for tag verification: %s",
                note_id[:8],
                exc,
            )
            return None

        if not events:
            return None

        event = events[0]
        pubkey = event.get("pubkey")
        if not isinstance(pubkey, str) or pubkey.strip().lower() != author_hint:
            return None

        if self._event_has_tracked_tag(event, tag_norm):
            return event
        return None

    async def _get_today_note_ids(self, settings) -> tuple[bool, list[str]]:
        """Return (success, note_ids) for today's #CyberHerd notes."""
        if not self.app:
            return False, []

        try:
            from .headbutt import EnhancedHeadbuttService

            helper = EnhancedHeadbuttService(
                db=crud,
                messaging_module=self.messaging,
                app=self.app,
                user_id=self.user_id,
            )
            notes = await helper._get_today_cyberherd_notes()
            if not isinstance(notes, list):
                return True, []
            # Normalize to lowercase 64-hex strings
            cleaned = []
            for nid in notes:
                if isinstance(nid, str):
                    cleaned.append(nid.strip().lower())
            return True, cleaned
        except Exception as exc:
            logger.debug(
                "Zap monitor: failed to fetch today's note list for user %s: %s",
                self.user_id,
                exc,
            )
            return False, []

    @staticmethod
    def _event_has_tracked_tag(event: dict, tracked_tags: list[str]) -> bool:
        """Return True if *event* carries any of *tracked_tags*."""
        tags = event.get("tags") or []
        for tag in tags:
            if not (isinstance(tag, list) and len(tag) > 1):
                continue
            if tag[0] != "t":
                continue
            try:
                value = str(tag[1]).lstrip("#").lower()
            except Exception:
                continue
            if value in tracked_tags:
                return True

        # Fallback to scanning content for hashtags
        try:
            content = event.get("content", "") or ""
        except Exception:
            content = ""
        if not content:
            return False

        try:
            hashtags = {match.lower() for match in re.findall(r"#([\w\-]+)", content, flags=re.UNICODE)}
        except Exception:
            hashtags = set()

        return any(tag in hashtags for tag in tracked_tags)
    
    async def _opportunistic_registry_update(self, note_id: str, max_attempts: int = 7, delay: float = 2.0):
        """Best-effort registry update when subscriptions become ready.
        
        This is called after successful headbutt processing when the note wasn't
        initially in tracked_event_ids (likely because subscriptions weren't ready yet).
        We periodically check if subscriptions are ready and mark the note when possible.
        
        Args:
            note_id: Note ID to mark in registry
            max_attempts: Maximum retry attempts (default: 7)
            delay: Initial delay between retries in seconds (default: 2.0)
        """
        for attempt in range(1, max_attempts + 1):
            try:
                await asyncio.sleep(delay)
                
                # Check if subscriptions are ready now
                status = get_subscription_status(self.app)
                if bool(status.get("connected")):
                    # Subscriptions ready - verify note is in tracked_event_ids
                    settings = await crud.get_settings(self.user_id)
                    if settings:
                        tracked_notes = getattr(settings, 'tracked_event_ids', []) or []
                        if note_id in tracked_notes:
                            logger.debug(
                                f"✅ Registry update: note {note_id[:8]}... now in tracked_event_ids "
                                f"(attempt {attempt}/{max_attempts})"
                            )
                            return  # Success
                        else:
                            logger.debug(
                                f"ℹ️  Registry update: note {note_id[:8]}... still not in tracked list "
                                f"(attempt {attempt}/{max_attempts})"
                            )
                else:
                    logger.debug(
                        f"⏳ Registry update: subscriptions still not ready "
                        f"(attempt {attempt}/{max_attempts})"
                    )
                
                # Increase delay slightly for next attempt (gentle backoff)
                delay = min(delay * 1.2, 5.0)
                
            except Exception as e:
                logger.debug(f"Error in opportunistic registry update (attempt {attempt}): {e}")
        
        # Max attempts reached - log as debug (not a failure, just informational)
        logger.debug(
            f"ℹ️  Opportunistic registry update completed after {max_attempts} attempts "
            f"for note {note_id[:8]}... (note: zap was already processed successfully)"
        )
    
    async def payment_listener(self):
        """Listen for invoice payments and detect LNURLp zaps.
        
        This is a background task that should be started when monitoring begins.
        It registers with the LNbits invoice listener system (once) and processes payments.
        """
        global _register_invoice_listener, _monitoring_available
        try:
            if _register_invoice_listener is None:
                from lnbits.tasks import register_invoice_listener as _imported  # type: ignore

                _register_invoice_listener = _imported  # type: ignore
                _monitoring_available = True
        except Exception as e:  # pragma: no cover - defensive import guard
            logger.error(
                f"Cyberherd payment listener unavailable for user {self.user_id}: {e}"
            )
            self.last_error = str(e)
            _monitoring_available = False
            return

        if _register_invoice_listener is None:
            logger.error(
                f"Cyberherd payment listener could not be registered for user {self.user_id} (missing handler)"
            )
            self.last_error = "invoice_listener_unavailable"
            _monitoring_available = False
            return

        try:
            # Register listener only once per monitor instance (idempotent)
            if self._invoice_queue is None:
                self._invoice_queue = asyncio.Queue()
                _register_invoice_listener(  # type: ignore[misc]
                    self._invoice_queue, f"cyberherd_zap_monitor_{self.user_id}"
                )
                logger.info(f"✅ Payment listener registered for user {self.user_id}")
            else:
                logger.debug(
                    f"Payment listener already registered for user {self.user_id}, reusing queue"
                )

            while self._running:
                try:
                    # Wait for payment with timeout to allow checking _running flag
                    payment = await asyncio.wait_for(
                        self._invoice_queue.get(), timeout=1.0
                    )
                    # Process payment for zap detection (if applicable)
                    await self._process_payment_for_zap(payment)
                    
                    # Check if automatic splits should be triggered for ANY payment
                    # to the herd wallet (not just zaps)
                    await self._check_and_trigger_splits(payment)
                except asyncio.TimeoutError:
                    # Normal timeout, check if still running
                    continue
                except Exception as e:
                    logger.error(f"Error in payment listener for user {self.user_id}: {e}")
                    await asyncio.sleep(1)  # Brief pause before retrying

            logger.info(f"Payment listener stopped for user {self.user_id}")

        except Exception as e:
            logger.error(f"Fatal error in payment listener for user {self.user_id}: {e}")
            self.last_error = str(e)
    
    async def update_tracked_notes(self, note_ids: list[str], author_pubkey: str | None = None,
                                   enable_zaps: bool = True, enable_reposts: bool = False, 
                                   enable_reactions: bool = False):
        """Update the list of tracked notes and event type preferences for Nostr monitoring.
        
        This method is called when:
        - Tracked notes change in settings
        - User enables/disables event types
        - Settings are updated via API
        
        Note: Actual Nostr subscriptions are managed by subscriptions.py via event-driven
        refresh (_refresh_event). This method exists for API compatibility but the real
        subscription updates happen automatically when settings change.
        
        Args:
            note_ids: List of Nostr note IDs (event IDs) to track
            author_pubkey: Optional author pubkey for filtering
            enable_zaps: Subscribe to zap receipts (default: True)
            enable_reposts: Subscribe to reposts (default: False)
            enable_reactions: Subscribe to reactions (default: False)
        
        Returns:
            bool: True if update was acknowledged, False otherwise
        """
        # The NostrEventMonitor class was never implemented - monitoring is handled
        # by subscriptions.py which updates automatically via _refresh_event.
        # This method is kept for API compatibility.
        
        logger.info(
            f"📝 Tracked notes update requested for user {self.user_id}: "
            f"{len(note_ids)} notes, zaps={enable_zaps}, reposts={enable_reposts}, reactions={enable_reactions}"
        )
        logger.debug(
            "Nostr subscriptions will be updated automatically by subscriptions.py "
            "when settings change triggers _refresh_event"
        )
        return True
    
    async def _start_nostr_monitoring(self, settings, note_timestamps: dict[str, int] | None = None):
        """Start Nostr event monitoring for tracked notes.
        
        Note: NostrEventMonitor class was planned but not implemented.
        Event monitoring is handled by the subscriptions system (subscriptions.py).
        This method is kept for compatibility but does nothing.
        
        Args:
            settings: User settings with tracked_event_ids and event type preferences
            note_timestamps: Optional dict mapping note_id -> created_at timestamp (for event recovery)
        """
        # NostrEventMonitor not implemented - monitoring handled by subscriptions.py
        logger.debug(f"Event monitoring for user {self.user_id} is handled by subscriptions system")
        return
    
    def _mark_event_processed(self, event_type: str, event_id: str, note_id: str, pubkey: str) -> bool:
        """Mark an event as processed and check if it was already processed.
        
        Returns:
            True if event was already processed (should skip), False if new (should process)
        """
        from datetime import datetime, timezone
        
        # Create cache key
        cache_key = (event_type, event_id, note_id, pubkey)
        
        # Check if already processed
        if cache_key in self._processed_events:
            logger.debug(
                f"🔄 Duplicate {event_type} event detected: {event_id[:16]}... "
                f"(note: {note_id[:8]}..., pubkey: {pubkey[:8]}...). Skipping."
            )
            return True
        
        # Mark as processed
        self._processed_events[cache_key] = utc_now_timestamp()
        
        # Prevent unbounded growth - remove oldest entries if too large
        if len(self._processed_events) > self._processed_events_max_size:
            # Remove oldest 20% of entries
            sorted_entries = sorted(self._processed_events.items(), key=lambda x: x[1])
            num_to_remove = len(sorted_entries) // 5
            for key, _ in sorted_entries[:num_to_remove]:
                del self._processed_events[key]
            logger.debug(f"Pruned {num_to_remove} old entries from processed events cache")
        
        return False
    
    async def _handle_nostr_repost(self, reposter_pubkey: str, reposted_note_id: str,
                                    event_id: str, event: dict):
        """Handle repost detected from Nostr.
        
        Args:
            reposter_pubkey: Pubkey of the reposter
            reposted_note_id: Note ID that was reposted
            event_id: Repost event ID
            event: Full event dict
        """
        try:
            # Persistent dedupe: skip if persisted as processed (prevents cross-restart reprocessing)
            try:
                if event_id and await crud.is_event_processed(cast(str, self.user_id), event_id):
                    logger.debug(f"Persisted repost event {event_id[:16]}... already processed. Skipping.")
                    return
            except Exception:
                # If persistence check fails, fall back to in-memory dedupe
                pass

            # In-memory duplicate check
            if self._mark_event_processed('repost', event_id, reposted_note_id, reposter_pubkey):
                return  # Already processed
            
            logger.info(
                f"🔄 Processing Nostr repost from {reposter_pubkey[:8]}... "
                f"of note {reposted_note_id[:8]}... (event: {event_id[:16]}...)"
            )
            
            # Get settings
            settings = await crud.get_settings(self.user_id)
            if not settings:
                logger.warning(f"No settings found for user {self.user_id}, cannot process Nostr repost")
                return
            
            # Verify the reposted note is one we're tracking
            tracked_notes = getattr(settings, 'tracked_event_ids', []) or []
            if reposted_note_id not in tracked_notes:
                logger.debug(f"Reposted note {reposted_note_id} not in tracked notes, ignoring")
                return
            
            # Check if reposts are enabled
            if not getattr(settings, 'repost_tracking_enabled', False):  # Note: singular "repost"
                logger.debug(f"Repost tracking not enabled for user {self.user_id}")
                return
            
            # Trigger headbutt processing for repost
            from .headbutt import trigger_headbutt_from_repost
            
            result = await trigger_headbutt_from_repost(
                user_id=cast(str, self.user_id),
                pubkey=reposter_pubkey,
                note_id=reposted_note_id,
                event_id=event_id,
                app=self.app
            )
            
            if result:
                self.total_reposts_processed += 1
                self.last_message_at = utc_now_timestamp()
                logger.info(f"Successfully processed Nostr repost from {reposter_pubkey[:8]}...")
                # Persist processed status so reposts are never reprocessed across restarts
                try:
                    if event_id and isinstance(event_id, str):
                            res = await crud.register_processed_event(
                                cast(str, self.user_id),
                                event_id,
                                reposted_note_id,
                                reposter_pubkey,
                                event_type="repost",
                            )
                            # Metrics: increment counters on success/failure when app available
                            try:
                                st = getattr(self.app, 'state', self.app)
                                metrics = getattr(st, 'cyberherd_metrics', None)
                                if metrics is None:
                                    metrics = {}
                                    try:
                                        setattr(st, 'cyberherd_metrics', metrics)
                                    except Exception:
                                        pass
                                # success if register_processed_event returned truthy
                                if res:
                                    metrics['repost_persist_success'] = metrics.get('repost_persist_success', 0) + 1
                                else:
                                    metrics['repost_persist_failure'] = metrics.get('repost_persist_failure', 0) + 1
                            except Exception:
                                pass
                            if res:
                                logger.debug("Persisted repost event %s for user %s", event_id, self.user_id)
                            else:
                                logger.warning(f"Failed to persist repost event {event_id} for user {self.user_id}")
                except Exception:
                    # Non-fatal: persistence failure should not break processing
                        try:
                            st = getattr(self.app, 'state', self.app)
                            metrics = getattr(st, 'cyberherd_metrics', None)
                            if metrics is None:
                                metrics = {}
                                try:
                                    setattr(st, 'cyberherd_metrics', metrics)
                                except Exception:
                                    pass
                            metrics['repost_persist_failure'] = metrics.get('repost_persist_failure', 0) + 1
                        except Exception:
                            pass
            else:
                logger.warning(f"Failed to process Nostr repost from {reposter_pubkey[:8]}...")
            
        except Exception as e:
            logger.error(f"Error handling Nostr repost: {e}")
            self.last_error = str(e)
    
    async def _handle_nostr_reaction(self, reactor_pubkey: str, reacted_note_id: str,
                                      reaction_content: str, event_id: str, event: dict):
        """Handle reaction detected from Nostr.
        
        Args:
            reactor_pubkey: Pubkey of the reactor
            reacted_note_id: Note ID that was reacted to
            reaction_content: Reaction content ("+", "❤️", etc.)
            event_id: Reaction event ID
            event: Full event dict
        """
        try:
            # Persistent dedupe first: prevents cross-restart reprocessing of reactions
            try:
                if event_id and await crud.is_event_processed(cast(str, self.user_id), event_id):
                    logger.debug(f"Persisted reaction event {event_id[:16]}... already processed. Skipping.")
                    return
            except Exception:
                # Fall back to in-memory dedupe if persistence unavailable
                pass

            # In-memory duplicate check
            if self._mark_event_processed('reaction', event_id, reacted_note_id, reactor_pubkey):
                return  # Already processed
            
            logger.info(
                f"🔄 Processing Nostr reaction '{reaction_content}' from {reactor_pubkey[:8]}... "
                f"to note {reacted_note_id[:8]}... (event: {event_id[:16]}...)"
            )
            
            # Get settings
            settings = await crud.get_settings(self.user_id)
            if not settings:
                logger.warning(f"No settings found for user {self.user_id}, cannot process Nostr reaction")
                return
            
            # Verify the reacted note is one we're tracking
            tracked_notes = getattr(settings, 'tracked_event_ids', []) or []
            if reacted_note_id not in tracked_notes:
                logger.debug(f"Reacted note {reacted_note_id} not in tracked notes, ignoring")
                return
            
            # Check if reactions are enabled
            if not getattr(settings, 'likes_tracking_enabled', False):
                logger.debug(f"Reaction tracking not enabled for user {self.user_id}")
                return
            
            # Trigger headbutt processing for reaction
            from .headbutt import trigger_headbutt_from_reaction
            
            result = await trigger_headbutt_from_reaction(
                user_id=cast(str, self.user_id),
                pubkey=reactor_pubkey,
                note_id=reacted_note_id,
                event_id=event_id,
                app=self.app
            )
            
            if result:
                self.total_reactions_processed += 1
                self.last_message_at = utc_now_timestamp()
                logger.info(f"Successfully processed Nostr reaction from {reactor_pubkey[:8]}...")
                # Persist processed status to avoid reprocessing after restarts
                try:
                    if event_id and isinstance(event_id, str):
                            res = await crud.register_processed_event(
                                cast(str, self.user_id),
                                event_id,
                                reacted_note_id,
                                reactor_pubkey,
                                event_type="reaction",
                            )
                            try:
                                st = getattr(self.app, 'state', self.app)
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
                                logger.debug("Persisted reaction event %s for user %s", event_id, self.user_id)
                            else:
                                logger.warning(f"Failed to persist reaction event {event_id} for user {self.user_id}")
                except Exception:
                        try:
                            st = getattr(self.app, 'state', self.app)
                            metrics = getattr(st, 'cyberherd_metrics', None)
                            if metrics is None:
                                metrics = {}
                                try:
                                    setattr(st, 'cyberherd_metrics', metrics)
                                except Exception:
                                    pass
                            metrics['reaction_persist_failure'] = metrics.get('reaction_persist_failure', 0) + 1
                        except Exception:
                            pass
            else:
                logger.warning(f"Failed to process Nostr reaction from {reactor_pubkey[:8]}...")
            
        except Exception as e:
            logger.error(f"Error handling Nostr reaction: {e}")
            self.last_error = str(e)

    async def status(self) -> dict[str, Any]:
        """Get current monitoring status and statistics."""
        persisted_totals: dict[str, int] = {}
        user_id = cast(Optional[str], self.user_id)
        if user_id:
            try:
                persisted_totals = await crud.get_monitoring_totals(user_id)
            except Exception as exc:
                logger.debug(
                    f"Zap monitor: failed to fetch persisted totals for user {user_id}: {exc}"
                )
                persisted_totals = {}
        else:
            persisted_totals = {}

        total_zaps = max(self.total_zaps_processed, persisted_totals.get("zap", 0))
        total_reposts = max(
            self.total_reposts_processed, persisted_totals.get("repost", 0)
        )
        total_reactions = max(
            self.total_reactions_processed, persisted_totals.get("reaction", 0)
        )

        status = {
            'running': bool(self._running),
            'last_message_at': self.last_message_at,
            'last_zap_at': self.last_zap_at,
            'last_error': self.last_error,
            'mode': 'nostr',  # Nostr-only implementation
            'monitoring_available': bool(
                _monitoring_available or self._invoice_queue is not None
            ),
            'total_zaps_processed': total_zaps,
            'total_reposts_processed': total_reposts,
            'total_reactions_processed': total_reactions,
            'total_recovery_scans': getattr(self, 'total_recovery_scans', 0),
            'nostr_monitoring_enabled': self.nostr_monitor is not None,
        }
        
        # Add Nostr monitor detailed status if available
        if self.nostr_monitor:
            status['nostr_monitor'] = self.nostr_monitor.status()
        
        return status

    async def _recover_missed_payment_zaps(self, settings) -> dict:
        """Recover missed LNURLp zaps by scanning recent payments to the herd wallet.

        This method scans payments sent to the configured herd wallet since the
        start of the current day (UTC) and invokes the standard payment handler
        for any candidate payments that include a zap request.
        
        IMPORTANT: Only processes zaps for TODAY's tracked notes. This ensures
        consistency with the rule that zaps are only valid for the current day's
        CyberHerd notes.
        
        Returns a diagnostic dict with counts and any errors encountered.
        """
        from ..crud import get_settings as _get_settings  # noqa: F401
        try:
            # Resolve herd wallet from provided settings object
            herd_wallet = getattr(settings, 'herd_wallet', None)
            if not herd_wallet:
                msg = "No herd_wallet configured in settings; cannot recover payments"
                logger.warning(msg)
                return {"scanned": 0, "processed": 0, "error": msg}

            # CRITICAL: Recovery should only run after tracked_event_ids are populated
            # This prevents scanning payments before subscriptions have detected today's notes
            tracked_notes = getattr(settings, 'tracked_event_ids', []) or []
            if not tracked_notes:
                msg = "No tracked_event_ids found; recovery deferred until notes are detected"
                logger.info(msg)
                return {"scanned": 0, "processed": 0, "error": msg}

            # Use LOCAL midnight to match the boundary used for validating tracked_event_ids
            # This prevents timezone mismatches that could cause duplicate processing
            try:
                from .time_utils import get_day_boundaries_utc
                boundaries = get_day_boundaries_utc(days_ago=0)
                since_ts = int(boundaries.local_since_ts)  # Use local day boundary for consistency
            except Exception:
                # Fallback to 24 hours ago if boundaries unavailable
                from ..utils.common import utc_now_timestamp
                since_ts = utc_now_timestamp() - 86400

            # Lazy import of payments helper to avoid circular imports at module load
            try:
                from lnbits.core.services.payments import get_payments
            except Exception:
                logger.error("Failed to import payments.get_payments for recovery")
                return {"scanned": 0, "processed": 0, "error": "payments API unavailable"}

            logger.info(f"Starting payment-based zap recovery for wallet {herd_wallet} since {since_ts}")

            # Fetch recent incoming payments to the herd wallet
            payments = await get_payments(wallet_id=herd_wallet, incoming=True, since=since_ts, limit=1000)

            scanned = 0
            processed = 0
            successful = 0
            self.total_recovery_scans += 1

            for payment in payments or []:
                scanned += 1
                try:
                    # Compute canonical event ids used by persistence checks
                    raw_primary = getattr(payment, 'payment_hash', None)
                    raw_alt = getattr(payment, 'checking_id', None)

                    p_hash_norm = self._normalize_payment_hash(raw_primary)
                    alt_hash_norm = self._normalize_payment_hash(raw_alt)

                    # Fast persisted-processed check: if either payment identifier
                    # was already recorded, skip this payment to avoid reprocessing
                    already = False
                    try:
                        if p_hash_norm and await crud.is_payment_processed(cast(str, self.user_id), p_hash_norm):
                            already = True
                        elif alt_hash_norm and await crud.is_payment_processed(cast(str, self.user_id), alt_hash_norm):
                            already = True
                    except Exception:
                        # On persistence errors assume not processed so recovery can try
                        already = False

                    if already:
                        logger.debug(
                            f"Recovery: skipping already-processed payment {p_hash_norm[:16] if p_hash_norm else alt_hash_norm[:16] if alt_hash_norm else 'unknown'}"
                        )
                        continue

                    # Process payment for zap (enforces today's note check)
                    # Note: Recovery now enforces current day validation - zaps are only
                    # processed for today's tracked notes
                    processed += 1
                    res = await self._process_payment_for_zap(payment)
                    # Treat truthy result as success (some code paths may return True)
                    if res:
                        successful += 1
                except Exception as e:
                    logger.warning(f"Recovery: failed processing payment {getattr(payment, 'payment_hash', None)}: {e}")

            logger.info(f"Payment recovery complete: scanned={scanned} processed_attempts={processed} successful={successful}")
            return {"scanned": scanned, "processed": processed, "successful": successful}

        except Exception as e:
            logger.error(f"Error in _recover_missed_payment_zaps: {e}")
            return {"scanned": 0, "processed": 0, "error": str(e)}


def get_zap_monitor(**kwargs):
    """Get or create a ZapMonitorService instance for a user.
    
    This is a singleton factory that maintains one monitor instance per user_id.
    """
    uid = kwargs.get('user_id')
    if not uid:
        raise ValueError('user_id required')
    
    inst = _instances.get(uid)
    if inst is None:
        inst = ZapMonitorService(
            app=kwargs.get('app'),
            db=kwargs.get('db'),
            messaging=kwargs.get('messaging'),
            user_id=uid
        )
        _instances[uid] = inst
    else:
        # Update references if provided
        for k in ('app', 'db', 'messaging'):
            v = kwargs.get(k)
            if v is not None:
                setattr(inst, k, v)
    
    try:  # pragma: no cover
        _last_access[uid] = time.time()
    except Exception:
        pass
    
    return inst


# Alias for backward compatibility
ZapMonitor = ZapMonitorService
