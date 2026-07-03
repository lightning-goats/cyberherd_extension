"""Payment coordinator: split triggers + payment recovery helpers.

This module coordinates everything the invoice listener needs:
- Triggers splits when payments reach the herd wallet (see _check_and_trigger_splits).
- Provides diagnostics/testing helpers such as _process_payment_for_zap and
  _recover_missed_payment_zaps for admin APIs.

Zap detection and engagement processing now live in the WebSocket monitor +
subscriptions pipeline; this service only runs payment-related flows. The
legacy ZapMonitorService name remains available via alias for compatibility.
"""

from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from typing import Any, Optional, cast
import re

from loguru import logger

from .. import crud
from ..utils.common import utc_now_timestamp  # Centralized UTC timestamp function
from .pubkey import resolve_effective_pubkey
from .note_metadata import apply_event_address
from .headbutt import trigger_headbutt_from_zap

# Simplified payment monitoring: this service no longer registers its own
# invoice listener. A single central listener is registered in
# lnbits/extensions/cyberherd/tasks.py using the standard LNbits pattern
# (wait_for_paid_invoices + create_permanent_unique_task), similar to the
# lightning_goats extension. Split triggering is retained via
# PaymentCoordinatorService._check_and_trigger_splits, which is invoked by
# tasks.process_incoming_payment.

# Nostr helpers not used here anymore (zap detection handled elsewhere)

def payment_listener_required(settings) -> bool:
    """Return True when settings require the LNbits payment listener.

    The listener is necessary when we have a herd wallet configured because
    automatic split triggers and zap tracking depend on that wallet.
    """
    if not settings:
        return False

    herd_wallet = getattr(settings, "herd_wallet", None)
    has_herd_wallet = isinstance(herd_wallet, str) and herd_wallet.strip() != ""

    if not has_herd_wallet:
        return False

    zap_tracking = bool(getattr(settings, "zap_tracking_enabled", False))
    auto_splits = bool(getattr(settings, "send_splits_enabled", False))
    return zap_tracking or auto_splits

_instances: dict[str, 'PaymentCoordinatorService'] = {}
_last_access: dict[str, float] = {}
_TTL = 1800

# Support legacy registry imports
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


class PaymentCoordinatorService:
    """Payment coordinator and diagnostics shim.

    Responsibilities kept here during migration:
    - Start/stop lifecycle no-ops (invoice listener is centralized in tasks.py)
    - Trigger automatic splits on payment events (_check_and_trigger_splits)
    - Legacy/admin payment recovery helpers (disabled in live flows)
    - Status reporting for diagnostics endpoints

    Realtime zap detection and engagement handling are not in this class.
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
        self.mode = 'payment'

        # Recovery scans performed (payment-based recovery routine)
        self.total_recovery_scans = 0
        
        # Payment listener is centrally managed by cyberherd.tasks; no per-user
        # listener, task or queue is created here.
        
        # No internal Nostr monitoring; handled by websocket monitor/subscriptions

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

    @staticmethod
    def _coerce_timestamp(value: Any) -> int | None:
        """Best-effort conversion of payment timestamp fields to epoch seconds."""
        if value is None:
            return None
        if isinstance(value, (int, float)):
            try:
                return int(value)
            except Exception:
                return None
        if isinstance(value, str):
            candidate = value.strip()
            if not candidate:
                return None
            try:
                return int(float(candidate))
            except Exception:
                try:
                    iso = candidate
                    if iso.endswith("Z"):
                        iso = iso[:-1] + "+00:00"
                    dt = datetime.fromisoformat(iso)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    return int(dt.timestamp())
                except Exception:
                    return None
        return None

    @classmethod
    def _extract_payment_timestamp(cls, payment: Any) -> int | None:
        """Extract an approximate timestamp from LN payment records."""
        for attr in ("time", "created_at", "updated_at"):
            try:
                value = getattr(payment, attr, None)
            except Exception:
                value = None
            ts = cls._coerce_timestamp(value)
            if ts is not None:
                return ts
        return None

    async def _resolve_note_from_addresses(self, *args, **kwargs) -> tuple[str | None, str | None]:
        """Deprecated: address-based note resolution no longer performed here."""
        return None, None

    async def start_monitoring(self):
        """Mark monitor as running; payments are handled centrally.

        Payment listening is performed by the central listener registered in
        cyberherd.tasks.start_invoice_listener. This retains a public lifecycle
        API but does not start a per-user listener.
        """
        settings = await crud.get_settings(self.user_id)
        herd_wallet = getattr(settings, 'herd_wallet', None) if settings else None
        zap_tracking = bool(getattr(settings, 'zap_tracking_enabled', False)) if settings else False
        auto_splits = bool(getattr(settings, 'send_splits_enabled', False)) if settings else False

        if not payment_listener_required(settings):
            logger.info(
                f"Payment listener not required for user {self.user_id}: "
                f"herd_wallet={'set' if herd_wallet else 'missing'}, "
                f"zap_tracking_enabled={zap_tracking}, send_splits_enabled={auto_splits}"
            )
            return

        if self._running:
            return

        self._running = True
        logger.info(
            f"Zap monitor active for user {self.user_id} "
            f"(payments handled by central listener; zap_tracking={zap_tracking}, auto_splits={auto_splits})"
        )

    async def stop_monitoring(self):
        """Stop monitoring (no-op for payments)."""
        if not self._running:
            return
            
        self._running = False
    
    async def _process_payment_for_zap(self, payment, settings_override=None):
        """Process a payment notification to detect LNURLp zaps.
        
        This method parses payment.extra["nostr"] for NIP-57 zap requests.
        Payment records are the primary zap detection path; kind 9735 Nostr
        receipt subscriptions are intentionally disabled to avoid races.
        
        IMPORTANT: Only processes zaps for TODAY's tracked notes. This enforces
        the rule that zaps are only valid for the current day's CyberHerd notes.
        
        Args:
            payment: Payment object with extra data containing zap request
        """
        try:
            # Disabled in live flow; keep as no-op fast exit
            settings = settings_override or await crud.get_settings(self.user_id)
            if not settings:
                return False
            herd_wallet_id = getattr(settings, 'herd_wallet', None)
            if herd_wallet_id and payment.wallet_id != herd_wallet_id:
                return False
            
            # Extract zap request from payment.extra["nostr"] (preferred) or comment (fallback)
            zap_request = None
            zap_request_source = None

            # Support payment.extra being either a JSON string or a dict
            extra_obj = None
            if payment.extra:
                if isinstance(payment.extra, str):
                    try:
                        extra_obj = json.loads(payment.extra)
                    except Exception:
                        extra_obj = None
                elif isinstance(payment.extra, dict):
                    extra_obj = payment.extra
                else:
                    # Try best-effort attribute access
                    try:
                        extra_obj = dict(payment.extra)
                    except Exception:
                        extra_obj = None

            if extra_obj:
                # Prefer nostr field (LNURLp standard)
                nostr_json = extra_obj.get("nostr")
                if nostr_json:
                    # zap_request may already be a dict or a JSON string
                    if isinstance(nostr_json, dict):
                        zap_request = nostr_json
                        zap_request_source = "nostr"
                        logger.debug("Parsed zap request from payment.extra['nostr'] (dict)")
                    else:
                        try:
                            zap_request = json.loads(nostr_json)
                            zap_request_source = "nostr"
                            logger.debug("Parsed zap request from payment.extra['nostr']")
                        except Exception as e:
                            logger.warning(f"Failed to parse zap request from nostr field: {e}")

                # Check for new-style fields used by process_incoming_payment fallback
                if not zap_request:
                    for key in ("zap_request", "zapRequest"):
                        candidate = extra_obj.get(key)
                        if not candidate:
                            continue
                        if isinstance(candidate, dict):
                            zap_request = candidate
                        else:
                            try:
                                zap_request = json.loads(candidate)
                            except Exception:
                                zap_request = None
                        if zap_request:
                            zap_request_source = key
                            logger.debug(f"Parsed zap request from payment.extra['{key}']")
                            break

                # Fallback to comment field (legacy)
                if not zap_request:
                    comment = extra_obj.get("comment")
                    if comment and isinstance(comment, str) and comment.strip().startswith("{"):
                        try:
                            zap_request = json.loads(comment)
                            zap_request_source = "comment"
                            logger.debug("Parsed zap request from payment.extra['comment'] (legacy)")
                        except json.JSONDecodeError:
                            pass  # Not a JSON comment, ignore

            # Fallback: allow memo to carry zap request JSON (legacy clients)
            if not zap_request:
                memo_field = getattr(payment, "memo", None)
                if isinstance(memo_field, str) and memo_field.strip().startswith("{"):
                    try:
                        zap_request = json.loads(memo_field)
                        zap_request_source = "memo"
                        logger.debug("Parsed zap request from payment.memo field")
                        extra_obj = extra_obj or {}
                    except json.JSONDecodeError:
                        pass

            # Ensure normalized storage back into payment.extra for future calls
            # IMPORTANT: Do not store a dict under the "nostr" key because other
            # parts of the system (e.g. lnbits lnurlp.send_zap) expect a JSON
            # string and will call json.loads() on it. Store a JSON string to
            # remain compatible and avoid TypeError: json.loads(dict).
            if zap_request:
                try:
                    nostr_str = json.dumps(zap_request)
                    if isinstance(extra_obj, dict):
                        extra_obj["nostr"] = nostr_str
                        payment.extra = extra_obj
                    else:
                        payment.extra = {"nostr": nostr_str}
                except Exception:
                    # Best-effort; do not fail if assignment fails
                    pass
            
            if not zap_request:
                # No zap request found in payment data
                return False
            
            # Extract zapper pubkey
            zapper_pubkey = zap_request.get("pubkey")
            if not zapper_pubkey:
                logger.warning(f"Zap request missing pubkey field (source: {zap_request_source})")
                self.last_error = "missing_zapper_pubkey"
                return False

            # The paid amount is authoritative (from the settled invoice), but the
            # zapper *identity* comes from the caller-supplied zap request. Verify
            # its schnorr signature so a crafted 9734 cannot enroll/credit an
            # arbitrary pubkey (e.g. to headbutt a rival with someone else's name).
            try:
                from .nostr_helpers import verify_event_signature

                if not verify_event_signature(zap_request):
                    logger.warning(
                        f"Zap request signature invalid; refusing to attribute zap to "
                        f"{str(zapper_pubkey)[:16]}... (source: {zap_request_source})"
                    )
                    self.last_error = "invalid_zap_request_signature"
                    return False
            except Exception as e:
                logger.debug(f"Zap request signature check error: {e}")
                self.last_error = "zap_request_signature_error"
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
            relay_hints: list[str] = []
            seen_relay_hints: set[str] = set()

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
                elif marker == "relays":
                    for relay in tag[1:]:
                        if not isinstance(relay, str):
                            continue
                        relay = relay.strip()
                        if relay and relay not in seen_relay_hints:
                            seen_relay_hints.add(relay)
                            relay_hints.append(relay)

            # Fallbacks for non-standard fields
            if not target_note_id_raw:
                target_note_id_raw = zap_request.get("e")
            if not target_author_raw:
                target_author_raw = zap_request.get("p")

            extra_address = zap_request.get("a")
            if isinstance(extra_address, str) and extra_address:
                address_candidates.append(extra_address)

            extra_relays = zap_request.get("relays")
            if isinstance(extra_relays, list):
                for relay in extra_relays:
                    if not isinstance(relay, str):
                        continue
                    relay = relay.strip()
                    if relay and relay not in seen_relay_hints:
                        seen_relay_hints.add(relay)
                        relay_hints.append(relay)

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
            if amount_sats <= 0:
                logger.info(
                    f"Ignoring payment-based zap with non-positive amount: {amount_msats} msat"
                )
                self.last_error = "invalid_amount"
                return False
            
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
                    logger.info(
                        f"Zap target {target_note_id[:8]}... has no author metadata; "
                        f"cannot verify Lightning Goats ownership for existing member "
                        f"{zapper_pubkey[:8]}... - rejecting"
                    )
                    self.last_error = "missing_note_author"
                    return False
            else:
                # New member: enforce strict tracking requirements
                # Simple rule: If the zapped note is in tracked_event_ids, it's valid
                # tracked_event_ids only contains notes from TODAY with the required tags
                tracked_notes = {
                    note_id.strip().lower()
                    for note_id in (getattr(settings, 'tracked_event_ids', []) or [])
                    if isinstance(note_id, str) and note_id.strip()
                }
                
                # Check if target note is in tracked events. A stale note ID
                # left in settings is not enough for new-member admission; it
                # must still be valid for the current local day.
                is_tracked = target_note_id in tracked_notes
                if is_tracked:
                    is_current = await self._is_current_day_tracked_note(
                        settings=settings,
                        note_id=target_note_id,
                        author_hint=target_author,
                        relay_hints=relay_hints,
                    )
                    if not is_current:
                        logger.info(
                            f"❌ Zap target {target_note_id[:8]}... is present in "
                            "tracked_event_ids but is not valid for today's local window. "
                            "Ignoring zap."
                        )
                        self.last_error = "note_not_current"
                        return False
                
                if not is_tracked:
                    author_hint_for_tracking = target_author
                    if not author_hint_for_tracking:
                        try:
                            eff_hint = resolve_effective_pubkey(settings)
                            if isinstance(eff_hint, str):
                                author_hint_for_tracking = eff_hint.strip().lower()
                        except Exception:
                            author_hint_for_tracking = None

                    if author_hint_for_tracking:
                        logger.debug(
                            f"Zap target {target_note_id[:8]}... missing from tracked list; "
                            "attempting opportunistic tracking."
                        )
                        try:
                            ensured = await self._ensure_note_tracked(
                                settings=settings,
                                note_id=target_note_id,
                                author_hint=author_hint_for_tracking,
                                relay_hints=relay_hints,
                            )
                        except Exception as exc:
                            ensured = False
                            logger.debug(
                                "Opportunistic tracking attempt failed for note %s: %s",
                                target_note_id[:8],
                                exc,
                            )

                        if ensured:
                            tracked_notes = {
                                note_id.strip().lower()
                                for note_id in (getattr(settings, 'tracked_event_ids', []) or [])
                                if isinstance(note_id, str) and note_id.strip()
                            }
                            is_tracked = target_note_id in tracked_notes
                            if is_tracked:
                                logger.info(
                                    f"Auto-tracked note {target_note_id[:8]}... "
                                    f"after zap from {zapper_pubkey[:8]}..."
                                )
                    else:
                        logger.debug(
                            "Skipping opportunistic tracking for note %s: no author hint available",
                            target_note_id[:8],
                        )

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
            
            # Check/claim duplicate processing using payment hash as event ID
            event_id_raw = getattr(payment, "payment_hash", None) or getattr(payment, "checking_id", None)
            event_id = self._normalize_payment_hash(event_id_raw)

            # Atomically claim the payment hash BEFORE processing to avoid races
            # with other detection paths. register_processed_event performs a
            # serialized check+insert under a write lock and returns True iff
            # this process inserted a new row (i.e., claimed the hash).
            if event_id:
                try:
                    claimed = await crud.register_processed_event(
                        cast(str, self.user_id),
                        event_id,
                        note_id=target_note_id,
                        pubkey=zapper_pubkey,
                        event_type="payment_zap",
                    )
                except Exception:
                    claimed = False

                if not claimed:
                    pid_preview = event_id[:16] + "..."
                    nid_preview = target_note_id[:8] + "..." if isinstance(target_note_id, str) else "unknown"
                    ppk_preview = zapper_pubkey[:8] + "..." if isinstance(zapper_pubkey, str) else "unknown"
                    logger.debug(
                        f"🔄 Duplicate payment detected (persisted): payment_hash={pid_preview}, note={nid_preview}, pubkey={ppk_preview} Skipping."
                    )
                    self.last_error = None
                    return True

            # Trigger headbutt admission for zap (we have claimed the payment hash)
            try:
                result = await trigger_headbutt_from_zap(
                    user_id=cast(str, self.user_id),
                    pubkey=zapper_pubkey,
                    amount_sats=amount_sats,
                    note_id=target_note_id or "",
                    event_id=event_id or "",
                    app=self.app,
                )
            except Exception as e:
                logger.error(f"Error triggering headbutt from payment-based zap: {e}")
                result = None

            if result:
                # We already registered the payment hash as claimed above; update state
                self.last_zap_at = int(time.time())

                # The payment path is authoritative for zaps, but historically only
                # updated cyber_herd.amount. The leaderboard sorts by the zap_totals
                # table, so update it (and notify watchers) here too — otherwise the
                # leaderboard stays stale until a manual backfill.
                try:
                    target_pubkey = resolve_effective_pubkey(settings)
                except Exception:
                    target_pubkey = None
                if target_pubkey:
                    try:
                        try:
                            event_timestamp = int(zap_request.get("created_at") or 0) or int(time.time())
                        except Exception:
                            event_timestamp = int(time.time())
                        from .zap_totals import record_incremental_zap_total
                        from .leaderboard import broadcast_leaderboard_update

                        await record_incremental_zap_total(
                            user_id=cast(str, self.user_id),
                            zapper_pubkey=zapper_pubkey,
                            target_pubkey=target_pubkey,
                            amount_sats=amount_sats,
                            event_timestamp=event_timestamp,
                            event_id=event_id or "",
                        )
                        await broadcast_leaderboard_update(cast(str, self.user_id), settings=settings)
                    except Exception:
                        logger.debug("Failed to update zap totals/broadcast leaderboard", exc_info=True)

                logger.info(
                    f"✅ Payment-based zap processed: {amount_sats} sats from {zapper_pubkey[:8]}... "
                    f"to note {target_note_id[:8]}..."
                )
                return True
            else:
                logger.warning(
                    f"Headbutt processing returned no result for payment zap: "
                    f"{amount_sats} sats from {zapper_pubkey[:8]}..."
                )
                if event_id:
                    try:
                        await crud.delete_processed_event(
                            cast(str, self.user_id),
                            event_id,
                            note_id=target_note_id,
                        )
                    except Exception as exc:
                        logger.warning(
                            "Failed to release processed marker for failed zap %s: %s",
                            event_id[:16],
                            exc,
                        )
                return False
            
        except Exception as e:
            logger.error(f"Error processing payment for zap: {e}")
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
        relay_hints: list[str] | None = None,
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

            tagged_event = await self._fetch_tagged_note(
                settings,
                note_id,
                author_hint,
                relay_hints=relay_hints,
            )
            if not tagged_event:
                logger.info(
                    f"Zap monitor: refusing to auto-track note {note_id[:8]}... "
                    f"for user {self.user_id} because it was not found or did not "
                    f"match tracked tags (relay_hints={len(relay_hints or [])})"
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

    async def _is_current_day_tracked_note(
        self,
        settings,
        note_id: str,
        author_hint: str | None = None,
        relay_hints: list[str] | None = None,
    ) -> bool:
        """Return True only when a tracked note is valid for today's local day."""
        try:
            from .time_utils import get_day_boundaries_utc

            boundaries = get_day_boundaries_utc(days_ago=0)
            timestamps = dict(getattr(settings, "tracked_event_timestamps", {}) or {})
            raw_ts = timestamps.get(note_id)
            if raw_ts is None:
                raw_ts = timestamps.get(str(note_id).strip().lower())

            if raw_ts is not None:
                try:
                    note_ts = int(raw_ts)
                except Exception:
                    note_ts = 0
                if note_ts > 0:
                    if boundaries.is_timestamp_in_local_day(note_ts):
                        return True
                    logger.info(
                        f"Tracked note {note_id[:8]}... has timestamp {note_ts}, "
                        "outside today's local window; rejecting for new-member zap."
                    )
                    return False

            # Legacy/manual tracked IDs may not have timestamps. In that case,
            # require a relay fetch and reuse the canonical tag + author + local-day
            # validator before admitting a new member from this note.
            tagged_event = await self._fetch_tagged_note(
                settings,
                note_id,
                author_hint,
                relay_hints=relay_hints,
            )
            return bool(tagged_event)
        except Exception as exc:
            logger.debug(
                "Failed to validate tracked note %s against today's local window: %s",
                note_id[:8] if isinstance(note_id, str) else "unknown",
                exc,
            )
            return False

    async def _fetch_tagged_note(
        self,
        settings,
        note_id: str,
        author_hint: str | None = None,
        relay_hints: list[str] | None = None,
    ) -> dict | None:
        """Fetch a note by ID and verify it matches CyberHerd tracking rules."""
        if not note_id:
            return None

        try:
            from . import nostr_helpers
            from .subscriptions import _would_event_be_tracked

            events = await nostr_helpers.query_events(
                {"ids": [note_id]},
                limit=1,
                timeout=5.0,
                extra_relays=relay_hints or None,
            )
            if not events:
                logger.info(
                    f"Zap monitor: target note {note_id[:8]}... was not found "
                    f"while checking tracked tags (relay_hints={len(relay_hints or [])})"
                )
            for event in events or []:
                if not isinstance(event, dict):
                    continue
                if event.get("id") != note_id:
                    continue

                try:
                    kind = int(event.get("kind") or 0)
                except Exception:
                    kind = 0
                if kind not in (1, 30311):
                    continue

                event_pubkey = str(event.get("pubkey") or "").strip().lower()
                if author_hint and event_pubkey != author_hint:
                    continue

                eff_pub = resolve_effective_pubkey(settings)
                tracked_tags = getattr(settings, "tracked_tags", []) or []
                if _would_event_be_tracked(event, eff_pub, tracked_tags):
                    return event
                logger.info(
                    f"Zap monitor: target note {note_id[:8]}... was found but "
                    "does not match today's tracked tag rules"
                )
        except Exception as exc:
            logger.debug(
                "Zap monitor: failed to fetch/verify tagged note %s for user %s: %s",
                note_id[:8],
                self.user_id,
                exc,
            )
        return None

    async def _get_today_note_ids(self, *args, **kwargs) -> tuple[bool, list[str]]:
        """Deprecated: today's notes handled by subscriptions."""
        return False, []

    @staticmethod
    def _event_has_tracked_tag(*args, **kwargs) -> bool:
        """Deprecated: tag checks handled by subscriptions."""
        return False
    
    async def _opportunistic_registry_update(self, *args, **kwargs):
        return
    
    # Removed: per-instance payment listener runner/worker. Payment events are
    # consumed by the central listener in cyberherd.tasks, which calls into
    # _check_and_trigger_splits as needed.
    
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
    
    async def recover_events_since_midnight(self):
        return False

    async def _start_nostr_monitoring(self, *args, **kwargs):
        return

    # Removed deprecated Nostr handlers (_mark_event_processed, _handle_nostr_zap/repost/reaction)
    
    async def _handle_nostr_repost(self, *args, **kwargs):
        return
    
    async def _handle_nostr_reaction(self, *args, **kwargs):
        return  
          
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

        total_zaps = int(persisted_totals.get("zap", 0) or 0)
        total_reposts = int(persisted_totals.get("repost", 0) or 0)
        total_reactions = int(persisted_totals.get("reaction", 0) or 0)

        # Reflect central invoice listener registration status
        invoice_listener_registered = False
        try:
            from lnbits.tasks import invoice_listeners  # type: ignore
            invoice_listener_registered = 'ext_cyberherd' in invoice_listeners
        except Exception:
            invoice_listener_registered = False

        status = {
            'running': bool(self._running),
            'last_message_at': self.last_message_at,
            'last_zap_at': self.last_zap_at,
            'last_error': self.last_error,
            'mode': 'payment',
            'monitoring_available': bool(invoice_listener_registered),
            'total_zaps_processed': total_zaps,
            'total_reposts_processed': total_reposts,
            'total_reactions_processed': total_reactions,
            'total_recovery_scans': getattr(self, 'total_recovery_scans', 0),
        }
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
        try:
            # Resolve herd wallet from provided settings object
            herd_wallet = getattr(settings, 'herd_wallet', None)
            if not herd_wallet:
                msg = "No herd_wallet configured in settings; cannot recover payments"
                logger.warning(msg)
                return {"scanned": 0, "processed": 0, "error": msg}

            # Continue even when tracked_event_ids is empty. _process_payment_for_zap
            # can opportunistically fetch and validate the zap target note by ID,
            # then persist it if it matches the user's tracked tags.
            tracked_notes = getattr(settings, 'tracked_event_ids', []) or []
            if not tracked_notes:
                logger.info(
                    "No tracked_event_ids found; scanning payments for "
                    "opportunistic tagged-note recovery"
                )

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

            # OPTIMIZATION: If we have timestamps for tracked events, start scanning
            # from the earliest event timestamp. This avoids scanning payments that
            # occurred before the notes even existed.
            timestamps = getattr(settings, 'tracked_event_timestamps', {}) or {}
            earliest_ts = None
            for nid in tracked_notes:
                ts = timestamps.get(nid)
                if ts:
                    if earliest_ts is None or ts < earliest_ts:
                        earliest_ts = ts
            
            if earliest_ts:
                # Ensure we don't go further back than midnight (sanity check, though notes shouldn't be before midnight)
                # Actually, if a note was created before midnight (e.g. timezone edge case), we should respect its timestamp.
                # But tracked_event_ids are supposed to be "today's" notes.
                # Let's just use the earliest timestamp, it's safer and more precise.
                since_ts = earliest_ts
                logger.info(f"Recovery start time set to earliest event timestamp: {since_ts}")

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
                    res = await self._process_payment_for_zap(
                        payment,
                        settings_override=settings,
                    )
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
    """Get or create a PaymentCoordinatorService instance for a user.

    This singleton factory maintains one coordinator instance per user_id.
    """
    uid = kwargs.get('user_id')
    if not uid:
        raise ValueError('user_id required')
    
    inst = _instances.get(uid)
    if inst is None:
        inst = PaymentCoordinatorService(
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


ZapMonitorService = PaymentCoordinatorService
ZapMonitor = PaymentCoordinatorService


def get_payment_coordinator(**kwargs) -> PaymentCoordinatorService:
    """Preferred factory alias for external imports.

    Mirrors get_zap_monitor for compatibility with imports that expect
    get_payment_coordinator in this module.
    """
    return get_zap_monitor(**kwargs)
