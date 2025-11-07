"""Headbutt service adapted for the Cyberherd LNbits extension.

This module provides an EnhancedHeadbuttService that mirrors the middleware
behavior for admission/headbutt decisions but does not perform zap detection
or nostr subscriptions. External services should call the exposed API endpoints
to manage users and trigger headbutt logic when appropriate.
"""

import asyncio
import ast
import json
import os
import re
import time
from datetime import datetime, timezone
from typing import Any, Optional

import httpx

from loguru import logger

from .. import crud
from . import nostr_helpers
from .note_metadata import refresh_tracked_event_addresses
from ..utils.common import parse_int_env, utc_now, extract_t_tags_from_event
from bech32 import bech32_decode, convertbits

try:
    from lnbits.extensions.cyberherd_messaging.services import (
        normalize_relay_hint as _normalize_relay_hint,
    )
except Exception:  # pragma: no cover - messaging extension optional at import time
    def _normalize_relay_hint(raw: str | None) -> str | None:
        if not raw:
            return None
        candidate = str(raw).strip()
        if not candidate:
            return None
        if candidate.startswith(("wss://", "ws://")):
            return candidate
        if candidate.startswith("https://"):
            return "wss://" + candidate[len("https://") :]
        if candidate.startswith("http://"):
            return "ws://" + candidate[len("http://") :]
        return None


_METADATA_CACHE: dict[str, dict[str, Any]] = {}
_METADATA_REFRESH_SECONDS = parse_int_env("CYBERHERD_METADATA_REFRESH_SECONDS", 3600)
_NIP05_REFRESH_SECONDS = parse_int_env("CYBERHERD_NIP05_REFRESH_SECONDS", 10800)


def _now() -> float:
    return time.time()


def _sanitize_metadata_payload(data: dict[str, Any] | None) -> dict[str, Any]:
    """Return a shallow copy with harmless fields only."""
    if not data:
        return {}
    allowed = ("display_name", "lud16", "picture", "nip05", "relays")
    cleaned: dict[str, Any] = {}
    for key in allowed:
        value = data.get(key)
        if value is None:
            continue
        if isinstance(value, str):
            candidate = value.strip()
            if not candidate:
                continue
            cleaned[key] = candidate
        else:
            cleaned[key] = value
    return cleaned


def _metadata_cache_get(pubkey: str) -> dict[str, Any] | None:
    return _METADATA_CACHE.get(pubkey)


def _metadata_cache_store(pubkey: str, data: dict[str, Any], *, fetched_at: float | None = None) -> dict[str, Any]:
    entry = _METADATA_CACHE.setdefault(pubkey, {})
    entry["data"] = _sanitize_metadata_payload(data)
    entry["fetched_at"] = fetched_at if fetched_at is not None else _now()
    return entry


def _metadata_cache_mark_verified(pubkey: str, result: Optional[bool]) -> None:
    if result is None:
        return
    entry = _METADATA_CACHE.setdefault(pubkey, {})
    entry["verified"] = bool(result)
    entry["verified_at"] = _now()


async def _verify_nip05_relaxed(pubkey: str, nip05: str) -> tuple[Optional[bool], str]:
    """Return (result, reason) where result is True/False/None for match/mismatch/unknown."""
    identity = (nip05 or "").strip()
    pubkey_norm = (pubkey or "").strip().lower()
    if not identity or "@" not in identity:
        return False, "invalid_format"

    username_raw, domain = identity.split("@", 1)
    username_raw = username_raw.strip()
    domain = domain.strip()
    username_lower = username_raw.lower()
    if not username_raw or not domain:
        return False, "invalid_format"

    errors: list[str] = []
    last_reason: str | None = None

    def _normalize_candidate(value: str | None) -> Optional[str]:
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

    # Prefer LNbits helper for canonical mapping
    try:
        from lnbits.core.services.nostr import fetch_nip5_details  # type: ignore

        fetched_pubkey, _relays = await fetch_nip5_details(identity)
        fetched_norm = _normalize_candidate(fetched_pubkey)
        if fetched_norm and fetched_norm == pubkey_norm:
            return True, "fallback_match"
        if fetched_norm:
            last_reason = "fallback_mismatch"
            errors.append(f"fallback_mismatch:{fetched_norm}")
        else:
            last_reason = "fallback_missing"
            errors.append("fallback_missing")
    except Exception as exc:
        errors.append(f"fallback:{exc}")

    # First attempt: direct ?name= lookup
    async def _try_lookup(query_name: str | None, label: str) -> tuple[Optional[bool], Optional[str]]:
        url = f"https://{domain}/.well-known/nostr.json"
        if query_name is not None:
            url = f"{url}?name={query_name}"
        try:
            async with httpx.AsyncClient() as client:
                resp = await client.get(url, timeout=10)
                resp.raise_for_status()
                parsed = resp.json()
        except Exception as exc:
            errors.append(f"{label}:{exc}")
            return None, None

        if not isinstance(parsed, dict):
            errors.append(f"{label}:invalid_payload")
            return None, "invalid_payload"

        candidate_raw = None
        try:
            names = parsed.get("names")
            if isinstance(names, dict):
                for key, value in names.items():
                    if isinstance(key, str) and key.strip().lower() == username_lower:
                        candidate_raw = value
                        break
            if candidate_raw is None:
                profiles = parsed.get("profiles")
                if isinstance(profiles, dict):
                    for key, value in profiles.items():
                        if isinstance(key, str) and key.strip().lower() == username_lower:
                            if isinstance(value, dict):
                                candidate_raw = value.get("pubkey")
                            elif isinstance(value, str):
                                candidate_raw = value
                            break
        except Exception:
            candidate_raw = None

        candidate_norm = _normalize_candidate(candidate_raw)
        if candidate_norm and candidate_norm == pubkey_norm:
            return True, "direct_match"
        if candidate_norm:
            errors.append(f"{label}:mismatch:{candidate_norm}")
            return False, "direct_mismatch"
        errors.append(f"{label}:mapping_missing")
        return False, "mapping_missing"

    lookup_attempts: list[tuple[Optional[str], str]] = []
    if username_lower:
        lookup_attempts.append((username_lower, "direct_lower"))
    if username_raw and username_raw != username_lower:
        lookup_attempts.append((username_raw, "direct_case"))
    lookup_attempts.append((None, "direct_full"))

    for query_name, label in lookup_attempts:
        result, reason = await _try_lookup(query_name, label)
        if result is True:
            return True, reason or "direct_match"
        if result is False:
            last_reason = reason
            if reason != "mapping_missing":
                return False, reason
            # mapping missing - try next variant
            continue
        # result is None - keep trying other strategies
        continue

    if last_reason:
        return False, last_reason
    if errors:
        return None, "; ".join(errors)
    return None, "unknown"


def _pick_first_relay(relays_field: Any) -> str | None:
    """Normalize various stored relay formats and return the first ws/wss URL.

    Accepts: None, list/tuple, JSON array string, Python literal list, or CSV string.
    Returns a single relay string starting with ws:// or wss://, or None.
    """
    if not relays_field:
        return None

    # If it's already a sequence, pick first valid
    if isinstance(relays_field, (list, tuple)):
        for r in relays_field:
            try:
                normalized = _normalize_relay_hint(r if isinstance(r, str) else str(r))
                if normalized:
                    return normalized
            except Exception:
                continue
        return None

    # Try string-based formats
    if isinstance(relays_field, str):
        s = relays_field.strip()
        if not s:
            return None

        # Try JSON array
        try:
            parsed = json.loads(s)
            if isinstance(parsed, list):
                return _pick_first_relay(parsed)
            if isinstance(parsed, dict):
                for value in parsed.values():
                    candidate = _pick_first_relay(value)
                    if candidate:
                        return candidate
        except Exception:
            pass

        # Try Python literal list (single-quoted dicts/lists)
        try:
            parsed = ast.literal_eval(s)
            if isinstance(parsed, (list, tuple)):
                return _pick_first_relay(parsed)
            if isinstance(parsed, dict):
                for value in parsed.values():
                    candidate = _pick_first_relay(value)
                    if candidate:
                        return candidate
        except Exception:
            pass

        # Treat as CSV
        parts = [p.strip() for p in s.split(",") if p.strip()]
        for p in parts:
            normalized = _normalize_relay_hint(p)
            if normalized:
                return normalized

    return None


def _normalize_event_id(value: Any) -> str | None:
    """Return lower-case 64-hex event ids when possible.

    Supports raw hex strings, nostr: prefixes, and note/nevent bech32 tokens.
    """
    if not isinstance(value, str):
        return None
    candidate = value.strip()
    if not candidate:
        return None
    if candidate.startswith("nostr:"):
        candidate = candidate.split(":", 1)[1] or ""
    candidate = candidate.lower()

    if re.fullmatch(r"[0-9a-f]{64}", candidate):
        return candidate

    if candidate.startswith(("note1", "nevent1")):
        from ..utils.common import get_nip19_decoder
        
        nip19 = get_nip19_decoder()

        try:
            if nip19:
                decoded_type, data = nip19.decode(candidate)
                event_id = None
                if decoded_type == "note" and isinstance(data, str):
                    event_id = data
                elif decoded_type == "nevent" and isinstance(data, dict):
                    event_id = data.get("id")
                if isinstance(event_id, str) and re.fullmatch(
                    r"[0-9a-f]{64}", event_id.lower()
                ):
                    return event_id.lower()

            hrp, data = bech32_decode(candidate)
            if hrp in ("note", "nevent") and data:
                decoded = convertbits(data, 5, 8, False)
                if decoded:
                    return bytes(decoded).hex()
        except Exception:
            return None

    return None


def calculate_payout(amount: float) -> float:
    # 1% of decimated amount with a cap; mirror middleware rough behavior
    try:
        return round(min((int(amount) // 10) * 0.01, 1.0), 2)
    except Exception:
        return 0.0


class EnhancedHeadbuttService:
    """Basic headbutt service for processing zaps."""

    COOLDOWN_SECONDS = 5

    def __init__(
        self,
        db=None,
        messaging_module=None,
        max_herd_size: int = 3,
        headbutt_min_sats: int = 10,
        config: dict[str, Any] | None = None,
        make_messages_func=None,
        app=None,
        user_id: str | None = None,
        recovery_mode: bool = False,
    ):
        """Create a headbutt service.
        Args:
            db: module/object exposing the expected async CRUD methods (see crud.py)
            messaging_module: module exposing send_to_websocket_clients and
                send_cyberherd_update
            make_messages_func: optional function to format messages (content+payload)
            app: FastAPI app object to access state
            user_id: User ID for user-specific settings and templates
            recovery_mode: If True, skip all note publishing (for startup recovery)
        """
        # core references (annotate as Any so static analyzers know these may be provided at runtime)
        self.db: Any = db
        self.messaging: Any = messaging_module
        self.max_herd_size = max_herd_size
        self.headbutt_min_sats = headbutt_min_sats
        self.config = config or {}
        # Use provided make_messages_func or get it from messaging module
        self.make_messages = make_messages_func or getattr(
            messaging_module, "make_messages", None
        )
        self.send_cyberherd_update = None
        self._lock = asyncio.Lock()
        self._last_bump_ts = 0.0
        self.app: Any = app
        self.user_id = user_id
        self.recovery_mode = recovery_mode

    def _is_cyberherd_messaging_available(self) -> bool:
        """Check if cyberherd_messaging extension is available and ready."""
        try:
            from lnbits.extensions.cyberherd_messaging import services as msg_services
            # Check if the key functions exist
            return hasattr(msg_services, 'try_publish_note') and hasattr(msg_services, 'render_and_publish_template')
        except ImportError:
            return False
        except Exception:
            return False

    async def _get_websocket_topic(self) -> str:
        """Get the websocket topic (herd wallet invoice key) from settings, defaulting to 'cyberherd'."""
        try:
            settings = await self.db.get_settings(self.user_id)
            herd_wallet_id = getattr(settings, 'herd_wallet', None)
            if herd_wallet_id:
                # Import here to avoid circular dependency
                from lnbits.core.crud import get_wallet
                wallet = await get_wallet(herd_wallet_id)
                if wallet and wallet.inkey:
                    return wallet.inkey
                else:
                    logger.warning(f"Could not fetch wallet or inkey for herd_wallet {herd_wallet_id}")
        except Exception as e:
            logger.debug(f"Failed to get herd wallet invoice key from settings: {e}")
        return "cyberherd"

    async def _compute_feeder_difference(self, settings) -> int | None:
        """Return remaining sats until feeder trigger if configured."""
        if not settings:
            return None
        trigger = getattr(settings, "feeder_trigger_sats", None)
        try:
            trigger_int = int(trigger) if trigger is not None else None
        except Exception:
            trigger_int = None
        if trigger_int is None or trigger_int <= 0:
            return None

        herd_wallet_id = getattr(settings, "herd_wallet", None)
        balance_sats: int | None = None

        if herd_wallet_id:
            try:
                from lnbits.core.crud import get_wallet

                wallet = await get_wallet(herd_wallet_id)
                if wallet:
                    balance_msat = getattr(wallet, "balance_msat", None)
                    if balance_msat is not None:
                        try:
                            balance_sats = int(balance_msat) // 1000
                        except Exception:
                            balance_sats = None
                    else:
                        balance = getattr(wallet, "balance", None)
                        try:
                            balance_sats = int(balance) if balance is not None else None
                        except Exception:
                            balance_sats = None
            except Exception as exc:
                logger.debug("cyberherd: failed to load herd wallet for feeder diff: %s", exc)

        if balance_sats is None:
            # Fallback to summing active member amounts if wallet balance isn't accessible.
            try:
                members = await self.db.get_active_cyberherd_members(user_id=self.user_id)
                balance_sats = sum(
                    max(0, int(member.get("amount", 0) or 0)) for member in members
                )
            except Exception as exc:
                logger.debug("cyberherd: unable to compute fallback feeder diff: %s", exc)
                return None

        return max(0, trigger_int - balance_sats)

    async def _resolve_reply_params(
        self,
        note_id: str | None,
        *,
        settings=None,
    ) -> tuple[str | None, str | None]:
        """Return (event_id, a_tag) for 30311 replies if available."""
        if not note_id or not isinstance(note_id, str):
            return None, None

        settings_obj = settings
        if settings_obj is None:
            try:
                settings_obj = await self.db.get_settings(self.user_id)
            except Exception as exc:
                logger.debug(
                    "cyberherd: unable to load settings for reply params (note=%s): %s",
                    note_id[:8] if isinstance(note_id, str) else note_id,
                    exc,
                )
                return None, None

        if not settings_obj:
            return None, None

        addresses = getattr(settings_obj, "tracked_event_addresses", {}) or {}
        address = addresses.get(note_id)
        if address:
            return note_id, address

        try:
            refreshed = await refresh_tracked_event_addresses([note_id], addresses)
        except Exception as exc:
            logger.debug(
                "cyberherd: refresh_tracked_event_addresses failed for note %s: %s",
                note_id[:8],
                exc,
            )
            return None, None

        address = refreshed.get(note_id)
        if not address:
            return None, None

        settings_obj.tracked_event_addresses = refreshed
        try:
            await self.db.upsert_settings(settings_obj, self.user_id)
        except Exception as exc:
            logger.debug(
                "cyberherd: failed to persist refreshed 30311 address for note %s: %s",
                note_id[:8],
                exc,
            )
        return note_id, address

    async def _get_today_cyberherd_notes(self) -> list[str]:
        """
        Return IDs of today's kind-1 notes authored by our project account
        that match tracked tags.
        """
        logger.info(f"🔍 _get_today_cyberherd_notes called for user {self.user_id}, app={self.app is not None}")
        if not self.app:
            logger.warning(f"⚠️  _get_today_cyberherd_notes: self.app is None for user {self.user_id}")
            return []
        try:
            # This logic is duplicated from views_api.py for service-layer use
            from ..views_api import _cache_notes, _get_cached_notes, _get_effective_pubkey, _get_user_private_key

            s = await self.db.get_settings(self.user_id)
            tags = [t.lstrip("#").lower() for t in (s.tracked_tags or [])]
            # Allow empty tags - if not set, any note from author will count

            eff_pub = _get_effective_pubkey(s)
            if not eff_pub:
                return []

            # Use UTC for cache keys - consistent with Nostr event timestamps
            day_key = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            # Use neutral cache key shape consistent with views_api/subscriptions
            cache_key = (day_key, None, eff_pub, tuple(sorted(tags)))

            cached_notes = _get_cached_notes(self.app, cache_key)
            if cached_notes is not None:
                return cached_notes

            # Use UTC midnight - Nostr events use UTC timestamps
            since = int(
                datetime.combine(
                    datetime.now(timezone.utc).date(), datetime.min.time()
                )
                .replace(tzinfo=timezone.utc)
                .timestamp()
            )
            until = since + 86400
            # Query events using configured relays - include kind 1 (notes) and kind 30311 (long-form content)
            logger.info(f"🔍 Querying Nostr for today's notes: kinds=[1,30311], author={eff_pub[:16]}..., since={since}, until={until}")
            events = await nostr_helpers.query_events({"kinds": [1, 30311], "authors": [eff_pub], "since": since, "until": until}, limit=100, timeout=8.0)
            logger.info(f"📬 Query returned {len(events or [])} events")
            matched: list[tuple[str, int]] = []
            for ev in events or []:
                try:
                    ca = int(ev.get("created_at") or 0)
                except Exception:
                    ca = 0
                if not (since <= ca < until):
                    continue
                # If tags are empty, accept all notes from the author
                # Otherwise, require tag match
                if not tags:
                    # No tags set - accept any note from author
                    if isinstance(ev.get("id"), str):
                        matched.append((ev["id"], ca))
                else:
                    tag_values = extract_t_tags_from_event(ev)
                    # Strict match: require explicit 't' tag equality only to avoid false positives
                    if set(tags) & tag_values:
                        if isinstance(ev.get("id"), str):
                            matched.append((ev["id"], ca))

            # Newest first, limit to 10
            matched.sort(key=lambda x: x[1], reverse=True)
            ids = [m[0] for m in matched[:10]]

            logger.info(f"📋 Found {len(ids)} today's #CyberHerd notes for user {self.user_id}: {[i[:16]+'...' for i in ids]}")
            _cache_notes(self.app, cache_key, ids)
            return ids
        except Exception as e:
            logger.error(f"Error getting today's cyberherd notes: {e}", exc_info=True)
            return []

    async def _resolve_attacker_metadata(self, attacker: Any) -> tuple[dict[str, Any], Optional[str]]:
        """Fetch metadata for attacker with caching and local fallback."""
        pubkey = getattr(attacker, "pubkey", None)
        if not pubkey:
            return {}, None

        entry = _metadata_cache_get(pubkey)
        cached_data = dict(entry.get("data", {})) if entry and entry.get("data") else {}
        now = _now()
        age = None
        if entry and entry.get("fetched_at") is not None:
            try:
                age = now - float(entry["fetched_at"])
            except Exception:
                age = None

        needs_refresh = not cached_data or (age is not None and age > _METADATA_REFRESH_SECONDS)
        source: Optional[str] = None

        metadata: dict[str, Any] | None = None
        if needs_refresh:
            try:
                from .nostr_lookup import lookup_metadata  # type: ignore

                metadata = await lookup_metadata(pubkey)
            except Exception as exc:  # pragma: no cover - network dependent
                logger.debug(f"cyberherd: metadata lookup failed for {pubkey[:8]}...: {exc}")

        if metadata:
            source = "nostr"
            entry = _metadata_cache_store(pubkey, metadata, fetched_at=now)
            cached_data = dict(entry.get("data", {}))
        elif cached_data:
            source = "cache"
        else:
            try:
                local = await self.db.get_cyberherd_member_by_pubkey(pubkey, user_id=self.user_id)
            except Exception as exc:
                logger.debug(f"cyberherd: local metadata lookup failed for {pubkey[:8]}...: {exc}")
                local = None
            if local:
                source = "local"
                fallback = {
                    "display_name": local.get("display_name"),
                    "lud16": local.get("lud16"),
                    "picture": local.get("picture"),
                    "nip05": local.get("nip05"),
                    "relays": local.get("relays"),
                }
                entry = _metadata_cache_store(pubkey, fallback, fetched_at=now)
                cached_data = dict(entry.get("data", {}))
            elif entry and entry.get("data"):
                # Fall back to stale cache even if refresh failed
                source = "cache"
                cached_data = dict(entry.get("data", {}))
            else:
                source = None
                cached_data = {}

        return cached_data, source

    async def process_headbutting_attempts(
        self, attempts: list[Any]
    ) -> list[dict[str, Any]]:
        headbutt_attempts = [
            item for item in attempts if getattr(item, "amount", 0) and item.amount > 0
        ]
        if not headbutt_attempts:
            return []

        headbutt_attempts.sort(key=lambda x: x.amount, reverse=True)
        successful = []

        for attempt in headbutt_attempts:
            r = await self.attempt_headbutt(attempt)
            if r:
                successful.append(r)

        return successful

    async def attempt_headbutt(self, attacker: Any) -> dict[str, Any] | None:
        async with self._lock:
            if self._in_cooldown():
                logger.info("AdmissionGuard cooldown: in_cooldown=True")
                return None

            # Reject actions from the effective pubkey (operator's own reposts/likes/zaps)
            try:
                from ..views_api import _get_effective_pubkey
                settings = await self.db.get_settings(self.user_id)
                eff_pub = _get_effective_pubkey(settings)
                if eff_pub and attacker.pubkey == eff_pub:
                    logger.info(
                        f"AdmissionGuard: rejecting action from effective pubkey (operator's own action) "
                        f"pubkey={attacker.pubkey[:16]}..."
                    )
                    return None
            except Exception as e:
                logger.warning(f"Could not check effective pubkey: {e}")

            # Check if pubkey is banned
            try:
                from .. import crud
                if await crud.is_pubkey_banned(attacker.pubkey, self.user_id):
                    logger.info(
                        f"AdmissionGuard: rejecting action from banned pubkey "
                        f"pubkey={attacker.pubkey[:16]}..."
                    )
                    return None
            except Exception as e:
                logger.warning(f"Could not check if pubkey is banned: {e}")

            active_members = await self.db.get_active_cyberherd_members(user_id=self.user_id)
            max_members = await self._get_max_members()

            # Prefer explicit boolean flags set on _Attacker, fallback to membership checks
            is_repost = getattr(attacker, "is_repost", False) or (
                getattr(attacker, "kinds", None) and 6 in getattr(attacker, "kinds", [])
            )
            is_reaction = getattr(attacker, "is_reaction", False) or (
                getattr(attacker, "kinds", None) and 7 in getattr(attacker, "kinds", [])
            )
            if is_repost or is_reaction:
                # For reposts and reactions, set amount to 0
                    if not (is_repost and is_reaction):
                        attacker.amount = 0

            is_existing_member = any(
                m.get("pubkey") == attacker.pubkey for m in active_members
            )

            if is_existing_member:
                # Existing members: reposts/reactions should update kinds and
                # trigger small cumulative payouts (no amount increase), rather
                # than being ignored. Zaps (amount>0) still increase amount.
                logger.info(
                    f"AdmissionGuard existing_member: cumulative_update (member {attacker.pubkey[:8]}...)"
                )
                
                # Get the current member data before update for message generation
                existing_member = await self.db.get_cyberherd_member_by_pubkey(attacker.pubkey, user_id=self.user_id)
                old_amount = existing_member.get('amount', 0) if existing_member else 0
                was_active = bool(existing_member and existing_member.get('is_active'))
                
                # Check if this is a repost or reaction event
                attacker_kinds = getattr(attacker, 'kinds', []) or []
                is_repost_event = 6 in attacker_kinds
                is_reaction_event = 7 in attacker_kinds
                
                # Get existing kinds from the member record
                existing_kinds_set = set()
                if existing_member:
                    kinds_raw = existing_member.get('kinds')
                    if kinds_raw:
                        if isinstance(kinds_raw, str):
                            # Parse comma-separated string like "6,7"
                            for part in kinds_raw.split(','):
                                try:
                                    existing_kinds_set.add(int(part.strip()))
                                except Exception:
                                    pass
                        elif isinstance(kinds_raw, (list, tuple)):
                            for k in kinds_raw:
                                try:
                                    existing_kinds_set.add(int(k))
                                except Exception:
                                    pass
                
                # Skip processing if this is a duplicate repost or reaction
                # Reposts and reactions should only be counted once per user
                if is_repost_event and 6 in existing_kinds_set:
                    logger.info(
                        f"⏭️  Skipping duplicate kind 6 (repost) for existing member {attacker.pubkey[:8]}... "
                        f"(already has kind 6 recorded)"
                    )
                    return None
                
                if is_reaction_event and 7 in existing_kinds_set:
                    logger.info(
                        f"⏭️  Skipping duplicate kind 7 (reaction) for existing member {attacker.pubkey[:8]}... "
                        f"(already has kind 7 recorded)"
                    )
                    return None
                
                # Normalize the attacker metadata before templating:
                # Copy display_name, nprofile, lud16, picture, etc. from existing_member onto attacker when missing
                if existing_member:
                    if not getattr(attacker, 'display_name', None):
                        attacker.display_name = existing_member.get('display_name')
                    if not getattr(attacker, 'nprofile', None):
                        attacker.nprofile = existing_member.get('nprofile')
                    if not getattr(attacker, 'lud16', None):
                        attacker.lud16 = existing_member.get('lud16')
                    if not getattr(attacker, 'picture', None):
                        attacker.picture = existing_member.get('picture')
                
                increase_amount = attacker.amount  # The amount of this specific zap

                # Determine a small payouts bump for reposts/reactions based on
                # the member's existing total so interactions still contribute.
                try:
                    existing_amount = int((existing_member or {}).get('amount', 0) or 0)
                except Exception:
                    existing_amount = 0

                # Calculate payout increase:
                # - For reactions (kind 7): Fixed 1 sat increase
                # - For reposts (kind 6): Fixed 1 sat increase (same as reactions)
                # - For zaps (amount>0): Calculate from zap amount
                if int(increase_amount or 0) == 0:
                    # This is a repost or reaction (no zap amount)
                    # Both get the same 1 sat payout increase
                    payouts_increase = 1.0
                    event_type_name = "reaction" if is_reaction_event else "repost"
                    logger.info(f"💰 {event_type_name.capitalize()} (kind {'7' if is_reaction_event else '6'}) for existing member: fixed 1 sat payout increase")
                else:
                    # Zaps use their own amount for payout calculation
                    payouts_increase = calculate_payout(float(increase_amount))
                    logger.info(f"💰 Zap for existing member: {payouts_increase} sat payout increase from {increase_amount} sat zap")

                await self.db.update_and_activate_member(
                    attacker.pubkey,
                    int(increase_amount or 0),
                    payouts_increase,
                    user_id=self.user_id,
                    kinds=getattr(attacker, 'kinds', None),
                )
                
                # Fetch the refreshed row once to compute new_total defensively
                updated_member = await self.db.get_cyberherd_member_by_pubkey(attacker.pubkey, user_id=self.user_id)
                if updated_member and updated_member.get('amount') is not None:
                    new_total = updated_member.get('amount', 0)
                else:
                    # Fallback if fresh row unavailable or amount is None
                    new_total = old_amount + increase_amount
                
                member_nprofile = getattr(attacker, 'nprofile', None) or (updated_member or {}).get("nprofile")
                member_display_name = (
                    getattr(attacker, "display_name", None)
                    or (updated_member or {}).get("display_name")
                    or "Member"
                )
                member_name = f"nostr:{member_nprofile}" if member_nprofile else member_display_name
                member_pubkey = attacker.pubkey if attacker.pubkey else None
                raw_note_id = getattr(attacker, "note", None) or getattr(attacker, "note_id", None)
                note_id = _normalize_event_id(raw_note_id)
                raw_event_id = getattr(attacker, "event_id", None)
                event_id_value = _normalize_event_id(raw_event_id) or raw_event_id
                
                # Determine the base event type (new member vs increase)
                base_event_type = "member_increase" if was_active else "new_member"
                
                # Determine the source event type (what triggered this: zap, repost, or reaction)
                # Check attacker.kinds to see if it's a kind 6 (repost) or kind 7 (reaction)
                attacker_kinds = getattr(attacker, 'kinds', []) or []
                is_repost = 6 in attacker_kinds
                is_reaction = 7 in attacker_kinds
                
                # Select the appropriate event_type for messaging:
                # - For new members: use "new_member" (generic) since we want consistent welcome messages
                # - For existing members: use specific types if triggered by repost/reaction
                if base_event_type == "member_increase":
                    if is_repost:
                        event_type = "kind_6_repost"  # Existing member reposted
                    elif is_reaction:
                        event_type = "kind_7_reaction"  # Existing member reacted
                    else:
                        event_type = "member_increase"  # Existing member zapped (or other)
                else:
                    # New member - could use specific types here too if desired
                    event_type = base_event_type  # "new_member"
                
                message_event_id = None
                if event_id_value:
                    event_id_str = str(event_id_value)
                    message_event_id = event_id_str if event_id_str.startswith("msg:") else f"msg:{event_id_str}"
                increase_amount_int = int(increase_amount or 0)
                new_total_int = int(new_total or 0)
                member_picture = (
                    getattr(attacker, "picture", None)
                    or (updated_member or {}).get("picture")
                    or (existing_member or {}).get("picture")
                )
                await self._publish_membership_event(
                    event_type,
                    member_pubkey=member_pubkey,
                    member_name=member_name,
                    member_display_name=member_display_name,
                    member_nprofile=member_nprofile,
                    increase_amount=increase_amount_int,
                    new_total=new_total_int,
                    initial_amount=increase_amount_int if not was_active else None,
                    note_id=note_id,
                    event_id=message_event_id,
                    source_event_id=event_id_value,
                    member_picture=member_picture,
                )

                await self._post_admission_tasks(attacker.pubkey, event_id=event_id_value, note_id=note_id)
                return {
                    "updated": attacker.pubkey,
                    "reason": "cumulative_zap",
                    "status": "updated",
                }

            # New members MUST zap today's #CyberHerd note (which are the Tracked Note IDs).
            # Fetch today's notes for validation, but allow fallback if note_id is provided.
            todays_notes = await self._get_today_cyberherd_notes()
            attacker_note_id_raw = getattr(attacker, "note", None) or getattr(attacker, "note_id", None)
            attacker_note_id_norm = _normalize_event_id(attacker_note_id_raw)
            attacker_note_id = attacker_note_id_norm or attacker_note_id_raw

            tracked_event_ids_raw: list[str] = []
            try:
                settings_for_tracking = await self.db.get_settings(self.user_id)
                tracked_event_ids_raw = getattr(settings_for_tracking, "tracked_event_ids", []) or []
            except Exception:
                tracked_event_ids_raw = []

            tracked_event_ids = {
                _normalize_event_id(t) or (t.strip().lower() if isinstance(t, str) else None)
                for t in tracked_event_ids_raw
            }
            tracked_event_ids.discard(None)

            # Check if tracked_tags are configured
            tracked_tags = getattr(settings_for_tracking, "tracked_tags", []) or []
            has_tracked_tags = bool([t for t in tracked_tags if t and t.strip()])

            # If tracked_tags are NOT set AND no tracked_event_ids exist, accept any note
            # If tracked_tags ARE set (even if no notes detected yet), require validation
            if not has_tracked_tags and not tracked_event_ids:
                logger.info(
                    "AdmissionGuard: no tracked tags or notes configured — accepting any note from author"
                )
                # Allow admission without specific note validation
                # The note_id check below will be skipped
            elif not tracked_event_ids:
                # Tracked tags are set but no notes detected yet - deny until notes exist
                logger.info(
                    f"AdmissionGuard: tracked tags configured but no notes detected yet — denying admission "
                    f"(tracked_tags={len(tracked_tags)}, pubkey={attacker.pubkey[:16]}...)"
                )
                return None
            elif not attacker_note_id_norm or attacker_note_id_norm not in tracked_event_ids:
                logger.info(
                    f"AdmissionGuard tracking_mismatch: note_not_registered "
                    f"(note_id={attacker_note_id[:16] if attacker_note_id else 'none'}, "
                    f"tracked_count={len(tracked_event_ids)}, pubkey={attacker.pubkey[:16]}...)"
                )
                return None

            if not todays_notes:
                # Cannot fetch today's notes - but if we have a note_id, trust it
                # (it was already validated by the zap monitor's tracked notes registry)
                if attacker_note_id:
                    logger.info(
                        f"AdmissionGuard today_notes_unavailable: proceeding_with_note_id "
                        f"(cannot fetch today's notes but note_id provided, note_id={attacker_note_id[:16] if attacker_note_id else 'none'}, pubkey={attacker.pubkey[:16]}...)"
                    )
                    # Proceed with admission checks below
                else:
                    # No today's notes AND no note_id - must deny
                    logger.info(
                        f"AdmissionGuard missing_note: no_today_note_and_no_attacker_note "
                        f"(cannot fetch today's notes and no note_id provided, pubkey={attacker.pubkey[:16]}...)"
                    )
                    return None
            else:
                # We have today's notes - validate the note_id is one of them
                logger.debug(f"📋 Verifying note {attacker_note_id[:16] if attacker_note_id else 'none'}... "
                            f"against {len(todays_notes)} tracked notes: {[n[:16]+'...' for n in todays_notes[:5]]}")
                
                if attacker_note_id not in todays_notes:
                    logger.info(
                        f"AdmissionGuard wrong_note: not_today_cyberherd_note "
                        f"(note_id={attacker_note_id[:16] if attacker_note_id else 'none'} not in today's tracked notes, pubkey={attacker.pubkey[:16]}...)"
                    )
                    return None

            if len(active_members) < max_members:
                return await self._try_admit_free_spot(attacker)

            return await self._try_replace_lowest(attacker, active_members)

    async def _get_max_members(self) -> int:
        try:
            s = await self.db.get_settings(self.user_id)
            return int(
                getattr(s, "max_members", self.max_herd_size) or self.max_herd_size
            )
        except Exception:
            return self.max_herd_size

    async def _compute_spots_remaining(self) -> Optional[int]:
        if not self.user_id:
            return None
        try:
            max_members = await self._get_max_members()
            active = await self.db.get_active_cyberherd_size(self.user_id)
            active_int = int(active or 0)
            remaining = int(max_members or 0) - active_int
            return max(0, remaining)
        except Exception as exc:
            logger.debug(f"cyberherd: failed to compute spots_remaining for user {self.user_id}: {exc}")
            return None

    async def _try_admit_free_spot(self, attacker: Any) -> dict[str, Any] | None:
        # Enforce minimum zap threshold even when a free slot is available.
        is_repost = getattr(attacker, "is_repost", False)
        is_reaction = getattr(attacker, "kinds", None) and 7 in getattr(attacker, "kinds", [])

        if not is_repost and not is_reaction:
            min_required = int(self.headbutt_min_sats)
            if int(getattr(attacker, "amount", 0) or 0) < min_required:
                logger.info(
                    f"AdmissionGuard min_threshold: amount={getattr(attacker, 'amount', 0)} required_min={min_required} (pubkey={attacker.pubkey[:8]}...)"
                )
                await self._send_headbutt_failure_notification(
                    attacker,
                    {
                        "display_name": "lowest",
                        "amount": min_required - 1,
                        "pubkey": "",
                    },
                    min_required,
                )
                return None

        logger.info("Free spot available — admitting attacker as active member")
        status = await self._handle_attacker_admission(attacker)
        if status in ["new", "reactivated"]:
            await self._maybe_publish_new_member(attacker, status)
            await self._post_admission_tasks(
                attacker.pubkey,
                getattr(attacker, "event_id", None),
                getattr(attacker, "note", None) or getattr(attacker, "note_id", None),
            )
            return {
                "newly_activated": attacker.pubkey,
                "reason": "free_spot",
                "status": status,
            }
        return None

    async def _try_replace_lowest(
        self, attacker: Any, active_members: list[dict]
    ) -> dict[str, Any] | None:
        is_repost = getattr(attacker, "is_repost", False)
        # Check for reaction via explicit flag or kinds membership
        is_reaction = getattr(attacker, "is_reaction", False) or (
            getattr(attacker, "kinds", None) and 7 in getattr(attacker, "kinds", [])
        )
        
        if is_repost:
            # For reposts (kind 6), can replace:
            # - Other reposts (has kind 6)
            # - Reactions (has only kind 7, no kind 6)
            # Essentially: any member with amount=0 (no zaps)
            replaceable = [m for m in active_members if m.get("amount", 0) == 0]
            if not replaceable:
                logger.info("No amount=0 members (reposts/reactions) to replace — repost headbutt skipped")
                return None
            lowest = self._get_lowest_member(replaceable)
            logger.info(f"Repost (kind 6) can replace any amount=0 member: {len(replaceable)} candidates")
        elif is_reaction:
            # For reactions (kind 7), can only replace reactions that have only kind 7 events
            reactions_only = []
            for m in active_members:
                if int(m.get("amount", 0) or 0) != 0:
                    continue
                kinds_raw = m.get("kinds")
                if not kinds_raw:
                    continue
                kinds_set = set()
                try:
                    for part in [p.strip() for p in str(kinds_raw).split(',') if p.strip()]:
                        try:
                            kinds_set.add(int(part))
                        except Exception:
                            pass
                except Exception:
                    kinds_set = set()
                if kinds_set and 7 in kinds_set and 6 not in kinds_set:
                    reactions_only.append(m)
            if not reactions_only:
                logger.info("No reactions-only to replace — reaction headbutt skipped")
                return None
            lowest = self._get_lowest_member(reactions_only)
        else:
            # For zaps, prioritize displacing repost/reaction-only members first
            # Get members with only kind 6 or kind 7 events (amount=0)
            non_zap_members = [m for m in active_members if m.get("amount", 0) == 0]
            
            if non_zap_members:
                # Zaps ALWAYS displace repost/reaction-only members, regardless of zap amount
                lowest = self._get_lowest_member(non_zap_members)
                lb_pub = lowest.get('pubkey', 'unknown')[:8] + '...' if isinstance(lowest, dict) else 'unknown'
                logger.info(
                    f"Zap headbutt targeting non-zap member (kind 6/7 only) {lb_pub}"
                )
            else:
                # All members are zap-based, use amount comparison
                lowest = self._get_lowest_member(active_members)
                if not lowest:
                    logger.error("Expected lowest active member but none found")
                    return None

                required = max(self.headbutt_min_sats, int(lowest.get("amount", 0)) + 1)
                if int(getattr(attacker, "amount", 0) or 0) < required:
                    logger.info(
                        f"AdmissionGuard headbutt_threshold: amount={getattr(attacker, 'amount', 0)} required={required} (pubkey={attacker.pubkey[:8]}..., target={lowest.get('pubkey', '')[:8]}...)"
                    )
                    await self._send_headbutt_failure_notification(attacker, lowest, required)
                    return None

        logger.info("Headbutt successful — replacing lowest member")
        # Ensure lowest is a dict-like object before proceeding
        lowest_pubkey = None
        if isinstance(lowest, dict):
            lowest_pubkey = lowest.get("pubkey")
        status = await self._handle_successful_headbutt(attacker, lowest)
        if status in ["new", "reactivated"]:
            await self._maybe_publish_new_member(attacker, status)
            await self._post_admission_tasks(
                attacker.pubkey,
                getattr(attacker, "event_id", None),
                getattr(attacker, "note", None) or getattr(attacker, "note_id", None),
            )
            return {
                "newly_activated": attacker.pubkey,
                "deactivated": lowest_pubkey,
                "reason": "headbutt",
                "status": status,
            }
        return None

    async def _handle_successful_headbutt(self, attacker: Any, lowest: dict | None) -> str | None:
        """Perform DB updates to replace the lowest member with the attacker.

        This is a conservative, minimal implementation: deactivate the lowest
        member (if present), add/update the attacker as active, and return
        the admission status string ('new'|'reactivated') as used by callers.
        """
        try:
            if lowest and isinstance(lowest, dict):
                try:
                    await self.db.deactivate_cyberherd_member(lowest.get("pubkey"), user_id=self.user_id)
                except Exception:
                    # Best-effort: log and continue
                    logger.debug("Failed to deactivate previous lowest member during replacement")

            # Add or update attacker as active (reuse existing handler where possible)
            existing = await self.db.get_cyberherd_member_by_pubkey(attacker.pubkey, user_id=self.user_id)
            
            # Calculate payouts based on event type
            # Reactions (kind 7) and Reposts (kind 6) both get fixed 1 sat
            # Zaps use amount-based calculation
            attacker_kinds = getattr(attacker, 'kinds', []) or []
            is_reaction = 7 in attacker_kinds
            is_repost = 6 in attacker_kinds
            
            if is_reaction or is_repost:
                payouts = 1.0  # Fixed 1 sat for reactions and reposts
                event_type = "reaction" if is_reaction else "repost"
                logger.info(f"💰 Headbutt via {event_type} (kind {'7' if is_reaction else '6'}): fixed 1 sat payout")
            else:
                payouts = calculate_payout(float(getattr(attacker, "amount", 0) or 0))
                logger.info(f"💰 Headbutt via zap: {payouts} sat payout from amount {getattr(attacker, 'amount', 0)}")
            
            if existing:
                await self.db.update_and_activate_member(
                    attacker.pubkey,
                    int(getattr(attacker, "amount", 0) or 0),
                    payouts,
                    user_id=self.user_id,
                    kinds=getattr(attacker, "kinds", None),
                )
                return "reactivated"
            else:
                await self._add_new_member(attacker, payouts)
                return "new"
        except Exception as e:
            logger.error(f"Error in _handle_successful_headbutt: {e}")
            return None

    async def _publish_membership_event(
        self,
        event_type: str,
        *,
        member_pubkey: str | None,
        member_name: str,
        member_display_name: str | None,
        member_nprofile: str | None,
        increase_amount: int | None = None,
        new_total: int | None = None,
        initial_amount: int | None = None,
        note_id: str | None = None,
        event_id: str | None = None,
        source_event_id: str | None = None,
        member_picture: str | None = None,
    ):
        # Check if we already processed this event (published by another path,
        # e.g. views_api), skip publishing to avoid duplicate messages.
        # This applies to both realtime and recovery modes - we publish messages
        # for historical events during recovery as long as they're new (not duplicates).
        try:
            if event_id and self.user_id:
                from .. import crud
                try:
                    if await crud.is_event_processed(str(self.user_id), event_id):
                        logger.debug("Skipping publish for event %s because it's already processed", event_id)
                        return
                except Exception:
                    # If persistence check fails, continue to attempt publish
                    pass
        except Exception:
            pass
        if not self.make_messages or not self.user_id:
            return
        try:
            display_name = member_display_name or "Member"
            member_name_value = member_name or display_name
            msg_kwargs = {
                "owner_user_id": self.user_id,
                "member_name": member_name_value,
                "member_display_name": display_name,
                "member_pubkey": member_pubkey,
                "member_nprofile": member_nprofile,
                "event_id": source_event_id if source_event_id is not None else event_id,
                "note_id": note_id,
            }
            if source_event_id is not None:
                msg_kwargs["source_event_id"] = source_event_id
            spots_remaining = await self._compute_spots_remaining()
            if spots_remaining is not None:
                msg_kwargs["spots_remaining"] = spots_remaining
            if event_type == "new_member":
                amount_value = (
                    initial_amount
                    if initial_amount is not None
                    else (increase_amount if increase_amount is not None else new_total)
                )
                msg_kwargs["initial_amount"] = int(amount_value or 0)
                msg_kwargs["increase_amount"] = int(increase_amount or 0)
                msg_kwargs["new_amount"] = int(amount_value or 0)
            else:
                msg_kwargs["increase_amount"] = int(increase_amount or 0)
                msg_kwargs["new_total"] = int(new_total or 0)

            settings = None
            try:
                settings = await self.db.get_settings(self.user_id)
            except Exception as exc:
                logger.debug("cyberherd: failed to load settings for membership publish: %s", exc)
                settings = None

            feeder_difference = await self._compute_feeder_difference(settings)
            if feeder_difference is not None:
                msg_kwargs["difference"] = feeder_difference

            # Generate messages (may use shared templates) and capture for diagnostics
            try:
                msg_generated = await self.make_messages(event_type, **msg_kwargs)
            except Exception as e:
                logger.warning(f"cyberherd: make_messages failed for {event_type}: {e}")
                msg_generated = None

            from ..views_api import _get_user_private_key
            from . import messaging as ch_msg

            private_key = await _get_user_private_key(self.user_id)
            websocket_topic = await self._get_websocket_topic()

            e_tags = [note_id] if note_id else None
            p_tags = [member_pubkey] if member_pubkey else None
            reply_event_id, reply_a_tag = await self._resolve_reply_params(note_id, settings=settings)

            if event_type == "new_member":
                initial_amount_val = int(
                    msg_kwargs.get("initial_amount", msg_kwargs.get("increase_amount", 0))
                )
                # friendly thank-you string used by legacy templates
                thanks_part = f"Thank you for the contribution of {initial_amount_val} sats." if initial_amount_val > 0 else "Welcome to the herd!"
                values = {
                    "member_name": member_name_value,
                    "member_display_name": display_name,
                    "initial_amount": initial_amount_val,
                    "new_amount": initial_amount_val,
                    # Backwards-compatible aliases expected by cyber_herd templates
                    "name": member_name_value,
                    "thanks_part": thanks_part,
                    # 'difference' is a safely-computed alias; callers/templates may compute display logic
                    "difference": 0,
                }
            else:
                increase_val = int(msg_kwargs.get("increase_amount", 0))
                new_total_val = int(msg_kwargs.get("new_total", 0))
                # friendly thank-you string for increases
                thanks_part = f"Thank you for the contribution of {increase_val} sats." if increase_val > 0 else "Thank you."
                values = {
                    "member_name": member_name_value,
                    "member_display_name": display_name,
                    "increase_amount": increase_val,
                    "new_total": new_total_val,
                    # Backwards-compatible aliases
                    "name": member_name_value,
                    "thanks_part": thanks_part,
                    # difference between previous and new total when known
                    "difference": new_total_val - increase_val if new_total_val and increase_val else 0,
                }

            if feeder_difference is not None:
                values["difference"] = feeder_difference
            if spots_remaining is not None:
                values["spots_remaining"] = spots_remaining
            if source_event_id is not None:
                values.setdefault("source_event_id", source_event_id)
            picture_value = (
                member_picture
                or msg_kwargs.get("picture")
                or msg_kwargs.get("cyber_herd_item", {}).get("picture")
            )
            if picture_value:
                values.setdefault("picture", picture_value)
                values.setdefault("member_picture", picture_value)
                values.setdefault("imageUrl", picture_value)

            amount_for_message = int(
                values.get("initial_amount")
                or values.get("increase_amount")
                or values.get("new_total")
                or msg_kwargs.get("initial_amount")
                or msg_kwargs.get("increase_amount")
                or msg_kwargs.get("new_total")
                or 0
            )
            cyber_herd_item = {
                "display_name": display_name,
                "pubkey": member_pubkey,
                "nprofile": member_nprofile,
                "event_id": event_id,
                "amount": amount_for_message,
            }
            if picture_value:
                cyber_herd_item["picture"] = picture_value
                cyber_herd_item.setdefault("imageUrl", picture_value)
            msg_kwargs["cyber_herd_item"] = cyber_herd_item
            values["cyber_herd_item"] = cyber_herd_item

            # Diagnostic log: what we're about to publish (avoid private keys)
            logger.debug(
                "cyberherd: publishing event_type=%s owner=%s e_tags=%s p_tags=%s",
                event_type,
                self.user_id,
                e_tags,
                p_tags,
            )

            # Determine reply_relay from member rows if available
            chosen_relay = None
            try:
                candidate_relays = None
                cy_item = msg_kwargs.get("cyber_herd_item") or {}
                if isinstance(cy_item, dict) and cy_item.get("relays"):
                    candidate_relays = cy_item.get("relays")
                elif values.get("relays"):
                    candidate_relays = values.get("relays")
                elif msg_kwargs.get("relays"):
                    candidate_relays = msg_kwargs.get("relays")

                if candidate_relays:
                    chosen_relay = _pick_first_relay(candidate_relays)

                if not chosen_relay and member_pubkey:
                    try:
                        db_row = await self.db.get_cyberherd_member_by_pubkey(member_pubkey, user_id=self.user_id)
                        if db_row and db_row.get("relays"):
                            chosen_relay = _pick_first_relay(db_row.get("relays"))
                    except Exception:
                        pass
            except Exception:
                chosen_relay = None

            published = await ch_msg.publish_event_message(
                event_type,
                owner_user_id=self.user_id,
                values=values,
                e_tags=e_tags,
                p_tags=p_tags,
                private_key=private_key,
                websocket_topic=websocket_topic,
                reply_to_30311_event=reply_event_id,
                reply_to_30311_a_tag=reply_a_tag,
                reply_relay=chosen_relay,
            )
            # Log publish outcome and a bit of the generated content where available
            try:
                content_preview = None
                if isinstance(msg_generated, dict):
                    content_preview = (msg_generated.get("content") or "")
                    if isinstance(content_preview, str) and len(content_preview) > 200:
                        content_preview = content_preview[:200] + "..."
                if published:
                    logger.info(
                        "cyberherd: %s message published successfully (preview=%s)",
                        event_type,
                        content_preview,
                    )
                    # Persist processed status so other paths don't republish the same event
                    try:
                        if event_id:
                            from .. import crud
                            try:
                                await crud.register_processed_event(
                                    self.user_id,
                                    event_id,
                                    note_id=note_id,
                                    pubkey=member_pubkey,
                                    event_type="message",
                                )
                                logger.debug("Registered processed event %s after membership publish", event_id)
                            except Exception:
                                logger.warning(f"Failed to register processed event {event_id} after membership publish")
                    except Exception:
                        pass
                else:
                    logger.warning(
                        "cyberherd: %s message publish returned False (preview=%s)",
                        event_type,
                        content_preview,
                    )
            except Exception:
                # Don't allow logging issues to break flow
                if published:
                    logger.info("cyberherd: %s message published successfully", event_type)
                else:
                    logger.warning("cyberherd: %s message publish returned False", event_type)
        except Exception as exc:
            logger.error(f"cyberherd: membership message publish failed: {exc}")

    async def _maybe_publish_new_member(self, attacker: Any, status: str):
        if status not in ("new", "reactivated"):
            return
        try:
            member_row = await self.db.get_cyberherd_member_by_pubkey(attacker.pubkey, user_id=self.user_id)
            member_pubkey = getattr(attacker, "pubkey", None)
            member_nprofile = getattr(attacker, "nprofile", None) or (member_row or {}).get("nprofile")
            display_name = (
                getattr(attacker, "display_name", None)
                or (member_row or {}).get("display_name")
                or "Member"
            )
            member_name = f"nostr:{member_nprofile}" if member_nprofile else display_name
            amount_total = int((member_row or {}).get("amount") or getattr(attacker, "amount", 0) or 0)
            contribution = int(getattr(attacker, "amount", 0) or 0)
            raw_note_id = getattr(attacker, "note", None) or getattr(attacker, "note_id", None)
            note_id = _normalize_event_id(raw_note_id)
            raw_event_id = getattr(attacker, "event_id", None)
            event_id = _normalize_event_id(raw_event_id) or raw_event_id
            message_event_id = None
            if event_id:
                event_id_str = str(event_id)
                message_event_id = event_id_str if event_id_str.startswith("msg:") else f"msg:{event_id_str}"

            tracked_raw = []
            try:
                settings = await self.db.get_settings(self.user_id)
                tracked_raw = getattr(settings, "tracked_event_ids", []) or []
            except Exception:
                tracked_raw = []
            tracked_set = {
                _normalize_event_id(t) or (t.strip().lower() if isinstance(t, str) else None)
                for t in tracked_raw
            }
            tracked_set.discard(None)

            if not tracked_set or not note_id or note_id not in tracked_set:
                logger.info(
                    f"Skipping new member publish for untracked note "
                    f"(note_id={note_id[:16] if note_id else 'none'}, tracked_count={len(tracked_set)})"
                )
                return
            # Determine message category based on kinds when available
            try:
                kinds_val = getattr(attacker, 'kinds', None) or (member_row or {}).get('kinds')
                kinds_set = set()
                if isinstance(kinds_val, list):
                    for k in kinds_val:
                        try:
                            kinds_set.add(int(k))
                        except Exception:
                            continue
                elif isinstance(kinds_val, str) and kinds_val:
                    # DB may store comma-separated kinds string
                    for part in kinds_val.split(','):
                        try:
                            kinds_set.add(int(part.strip()))
                        except Exception:
                            continue
            except Exception:
                kinds_set = set()

            if 7 in kinds_set:
                publish_type = 'kind_7_reaction'
            elif 6 in kinds_set:
                publish_type = 'kind_6_repost'
            else:
                publish_type = 'new_member'

            await self._publish_membership_event(
                publish_type,
                member_pubkey=member_pubkey,
                member_name=member_name,
                member_display_name=display_name,
                member_nprofile=member_nprofile,
                increase_amount=contribution,
                new_total=amount_total,
                initial_amount=contribution,
                note_id=note_id,
                event_id=message_event_id,
                source_event_id=event_id,
                member_picture=(getattr(attacker, "picture", None) or (member_row or {}).get("picture")),
            )
        except Exception as exc:
            logger.error(f"cyberherd: failed to publish new member message: {exc}")

    async def _post_admission_tasks(self, pubkey: str, event_id: str | None = None, note_id: str | None = None):
        try:
            await self._maybe_update_splits()
        except Exception as e:
            logger.warning(e)
        try:
            # Get websocket topic from settings
            websocket_topic = await self._get_websocket_topic()
            
            p_tags = [pubkey] if pubkey else []
            await self.messaging.send_cyberherd_update(
                pubkey,
                getattr(self.db, "database", None),
                event_id=event_id,
                note_id=note_id,
                p_tags=p_tags,
                websocket_topic=websocket_topic,
            )
        except Exception as e:
            logger.warning(e)

    def _get_lowest_member(
        self, active_members: list[dict[str, Any]]
    ) -> dict[str, Any] | None:
        if not active_members:
            return None
        return min(
            active_members, key=lambda m: (m.get("amount", 0), m.get("pubkey", ""))
        )

    def _in_cooldown(self) -> bool:
        return (time.time() - self._last_bump_ts) < self.COOLDOWN_SECONDS

    def _set_cooldown(self):
        self._last_bump_ts = time.time()

    async def _handle_attacker_admission(self, attacker: Any) -> str:
        existing = await self.db.get_cyberherd_member_by_pubkey(attacker.pubkey, user_id=self.user_id)
        
        # Calculate payouts based on event type
        # Reactions (kind 7) and Reposts (kind 6) both get fixed 1 sat
        # Zaps use amount-based calculation
        attacker_kinds = getattr(attacker, 'kinds', []) or []
        is_reaction = 7 in attacker_kinds
        is_repost = 6 in attacker_kinds
        
        if is_reaction or is_repost:
            payouts = 1.0  # Fixed 1 sat for reactions and reposts
            event_type = "reaction" if is_reaction else "repost"
            logger.info(f"💰 New member via {event_type} (kind {'7' if is_reaction else '6'}): fixed 1 sat payout")
        else:
            payouts = calculate_payout(float(attacker.amount))
            logger.info(f"💰 New member via zap: {payouts} sat payout from amount {attacker.amount}")
        
        if existing:
            if not getattr(attacker, "nip05", None):
                attacker.nip05 = (existing or {}).get("nip05")
            await self.db.update_and_activate_member(
                attacker.pubkey,
                attacker.amount,
                payouts,
                user_id=self.user_id,
                kinds=getattr(attacker, 'kinds', None),
                nip05=getattr(attacker, "nip05", None),
            )
            return "reactivated"
        else:
            zap_amount = int(getattr(attacker, "amount", 0) or 0)
            requires_lud16 = zap_amount <= 0

            metadata, metadata_source = await self._resolve_attacker_metadata(attacker)
            cache_entry = _metadata_cache_get(attacker.pubkey)
            metadata_timestamp = int(cache_entry.get("fetched_at", _now())) if cache_entry else int(_now())

            if metadata:
                if not getattr(attacker, "display_name", None):
                    attacker.display_name = metadata.get("display_name") or "Anon"
                if not getattr(attacker, "lud16", None) and metadata.get("lud16"):
                    attacker.lud16 = metadata.get("lud16")
                if not getattr(attacker, "picture", None) and metadata.get("picture"):
                    attacker.picture = metadata.get("picture")
                if metadata.get("relays") and not getattr(attacker, "relays", None):
                    attacker.relays = metadata.get("relays")
                if metadata.get("nip05"):
                    attacker.nip05 = metadata.get("nip05")
            else:
                logger.debug(
                    f"AdmissionGuard metadata_unavailable: pubkey={attacker.pubkey[:16]}..., source={metadata_source or 'none'}"
                )

            if not getattr(attacker, "display_name", None):
                attacker.display_name = "Anon"

            has_lud16 = bool(str(getattr(attacker, "lud16", "") or "").strip())
            if requires_lud16 and not has_lud16:
                logger.info(
                    f"AdmissionGuard lud16_required: missing (pubkey={attacker.pubkey[:16]}..., zap_amount={zap_amount})"
                )
                return "lud16_required"

            # Check if NIP-05 verification is enabled in settings
            nip05_verification_enabled = True  # Default to enabled
            try:
                settings = await self.db.get_settings(self.user_id)
                nip05_verification_enabled = getattr(settings, "nip05_verification_enabled", True)
            except Exception as e:
                logger.debug(f"Failed to fetch NIP-05 verification setting, defaulting to enabled: {e}")

            # Only enforce NIP-05 verification if it's enabled
            if nip05_verification_enabled:
                nip05_value = str(getattr(attacker, "nip05", "") or "").strip()
                if not nip05_value and metadata:
                    nip05_candidate = metadata.get("nip05")
                    if isinstance(nip05_candidate, str):
                        nip05_value = nip05_candidate.strip()
                        if nip05_value:
                            attacker.nip05 = nip05_value

                if not nip05_value:
                    logger.info(
                        f"AdmissionGuard nip05_required: missing (pubkey={attacker.pubkey[:16]}...)"
                    )
                    return "nip05_required"

                verification_result, verification_reason = await _verify_nip05_relaxed(attacker.pubkey, nip05_value)
                cached_verified = bool(cache_entry and cache_entry.get("verified"))
                if verification_result is False:
                    _metadata_cache_mark_verified(attacker.pubkey, False)
                    logger.info(
                        f"AdmissionGuard nip05_required: verification_failed (pubkey={attacker.pubkey[:16]}..., nip05={nip05_value}, reason={verification_reason})"
                    )
                    return "nip05_required"
                if verification_result is True:
                    _metadata_cache_mark_verified(attacker.pubkey, True)
                elif not cached_verified:
                    logger.warning(
                        f"AdmissionGuard nip05_unverified: proceeding without confirmation (pubkey={attacker.pubkey[:16]}..., nip05={nip05_value}, reason={verification_reason})"
                    )
                else:
                    logger.debug(
                        f"AdmissionGuard nip05_cached_ok: using cached verification (pubkey={attacker.pubkey[:16]}..., reason={verification_reason})"
                    )
            else:
                logger.info(
                    f"AdmissionGuard nip05_verification_disabled: skipping NIP-05 verification (pubkey={attacker.pubkey[:16]}...)"
                )

            # Generate nprofile for attacker (without relay hints - faster and still valid)
            try:
                from ..utils.tlv import hex_to_nprofile  # type: ignore

                if not getattr(attacker, "nprofile", None):
                    attacker.nprofile = hex_to_nprofile(attacker.pubkey)
            except Exception:
                if not getattr(attacker, "nprofile", None):
                    attacker.nprofile = None

            setattr(attacker, "metadata_last_checked_at", metadata_timestamp)

            await self._add_new_member(attacker, payouts)
            return "new"

    def _select_template_key_for_headbutt(
        self,
        base: str,
        attacker: Any = None,
        victim: dict | None = None,
        headbutt_result: dict | None = None,
    ) -> str:
        """Return a strict template key name based on attacker kinds, victim kinds, and whether attacker paid."""
        try:
            kinds = getattr(attacker, "kinds", None) if attacker is not None else None

            # attacker_amount preference from headbutt_result, else from attacker
            attacker_amount = None
            if isinstance(headbutt_result, dict):
                aa = headbutt_result.get("attacker_amount")
                attacker_amount = int(aa) if aa is not None else None
            if attacker_amount is None and attacker is not None:
                try:
                    attacker_amount = int(getattr(attacker, "amount", 0) or 0)
                except Exception:
                    attacker_amount = 0

            # parse victim kinds into a set of ints if present
            victim_kinds = None
            if isinstance(victim, dict):
                vk = victim.get("kinds")
                if vk is not None:
                    if isinstance(vk, str):
                        try:
                            victim_kinds = {int(x) for x in vk.split(",") if x}
                        except Exception:
                            victim_kinds = None
                    elif isinstance(vk, (list, tuple, set)):
                        try:
                            victim_kinds = {int(x) for x in vk}
                        except Exception:
                            victim_kinds = set()

            # Normalize attacker kinds into integer set for robust checks
            attacker_kinds_set = set()
            if kinds:
                if isinstance(kinds, (list, tuple, set)):
                    try:
                        attacker_kinds_set = {int(x) for x in kinds}
                    except Exception:
                        attacker_kinds_set = set()
                else:
                    try:
                        attacker_kinds_set = {int(x) for x in str(kinds).split(",") if x.strip()}
                    except Exception:
                        attacker_kinds_set = set()

            # now select specific keys
            if base == "headbutt_failure":
                if attacker_kinds_set:
                    if 7 in attacker_kinds_set:
                        return "kind_7_headbutt_failure"
                    if 6 in attacker_kinds_set:
                        return "kind_6_headbutt_failure"
                return "headbutt_failure"

            if base == "headbutt_success":
                # If attacker didn't pay (amount == 0) treat as repost or reaction join
                if attacker_amount == 0 and attacker_kinds_set:
                    if 6 in attacker_kinds_set:
                        return "kind_6_repost"
                    if 7 in attacker_kinds_set:
                        return "kind_7_reaction"

                # attacker paid — check victim kinds to choose zapper_displaces variant
                if victim_kinds:
                    if 6 in victim_kinds:
                        return "zapper_displaces_kind_6"
                    if 7 in victim_kinds:
                        return "zapper_displaces_kind_7"

                return "headbutt_success"

        except Exception:
            # On any error, fall back to the generic base key
            return base

        return base

    # If nothing matched, return the base key as a safe fallback

    async def _add_new_member(self, member: Any, payouts: float):
        kinds_str = None
        try:
            kinds = getattr(member, "kinds", None)
            if isinstance(kinds, list):
                kinds_str = ",".join(map(str, kinds))
            elif kinds is not None:
                # Keep as-is; CRUD will normalize string into canonical comma-separated ints
                kinds_str = str(kinds)
        except Exception:
            kinds_str = None

        member_data = {
            "pubkey": getattr(member, "pubkey", None),
            "display_name": getattr(member, "display_name", None) or "Anon",
            "event_id": getattr(member, "event_id", None),
            "note": getattr(member, "note", None),
            "kinds": kinds_str,
            "nprofile": getattr(member, "nprofile", None),
            "lud16": getattr(member, "lud16", None),
            "nip05": getattr(member, "nip05", None),
            "payouts": float(payouts or 0.0),
            "amount": int(getattr(member, "amount", 0) or 0),
            "picture": getattr(member, "picture", None),
            # Try to persist any relays present on the member object (from lookup)
            "relays": getattr(member, "relays", None),
            "metadata_last_checked_at": getattr(member, "metadata_last_checked_at", None),
        }
        await self.db.add_new_active_member(member_data, user_id=self.user_id)

    async def _send_headbutt_failure_notification(
        self, attacker: Any, victim: dict[str, Any], required_amount: int
    ):
        try:
            existing_attacker = None
            existing_victim = None
            # Get nprofile for attacker
            attacker_nprofile = getattr(attacker, 'nprofile', None)
            if not attacker_nprofile:
                # Try to get from database
                try:
                    existing_attacker = await self.db.get_cyberherd_member_by_pubkey(attacker.pubkey, user_id=self.user_id)
                    attacker_nprofile = existing_attacker.get('nprofile') if existing_attacker else None
                except Exception:
                    attacker_nprofile = None
            
            # Get nprofile for victim
            victim_nprofile = None
            if victim.get("pubkey"):
                try:
                    existing_victim = await self.db.get_cyberherd_member_by_pubkey(victim["pubkey"], user_id=self.user_id)
                    victim_nprofile = existing_victim.get('nprofile') if existing_victim else None
                except Exception:
                    victim_nprofile = None
            
            # Format names with nprofiles
            attacker_name = f"nostr:{attacker_nprofile}" if attacker_nprofile else (getattr(attacker, "display_name", None) or "Anon")
            victim_name = f"nostr:{victim_nprofile}" if victim_nprofile else (victim.get("display_name") or "Anon")

            attacker_amount = int(getattr(attacker, "amount", 0) or 0)
            victim_amount = int(victim.get("amount", 0) or 0)
            required_sats = int(required_amount)
            difference_needed = max(0, required_sats - attacker_amount)

            note_id = getattr(attacker, "note", None) or getattr(attacker, "note_id", None)
            e_tags = [note_id] if note_id else []
            p_tags = [p for p in (getattr(attacker, "pubkey", None), victim.get("pubkey")) if p]

            template_key = self._select_template_key_for_headbutt(
                "headbutt_failure", attacker=attacker, victim=victim, headbutt_result=None
            )

            values = {
                "attacker_name": attacker_name,
                "attacker_amount": attacker_amount,
                "attacker_pubkey": getattr(attacker, "pubkey", None),
                "attacker_nprofile": attacker_nprofile,
                "victim_name": victim_name,
                "victim_amount": victim_amount,
                "victim_pubkey": victim.get("pubkey"),
                "victim_nprofile": victim_nprofile,
                "required_amount": required_sats,
                "required_sats": required_sats,
                "difference": difference_needed,
                "name": attacker_name,
                "event_id": getattr(attacker, "event_id", None),
                "note_id": note_id,
            }

            settings = None
            try:
                settings = await self.db.get_settings(self.user_id)
            except Exception:
                pass

            websocket_topic = await self._get_websocket_topic()
            reply_event_id, reply_a_tag = await self._resolve_reply_params(note_id, settings=settings)

            from . import messaging as ch_msg

            # Try to derive a reply_relay from attacker or existing/victim member rows
            chosen_relay = None
            try:
                relays_source = getattr(attacker, 'relays', None)
                if not relays_source and 'existing_attacker' in locals() and existing_attacker:
                    relays_source = existing_attacker.get('relays')
                if not relays_source and 'existing_victim' in locals() and existing_victim:
                    relays_source = existing_victim.get('relays')
                if relays_source:
                    chosen_relay = _pick_first_relay(relays_source)
                else:
                    chosen_relay = None
            except Exception:
                chosen_relay = None

            success = await ch_msg.publish_event_message(
                template_key,
                owner_user_id=self.user_id,
                values=values,
                e_tags=e_tags,
                p_tags=p_tags,
                private_key=getattr(settings, "nostr_private_key", None) if settings else None,
                websocket_topic=websocket_topic,
                reply_to_30311_event=reply_event_id,
                reply_to_30311_a_tag=reply_a_tag,
                reply_relay=chosen_relay,
            )
            if success:
                logger.info(f"cyberherd: {template_key} message published")
            else:
                logger.warning(f"cyberherd: {template_key} message publish returned False")
        except Exception as e:
            logger.error(f"Error sending headbutt failure notification: {e}")

    async def _handle_repost_admission(self, attacker: Any) -> dict[str, Any] | None:
        """Handle admission for repost events (kind 6) with 1% allocation."""
        try:
            # Check if already a member
            existing = await self.db.get_cyberherd_member_by_pubkey(attacker.pubkey, user_id=self.user_id)
            if existing:
                # Already a member, no need to add again
                logger.info(f"Reposter {attacker.pubkey[:8]}... already a member, skipping repost admission")
                return None

            # Add as new member with 1% allocation
            data = {
                'pubkey': attacker.pubkey,
                'display_name': f'Reposter_{attacker.pubkey[:8]}',
                'event_id': getattr(attacker, "event_id", None),
                'note': f'Reposted {getattr(attacker, "note_id", "unknown")}',
                'kinds': '6',
                'nprofile': '',
                'lud16': '',
                'notified': 0,
                'payouts': 0.0,
                'amount': 0,  # Reposts don't have amounts
                'picture': '',
                'relays': '',
                'metadata_last_checked_at': int(datetime.now().timestamp()),  # Local time
                'is_active': 1,
            }
            
            await self.db.add_new_active_member(data, user_id=self.user_id)
            
            # Set allocation to 1% for reposts
            await self.db.update_cyberherd_member_allocation(attacker.pubkey, 1.0, user_id=self.user_id)
            
            # Update splits
            try:
                from .splits import update_split_targets_proportional
                s = await self.db.get_settings(self.user_id)
                sw = getattr(s, 'source_wallet', None)
                if sw:
                    await update_split_targets_proportional(str(sw))
            except Exception:
                pass
            
            await self._post_admission_tasks(attacker.pubkey)
            
            try:
                await self._maybe_publish_new_member(attacker, "new")
            except Exception as e:
                logger.warning(f"Error publishing repost admission message: {e}")
            
            logger.info(f"Admitted reposter {attacker.pubkey[:8]}... with 1% allocation")
            return {
                "newly_activated": attacker.pubkey,
                "reason": "repost",
                "status": "new",
                "allocation_percentage": 1.0
            }
            
        except Exception as e:
            logger.error(f"Error handling repost admission for {attacker.pubkey[:8]}...: {e}")
            return None

    async def _maybe_update_splits(self):
        """Update splits for active members based on their amounts."""
        try:
            active_members = await self.db.get_active_cyberherd_members(user_id=self.user_id)
            total_amount = sum(m.get("amount", 0) for m in active_members)
            if total_amount <= 0:
                return

            # Update each member's allocation percentage based on their amount
            for member in active_members:
                try:
                    pubkey = member.get("pubkey")
                    amount = member.get("amount", 0)
                    # Calculate new allocation percentage
                    allocation_percentage = (amount / total_amount) * 100.0
                    allocation_percentage = round(allocation_percentage, 4)  # Limit to 4 decimal places

                    # Update member record
                    await self.db.update_cyberherd_member_allocation(
                        pubkey, allocation_percentage, user_id=self.user_id
                    )
                except Exception as e:
                    logger.warning(f"Error updating split for member {member.get('pubkey')}: {e}")
        except Exception as e:
            logger.error(f"Error in _maybe_update_splits: {e}")


# ---- Public Trigger Functions for External Services ----


class _Attacker:
    """Simple attacker object for headbutt processing."""
    def __init__(self, pubkey: str, amount: int = 0, kinds: list[int] | None = None,
                 note_id: str | None = None, event_id: str | None = None,
                 display_name: str | None = None, nprofile: str | None = None):
        self.pubkey = pubkey
        self.amount = amount
        # Store kinds as provided but be defensive about element types
        self.kinds = kinds or []
        try:
            # Precompute integer-kind set for reliable membership checks
            self._kinds_int_set = {int(x) for x in self.kinds}
        except Exception:
            # Fallback: try to coerce strings carefully, else empty set
            s = set()
            for x in self.kinds:
                try:
                    s.add(int(x))
                except Exception:
                    pass
            self._kinds_int_set = s
        # Backwards-compatible boolean flags used elsewhere in the codebase
        self.is_repost = 6 in self._kinds_int_set
        self.is_reaction = 7 in self._kinds_int_set
        self.note = note_id
        self.note_id = note_id
        self.event_id = event_id
        self.display_name = display_name
        self.nprofile = nprofile


async def trigger_headbutt_from_zap(
    user_id: str,
    pubkey: str,
    amount_sats: int,
    note_id: str,
    event_id: str,
    app,
    display_name: str | None = None,
    nprofile: str | None = None
) -> dict[str, Any] | None:
    """Trigger headbutt processing from a zap event.
    
    Args:
        user_id: User ID for the headbutt service
        pubkey: Pubkey of the zapper
        amount_sats: Amount of sats zapped
        note_id: Note ID that was zapped
        event_id: Event ID of the zap receipt
        app: FastAPI app object
        display_name: Optional display name of the zapper
        nprofile: Optional nprofile of the zapper
        
    Returns:
        Result dict from attempt_headbutt or None
    """
    try:
        # Import here to avoid circular dependencies
        from .. import crud
        from . import messaging as ch_msg
        
        # Create headbutt service
        service = EnhancedHeadbuttService(
            db=crud,
            messaging_module=ch_msg,
            app=app,
            user_id=user_id
        )
        
        # Create attacker object for zap (kind 9735)
        attacker = _Attacker(
            pubkey=pubkey,
            amount=amount_sats,
            kinds=[9735],
            note_id=note_id,
            event_id=event_id,
            display_name=display_name,
            nprofile=nprofile
        )
        
        # Trigger headbutt attempt
        return await service.attempt_headbutt(attacker)
        
    except Exception as e:
        logger.error(f"Error in trigger_headbutt_from_zap: {e}")
        return None


async def trigger_headbutt_from_repost(
    user_id: str,
    pubkey: str,
    note_id: str,
    event_id: str,
    app,
    recovery_mode: bool = False,
    display_name: str | None = None,
    nprofile: str | None = None
) -> dict[str, Any] | None:
    """Trigger headbutt processing from a repost event.
    
    Args:
        user_id: User ID for the headbutt service
        pubkey: Pubkey of the reposter
        note_id: Note ID that was reposted
        event_id: Event ID of the repost
        app: FastAPI app object
        display_name: Optional display name of the reposter
        nprofile: Optional nprofile of the reposter
        
    Returns:
        Result dict from attempt_headbutt or None
    """
    try:
        # Import here to avoid circular dependencies
        from .. import crud
        from . import messaging as ch_msg

        # Create headbutt service with recovery mode passed through
        service = EnhancedHeadbuttService(
            db=crud,
            messaging_module=ch_msg,
            app=app,
            user_id=user_id,
            recovery_mode=recovery_mode,
        )

        # Create attacker object for repost (kind 6)
        attacker = _Attacker(
            pubkey=pubkey,
            amount=0,  # Reposts don't have amounts
            kinds=[6],
            note_id=note_id,
            event_id=event_id,
            display_name=display_name,
            nprofile=nprofile,
        )

        # Trigger headbutt attempt
        return await service.attempt_headbutt(attacker)

    except Exception as e:
        logger.error(f"Error in trigger_headbutt_from_repost: {e}")
        return None


async def trigger_headbutt_from_reaction(
    user_id: str,
    pubkey: str,
    note_id: str,
    event_id: str,
    app,
    recovery_mode: bool = False,
    display_name: str | None = None,
    nprofile: str | None = None
) -> dict[str, Any] | None:
    """Trigger headbutt processing from a reaction/like event.
    
    Args:
        user_id: User ID for the headbutt service
        pubkey: Pubkey of the reactor
        note_id: Note ID that was reacted to
        event_id: Event ID of the reaction
        app: FastAPI app object
        display_name: Optional display name of the reactor
        nprofile: Optional nprofile of the reactor
        
    Returns:
        Result dict from attempt_headbutt or None
    """
    try:
        # Import here to avoid circular dependencies
        from .. import crud
        from . import messaging as ch_msg

        # Create headbutt service with recovery mode passed through
        service = EnhancedHeadbuttService(
            db=crud,
            messaging_module=ch_msg,
            app=app,
            user_id=user_id,
            recovery_mode=recovery_mode,
        )

        # Create attacker object for reaction (kind 7)
        attacker = _Attacker(
            pubkey=pubkey,
            amount=0,  # Reactions don't have amounts
            kinds=[7],
            note_id=note_id,
            event_id=event_id,
            display_name=display_name,
            nprofile=nprofile,
        )

        # Trigger headbutt attempt
        return await service.attempt_headbutt(attacker)

    except Exception as e:
        logger.error(f"Error in trigger_headbutt_from_reaction: {e}")
        return None
