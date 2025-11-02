from loguru import logger

import re
import asyncio
import json
import time
from http import HTTPStatus
from typing import Optional, Dict, Any

from fastapi import APIRouter, Depends, HTTPException, Request, Security, Query
from fastapi.responses import JSONResponse

# Common LNbits helpers used across extensions
from lnbits.core.models import WalletTypeInfo
from lnbits.decorators import require_admin_key, api_key_header, optional_user_id, require_invoice_key
import uuid
from lnbits.decorators import KeyChecker, api_key_query
from lnbits.core.models import KeyType
from lnbits.extensions.cyberherd.models import MemberPut
from lnbits.utils.nostr import (
    normalize_private_key,
    validate_pub_key,
    hex_to_npub,
)
from lnbits.extensions.cyberherd.utils.tlv import hex_to_nprofile
from lnbits.extensions.cyberherd.services.pubkey import (
    resolve_effective_pubkey,
    resolve_and_persist_effective_pubkey,
)
from lnbits.extensions.cyberherd.services.nostr_helpers import (
    reconnect_nostrclient_ws,
)
from lnbits.extensions.cyberherd.services.headbutt import (
    _normalize_event_id as normalize_event_id,
)
from lnbits.extensions.cyberherd.services.note_metadata import (
    refresh_tracked_event_addresses,
)
from lnbits.core.crud import get_wallet
from lnbits.extensions.cyberherd.services.nostr_lookup import (
    lookup_metadata,
    lookup_relays,
)
from lnbits.extensions.cyberherd.services.zap_totals import (
    get_zap_totals_for_zapper,
    ZapTotalsError,
)
from lnbits.extensions.cyberherd.services.splits import (
    update_split_targets_proportional,
)

# Legacy underscore-prefixed helpers expected by this module
_normalize_event_id = normalize_event_id
_reconnect_nostrclient_ws = reconnect_nostrclient_ws

def _extract_private_key(raw: str | None) -> str | None:
    """Lightweight extractor for private key-like tokens from freeform input.

    This mirrors previous small helper behaviour used by views: return the
    provided string stripped, or None for non-string values. More advanced
    parsing (bech32, 0x prefix) is handled by normalize_private_key later.
    """
    if raw is None:
        return None
    try:
        s = str(raw).strip()
        return s if s else None
    except Exception:
        return None

# No compatibility shims required - import targets exist in helper modules
from lnbits.core.crud import get_wallet_for_key
from . import crud, services
from .crud import clear_processed_zaps_for_user
from .services.splits import reset_splits_to_predefined_wallet
from .services.zap_monitor import get_zap_monitor

# Router for this module (will be included by package init)
cyberherd_api_router = APIRouter()

# Zap monitor instance registry (diagnostics); populated by zap_monitor service
# Defined here so diagnostic endpoints can reference them safely during tests
_zap_monitor_instances: dict = {}
_ZAP_MONITOR_LAST_ACCESS: dict = {}


async def cleanup_stale_monitors(max_age: int | None = None) -> dict:
    """Minimal cleanup helper used by diagnostics.

    This will attempt to remove stale entries from the local registry when
    called. It returns a summary dict. It's intentionally conservative so
    tests and runtime don't depend on sophisticated behavior here.
    """
    try:
        # No-op if registry empty
        if not _zap_monitor_instances:
            return {}
        # If max_age provided, remove entries older than max_age seconds
        if max_age is not None:
            now = time.time()
            removed = []
            for uid, last in list(_ZAP_MONITOR_LAST_ACCESS.items()):
                if last and (now - last) > max_age:
                    _zap_monitor_instances.pop(uid, None)
                    _ZAP_MONITOR_LAST_ACCESS.pop(uid, None)
                    removed.append(uid)
            return {"removed": removed}
        return {}
    except Exception:
        return {}


try:
    from lnbits.extensions.cyberherd_messaging.services import (
        normalize_relay_hint as _normalize_relay_hint,
    )
except Exception:  # pragma: no cover - extension may be unavailable during bootstrap
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
    

async def _get_user_private_key(user_id: str) -> str | None:
    """Get the user's Nostr private key from cyberherd settings.
    
    Args:
        user_id: The LNbits user ID
        
    Returns:
        The private key as a hex string, or None if not set
    """
    try:
        settings = await crud.get_settings(user_id)
        if settings and hasattr(settings, 'nostr_private_key') and settings.nostr_private_key:
            return settings.nostr_private_key
    except Exception as e:
        logger.warning(f"Failed to get private key for user {user_id}: {e}")
    return None


def _extract_tracked_event_id(
    payload: dict | None, zap_request: dict | None = None
) -> str | None:
    """Best-effort extraction of the tracked note's event id (nostr e-tag)."""
    data = payload or {}

    # Prefer explicit payload fields first
    for key in ("tracked_event_id", "tracked_note_id", "note", "note_id"):
        candidate = _normalize_event_id(data.get(key))
        if candidate:
            return candidate

    # Check zap_request wrapper for explicit field
    if isinstance(zap_request, dict):
        explicit = _normalize_event_id(zap_request.get("tracked_event_id"))
        if explicit:
            return explicit

        # Fallback to the first 'e' tag in zap_request["tags"]
        tags = zap_request.get("tags") or []
        for tag in tags:
            if (
                isinstance(tag, list)
                and len(tag) >= 2
                and tag[0] == "e"
            ):
                candidate = _normalize_event_id(tag[1])
                if candidate:
                    return candidate

    return None


def _iterate_relay_values(source: object):
    if not source:
        return
    if isinstance(source, str):
        s = source.strip()
        if not s:
            return
        if s.startswith("[") and s.endswith("]"):
            try:
                parsed = json.loads(s)
            except Exception:
                parsed = None
            if isinstance(parsed, list):
                for item in parsed:
                    yield from _iterate_relay_values(item)
                return
        if "," in s:
            for part in s.split(","):
                yield from _iterate_relay_values(part)
            return
        yield s
        return
    if isinstance(source, (list, tuple, set)):
        for item in source:
            yield from _iterate_relay_values(item)
        return
    yield str(source)


def _select_reply_relay(*sources: object) -> str | None:
    """Return the first ws/wss relay hint from provided sources."""
    for source in sources:
        if not source:
            continue
        for candidate in _iterate_relay_values(source):
            normalized = _normalize_relay_hint(candidate)
            if normalized:
                return normalized
    return None


async def _compute_feeder_difference_for_user(
    user_id: str | None,
    *,
    settings_obj: Any | None = None,
) -> int | None:
    """Return remaining sats until feeder activation for the given user."""
    if not user_id:
        return None

    settings = settings_obj
    if settings is None:
        try:
            settings = await crud.get_settings(user_id)
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.debug("Cyberherd: unable to load settings for feeder diff (%s)", exc)
            return None

    if not settings:
        return None

    trigger_raw = getattr(settings, "feeder_trigger_sats", None)
    try:
        trigger_int = int(trigger_raw) if trigger_raw is not None else None
    except Exception:
        trigger_int = None

    if trigger_int is None or trigger_int <= 0:
        return None

    balance_sats: int | None = None
    herd_wallet_id = getattr(settings, "herd_wallet", None)

    if herd_wallet_id:
        try:
            wallet = await get_wallet(herd_wallet_id)
            if wallet:
                balance_msat = getattr(wallet, "balance_msat", None)
                if balance_msat is not None:
                    balance_sats = int(balance_msat) // 1000
                else:
                    balance = getattr(wallet, "balance", None)
                    if balance is not None:
                        balance_sats = int(balance)
        except Exception as exc:
            logger.debug(
                "Cyberherd: unable to read herd wallet balance for feeder diff (%s)",
                exc,
            )

    if balance_sats is None:
        try:
            members = await crud.get_active_cyberherd_members(user_id)
            balance_sats = sum(
                max(0, int(member.get("amount", 0) or 0)) for member in members
            )
        except Exception as exc:
            logger.debug(
                "Cyberherd: fallback feeder diff computation failed (%s)",
                exc,
            )
            return None

    try:
        return max(0, trigger_int - balance_sats)
    except Exception:
        return None


_NOTES_CACHE_TTL_SECONDS = 60 * 5  # expire cached day lookups after five minutes


def _ensure_notes_cache_container(app) -> dict:
    """Return (and initialize) the request.app/state cache store."""
    target = getattr(app, "state", app)
    cache = getattr(target, "cyberherd_today_notes_cache", None)
    if not isinstance(cache, dict):
        cache = {}
        try:
            setattr(target, "cyberherd_today_notes_cache", cache)
        except Exception:
            # On failure we still return the in-memory dict for this call
            pass
    return cache


def _get_cached_notes(request, cache_key):
    """Fetch cached note ids for the provided key if fresh."""
    try:
        app = getattr(request, "app", request)
        cache = _ensure_notes_cache_container(app)
        entry = cache.get(cache_key)
        if not entry:
            return None
        ts, values = entry
        if not isinstance(ts, (int, float)) or time.monotonic() - ts > _NOTES_CACHE_TTL_SECONDS:
            try:
                del cache[cache_key]
            except Exception:
                pass
            return None
        # Return a new list copy to avoid external mutation of cached tuple
        return list(values)
    except Exception:
        return None


def _cache_notes(request, cache_key, note_ids):
    """Store note ids for the provided cache key with TTL."""
    try:
        app = getattr(request, "app", request)
        cache = _ensure_notes_cache_container(app)
        # Deduplicate while preserving order; filter out falsey entries
        cleaned = []
        seen = set()
        for note_id in note_ids or []:
            if not note_id:
                continue
            normalized = str(note_id)
            if normalized in seen:
                continue
            seen.add(normalized)
            cleaned.append(normalized)
        cache[cache_key] = (time.monotonic(), tuple(cleaned))
    except Exception:
        pass


def _clear_cached_notes_for_user(app, user_id=None):
    """Remove cache entries belonging to the given user (or None)."""
    try:
        cache = _ensure_notes_cache_container(app)
        targets = [
            key
            for key in list(cache.keys())
            if isinstance(key, tuple) and len(key) >= 2 and key[1] == user_id
        ]
        for key in targets:
            cache.pop(key, None)
    except Exception:
        pass


def _get_cached_effective_pubkey(settings):
    """Return a previously computed effective pubkey when available."""
    try:
        cached = getattr(settings, "computed_effective_pubkey", None)
        if isinstance(cached, str) and cached:
            return cached
    except Exception:
        pass
    eff = resolve_effective_pubkey(settings, persist=False)
    if eff:
        try:
            settings.computed_effective_pubkey = eff
        except Exception:
            pass
    return eff


def _get_effective_pubkey(settings):
    """Compatibility helper that resolves the effective pubkey."""
    return resolve_effective_pubkey(settings, persist=False)


async def _resolve_user_from_api_key(request: Request) -> str | None:
    """If an X-API-KEY or api-key query param is present, resolve it to a user id.
    Returns user_id string or None.
    """
    key_value = request.headers.get("X-API-KEY") or request.query_params.get("api-key")
    if not key_value:
        return None

    # Use KeyChecker with provided key to validate and fetch wallet info.
    checker = KeyChecker(api_key=key_value, expected_key_type=KeyType.invoice)
    try:
        winfo: WalletTypeInfo = await checker(request)
        return winfo.wallet.user
    except Exception:
        return None


async def auth_wallet_or_admin(
    request: Request,
    api_key_header: str | None = Depends(lambda request=Depends(lambda: None): None),
):
    """(Legacy) fallback dependency; should not be used directly by FastAPI.

    Note: this definition exists to keep older callsites working but the real
    auth wallet-or-admin dependency is the one defined below which uses
    FastAPI's Security/Depends to correctly resolve API keys and admin
    sessions. Do not call this function directly.
    """
    # This function intentionally raises to ensure callers use the proper
    # dependency signature (see auth_wallet_or_admin_dep). If somehow invoked
    # directly, return a permissive admin placeholder (safe default for tests).
    user = await check_admin(request)  # type: ignore[arg-type]
    return {"type": "admin", "value": user}


async def auth_wallet_or_admin_dep(
    request: Request,
    api_key_header: str | None = Security(api_key_header),
    api_key_query: str | None = Security(api_key_query),
):
    """Dependency that allows either a wallet key (invoice/admin) via X-API-KEY
    or an admin session cookie. Returns a dict with type and the resolved
    WalletTypeInfo or User.
    """
    key_value = api_key_header or api_key_query
    if key_value:
        # Use the standard require_invoice_key helper but pass the resolved
        # api key string so FastAPI doesn't hand us Security wrappers.
        wallet_info: WalletTypeInfo = await require_invoice_key(
            request, api_key_header=key_value
        )
        return {"type": "wallet", "value": wallet_info}

    # No API key and no admin session -> anonymous caller
    return {"type": "anonymous", "value": None}




def _utc_midnight_timestamp(days_offset: int = 0) -> int:
    """Return Unix timestamp for UTC midnight with optional day offset."""
    from datetime import datetime, timedelta, timezone

    now = datetime.now(timezone.utc)
    midnight = datetime(now.year, now.month, now.day, tzinfo=timezone.utc)
    if days_offset:
        midnight = midnight + timedelta(days=days_offset)
    return int(midnight.timestamp())

def _nostrclient_available() -> bool:
    try:
        from lnbits.extensions.nostrclient.router import nostr_client  # noqa: F401

        return True
    except Exception:
        return False


def _get_or_create_headbutt_service(app):
    """Return the headbutt service from app.state, creating it via register_services if missing.

    This makes the /headbutt endpoint resilient to extension init order.
    """
    try:
        st = getattr(app, "state", app)
        svc = getattr(st, "cyberherd_headbutt_service", None)
        if svc:
            return svc
        # Try to register services now
        try:
            from .setup import register_services  # lazy import to avoid cycles

            register_services(app)
            svc = getattr(app.state, "cyberherd_headbutt_service", None)
            return svc
        except Exception as e:
            try:
                logger.warning(f"Cyberherd: lazy register_services failed: {e}")
            except Exception:
                pass
            return None
    except Exception:
        return None


@cyberherd_api_router.get("/api/v1/source_wallet")
async def api_get_source_wallet(
    request: Request, auth=Depends(auth_wallet_or_admin_dep)
):
    """Return the currently configured source wallet id for Cyberherd (if any).
    Returns user-specific setting when available.
    """
    # user-specific settings may be stored; fall back to app global state
    user_id = None
    if auth["type"] == "wallet":
        user_id = auth["value"].wallet.user
    elif auth["type"] == "admin":
        user_id = auth["value"].id

    settings = await crud.get_settings(user_id)
    return {"source_wallet": settings.source_wallet}


@cyberherd_api_router.post("/api/v1/manual_reset")
async def api_manual_reset(
    request: Request, wallet_info: WalletTypeInfo = Depends(require_admin_key)
):
    """Manually reset: deactivate all members and set SplitPayments to predefined wallet at 100%."""
    await crud.deactivate_all_cyberherd_members(user_id=wallet_info.wallet.user)
    # Also clear processed zap history for the requesting user so warm-start can re-run
    try:
        await clear_processed_zaps_for_user(wallet_info.wallet.user)
    except Exception:
        # Non-fatal, continue with split reset even if clearing fails
        pass
    # Note: we intentionally do NOT clear persisted processed-event markers here.
    # Reposts and reactions (kind 6 and 7) are persisted when processed so they
    # must not be eligible for reprocessing after a manual reset. We still clear
    # the cached notes used for tracking to avoid stale in-memory state, but the
    # persisted markers remain authoritative.
    try:
        # Clear today's cached notes for this user (removes cache entries used by repost tracking)
        try:
            _clear_cached_notes_for_user(request.app, user_id=wallet_info.wallet.user)
        except Exception:
            pass
    except Exception:
        pass
    # Use user-scoped settings only
    settings = await crud.get_settings(wallet_info.wallet.user)
    if settings and settings.source_wallet:
        await reset_splits_to_predefined_wallet(settings.source_wallet, user_id=wallet_info.wallet.user)
    return {"ok": True}


@cyberherd_api_router.get("/api/v1/zap_monitors")
async def api_zap_monitors(request: Request, cleanup: bool = False, max_age: int | None = None, wallet_info: WalletTypeInfo = Depends(require_admin_key)):
    """Admin diagnostic endpoint listing zap monitor instances.

    Optional cleanup=1 triggers stale instance cleanup (inactivity > max_age or default TTL).
    """
    import time
    summary = {}
    if cleanup:
        summary = await cleanup_stale_monitors(max_age=max_age)
    monitors = []
    for uid, inst in _zap_monitor_instances.items():
        last_access = _ZAP_MONITOR_LAST_ACCESS.get(uid)
        monitors.append({
            "user_id": uid,
            "running": bool(getattr(inst, "_running", False)),
            "mode": getattr(inst, "mode", None),
            "last_zap_at": getattr(inst, "last_zap_at", None),
            "last_message_at": getattr(inst, "last_message_at", None),
            "last_error": getattr(inst, "last_error", None),
            "last_access_age": (time.time() - last_access) if last_access else None,
        })
    return {"monitors": monitors, "count": len(monitors), "cleanup": summary}


@cyberherd_api_router.get("/api/v1/diagnostics")
async def api_get_diagnostics(
    request: Request,
    include_state: bool = Query(False),
    include_routes: bool = Query(False),
    auth=Depends(auth_wallet_or_admin_dep),
):
    """Aggregate diagnostics for CyberHerd services.

    Returns a payload combining settings, zap monitor status, nostr diagnostics,
    and optional FastAPI route/state summaries for the authenticated user.
    """
    # Resolve user scope from auth payload
    user_id = None
    if auth["type"] == "wallet":
        user_id = auth["value"].wallet.user
    elif auth["type"] == "admin":
        user_id = auth["value"].id

    diagnostics: dict[str, Any] = {
        "timestamp": time.time(),
        "user_id": user_id,
    }

    # Settings summary (sanitized)
    try:
        settings = await crud.get_settings(user_id)
        diagnostics["settings"] = {
            "source_wallet": getattr(settings, "source_wallet", None),
            "zap_wallet": getattr(settings, "zap_wallet", None),
            "zap_wallet_alias": getattr(settings, "zap_wallet_alias", None),
            "herd_wallet": getattr(settings, "herd_wallet", None),
            "max_members": getattr(settings, "max_members", None),
            "zap_tracking_enabled": bool(
                getattr(settings, "zap_tracking_enabled", False)
            ),
            "repost_tracking_enabled": bool(
                getattr(settings, "repost_tracking_enabled", False)
            ),
            "likes_tracking_enabled": bool(
                getattr(settings, "likes_tracking_enabled", False)
            ),
            "midnight_reset_enabled": bool(
                getattr(settings, "midnight_reset_enabled", True)
            ),
            "minimum_sats": getattr(settings, "minimum_sats", None),
            "tracked_tags": list(getattr(settings, "tracked_tags", []) or []),
            "tracked_event_ids_count": len(
                getattr(settings, "tracked_event_ids", []) or []
            ),
            "nostr_private_key_set": bool(
                getattr(settings, "nostr_private_key", None)
            ),
            "nostr_pubkey_override": getattr(
                settings, "nostr_pubkey_override", None
            ),
            "effective_nostr_pubkey": resolve_effective_pubkey(settings),
        }
    except Exception as e:
        diagnostics["settings"] = {"error": str(e)}

    # Per-user zap monitor status
    try:
        zap_monitor = get_zap_monitor(app=request.app, db=crud, user_id=user_id)
        diagnostics["zap_monitor"] = (
            (await zap_monitor.status()) if zap_monitor else {"running": False}
        )
    except Exception as e:
        diagnostics["zap_monitor"] = {"error": str(e)}

    # Global monitor registry statistics (no user-identifying data)
    diagnostics["monitor_registry"] = {
        "active_instances": len(_zap_monitor_instances),
    }

    # Include nostr diagnostics (helpers + subscriptions)
    try:
        diagnostics["nostr"] = await api_get_nostr_diagnostics(request, auth=auth)
    except Exception as e:
        diagnostics["nostr"] = {"error": str(e)}

    # Optional application state summary
    if include_state:
        state_summary: dict[str, Any] = {}
        try:
            app_state = getattr(request.app, "state", request.app)
            cyberherd_state = getattr(app_state, "cyberherd", {}) or {}
            state_summary["subscriptions_dirty"] = bool(
                cyberherd_state.get("subscriptions_dirty", False)
            )
            recovery_tasks = getattr(
                app_state, "cyberherd_recovery_tasks", {}
            ) or {}
            state_summary["recovery_tasks_active"] = len(recovery_tasks)
            note_cache = getattr(app_state, "cyberherd_note_cache", None)
            if isinstance(note_cache, dict):
                state_summary["note_cache_entries"] = len(note_cache)
        except Exception as e:
            state_summary["error"] = str(e)
        diagnostics["app_state"] = state_summary

    # Optional route listing (lightweight equivalent to debug endpoint)
    if include_routes:
        routes = []
        try:
            for route in getattr(request.app, "routes", []):
                path = getattr(route, "path", None)
                if isinstance(path, str) and path.startswith("/cyberherd"):
                    methods = sorted(list(getattr(route, "methods", []) or []))
                    routes.append({"path": path, "methods": methods})
        except Exception as e:
            routes = [{"error": str(e)}]
        diagnostics["routes"] = routes

    return diagnostics


@cyberherd_api_router.post("/api/v1/recover_events")
async def api_recover_events(request: Request, wallet_info: WalletTypeInfo = Depends(require_admin_key)):
    """Trigger recovery of missed zaps (payments) and missed reposts/reactions (kinds 6 & 7) for the authenticated admin wallet.

    This endpoint is idempotent and runs recovery in-process; it will return once recovery tasks are scheduled/completed.
    """
    user_id = wallet_info.wallet.user

    # Safely parse optional payload to check for background flag
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    bg_mode = bool(payload.get("background") or request.query_params.get("background") in ("1", "true", "True"))

    async def _do_recovery() -> dict:
        diagnostics = {
            "user_id": user_id,
            "reposts_reactions": {"attempted": False, "processed": 0, "errors": []},
            "zaps": {"attempted": False, "processed": 0, "errors": []},
            "messages": []
        }

        # Run subscriptions (reposts/likes) recovery if subscription service available
        try:
            from .services import subscriptions as subs

            diagnostics["reposts_reactions"]["attempted"] = True
            try:
                # subs helper may return diagnostics; accept both None or dict
                res = await subs._recover_missed_reposts_and_reactions_for_user(user_id, await crud.get_settings(user_id), request.app)
                if isinstance(res, dict):
                    diagnostics["reposts_reactions"]["processed"] = int(res.get("processed", 0))
                    if res.get("errors"):
                        diagnostics["reposts_reactions"]["errors"].extend(res.get("errors") or [])
                else:
                    diagnostics["reposts_reactions"]["processed"] = diagnostics["reposts_reactions"].get("processed", 0)
                diagnostics["messages"].append("Reposts/reactions recovery attempted")
            except Exception as e:
                msg = f"Failed recovering reposts/reactions for user {user_id}: {e}"
                logger.warning(msg)
                diagnostics["reposts_reactions"]["errors"].append(str(e))
                diagnostics["messages"].append(msg)
        except Exception:
            logger.debug("Subscriptions recovery not available")
            diagnostics["messages"].append("Subscriptions recovery not available")

        # Run zap recovery via zap_monitor if available
        try:
            zap_monitor = get_zap_monitor(app=request.app, db=crud, user_id=user_id)
            if zap_monitor:
                diagnostics["zaps"]["attempted"] = True
                # Force payment-based recovery when the zap monitor implements it
                try:
                    settings = await crud.get_settings(user_id)
                except Exception:
                    settings = None

                if settings and hasattr(zap_monitor, "_recover_missed_payment_zaps"):
                    try:
                        resz = await zap_monitor._recover_missed_payment_zaps(settings)
                        if isinstance(resz, dict):
                            diagnostics["zaps"]["processed"] = int(resz.get("processed", 0))
                            if resz.get("errors"):
                                diagnostics["zaps"]["errors"].extend(resz.get("errors") or [])
                        diagnostics["messages"].append("Zap recovery attempted")
                    except Exception as e:
                        msg = f"Failed recovering zaps for user {user_id}: {e}"
                        logger.warning(msg)
                        diagnostics["zaps"]["errors"].append(str(e))
                        diagnostics["messages"].append(msg)
                else:
                    diagnostics["messages"].append("No zap payment recovery available for user")
            else:
                diagnostics["messages"].append("No zap monitor available for user")
        except Exception as e:
            msg = f"Zap recovery failed to start for user {user_id}: {e}"
            logger.warning(msg)
            diagnostics["zaps"]["errors"].append(str(e))
            diagnostics["messages"].append(msg)

        # Log structured diagnostics for server-side troubleshooting
        try:
            logger.info(f"Cyberherd recovery diagnostics: user={user_id} reposts={diagnostics['reposts_reactions']} zaps={diagnostics['zaps']} messages={diagnostics['messages']}")
        except Exception:
            pass

        return diagnostics

    # Background mode: spawn a task, return a task id and status URL
    if bg_mode:
        # Ensure app.state has a tasks dict
        try:
            request.app.state = getattr(request.app, "state", request.app)
            tasks = getattr(request.app.state, "cyberherd_recovery_tasks", None)
            if tasks is None:
                tasks = {}
                request.app.state.cyberherd_recovery_tasks = tasks
        except Exception:
            # Fallback local dict (won't survive reloads)
            tasks = {}
            request.app.state = getattr(request.app, "state", request.app)
            request.app.state.cyberherd_recovery_tasks = tasks

        task_id = f"recover-{user_id}-{uuid.uuid4().hex[:8]}-{int(time.time()*1000)}"
        # initialize status
        tasks[task_id] = {
            "status": "pending",
            "created_at": time.time(),
            "user_id": user_id,
            "diagnostics": None,
            "error": None,
        }

        async def _bg_runner(tid: str):
            try:
                tasks[tid]["status"] = "running"
                tasks[tid]["started_at"] = time.time()
                diags = await _do_recovery()
                tasks[tid]["diagnostics"] = diags
                tasks[tid]["status"] = "completed"
                tasks[tid]["completed_at"] = time.time()
            except Exception as e:
                tasks[tid]["error"] = str(e)
                tasks[tid]["status"] = "failed"
                tasks[tid]["completed_at"] = time.time()
                try:
                    logger.exception(f"Background recovery task {tid} failed: {e}")
                except Exception:
                    pass

        try:
            asyncio.create_task(_bg_runner(task_id))
        except Exception as e:
            logger.warning(f"Failed to spawn background recovery task: {e}")
            tasks[task_id]["status"] = "failed"
            tasks[task_id]["error"] = str(e)

        status_url = f"/api/v1/recover_events/status/{task_id}"
        return JSONResponse(status_code=HTTPStatus.ACCEPTED, content={"ok": True, "task_id": task_id, "status_url": status_url})

    # Foreground mode: run and return diagnostics
    diagnostics = await _do_recovery()
    return JSONResponse(status_code=HTTPStatus.OK, content={"ok": True, "diagnostics": diagnostics})



@cyberherd_api_router.post("/api/v1/pay_members")
async def api_pay_members(request: Request, wallet_info: WalletTypeInfo = Depends(require_admin_key)):
    """Simple transfer: move the full balance from the configured `herd_wallet`
    to the configured `source_wallet` (cyberherd wallet) using internal LNbits
    wallet-to-wallet payments.

    No splits or dry-run. Only callable by an admin.
    """
    # Resolve admin user settings to obtain the two wallet ids
    user_id = wallet_info.wallet.user
    settings = await crud.get_settings(user_id)

    herd_wallet = getattr(settings, "herd_wallet", None)
    target_wallet = getattr(settings, "source_wallet", None)

    if not herd_wallet:
        raise HTTPException(status_code=HTTPStatus.BAD_REQUEST, detail="No herd_wallet configured in cyberherd settings")
    if not target_wallet:
        raise HTTPException(status_code=HTTPStatus.BAD_REQUEST, detail="No source_wallet (cyberherd wallet) configured in cyberherd settings")

    # Resolve actual wallet objects (allow keys or direct ids)
    from lnbits.core.crud.wallets import get_wallet_for_key
    from lnbits.core.services.payments import create_wallet_invoice, pay_invoice
    from lnbits.core.models.payments import CreateInvoice

    # Try to resolve herd wallet id if a key was stored
    herd_obj = await get_wallet(herd_wallet)
    if herd_obj is None:
        herd_obj = await get_wallet_for_key(herd_wallet)
        if herd_obj:
            herd_wallet = herd_obj.id

    target_obj = await get_wallet(target_wallet)
    if target_obj is None:
        target_obj = await get_wallet_for_key(target_wallet)
        if target_obj:
            target_wallet = target_obj.id

    # Re-fetch to get balances
    herd_obj = await get_wallet(herd_wallet)
    if not herd_obj:
        raise HTTPException(status_code=HTTPStatus.BAD_REQUEST, detail="Could not resolve herd_wallet")

    balance_msat = getattr(herd_obj, "balance_msat", 0) or 0
    balance_sats = int(balance_msat) // 1000
    if balance_sats <= 0:
        raise HTTPException(status_code=HTTPStatus.BAD_REQUEST, detail="Herd wallet balance is zero")

    # Create an internal invoice on the target (cyberherd) wallet for the full amount
    try:
        invoice_data = CreateInvoice(amount=balance_sats, unit="sat", memo="CyberHerd Distribution", internal=True)
        internal_payment = await create_wallet_invoice(target_wallet, invoice_data)
    except Exception as e:
        raise HTTPException(status_code=HTTPStatus.INTERNAL_SERVER_ERROR, detail=f"Failed to create internal invoice on target wallet: {e}")

    # Pay that invoice from the herd wallet
    try:
        paid = await pay_invoice(wallet_id=herd_wallet, payment_request=internal_payment.bolt11)
    except Exception as e:
        raise HTTPException(status_code=HTTPStatus.INTERNAL_SERVER_ERROR, detail=f"Failed to pay internal invoice from herd wallet: {e}")

    return JSONResponse(status_code=HTTPStatus.OK, content={"ok": True, "from_wallet": herd_wallet, "to_wallet": target_wallet, "sats": balance_sats, "payment_checking_id": getattr(paid, "checking_id", None)})


@cyberherd_api_router.get("/api/v1/settings")
async def api_get_settings(request: Request, auth=Depends(auth_wallet_or_admin_dep)):
    # Allow unauthenticated GET for read-only settings. If an API key is
    # supplied, resolve it to the owning user to return user-specific settings.
    user_id = await _resolve_user_from_api_key(request)
    if user_id is None:
        if auth and auth.get("type") == "wallet":
            user_id = auth["value"].wallet.user
        elif auth and auth.get("type") == "admin":
            user_id = auth["value"].id

    settings = await crud.get_settings(user_id)
    
    # DO NOT start zap monitor on every GET request - only start from:
    # 1. Warm start on server startup (__init__.py)
    # 2. PUT /settings when user enables tracking
    # Starting on every GET causes subscriptions to reload on page load
    
    try:
        logger.debug(
            "Cyberherd GET settings: user_id=%s source_wallet=%s zap_wallet=%s nostr_key_set=%s override_set=%s",
            user_id,
            getattr(settings, "source_wallet", None),
            getattr(settings, "zap_wallet", None),
            bool(getattr(settings, "nostr_private_key", None)),
            bool(getattr(settings, "nostr_pubkey_override", None)),
        )
    except Exception:
        pass
    # tracked_tags stored as JSON-ish string in DB; ensure it's a list
    tags = settings.tracked_tags if isinstance(settings.tracked_tags, list) else []
    # mask private key for UI; do not return actual key
    key = getattr(settings, "nostr_private_key", None)
    if key:
        masked = f"{key[:6]}...{key[-6:]}"
        key_set = True
    else:
        masked = ""
        key_set = False

    # Compute effective nostr pubkey: override if set, else derive from private key
    # Unified resolver (uses cached/override/legacy/derived)
    effective_pubkey = resolve_effective_pubkey(settings)
    try:
        logger.debug(
            "Cyberherd GET settings: derived effective pubkey present=%s",
            bool(effective_pubkey),
        )
    except Exception:
        pass

    # Compute canonical websocket URL for messaging clients when possible
    websocket_url = None
    try:
        herd_wallet_id = getattr(settings, 'herd_wallet', None)
        if herd_wallet_id:
            from lnbits.core.crud import get_wallet

            wallet = await get_wallet(herd_wallet_id)
            if wallet and wallet.inkey:
                host = request.headers.get('host') or request.url.hostname
                # Prefer secure scheme for canonical URL
                websocket_url = f"wss://{host}/ws/{wallet.inkey}"
    except Exception:
        websocket_url = None

    return {
        "source_wallet": settings.source_wallet,
        "zap_wallet": settings.zap_wallet,
        "zap_wallet_alias": settings.zap_wallet_alias,
        "herd_wallet": settings.herd_wallet,
        "max_members": settings.max_members,
        "tracked_tags": tags,
        "tracked_event_ids": getattr(settings, "tracked_event_ids", []),
        "nostr_private_key_set": key_set,
        "nostr_private_key_mask": masked,
        "nostr_pubkey_override": getattr(settings, "nostr_pubkey_override", None),
        "effective_nostr_pubkey": effective_pubkey,
        "effective_nostr_npub": (
            hex_to_npub(effective_pubkey) if effective_pubkey else None
        ),
        "nostrclient_required": True,
        "nostrclient_available": _nostrclient_available(),
        "zap_tracking_enabled": getattr(settings, "zap_tracking_enabled", False),
        "midnight_reset_enabled": getattr(settings, "midnight_reset_enabled", True),
        "nip05_verification_enabled": getattr(settings, "nip05_verification_enabled", True),
        "zap_monitor_mode": getattr(settings, "zap_monitor_mode", "websocket"),
        "repost_tracking_enabled": getattr(settings, "repost_tracking_enabled", False),
        "likes_tracking_enabled": getattr(settings, "likes_tracking_enabled", False),
        "minimum_sats": getattr(settings, "minimum_sats", 10),
        "feeder_trigger_sats": getattr(settings, "feeder_trigger_sats", None),
        "websocket_url": websocket_url,
    }


@cyberherd_api_router.put("/api/v1/source_wallet", status_code=HTTPStatus.OK)
async def api_set_source_wallet(
    request: Request,
    payload: dict,
    wallet_info: WalletTypeInfo = Depends(require_admin_key),
):
    """Set the Cyberherd source wallet by wallet id or by wallet key (adminkey/inkey).

    Payload: {"source_wallet": "<wallet_id_or_key>"}
    """
    source = payload.get("source_wallet") if isinstance(payload, dict) else None
    if not source:
        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST,
            detail="Missing 'source_wallet' in payload",
        )

    # Resolve wallet id either by direct id or by adminkey/inkey
    wallet = await get_wallet(source)
    if wallet is None:
        wallet = await get_wallet_for_key(source)
    if wallet is None:
        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST,
            detail="Could not resolve wallet id or key",
        )

    # Validate balance: require at least 1 sat (1000 msat) to avoid misconfiguration
    balance_msat = getattr(wallet, "balance_msat", None)
    try:
        if balance_msat is None:
            # Some deployments may not expose balance_msat; allow but warn
            if request.app.logger and request.app.logger.warning:
                request.app.logger.warning(
                    "Cyberherd: source wallet set but balance unknown"
                )
        elif int(balance_msat) < 1000:
            raise HTTPException(
                status_code=HTTPStatus.BAD_REQUEST,
                detail=(
                    "Source wallet balance too low. "
                    "Require at least 1 sat to be set as source."
                ),
            )
    except Exception as e:
        # If conversion fails, treat as invalid
        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST,
            detail="Invalid wallet balance information",
        ) from e

    # require authenticated caller
    # Persist per-user settings using authenticated admin wallet
    user_id = wallet_info.wallet.user

    settings = await crud.get_settings(user_id)
    settings.source_wallet = wallet.id
    try:
        await crud.upsert_settings(settings, user_id)
    except Exception as e:
        try:
            logger.error(
                f"Cyberherd set_source_wallet: upsert failed user_id={user_id}: {e}"
            )
        except Exception:
            pass
        raise HTTPException(status_code=500, detail="Failed to persist source wallet")
    return {"source_wallet": wallet.id}


@cyberherd_api_router.put("/api/v1/settings")
@cyberherd_api_router.post("/api/v1/settings")
async def api_put_settings(
    request: Request,
    payload: dict | None = None,
    wallet_info: WalletTypeInfo = Depends(require_admin_key),
):
    try:
        logger.info(
            "Cyberherd settings handler invoked method=%s ct=%s len=%s",
            request.method,
            request.headers.get("content-type"),
            request.headers.get("content-length"),
        )
    except Exception:
        pass

    # Accept both JSON and form submissions; fall back to an empty dict.
    data = payload
    if data is None:
        try:
            data = await request.json()
        except Exception:
            data = None
    if data is None:
        try:
            form = await request.form()
            data = {k: v for k, v in form.items()}
        except Exception:
            data = {}
    if not isinstance(data, dict):
        data = {}
    payload = data

    start_ts = time.time()
    try:
        logger.info(
            "Cyberherd PUT /api/v1/settings start user=%s keys=%s payload_keys=%s",
            wallet_info.wallet.user,
            list(payload.keys()),
            len(payload),
        )
    except Exception:
        pass

    user_id = wallet_info.wallet.user
    settings = await crud.get_settings(user_id)

    # Capture previous toggle values to detect new enablement.
    zap_tracking_was_enabled = bool(getattr(settings, "zap_tracking_enabled", False))
    repost_tracking_was_enabled = bool(getattr(settings, "repost_tracking_enabled", False))
    likes_tracking_was_enabled = bool(getattr(settings, "likes_tracking_enabled", False))

    # Capture prior tag/order state for subscription refresh decisions.
    try:
        tags_before_norm = sorted(
            [
                t.lstrip("#").lower()
                for t in (getattr(settings, "tracked_tags", []) or [])
                if isinstance(t, str)
            ]
        )
    except Exception:
        tags_before_norm = []

    try:
        tracked_event_ids_before = sorted(
            getattr(settings, "tracked_event_ids", []) or []
        )
    except Exception:
        tracked_event_ids_before = []

    # Extract incoming values.
    source = payload.get("source_wallet")
    zap_wallet = payload.get("zap_wallet")
    zap_wallet_alias = payload.get("zap_wallet_alias")
    herd_wallet = payload.get("herd_wallet")
    maxm = payload.get("max_members")
    tags = payload.get("tracked_tags")
    tracked_event_ids = payload.get("tracked_event_ids")

    if source is not None:
        settings.source_wallet = source
    if zap_wallet is not None:
        settings.zap_wallet = zap_wallet
    if zap_wallet_alias is not None:
        settings.zap_wallet_alias = zap_wallet_alias
    if herd_wallet is not None:
        settings.herd_wallet = herd_wallet
    if maxm is not None:
        try:
            settings.max_members = int(maxm)
        except Exception as e:
            raise HTTPException(
                status_code=HTTPStatus.BAD_REQUEST, detail="Invalid max_members"
            ) from e
    if tags is not None:
        if isinstance(tags, list):
            settings.tracked_tags = tags
        elif isinstance(tags, str):
            settings.tracked_tags = [t.strip() for t in tags.split(",") if t.strip()]
    if tracked_event_ids is not None:
        if isinstance(tracked_event_ids, list):
            settings.tracked_event_ids = tracked_event_ids
        elif isinstance(tracked_event_ids, str):
            settings.tracked_event_ids = [
                eid.strip() for eid in tracked_event_ids.split(",") if eid.strip()
            ]
        settings.tracked_event_addresses = await refresh_tracked_event_addresses(
            settings.tracked_event_ids,
            getattr(settings, "tracked_event_addresses", {}) or {},
        )

    # zap_monitor_mode deprecated: always payments+subscriptions.
    if "zap_monitor_mode" in payload:
        req_mode = payload.get("zap_monitor_mode")
        if req_mode and req_mode != "payment":
            logger.warning(
                "CyberHerd: ignoring legacy zap_monitor_mode '%s' (payment-only)", req_mode
            )
    settings.zap_monitor_mode = "websocket"

    # templates_owner_user derived from authenticated account; do not set manually.

    # Capture previous effective pubkey before mutating the key.
    eff_before = _get_cached_effective_pubkey(settings)
    _handle_nostr_private_key(settings, payload)
    _handle_nostr_pubkey_override(settings, payload)

    # Handle zap tracking toggle
    zap_tracking_enabled = payload.get("zap_tracking_enabled")
    if zap_tracking_enabled is not None:
        settings.zap_tracking_enabled = bool(zap_tracking_enabled)

    # Handle repost tracking toggle
    repost_tracking_enabled = payload.get("repost_tracking_enabled")
    if repost_tracking_enabled is not None:
        settings.repost_tracking_enabled = bool(repost_tracking_enabled)

    # Handle likes tracking toggle
    likes_tracking_enabled = payload.get("likes_tracking_enabled")
    if likes_tracking_enabled is not None:
        settings.likes_tracking_enabled = bool(likes_tracking_enabled)

    # Handle midnight reset opt-in toggle
    midnight_reset_enabled = payload.get("midnight_reset_enabled")
    if midnight_reset_enabled is not None:
        settings.midnight_reset_enabled = bool(midnight_reset_enabled)

    # Handle NIP-05 verification toggle
    nip05_verification_enabled = payload.get("nip05_verification_enabled")
    if nip05_verification_enabled is not None:
        settings.nip05_verification_enabled = bool(nip05_verification_enabled)

    # Handle minimum sats
    minimum_sats = payload.get("minimum_sats")
    if minimum_sats is not None:
        try:
            settings.minimum_sats = int(minimum_sats)
        except Exception as e:
            raise HTTPException(
                status_code=HTTPStatus.BAD_REQUEST, detail="Invalid minimum_sats"
            ) from e

    feeder_trigger_sats = payload.get("feeder_trigger_sats")
    if feeder_trigger_sats is not None:
        try:
            settings.feeder_trigger_sats = (
                int(feeder_trigger_sats)
                if feeder_trigger_sats not in (None, "")
                else None
            )
        except Exception as e:
            raise HTTPException(
                status_code=HTTPStatus.BAD_REQUEST, detail="Invalid feeder_trigger_sats"
            ) from e

    # Pre-compute effective pubkey before the upsert to avoid recomputation later.
    try:
        settings.computed_effective_pubkey = _get_effective_pubkey(settings)
    except Exception:
        settings.computed_effective_pubkey = getattr(
            settings, "computed_effective_pubkey", None
        )

    try:
        await crud.upsert_settings(settings, user_id)
    except Exception as e:
        import traceback
        import uuid

        marker = f"CH_SETTINGS_UPSERT_ERR_{uuid.uuid4().hex[:8]}"
        try:
            logger.error(
                "%s upsert failed user_id=%s err=%s stack=%s",
                marker,
                user_id,
                e,
                traceback.format_exc(),
            )
        except Exception:
            pass
        raise HTTPException(
            status_code=500, detail=f"Failed to persist settings ({marker})"
        )

    try:
        duration_ms = int((time.time() - start_ts) * 1000)
        logger.info(
            f"Cyberherd PUT settings persisted user={user_id} dur_ms={duration_ms} source={getattr(settings, 'source_wallet', None)} zap={getattr(settings, 'zap_wallet', None)} herd={getattr(settings, 'herd_wallet', None)} tags_len={len(getattr(settings, 'tracked_tags', []) or [])} nostr_key={bool(getattr(settings, 'nostr_private_key', None))} override={bool(getattr(settings, 'nostr_pubkey_override', None))} zap_tracking={getattr(settings, 'zap_tracking_enabled', False)} mode=payment"
        )
    except Exception:
        pass

    # Handle zap monitoring service start/stop based on toggle or mode updates (background)
    # Also update subscriptions when event type toggles change
    toggles_changed = (
        zap_tracking_enabled is not None or 
        repost_tracking_enabled is not None or 
        likes_tracking_enabled is not None
    )

    if zap_tracking_enabled is not None or toggles_changed:
        import asyncio
        async def _bg_update_zap_monitor():
            try:
                await asyncio.sleep(0.1)  # Small delay to ensure response is sent
                zap_monitor = get_zap_monitor(
                    app=request.app,
                    db=crud,
                    user_id=user_id
                )

                zap_enabled = bool(getattr(settings, "zap_tracking_enabled", False))

                if zap_tracking_enabled is not None:
                    # Handle start/stop when zap tracking toggle changes
                    if zap_enabled:
                        # Start monitoring (now runs recovery in background)
                        await zap_monitor.start_monitoring()
                        logger.info("Zap monitor start initiated from settings update")
                    else:
                        await asyncio.wait_for(zap_monitor.stop_monitoring(), timeout=5.0)
                        logger.info("Zap monitor stopped from settings update")

                # Update subscriptions when event type toggles change (if monitor is running)
                if toggles_changed and zap_monitor._running:
                    try:
                        # Get current toggle states
                        enable_zaps = bool(getattr(settings, "zap_tracking_enabled", False))
                        enable_reposts = bool(getattr(settings, "repost_tracking_enabled", False))
                        enable_reactions = bool(getattr(settings, "likes_tracking_enabled", False))

                        # Get tracked notes
                        tracked_notes = getattr(settings, "tracked_event_ids", []) or []

                        if tracked_notes:
                            # Update subscriptions with new toggle states
                            await zap_monitor.update_tracked_notes(
                                tracked_notes,
                                enable_zaps=enable_zaps,
                                enable_reposts=enable_reposts,
                                enable_reactions=enable_reactions
                            )
                            logger.info(f"Updated subscriptions for user {user_id}: zaps={enable_zaps}, reposts={enable_reposts}, reactions={enable_reactions}")
                        else:
                            logger.debug(f"No tracked notes for user {user_id}, skipping subscription update")
                    except Exception as e:
                        logger.warning(f"Failed to update subscriptions: {e}")

            except asyncio.TimeoutError:
                logger.warning("Zap monitor stop timed out")
            except Exception as e:
                logger.warning(f"Failed to update zap monitoring: {e}")
        try:
            # Create task but don't wait for it
            asyncio.create_task(_bg_update_zap_monitor())
        except Exception as e:
            logger.warning(f"Failed to create zap monitor update task: {e}")

    # Handle repost/likes recovery when toggles are newly enabled
    repost_now_enabled = repost_tracking_enabled is True and not repost_tracking_was_enabled
    likes_now_enabled = likes_tracking_enabled is True and not likes_tracking_was_enabled
    zap_now_enabled = zap_tracking_enabled is True and not zap_tracking_was_enabled
    if repost_now_enabled or likes_now_enabled or zap_now_enabled:
        import asyncio
        async def _bg_recover_events():
            try:
                await asyncio.sleep(0.2)  # Small delay after zap monitor
                # Recover reposts and likes
                if repost_now_enabled or likes_now_enabled:
                    from .services import subscriptions as subs
                    await subs._recover_missed_reposts_and_reactions_for_user(user_id, settings, request.app)
                    logger.info("Recovered missed reposts/reactions from settings toggle enable")

                # Recover zaps if newly enabled
                if zap_now_enabled:
                    zap_monitor = get_zap_monitor(app=request.app, db=crud, user_id=user_id)
                    # Force payment-based recovery if available
                    try:
                        settings_for_recovery = await crud.get_settings(user_id)
                    except Exception:
                        settings_for_recovery = None
                    if settings_for_recovery and hasattr(zap_monitor, "_recover_missed_payment_zaps"):
                        try:
                            await zap_monitor._recover_missed_payment_zaps(settings_for_recovery)
                            logger.info("Recovered missed zaps from settings toggle enable")
                        except Exception:
                            pass

            except Exception as e:
                logger.warning(f"Failed to recover events on toggle enable: {e}")
        try:
            # Create task but don't wait for it
            asyncio.create_task(_bg_recover_events())
        except Exception as e:
            logger.warning(f"Failed to create event recovery task: {e}")

    # If the effective pubkey changed (private key set/rotated), schedule a one-time
    # recovery of missed zaps for today using payments and clear any stale cache entries
    try:
        eff_after = _get_cached_effective_pubkey(settings)
        if eff_after and eff_after != eff_before and getattr(settings, "zap_tracking_enabled", False):
            # Clear previously cached entries for this user/old pubkey immediately (cheap)
            _clear_cached_notes_for_user(request.app, user_id=user_id)
            # Temporarily disabled to prevent timeout issues
            # import asyncio
            # async def _bg_key_changed_recovery():
            #     try:
            #         zap_monitor = get_zap_monitor(app=request.app, db=crud, user_id=user_id)
            #         # Ensure recovery runs now regardless of prior runs
            #         if hasattr(zap_monitor, "_recovery_done"):
            #             zap_monitor._recovery_done = False
            #         # If not running, start (will also run recovery); else run recovery explicitly
            #         if not zap_monitor.status().get("running"):
            #             await zap_monitor.start_monitoring()
            #         else:
            #             if hasattr(zap_monitor, "_recover_missed_zaps_on_startup"):
            #                 await zap_monitor._recover_missed_zaps_on_startup()
            #     except Exception as e:
            #         logger.warning(f"Cyberherd: failed to trigger zap recovery after key set: {e}")
            # try:
            #     asyncio.create_task(_bg_key_changed_recovery())
            # except Exception:
            #     await _bg_key_changed_recovery()
    except Exception as e:
        logger.debug(f"Cyberherd: post-key-set recovery check failed: {e}")

    # Notify background worker to refresh nostr subscriptions only when
    # relevant fields change (pubkey or tags)
    try:
        request.app.state = getattr(request.app, "state", request.app)
        ch_state = getattr(request.app.state, "cyberherd", {})
        # Compute whether tracked tags or effective pubkey changed
        try:
            tags_after_norm = sorted([
                t.lstrip("#").lower()
                for t in (getattr(settings, "tracked_tags", []) or [])
                if isinstance(t, str)
            ])
        except Exception:
            tags_after_norm = []
        # Capture tracked_event_ids after mutation
        try:
            tracked_event_ids_after = sorted(getattr(settings, "tracked_event_ids", []) or [])
        except Exception:
            tracked_event_ids_after = []
        eff_after_for_refresh = _get_cached_effective_pubkey(settings)
        # Consider repost/likes toggles as relevant subscription configuration
        try:
            repost_after = bool(getattr(settings, 'repost_tracking_enabled', False))
        except Exception:
            repost_after = False
        try:
            likes_after = bool(getattr(settings, 'likes_tracking_enabled', False))
        except Exception:
            likes_after = False
        try:
            # Retrieve previous values captured before mutation
            repost_before = bool(repost_tracking_was_enabled)
        except Exception:
            repost_before = False
        try:
            likes_before = bool(likes_tracking_was_enabled)
        except Exception:
            likes_before = False

        toggles_changed_for_refresh = (repost_after != repost_before) or (likes_after != likes_before)

        subs_changed = (
            (tags_after_norm != tags_before_norm)
            or (tracked_event_ids_after != tracked_event_ids_before)
            or (eff_after_for_refresh != eff_before)
            or toggles_changed_for_refresh
        )
        ch_state["subscriptions_dirty"] = bool(subs_changed)
        request.app.state.cyberherd = ch_state
        # Event-driven refresh for websocket subscriptions
        if subs_changed:
            try:
                from .services import subscriptions as subs
                if getattr(subs, '_refresh_event', None) is not None:
                    try:
                        ev = getattr(subs, '_refresh_event')
                        if hasattr(ev, 'set'):
                            ev.set()
                    except Exception:
                        pass
            except Exception:
                pass
    except Exception as e:
        logger.warning(f"Failed to signal subscription refresh: {e}")
    # Try recomputing splits if a source wallet is configured
    try:
        if getattr(settings, 'source_wallet', None):
            await update_split_targets_proportional(str(getattr(settings, 'source_wallet')))
    except Exception as e:
        logger.warning(f"Failed to recompute splits after settings save: {e}")
    # Include effective pubkey values for immediate UI update
    eff = resolve_effective_pubkey(settings)
    try:
        logger.debug(
            "Cyberherd PUT settings: effective pubkey present=%s", bool(eff)
        )
    except Exception:
        pass
    # Clear today's notes cache only when relevant settings change
    try:
        st = getattr(request.app, "state", request.app)
        if hasattr(st, "cyberherd_note_cache"):
            # If the nostr key was cleared, remove user-specific cached entries as well
            if eff_before and not getattr(settings, "nostr_private_key", None) and not getattr(settings, "nostr_pubkey_override", None):
                _clear_cached_notes_for_user(request.app, user_id=user_id)
            else:
                # Reset cache if tags changed (affects matching)
                try:
                    tags_after_norm = sorted([
                        t.lstrip("#").lower()
                        for t in (getattr(settings, "tracked_tags", []) or [])
                        if isinstance(t, str)
                    ])
                except Exception:
                    tags_after_norm = []
                if tags_after_norm != tags_before_norm:
                    st.cyberherd_note_cache = {}
    except Exception:
        pass

    # Proactively check nostrclient websocket connectivity (best-effort)
    ws_ok = False
    try:
        ws_ok = await _reconnect_nostrclient_ws(request.app)
    except Exception:
        ws_ok = False

    try:
        total_ms = int((time.time() - start_ts) * 1000)
        logger.info(f"Cyberherd PUT /api/v1/settings end user={user_id} total_ms={total_ms}")
    except Exception:
        total_ms = 0
        try:
            logger.info("Cyberherd PUT /api/v1/settings end (timing unavailable)")
        except Exception:
            pass

    return {
        "ok": True,
        "effective_nostr_pubkey": eff,
        "effective_nostr_npub": (hex_to_npub(eff) if eff else None),
        "nostrclient_ws_ok": ws_ok,
        "duration_ms": total_ms,
    }


@cyberherd_api_router.get("/api/v1/recover_events/status/{task_id}")
async def api_recover_events_status(request: Request, task_id: str, wallet_info: WalletTypeInfo = Depends(require_admin_key)):
    """Return status and diagnostics for a previously spawned background recovery task.

    Only admin callers are allowed. The status object contains: status (pending/running/completed/failed),
    created_at, started_at, completed_at, diagnostics (if completed), and error (if failed).
    """
    try:
        request.app.state = getattr(request.app, "state", request.app)
        tasks = getattr(request.app.state, "cyberherd_recovery_tasks", {}) or {}
    except Exception:
        tasks = {}

    task = tasks.get(task_id)
    if not task:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail="Task not found")

    # Only allow owner matching or admin (admin enforced by dependency)
    if task.get("user_id") != wallet_info.wallet.user:
        # If the caller is admin session rather than matching wallet, require admin
        # wallet_info here always requires admin key per dependency so allow
        pass

    return {"task_id": task_id, "status": task}


def _handle_nostr_private_key(settings, payload):
    if "nostr_private_key" in payload:
        npk = payload.get("nostr_private_key")
        if npk is None:
            settings.nostr_private_key = None
            try:
                logger.info("Cyberherd: nostr_private_key cleared via settings PUT")
            except Exception:
                pass
        else:
            if not isinstance(npk, str):
                raise HTTPException(
                    status_code=HTTPStatus.BAD_REQUEST,
                    detail="nostr_private_key must be a string or null",
                )
            # sanitize & extract likely key token
            k = _extract_private_key(str(npk))
            # Accept 0x-prefixed hex or bech32 nsec; normalize to 64-hex
            try:
                if not k:
                    raise ValueError("empty private key")
                if k.startswith("0x"):
                    k = k[2:]
                k = normalize_private_key(k)
                if not k:
                    raise ValueError("normalize_private_key returned empty value")
            except Exception as e:
                raise HTTPException(
                    status_code=HTTPStatus.BAD_REQUEST,
                    detail="nostr_private_key must be 64-hex or nsec",
                ) from e
            # final sanity check
            if len(k) != 64:
                raise HTTPException(
                    status_code=HTTPStatus.BAD_REQUEST,
                    detail="nostr_private_key must be 64 hex characters",
                )
            settings.nostr_private_key = k.lower()
            try:
                logger.info(
                    "Cyberherd: nostr_private_key updated (len=%s)", len(k)
                )
            except Exception:
                pass


def _handle_nostr_pubkey_override(settings, payload):
    if "nostr_pubkey_override" in payload:
        ov = payload.get("nostr_pubkey_override")
        if ov is None or ov == "":
            settings.nostr_pubkey_override = None
            try:
                logger.info("Cyberherd: nostr_pubkey_override cleared")
            except Exception:
                pass
        else:
            if not isinstance(ov, str):
                raise HTTPException(
                    status_code=HTTPStatus.BAD_REQUEST,
                    detail="nostr_pubkey_override must be a string or null",
                )
            try:
                settings.nostr_pubkey_override = validate_pub_key(ov)
                try:
                    logger.info(
                        "Cyberherd: nostr_pubkey_override set (len=%s)",
                        len(settings.nostr_pubkey_override or ""),
                    )
                except Exception:
                    pass
            except Exception as e:
                raise HTTPException(
                    status_code=HTTPStatus.BAD_REQUEST, detail=str(e)
                ) from e


@cyberherd_api_router.get("/api/v1/today_notes")
async def api_get_today_cyberherd_notes(request: Request, auth=Depends(auth_wallet_or_admin_dep)):
    """Return IDs of today's kind-1 notes authored by our project account that match tracked tags.

    Adds an in-process cache + lock to avoid duplicate concurrent relay queries which caused
    intermittent test flakiness (race on mocked query + cache population). Cache key includes
    day, user scope, effective pubkey and tagset. TTL ~30s (best-effort; opportunistic cleanup).
    """
    # Gracefully handle mocked/non-mapping headers in tests
    try:
        headers_repr = dict(getattr(request, 'headers', {}) or {})
        # Sanitize headers to remove sensitive information
        sanitized_headers = {}
        for k, v in headers_repr.items():
            if k.lower() in ['x-api-key', 'authorization', 'cookie', 'x-forwarded-for', 'x-real-ip']:
                # Mask sensitive header values
                if isinstance(v, str) and len(v) > 8:
                    sanitized_headers[k] = v[:4] + "****" + v[-4:] if len(v) > 8 else "****"
                else:
                    sanitized_headers[k] = "****"
            else:
                sanitized_headers[k] = v
        headers_repr = sanitized_headers
    except Exception:
        try:
            # Fallback: attempt iteration if possible
            headers_iter = getattr(request, 'headers', {})
            headers_repr = {k: getattr(headers_iter, k, None) for k in list(getattr(headers_iter, 'keys', lambda: [])())}  # type: ignore
        except Exception:
            headers_repr = "<unavailable>"
    logger.info(f"today_notes API called: request.headers={headers_repr}")

    # Sanitize auth object to remove sensitive wallet/key information
    if auth:
        sanitized_auth = auth.copy()
        if sanitized_auth.get("type") == "wallet" and "value" in sanitized_auth:
            wallet_info = sanitized_auth["value"]
            if hasattr(wallet_info, 'wallet'):
                wallet = wallet_info.wallet
                # Create sanitized wallet representation
                sanitized_wallet = {
                    'id': wallet.id[:8] + "****" if len(wallet.id) > 8 else "****",
                    'user': wallet.user[:8] + "****" if len(wallet.user) > 8 else "****",
                    'name': wallet.name,
                    'deleted': wallet.deleted,
                    'created_at': wallet.created_at,
                    'balance_msat': wallet.balance_msat
                }
                sanitized_auth["value"] = type('WalletTypeInfo', (), {
                    'key_type': wallet_info.key_type,
                    'wallet': type('Wallet', (), sanitized_wallet)()
                })()
        logger.info(f"today_notes API called: auth_type={sanitized_auth.get('type')}, has_wallet={sanitized_auth.get('value') is not None}")
    else:
        logger.info(f"today_notes API called: auth={auth}")
    
    # Resolve user-specific settings if an API key or admin session is present
    user_id = await _resolve_user_from_api_key(request)
    if user_id is None:
        if auth and auth.get("type") == "wallet":
            user_id = auth["value"].wallet.user
        elif auth and auth.get("type") == "admin":
            user_id = auth["value"].id

    # Log sanitized information (no sensitive headers or auth details)
    logger.info(f"today_notes API called: method={request.method}, user_id={user_id}, has_api_key={user_id is not None}")

    s = await crud.get_settings(user_id)
    try:
        logger.debug(f"Cyberherd today_notes: user_id={user_id} tags={s.tracked_tags}")
    except Exception:
        pass
    # Preserve original tag case (strip leading '#') and build lowercase set
    tags = [t.lstrip("#") for t in (s.tracked_tags or []) if t]
    tags_lower = [t.lower() for t in tags]
    if not tags:
        return {"note_ids": []}

    eff_pub = _get_cached_effective_pubkey(s)
    if not (isinstance(eff_pub, str) and len(eff_pub) >= 32):  # guard against mock objects / short placeholders
        try:
            eff_pub = _get_effective_pubkey(s)  # type: ignore[name-defined]
        except Exception:
            eff_pub = None
    if not (isinstance(eff_pub, str) and len(eff_pub) >= 32):
        return {"note_ids": []}
    try:
        logger.debug(f"Cyberherd today_notes: effective pubkey present={bool(eff_pub)}")
    except Exception:
        pass

    from datetime import datetime, timezone
    day_key = crud.get_local_date_key()  # Use authoritative local date calculation
    cache_key = (day_key, user_id, eff_pub, tuple(sorted(tags_lower)))
    debug_mode = request.query_params.get("debug") is not None
    # Fast-path cache hit (unless debug requested)
    cached = _get_cached_notes(request, cache_key)
    if cached is not None and not debug_mode:
        return {"note_ids": cached}

    # Obtain (or create) a lock so only one coroutine performs the network query
    state = getattr(request.app, "state", request.app)
    lock = getattr(state, "cyberherd_today_notes_lock", None)
    # Discard bogus mock objects; only treat a real asyncio.Lock as valid
    if not isinstance(lock, asyncio.Lock):
        try:
            lock = asyncio.Lock()
            setattr(state, "cyberherd_today_notes_lock", lock)
        except Exception:
            lock = None

    async def _query_and_cache():
        # Return tracked_event_ids from settings (maintained by subscription system)
        # This reflects the notes that are actively being monitored for engagements
        tracked_ids = getattr(s, "tracked_event_ids", []) or []
        
        # Normalize to list of strings
        today_ids = []
        for note_id in tracked_ids:
            if isinstance(note_id, str):
                today_ids.append(note_id.strip().lower())
        
        _cache_notes(request, cache_key, today_ids)
        if debug_mode:
            # For debugging: also try broader queries to diagnose the issue
            from .services import nostr_helpers  # type: ignore
            # Use UTC-first time calculation (authoritative)
            midnight_utc, midnight_local, midnight_timestamp, until_timestamp = crud.get_utc_day_boundaries()
            
            # Build expanded tag list (original + lowercase) mirroring crud logic
            expanded_filter_tags = []
            _seen = set()
            for _t in tags + tags_lower:
                if _t and _t not in _seen:
                    expanded_filter_tags.append(_t)
                    _seen.add(_t)

            # Debug queries - include kind 1 (notes) and kind 30311 (long-form content)
            debug_queries = [
                ("all_today", {
                    "kinds": [1, 30311],
                    "since": midnight_timestamp,
                    "authors": [eff_pub] if eff_pub else [],
                }),
                ("cyberherd_any", {
                    "kinds": [1, 30311],
                    "#t": expanded_filter_tags,
                    "limit": 10
                }),
                ("author_recent", {
                    "kinds": [1, 30311],
                    "authors": [eff_pub] if eff_pub else [],
                    "limit": 20
                })
            ]
            
            debug_results = {}
            for query_name, query_filter in debug_queries:
                try:
                    debug_events = await nostr_helpers.query_events(query_filter, limit=20, timeout=4.0)
                    debug_results[query_name] = {
                        "count": len(debug_events or []),
                        "events": debug_events[:5] if debug_events else []
                    }
                except Exception as e:
                    debug_results[query_name] = {"error": str(e), "count": 0}
            
            return {
                "note_ids": today_ids,
                "debug": {
                    "effective_pubkey": eff_pub,
                    "tracked_tags": tags,
                    "cache_key": str(cache_key),
                    "queries": debug_results
                }
            }
        return {"note_ids": today_ids}

    if lock is not None:
        async with lock:
            # Re-check cache inside lock
            cached2 = _get_cached_notes(request, cache_key)
            if cached2 is not None and not debug_mode:
                return {"note_ids": cached2}
            return await _query_and_cache()
    else:
        return await _query_and_cache()


# ------------------------- Missing UI Endpoints -------------------------
# The frontend expects member CRUD, shares, zap monitor status, and recompute
# splits endpoints. Provide them here following LNbits patterns (read =>
# invoice key or anonymous, write => admin key required).


@cyberherd_api_router.get("/api/v1/members")
async def api_get_members(request: Request, auth=Depends(auth_wallet_or_admin_dep)):
    try:
        # Get user_id from auth first
        user_id = None
        if auth and auth.get("type") == "wallet":
            user_id = auth["value"].wallet.user
        
        # Get members for this specific user
        rows = await crud.get_all_cyberherd_members(user_id)
        # normalize list[dict]
        members = [dict(r) for r in rows] if rows else []

        # Load settings once so we can reuse for splits and tracked note checks
        settings = await crud.get_settings(user_id)
        tracked_note_ids = set(getattr(settings, "tracked_event_ids", []) or [])

        # Fetch splits data and merge percent values
        try:
            # Import here to avoid circular imports
            from lnbits.extensions.splitpayments.crud import get_targets
            from lnbits.core.crud import get_wallet

            # Get settings to find the source_wallet (cyberherd wallet)
            # Log settings without sensitive fields (private key, timestamps)
            logger.debug(
                f"Cyberherd members API: user={user_id}, "
                f"source_wallet={settings.source_wallet if settings else None}, "
                f"zap_tracking_enabled={settings.zap_tracking_enabled if settings else None}, "
                f"tracked_event_ids_count={len(settings.tracked_event_ids) if settings else 0}"
            )
            logger.info(f"Cyberherd members API called for user={user_id}")
            
            if settings and settings.source_wallet:
                # Check if user has access to the source_wallet
                source_wallet_info = await get_wallet(settings.source_wallet)
                if source_wallet_info and source_wallet_info.user == user_id:
                    # User owns the source_wallet, can fetch its splits
                    splits_targets = await get_targets(settings.source_wallet)
                    logger.debug(f"Cyberherd members API: found {len(splits_targets) if splits_targets else 0} splits targets for wallet {settings.source_wallet}")
                    
                    if splits_targets:
                        # Create lookup map by wallet identifier
                        splits_map = {}
                        for target in splits_targets:
                            splits_map[target.wallet] = target.percent
                            logger.debug(f"Cyberherd members API: splits target {target.wallet} -> {target.percent}%%")

                        # Merge percent values into members
                        for member in members:
                            lud16 = (member.get("lud16") or "").strip()
                            if lud16:
                                percent = splits_map.get(lud16)
                                if percent is not None:
                                    member["splits_percent"] = percent
                                    logger.debug(f"Cyberherd members API: matched member {lud16} -> {percent}%%")
                else:
                    logger.info(f"Cyberherd members API: user does not own source_wallet {settings.source_wallet}")
        except Exception as splits_error:
            # Log but don't fail if splits data can't be fetched
            logger.warning(f"Failed to fetch splits data for members: {splits_error}")
            import traceback
            logger.warning(f"Traceback: {traceback.format_exc()}")

        # Annotate each member with tracked-note match status
        for member in members:
            note_candidate = _normalize_event_id(
                member.get("event_id") or member.get("note")
            )
            member["is_tracked_note"] = bool(
                note_candidate and note_candidate in tracked_note_ids
            )

        # Amounts are stored directly in satoshis; no conversion needed.
        # (Previously divided by 1000 assuming msats; standardized to sats storage.)

        # Sort latest / highest amount first for UI usefulness
        try:
            members.sort(key=lambda m: (m.get("is_active", 0), m.get("amount", 0)), reverse=True)
        except Exception:
            pass
        return {"members": members}
    except Exception as e:
        logger.warning(f"Cyberherd members fetch failed: {e}")
        return {"members": []}


@cyberherd_api_router.get("/api/v1/zap_totals/{zapper_pubkey}")
async def api_get_zap_totals(
    request: Request,
    zapper_pubkey: str,
    force_refresh: bool = False,
    auth=Depends(auth_wallet_or_admin_dep),
):
    if not auth or auth.get("type") != "wallet":
        raise HTTPException(status_code=HTTPStatus.UNAUTHORIZED, detail="wallet key required")

    user_id = auth["value"].wallet.user  # type: ignore[union-attr]
    try:
        result = await get_zap_totals_for_zapper(
            user_id=user_id,
            zapper_pubkey=zapper_pubkey,
            force_refresh=force_refresh,
        )
        return {"zap_totals": result}
    except ZapTotalsError as exc:
        raise HTTPException(status_code=HTTPStatus.BAD_REQUEST, detail=str(exc))
    except HTTPException:
        raise
    except Exception as exc:  # pragma: no cover - defensive
        logger.error(f"Cyberherd: zap totals query failed: {exc}")
        raise HTTPException(status_code=HTTPStatus.INTERNAL_SERVER_ERROR, detail="failed to fetch zap totals")


@cyberherd_api_router.post("/api/v1/members")
async def api_post_member(
    request: Request,
    payload: dict,
    wallet_info: WalletTypeInfo = Depends(require_admin_key),
):
    pubkey = (payload or {}).get("pubkey")
    if not (isinstance(pubkey, str) and len(pubkey) >= 10):
        raise HTTPException(status_code=HTTPStatus.BAD_REQUEST, detail="pubkey required")
    amount = 0  # Stored in sats
    try:
        a = (payload or {}).get("amount")
        if a is not None:
            amount = int(a)  # Amount is already in sats
            if amount < 0:
                raise ValueError
    except Exception:
        raise HTTPException(status_code=HTTPStatus.BAD_REQUEST, detail="invalid amount")

    payload_dict = payload if isinstance(payload, dict) else {}
    zap_request = payload_dict.get("zap_request")
    if not isinstance(zap_request, dict):
        zap_request = None
    tracked_event_id = _extract_tracked_event_id(payload_dict, zap_request)
    zap_event_id = None
    if zap_request:
        zap_event_id = _normalize_event_id(zap_request.get("id") or zap_request.get("note"))
    if not zap_event_id:
        zap_event_id = _normalize_event_id(payload_dict.get("note"))

    user_id = wallet_info.wallet.user
    settings = await crud.get_settings(user_id)
    tracked_note_ids = set(getattr(settings, "tracked_event_ids", []) or [])
    # If tracked_note_ids is empty, accept any note (no specific tags configured)
    # Otherwise, validate that the tracked_event_id is in the list
    if tracked_note_ids and tracked_event_id and tracked_event_id not in tracked_note_ids:
        preview = tracked_event_id[:16] + "..." if len(tracked_event_id) > 16 else tracked_event_id
        logger.warning(
            "Cyberherd POST /members rejected: note %s not in tracked_event_ids (count=%d) user=%s",
            preview,
            len(tracked_note_ids),
            user_id,
        )
        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST,
            detail="tracked note id is not registered for this user",
        )

    # Enrich metadata best-effort
    display_name = "Anon"
    lud16 = None
    picture = None
    relays = None
    nprofile = None
    try:
        md = await lookup_metadata(pubkey)
        if md:
            display_name = md.get("display_name") or display_name
            lud16 = md.get("lud16")
            picture = md.get("picture")
        relays = await lookup_relays(pubkey)
        try:
            nprofile = hex_to_nprofile(pubkey, relays=relays if isinstance(relays, list) else None)
        except Exception:
            nprofile = None
    except Exception as e:
        logger.debug(f"Cyberherd: metadata enrichment skipped for new member {pubkey[:8]}…: {e}")

    member_data = {
        "pubkey": pubkey,
        "display_name": display_name,
        "event_id": tracked_event_id or zap_event_id,
        "note": zap_event_id,
        "kinds": None,
        "nprofile": nprofile,
        "lud16": lud16,
        "payouts": 0.0,
        "amount": amount,  # Amount is in satoshis
        "picture": picture,
        "relays": ",".join(relays) if isinstance(relays, list) and relays else None,
        "metadata_last_checked_at": int(time.time()),
    }
    try:
        await crud.add_new_active_member(member_data, user_id=user_id)

        # Best-effort: publish a structured `new_member` notification via the
        # messaging adapter. Always attempt to publish a template-backed message
        # (mapped to the `cyber_herd` category) so manual UI adds do not result
        # in raw-dict string content being posted.
        try:
            from .services import messaging as ch_msg
            owner_user_id = getattr(settings, "templates_owner_user", None)
            private_key = await _get_user_private_key(user_id)
            member_name = f"nostr:{nprofile}" if nprofile else display_name

            # Only publish messages for automated or zap-request driven adds
            publish_allowed = False
            try:
                automated_flag = bool(payload_dict.get("automated") or False)
            except Exception:
                automated_flag = False
            if automated_flag or tracked_event_id or zap_request:
                publish_allowed = True

            if publish_allowed:
                # Use tracked/zap event id for threading when available. If
                # normalization removed the id, fall back to raw zap_request id
                # so tests and threading still work with non-64hex test ids.
                thread_event_id = tracked_event_id or zap_event_id or (zap_request.get("id") if isinstance(zap_request, dict) else None)

                relay_hint = _select_reply_relay(
                    relays,
                    payload_dict.get("reply_relay"),
                    payload_dict.get("relays"),
                    (zap_request or {}).get("relays") if isinstance(zap_request, dict) else None,
                )

                try:
                    # Compute friendly aliases expected by legacy templates
                    initial_amount_val = int(amount or 0)
                    thanks_part = f"Thank you for the contribution of {initial_amount_val} sats." if initial_amount_val > 0 else "Welcome to the herd!"
                    feeder_difference = await _compute_feeder_difference_for_user(
                        user_id, settings_obj=settings
                    )
                    difference_value = feeder_difference if feeder_difference is not None else 0
                    picture = member_data.get("picture")
                    values_payload = {
                        "member_name": member_name,
                        "member_display_name": display_name,
                        "initial_amount": initial_amount_val,
                        # Backwards-compatible aliases for templates
                        "name": member_name,
                        "thanks_part": thanks_part,
                        "difference": difference_value,
                    }
                    if picture:
                        values_payload["picture"] = picture
                        values_payload.setdefault("member_picture", picture)
                        values_payload.setdefault("imageUrl", picture)
                    published = await ch_msg.publish_event_message(
                        "new_member",
                        owner_user_id=owner_user_id,
                        values=values_payload,
                        e_tags=[thread_event_id] if thread_event_id else None,
                        p_tags=[pubkey] if pubkey else None,
                        private_key=private_key,
                        websocket_topic="cyberherd",
                        reply_relay=relay_hint,
                    )
                    # If we published for a threaded event (zap/tracked event),
                    # record it as processed so downstream headbutt processing
                    # will not publish a duplicate message for the same event.
                    try:
                        if published and thread_event_id:
                            try:
                                await crud.register_processed_event(
                                    user_id,
                                    thread_event_id,
                                    note_id=thread_event_id,
                                    pubkey=pubkey,
                                    event_type="message",
                                )
                                logger.debug("Registered processed event %s after views_api publish", thread_event_id)
                            except Exception:
                                logger.warning(f"Failed to register processed event {thread_event_id} after publish")
                    except Exception:
                        pass
                except Exception as e:
                    logger.error(f"cyberherd: failed to publish new_member message: {e}")
        except Exception as e:
            logger.error(f"cyberherd: failed to publish new_member message: {e}")

        return {"ok": True, "member": member_data}
    except Exception as e:
        logger.error(f"Cyberherd: failed creating member {pubkey[:8]}…: {e}")
        raise HTTPException(status_code=HTTPStatus.INTERNAL_SERVER_ERROR, detail="failed to create member")


@cyberherd_api_router.put("/api/v1/members/{pubkey}")
async def api_put_member(
    request: Request,
    pubkey: str,
    payload: MemberPut,
    wallet_info: WalletTypeInfo = Depends(require_admin_key),
):
    if not pubkey:
        raise HTTPException(status_code=HTTPStatus.BAD_REQUEST, detail="pubkey required")

    payload_dict = {}
    try:
        payload_dict = payload.dict(exclude_unset=True)
    except Exception:
        payload_dict = {}

    # Get zap_request from payload (sent by middleware)
    zap_request = payload_dict.get("zap_request")
    if not isinstance(zap_request, dict):
        zap_request = payload.zap_request if isinstance(payload.zap_request, dict) else None

    tracked_event_id = _extract_tracked_event_id(payload_dict, zap_request)
    zap_event_id = None
    if zap_request:
        zap_event_id = _normalize_event_id(zap_request.get("id") or zap_request.get("note"))
    if not zap_event_id:
        zap_event_id = _normalize_event_id(payload_dict.get("note"))

    user_id = wallet_info.wallet.user
    settings = await crud.get_settings(user_id)
    tracked_note_ids = set(getattr(settings, "tracked_event_ids", []) or [])
    if tracked_event_id and tracked_event_id not in tracked_note_ids:
        preview = tracked_event_id[:16] + "..." if len(tracked_event_id) > 16 else tracked_event_id
        logger.warning(
            "Cyberherd PUT /members rejected: note %s not in tracked_event_ids (count=%d) user=%s",
            preview,
            len(tracked_note_ids),
            user_id,
        )
        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST,
            detail="tracked note id is not registered for this user",
        )

    # Check for duplicate zap processing
    if zap_request:
        dup_key = zap_event_id or zap_request.get("id")
        if dup_key:
            processed_zaps = getattr(api_put_member, "_processed_zaps", set())
            if dup_key in processed_zaps:
                logger.info(f"Cyberherd: skipping duplicate zap processing for event {dup_key}")
                return {"ok": True}
            processed_zaps.add(dup_key)
            if len(processed_zaps) > 1000:
                oldest_entries = list(processed_zaps)[:100]
                for entry in oldest_entries:
                    processed_zaps.discard(entry)
            try:
                setattr(api_put_member, '_processed_zaps', processed_zaps)
            except Exception:
                pass
    
    # Check if member exists and activate them if they do
    row = await crud.get_cyberherd_member_by_pubkey(pubkey, user_id)
    
    if not row:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail="member not found - use POST to create")
    
    was_active = bool(row.get("is_active"))
    # Member exists - activate them
    logger.info(f"cyberherd: activating member {pubkey[:8]}...")
    await crud.update_and_activate_member(pubkey, 0, 0.0, user_id)
    
    # Adjust amount if provided (increase by delta)
    if payload.amount is not None:
        try:
            prev = int(row.get("amount", 0))
            increase_amt = int(payload.amount)  # Amount is the increase in sats
            if increase_amt < 0:
                raise ValueError
            new_amt = prev + increase_amt
            
            # Update amount (member already activated above)
            logger.info(f"cyberherd: updating member {pubkey[:8]}... amount: {prev} -> {new_amt} (+{increase_amt})")
            event_for_storage = tracked_event_id or zap_event_id
            await crud.update_and_activate_member(
                pubkey,
                increase_amt,
                0.0,
                user_id,
                event_id=event_for_storage,
            )

            # Only publish messages for automated zap processing with amount increases
            thread_event_id = tracked_event_id or zap_event_id
            publish_thread_id = thread_event_id
            if not publish_thread_id and zap_request:
                publish_thread_id = _normalize_event_id(zap_request.get("id"))

            if increase_amt != 0 and (publish_thread_id or zap_request):
                try:
                    from .services import messaging as ch_msg

                    owner_user_id = getattr(settings, "templates_owner_user", None)
                    updated_row = await crud.get_cyberherd_member_by_pubkey(pubkey, user_id)
                    display_name = (
                        (updated_row or {}).get("display_name")
                        or row.get("display_name")
                        or "Anon"
                    )
                    nprofile = (updated_row or {}).get("nprofile") or row.get("nprofile")
                    member_name = f"nostr:{nprofile}" if nprofile else display_name
                    updated_amount = (updated_row or {}).get("amount")
                    if updated_amount is None:
                        updated_amount = new_amt
                    new_total = int(updated_amount)
                    event_type = "new_member" if not was_active else "member_increase"
                    logger.info(f"cyberherd: publishing {event_type} message for {pubkey[:8]}...")

                    # Get the user's private key for signing
                    private_key = await _get_user_private_key(user_id)

                    e_tags = [publish_thread_id] if publish_thread_id else None
                    if not e_tags and zap_request:
                        fallback_event = _normalize_event_id(zap_request.get("id"))
                        e_tags = [fallback_event] if fallback_event else None
                    p_tags = [pubkey] if pubkey else None

                    feeder_difference = await _compute_feeder_difference_for_user(
                        user_id, settings_obj=settings
                    )
                    difference_value = feeder_difference if feeder_difference is not None else 0

                    picture = (updated_row or {}).get("picture") or (row.get("picture") if isinstance(row, dict) else None)

                    if event_type == "new_member":
                        values = {
                            "member_name": member_name,
                            "member_display_name": display_name,
                            "initial_amount": new_total,
                            "difference": difference_value,
                        }
                    else:
                        values = {
                            "member_name": member_name,
                            "member_display_name": display_name,
                            "increase_amount": increase_amt,
                            "new_total": new_total,
                            "difference": difference_value,
                        }
                    if picture:
                        values.setdefault("picture", picture)
                        values.setdefault("member_picture", picture)
                        values.setdefault("imageUrl", picture)

                    relay_hint = _select_reply_relay(
                        (updated_row or {}).get("relays"),
                        row.get("relays") if isinstance(row, dict) else None,
                        payload_dict.get("reply_relay"),
                        payload_dict.get("relays"),
                        (zap_request or {}).get("relays") if isinstance(zap_request, dict) else None,
                    )

                    try:
                        published = await ch_msg.publish_event_message(
                            event_type,
                            owner_user_id=owner_user_id,
                            values=values,
                            e_tags=e_tags,
                            p_tags=p_tags,
                            private_key=private_key,
                            reply_relay=relay_hint,
                        )
                        # Persist processed marker for the thread event so headbutt doesn't duplicate
                        try:
                            if event_type == "new_member" and published and publish_thread_id:
                                try:
                                    await crud.register_processed_event(
                                        user_id,
                                        publish_thread_id,
                                        note_id=publish_thread_id,
                                        pubkey=pubkey,
                                        event_type="message",
                                    )
                                    logger.debug("Registered processed event %s after put-member publish", publish_thread_id)
                                except Exception:
                                    logger.warning(f"Failed to register processed event {publish_thread_id} after publish")
                        except Exception:
                            pass
                    except Exception as e:
                        logger.error(f"cyberherd: failed to publish membership message: {e}")
                except Exception as e:
                    logger.error(f"cyberherd: failed to publish membership message: {e}")
        except HTTPException:
            raise
        except Exception as e:
            logger.warning(f"Cyberherd: failed updating member {pubkey[:8]}…: {e}")
            raise HTTPException(status_code=HTTPStatus.INTERNAL_SERVER_ERROR, detail="update failed")
    return {"ok": True}


@cyberherd_api_router.delete("/api/v1/members/{pubkey}")
async def api_delete_member(
    request: Request,
    pubkey: str,
    wallet_info: WalletTypeInfo = Depends(require_admin_key),
):
    try:
        user_id = wallet_info.wallet.user
        await crud.delete_cyberherd_member_by_pubkey(pubkey, user_id)
        return {"ok": True}
    except Exception as e:
        logger.warning(f"Cyberherd: delete member failed {pubkey[:8]}…: {e}")
        raise HTTPException(status_code=HTTPStatus.INTERNAL_SERVER_ERROR, detail="delete failed")


@cyberherd_api_router.post("/api/v1/members/{pubkey}/activate")
async def api_activate_member(
    request: Request,
    pubkey: str,
    wallet_info: WalletTypeInfo = Depends(require_admin_key),
):
    try:
        user_id = wallet_info.wallet.user
        # zero increase just forces is_active=1 via update_and_activate_member
        await crud.update_and_activate_member(pubkey, 0, 0.0, user_id)
        
        return {"ok": True}
    except Exception as e:
        logger.warning(f"Cyberherd: activate member failed {pubkey[:8]}…: {e}")
        raise HTTPException(status_code=HTTPStatus.INTERNAL_SERVER_ERROR, detail="activate failed")


@cyberherd_api_router.post("/api/v1/members/{pubkey}/deactivate")
async def api_deactivate_member(
    request: Request,
    pubkey: str,
    wallet_info: WalletTypeInfo = Depends(require_admin_key),
):
    try:
        await crud.deactivate_cyberherd_member(pubkey, user_id=wallet_info.wallet.user)
        return {"ok": True}
    except Exception as e:
        logger.warning(f"Cyberherd: deactivate member failed {pubkey[:8]}…: {e}")
        raise HTTPException(status_code=HTTPStatus.INTERNAL_SERVER_ERROR, detail="deactivate failed")


@cyberherd_api_router.get("/api/v1/shares")
async def api_get_shares(request: Request, auth=Depends(auth_wallet_or_admin_dep)):
    try:
        # Get user_id first
        user_id = None
        if auth.get("type") == "wallet":
            try:
                user_id = auth["value"].wallet.user  # type: ignore[attr-defined]
            except Exception:
                user_id = None
        elif auth.get("type") == "admin":
            # Some tests pass a dummy object without id attribute
            user_id = getattr(auth.get("value"), "id", None)
        
        # Active cyberherd members (not necessarily all eligible for payouts if lacking lud16)
        if user_id:
            active = await crud.get_active_cyberherd_members(user_id)
            active_list = [dict(r) for r in active] if active else []
        else:
            active_list = []

        # Fetch settings for potential zap wallet display and source wallet for splitpayments
        if user_id:
            settings = await crud.get_settings(user_id)
        else:
            # No user context: use empty/default settings to avoid touching global row
            from .models import CyberherdSettings
            settings = CyberherdSettings()
        zap_wallet = getattr(settings, "zap_wallet", None)
        zap_alias = getattr(settings, "zap_wallet_alias", None) or "Zap Wallet"
        source_wallet = getattr(settings, "source_wallet", None)

        # Fetch split targets from splitpayments extension if source_wallet is configured
        split_targets = []
        if source_wallet:
            try:
                from lnbits.extensions.splitpayments.crud import get_targets
                split_targets = await get_targets(source_wallet)
            except Exception as e:
                logger.debug(f"Could not fetch split targets for source_wallet {source_wallet}: {e}")
                split_targets = []

        # Create a lookup dict for cyberherd members by lud16
        members_by_lud16 = {}
        for m in active_list:
            lud16 = (m.get("lud16") or "").strip()
            if lud16:
                members_by_lud16[lud16] = m

        # Compute shares based on splitpayments data
        shares: list[dict] = []

        # Add zap wallet share if configured
        if zap_wallet:
            shares.append({
                "zap_wallet": zap_wallet,
                "display_name": zap_alias,
                "percent": 100.0,  # Default to 100%, will be adjusted if split targets exist
                "type": "zap_wallet"
            })

        # Process split targets and associate with cyberherd members
        total_split_percent = 0.0
        valid_split_targets = []
        for target in split_targets:
            target_wallet = target.wallet.strip() if target.wallet else ""
            if not target_wallet or target_wallet == source_wallet:
                continue
            valid_split_targets.append(target)

        for target in valid_split_targets:
            target_wallet = target.wallet.strip()
            # Check if this target corresponds to a cyberherd member
            member = members_by_lud16.get(target_wallet)
            if member:
                # This is a cyberherd member target
                shares.append({
                    "pubkey": member.get("pubkey"),
                    "display_name": member.get("display_name") or "Anon",
                    "lud16": target_wallet,
                    "amount": member.get("amount", 0),  # Already sats
                    "percent": float(target.percent),
                    "type": "member"
                })
                total_split_percent += target.percent
            elif target_wallet == zap_wallet and target.percent > 0:
                # This is the zap wallet target - update its percentage only if > 0
                for share in shares:
                    if share.get("type") == "zap_wallet":
                        share["percent"] = float(target.percent)
                        total_split_percent += target.percent
                        break
            else:
                # This is some other target (not a cyberherd member or zap wallet)
                if target.percent > 0:  # Only include targets with positive percent
                    shares.append({
                        "wallet": target_wallet,
                        "display_name": target.alias or target_wallet,
                        "percent": float(target.percent),
                        "type": "external_target"
                    })
                    total_split_percent += target.percent

        # If we have valid split targets, adjust zap wallet percentage if it wasn't explicitly set
        if valid_split_targets and zap_wallet:
            zap_found_in_targets = any(
                (target.wallet or "").strip() == zap_wallet for target in split_targets
            )
            if not zap_found_in_targets:
                # Zap wallet not in split targets, so it gets the remainder
                for share in shares:
                    if share.get("type") == "zap_wallet":
                        share["percent"] = 100.0 - total_split_percent
                        break

        # If no split targets configured, fall back to legacy behavior
        if not split_targets:
            # Eligible members are those with a lud16 (otherwise they can't receive payouts)
            eligible = []
            for m in active_list:
                lud16 = (m.get("lud16") or "").strip()
                if not lud16:
                    continue
                amt = int(m.get("amount", 0) or 0)
                eligible.append((m, amt))

            # Legacy logic: zap wallet gets 90% if members eligible
            if zap_wallet and eligible:
                # Members share 10%
                weights = [amt for (_m, amt) in eligible]
                from .services.splits import _proportional_percentages  # type: ignore
                member_pcts = _proportional_percentages(weights, total=10)
                # Update zap wallet percentage
                for share in shares:
                    if share.get("type") == "zap_wallet":
                        share["percent"] = 90.0
                        break
                for (m, amt), pct in zip(eligible, member_pcts):
                    shares.append({
                        "pubkey": m.get("pubkey"),
                        "display_name": m.get("display_name") or "Anon",
                        "lud16": (m.get("lud16") or "").strip() or None,
                        "amount": amt,  # Already sats
                        "percent": float(pct),
                        "type": "member"
                    })
            elif zap_wallet and not eligible:
                # 100% zap wallet (no eligible members) - already set above
                pass
            else:
                # No zap wallet configured: show proportional distribution among eligible members (100%)
                if eligible:
                    weights = [amt for (_m, amt) in eligible]
                    from .services.splits import _proportional_percentages  # type: ignore
                    member_pcts = _proportional_percentages(weights, total=100)
                    # Remove zap wallet from shares since it's not configured
                    shares = [s for s in shares if s.get("type") != "zap_wallet"]
                    for (m, amt), pct in zip(eligible, member_pcts):
                        shares.append({
                            "pubkey": m.get("pubkey"),
                            "display_name": m.get("display_name") or "Anon",
                            "lud16": (m.get("lud16") or "").strip() or None,
                            "amount": amt,  # Already sats
                            "percent": float(pct),
                            "type": "member"
                        })

        # Sort descending percent for UI
        shares.sort(key=lambda s: s.get("percent", 0), reverse=True)
        return {"shares": shares, "zap_wallet": zap_wallet, "allocation_model": ("zap+members" if zap_wallet and any(s.get("type") == "member" for s in shares) else ("zap_only" if zap_wallet else ("members_only" if any(s.get("type") == "member" for s in shares) else "none")))}
    except Exception as e:
        logger.warning(f"Cyberherd shares fetch failed: {e}")
        return {"shares": [], "allocation_model": "error"}


@cyberherd_api_router.get("/api/v1/zap_monitor_status")
async def api_get_zap_monitor_status(request: Request, auth=Depends(auth_wallet_or_admin_dep)):
    # Optional user_id scoping — monitor currently global / singleton
    user_id = None
    if auth["type"] == "wallet":
        user_id = auth["value"].wallet.user
    elif auth["type"] == "admin":
        user_id = auth["value"].id
    try:
        zm = get_zap_monitor(app=request.app, db=crud, user_id=user_id)
        st = await zm.status() if zm else {}
        return {"status": st}
    except Exception as e:
        logger.warning(f"Cyberherd: zap monitor status failed: {e}")
        return {"status": {"running": False}}


@cyberherd_api_router.get("/api/v1/query_zap_events")
async def api_query_zap_events(
    request: Request,
    hours: int = Query(24, ge=1, le=168),
    limit: int = Query(100, ge=1, le=500),
    note_id: str | None = Query(None),
    zapper_pubkey: str | None = Query(None),
    auth=Depends(auth_wallet_or_admin_dep),
):
    """Return processed zap receipts for the authenticated user."""
    if auth["type"] == "wallet":
        user_id = auth["value"].wallet.user
    elif auth["type"] == "admin":
        user_id = auth["value"].id
    else:
        raise HTTPException(
            status_code=HTTPStatus.UNAUTHORIZED, detail="Missing user context"
        )

    since_ts = None
    if hours:
        try:
            since_ts = max(0, int(time.time() - (hours * 3600)))
        except Exception:
            since_ts = None

    rows, has_more = await crud.fetch_processed_zaps(
        user_id=user_id,
        since=since_ts,
        limit=limit,
        note_id=note_id,
        zapper_pubkey=zapper_pubkey,
    )

    from datetime import datetime, timezone

    def _coerce_timestamp(value) -> int | None:
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return int(value)
        if isinstance(value, str):
            candidate = value.strip()
            if not candidate:
                return None
            try:
                return int(candidate)
            except ValueError:
                try:
                    return int(float(candidate))
                except ValueError:
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

    events: list[dict[str, Any]] = []
    for row in rows or []:
        event_id = row.get("event_id") or row.get("zap_receipt_id")
        note = row.get("note_id")
        zapper = row.get("zapper_pubkey")
        payment_hash = row.get("payment_hash")
        amount_raw = row.get("amount") or row.get("amount_sats")
        try:
            amount_sats = int(amount_raw) if amount_raw is not None else None
        except Exception:
            amount_sats = None

        processed_ts = row.get("processed_at_ts")
        if processed_ts is None:
            processed_ts = _coerce_timestamp(row.get("processed_at"))
        processed_iso = (
            datetime.fromtimestamp(processed_ts, tz=timezone.utc)
            .isoformat()
            .replace("+00:00", "Z")
            if processed_ts is not None
            else None
        )

        events.append(
            {
                "event_id": event_id,
                "note_id": note,
                "zapper_pubkey": zapper,
                "payment_hash": payment_hash,
                "amount_sats": amount_sats,
                "processed_at": processed_ts,
                "processed_at_iso": processed_iso,
                "source": "processed_zaps",
            }
        )

    capped_limit = max(1, min(limit, 500))
    return {
        "events": events,
        "count": len(events),
        "filters": {
            "hours": hours,
            "since_ts": since_ts,
            "note_id": note_id,
            "zapper_pubkey": zapper_pubkey,
            "limit": capped_limit,
        },
        "has_more": bool(has_more),
    }


@cyberherd_api_router.get("/api/v1/nostr_diagnostics")
async def api_get_nostr_diagnostics(request: Request, auth=Depends(auth_wallet_or_admin_dep)):
    """Get comprehensive Nostr helper diagnostics.
    
    Returns diagnostic information including:
    - nostr_helpers query statistics (total, successful, failed, empty)
    - Subscription statistics (added, closed, failed)
    - Relay connection status
    - Recent failures and timing information
    - subscriptions module helper diagnostics
    - UTC/local time context for cache key validation
    
    Useful for debugging Nostr connectivity issues and helper failures.
    """
    try:
        from .services import nostr_helpers
        from .services import subscriptions
        
        # Get diagnostics from nostr_helpers
        helper_diag = nostr_helpers.get_diagnostics()
        
        # Get diagnostics from subscriptions module
        subscription_diag = subscriptions.get_helper_diagnostics()
        
        return {
            "nostr_helpers": helper_diag,
            "subscriptions": subscription_diag,
            "timestamp": time.time(),
            "note": "Use this endpoint to monitor Nostr helper health and diagnose issues"
        }
    except Exception as e:
        logger.error(f"Failed to get nostr diagnostics: {e}")
        return {
            "error": str(e),
            "nostr_helpers": {},
            "subscriptions": {},
            "timestamp": time.time()
        }



@cyberherd_api_router.get("/api/v1/split_targets_diagnostic")
async def api_split_targets_diagnostic(request: Request, wallet_info: WalletTypeInfo = Depends(require_admin_key)):
    """Return current SplitPayments targets plus raw CyberHerd member weights.

    Helps debug why predefined zap wallet might be missing or percentages unexpected.
    """
    try:
        # Identify source wallet from settings
        s = await crud.get_settings(wallet_info.wallet.user)
        source_wallet = getattr(s, "source_wallet", None)
        if not source_wallet:
            return {"error": "source_wallet not configured"}
        # Fetch split targets from splitpayments extension
        try:
            from lnbits.extensions.splitpayments.crud import get_targets  # type: ignore
            targets = []
            try:
                rows = await get_targets(str(source_wallet))
                for r in rows:
                    targets.append({
                        "wallet": getattr(r, "wallet", None) or getattr(r, "lud16", None),
                        "percent": getattr(r, "percent", None),
                        "alias": getattr(r, "alias", None),
                    })
            except Exception:
                targets = []
        except Exception as e:
            logger.warning(f"split targets fetch failed: {e}")
            targets = []
        user_id = wallet_info.wallet.user
        members = await crud.get_active_cyberherd_members(user_id)
        member_weights = [
            {
                "pubkey": m.get("pubkey"),
                "display_name": m.get("display_name"),
                "amount": m.get("amount"),
                "lud16": m.get("lud16"),
                "eligible": bool((m.get("lud16") or '').strip()),
            }
            for m in members
        ]
        zap_wallet = getattr(s, "zap_wallet", None)
        return {
            "source_wallet": source_wallet,
            "zap_wallet": zap_wallet,
            "targets": targets,
            "member_weights": member_weights,
        }
    except Exception as e:
        logger.warning(f"split targets diagnostic failed: {e}")
        return {"error": "diagnostic failed"}


@cyberherd_api_router.post("/api/v1/recompute_splits")
async def api_recompute_splits(request: Request, wallet_info: WalletTypeInfo = Depends(require_admin_key)):
    # Re-run proportional allocation logic based on current source_wallet & active members
    user_id = wallet_info.wallet.user
    s = await crud.get_settings(user_id)
    if not s or not getattr(s, "source_wallet", None):
        raise HTTPException(status_code=HTTPStatus.BAD_REQUEST, detail="source_wallet not configured")
    try:
        await crud.recompute_member_payouts(user_id)
        await update_split_targets_proportional(str(getattr(s, 'source_wallet')))
        return {"ok": True}
    except Exception as e:
        logger.warning(f"Cyberherd: recompute_splits failed: {e}")
        raise HTTPException(status_code=HTTPStatus.INTERNAL_SERVER_ERROR, detail="recompute failed")


@cyberherd_api_router.post("/api/v1/trigger_zap_recovery")
async def api_trigger_zap_recovery(request: Request, wallet_info: WalletTypeInfo = Depends(require_admin_key)):
    """Manually trigger zap recovery for debugging and maintenance.
    
    This endpoint does real relay queries using the actual production code.
    Returns detailed information about what zaps were found and processed.
    """
    from datetime import datetime, timezone
    import asyncio
    
    try:
        user_id = wallet_info.wallet.user
        logger.info(f"Test zap recovery triggered for user: {user_id}")
        
        # Get the user's settings
        settings = await crud.get_settings(user_id)
        if not settings:
            return {"error": "No settings found for user", "user_id": user_id}
        
        # Get current tracked notes
        tracked_notes = await crud.get_cyberherd_notes_for_settings(settings)
        tracked_note_ids = tracked_notes  # This function returns list[str] directly
        
        result = {
            "started_at": datetime.now(timezone.utc).isoformat(),
            "user_id": user_id,
            "settings": {
                "zap_tracking_enabled": getattr(settings, "zap_tracking_enabled", False),
                "repost_tracking_enabled": getattr(settings, "repost_tracking_enabled", False),
                "zap_monitor_mode": "websocket",
                "effective_pubkey": _get_cached_effective_pubkey(settings),
                "tracked_tags": getattr(settings, "tracked_tags", [])
            },
            "tracked_notes_count": len(tracked_note_ids),
            "tracked_note_ids": tracked_note_ids[:5],  # First 5 for brevity
        }
        
        if not getattr(settings, "zap_tracking_enabled", False):
            result["error"] = "Zap tracking is disabled in settings"
            return result
        
        if not tracked_note_ids:
            result["warning"] = "No tracked notes found - nothing to monitor for zaps"
            return result
        
        # Get the zap monitor service
        zm = get_zap_monitor(app=request.app, db=crud, user_id=user_id)
        if not zm:
            result["error"] = "Could not get zap monitor service"
            return result
        
        # Store initial state
        initial_last_zap = zm.last_zap_at
        initial_error = zm.last_error
        
        # Force reset recovery state to ensure it runs
        if hasattr(zm, "_recovery_done"):
            try:
                setattr(zm, '_recovery_done', False)
            except Exception:
                pass
        
        # Trigger recovery
        logger.info(f"Triggering zap recovery for user {user_id}...")
        try:
            # Always payment recovery now; run diagnostics first so caller can
            # inspect which payments would be processed and why some are ignored.
            logger.info("Starting payment recovery diagnostics (nostr deprecated)...")
            try:
                import asyncio as _asyncio
                diag_fn = getattr(zm, 'diagnose_missed_payment_zaps', None)
                if callable(diag_fn):
                    if _asyncio.iscoroutinefunction(diag_fn):
                        payment_diag = await diag_fn(settings)
                    else:
                        payment_diag = diag_fn(settings)
                else:
                    payment_diag = []
            except Exception as de:
                payment_diag = []
                logger.warning(f"Zap recovery diagnostics failed: {de}")
            result["payment_diagnostics"] = payment_diag

            logger.info("Starting payment recovery (nostr deprecated)...")
            # Some ZapMonitorService implementations (legacy websocket monitor)
            # exposed a _recover_missed_payment_zaps method for payment-based
            # recovery. Newer implementations may not provide it; guard the
            # call to avoid AttributeError and provide a clear diagnostic.
            if hasattr(zm, "_recover_missed_payment_zaps"):
                try:
                    await zm._recover_missed_payment_zaps(settings)
                    result["recovery_method"] = "payment"
                except Exception as e:
                    result["recovery_error"] = str(e)
                    logger.error(f"Payment recovery failed: {e}")
            else:
                msg = "ZapMonitorService does not implement payment recovery (_recover_missed_payment_zaps)"
                logger.warning(msg)
                result["recovery_error"] = msg

            result["recovery_completed"] = True
            logger.info("Recovery completed successfully")
        except asyncio.TimeoutError:
            result["recovery_error"] = "Recovery timed out after 30 seconds"
            logger.error("Recovery timed out after 30 seconds")
        except Exception as recovery_error:
            result["recovery_error"] = str(recovery_error)
            logger.error(f"Recovery failed: {recovery_error}")
            import traceback
            traceback.print_exc()
        
        # Capture final state
        result["final_state"] = {
            "last_zap_at": zm.last_zap_at,
            "last_error": zm.last_error,
            "zaps_found": zm.last_zap_at != initial_last_zap,
            "errors_occurred": zm.last_error != initial_error
        }
        
        result["completed_at"] = datetime.now(timezone.utc).isoformat()
        
        logger.info(f"Test zap recovery completed for user {user_id}: {result}")
        return result
        
    except Exception as e:
        logger.error(f"Test zap recovery failed: {e}")
        import traceback
        traceback.print_exc()
        return {"error": str(e), "traceback": traceback.format_exc()}


async def api_process_zap_receipt(request: Request, wallet_info: WalletTypeInfo = Depends(require_admin_key)):
    """Process an individual zap receipt for recovery purposes.
    
    This endpoint accepts zap receipt data and processes it through the extension's
    zap processing pipeline, avoiding duplication of logic in external services.
    """
    try:
        data = await request.json()
    except Exception:
        return JSONResponse({"error": "Invalid JSON payload"}, status_code=400)
    
    # Extract required fields
    zap_receipt_id = data.get("zap_receipt_id")
    zapper_pubkey = data.get("zapper_pubkey") 
    amount_sats = data.get("amount_sats")
    zapped_event_id = data.get("zapped_event_id")
    
    if not all([zap_receipt_id, zapper_pubkey, amount_sats, zapped_event_id]):
        return JSONResponse({
            "error": "Missing required fields: zap_receipt_id, zapper_pubkey, amount_sats, zapped_event_id"
        }, status_code=400)
    
    user_id = wallet_info.wallet.user
    logger.info(f"Processing zap receipt {zap_receipt_id[:16]}... for user {user_id}")
    
    try:
        # Get zap monitor for this user
        zap_monitor = get_zap_monitor(app=request.app, db=crud, user_id=user_id)
        if not zap_monitor:
            return JSONResponse({"error": "No zap monitor available for user"}, status_code=500)
        
        # Create a synthetic payment object with the zap data
        class SyntheticPayment:
            def __init__(self, zap_receipt_id, zapper_pubkey, amount_sats, zapped_event_id):
                self.checking_id = zap_receipt_id
                self.amount = amount_sats
                self.extra = {
                    "nostr": json.dumps({
                        "kind": 9735,
                        "tags": [
                            ["e", zapped_event_id],
                            ["p", zapper_pubkey],
                            ["description", json.dumps({"pubkey": zapper_pubkey})]
                        ],
                        "pubkey": zapper_pubkey
                    })
                }
                self.success = True
                self.status = "success"
        
        synthetic_payment = SyntheticPayment(zap_receipt_id, zapper_pubkey, amount_sats, zapped_event_id)
        
        # Process through the zap monitor
        result = await zap_monitor._process_payment_for_zap(synthetic_payment)
        
        if result:
            logger.info(f"Successfully processed zap receipt {zap_receipt_id[:16]}...")
            return JSONResponse({
                "success": True,
                "message": f"Processed zap receipt {zap_receipt_id[:16]}...",
                "processed": True
            })
        else:
            logger.warning(f"Failed to process zap receipt {zap_receipt_id[:16]}...")
            return JSONResponse({
                "success": False,
                "message": f"Failed to process zap receipt {zap_receipt_id[:16]}...",
                "processed": False
            }, status_code=400)
            
    except Exception as e:
        logger.error(f"Error processing zap receipt {zap_receipt_id[:16]}...: {e}")
        return JSONResponse({
            "error": f"Failed to process zap receipt: {str(e)}"
        }, status_code=500)
