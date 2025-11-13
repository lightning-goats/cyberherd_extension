import asyncio
import json
import os
from datetime import datetime, time, timezone
from typing import Any

from loguru import logger

from lnbits.core.crud import get_wallet
from lnbits.tasks import create_permanent_unique_task, wait_for_paid_invoices

from .crud import (
    clear_processed_events_for_user,
    clear_processed_zaps_for_user,
    deactivate_all_cyberherd_members,
    list_settings_with_tracking,
    upsert_settings,
)
from .services.splits import reset_splits_to_predefined_wallet
from .services.payment_coordinator import (
    get_payment_coordinator as get_zap_monitor,
)
from .services.nostr_websocket_monitor import (
    start_monitor_for_user,
    trigger_immediate_refresh,
)
from . import crud as crud_module
from .views_api import _clear_cached_notes_for_user
from .utils.common import parse_bool_env, utc_now

# Conditional import for cyberherd_messaging
try:
    from lnbits.extensions.cyberherd_messaging.services import publish_note as ch_publish
    _ch_publish_available = True
except ImportError:
    ch_publish = None
    _ch_publish_available = False

INVOICE_LISTENER_NAME = "ext_cyberherd"
INVOICE_TASK_NAME = "ext_cyberherd_invoice_listener"
_APP_REF: Any | None = None


def parse_bool(value: str | bool | None, default: bool = False) -> bool:
    """Return True when *value* represents a truthy string/boolean."""
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y", "on"}
    return default


def _coerce_extra_dict(payment: Any) -> dict[str, Any]:
    """Ensure payment.extra is a dict for downstream processing."""
    extra = getattr(payment, "extra", None)
    if extra is None:
        extra = {}
    elif isinstance(extra, str):
        try:
            extra = json.loads(extra)
        except Exception:
            extra = {}
    elif not isinstance(extra, dict):
        try:
            extra = dict(extra)
        except Exception:
            extra = {}
    setattr(payment, "extra", extra)
    return extra


def _normalize_zap_request(payment: Any) -> dict[str, Any] | None:
    """
    Normalize zap request payload into payment.extra['nostr'].

    Accepts zap requests provided via:
    - payment.extra['nostr'] (dict or JSON string)
    - payment.extra['zap_request'] / ['zapRequest']
    - payment.extra['comment'] JSON payload (legacy)
    - payment.memo containing JSON zap request
    """
    extra = _coerce_extra_dict(payment)

    zap_request: dict[str, Any] | None = None
    zap_request_source: str | None = None

    nostr_value = extra.get("nostr")
    if nostr_value:
        if isinstance(nostr_value, dict):
            zap_request = nostr_value
        else:
            try:
                zap_request = json.loads(nostr_value)
            except Exception:
                zap_request = None
        zap_request_source = "nostr"

    if not zap_request:
        for key in ("zap_request", "zapRequest"):
            candidate = extra.get(key)
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
                break

    if not zap_request and isinstance(extra.get("comment"), str):
        comment = extra["comment"].strip()
        if comment.startswith("{"):
            try:
                zap_request = json.loads(comment)
                zap_request_source = "comment"
            except json.JSONDecodeError:
                zap_request = None

    if not zap_request and isinstance(getattr(payment, "memo", None), str):
        memo = payment.memo.strip()
        if memo.startswith("{"):
            try:
                zap_request = json.loads(memo)
                zap_request_source = "memo"
            except json.JSONDecodeError:
                zap_request = None

    if zap_request:
        extra["nostr"] = zap_request
        logger.debug(
            "CyberHerd: normalized zap request from %s field for payment %s",
            zap_request_source or "unknown",
            getattr(payment, "checking_id", None) or getattr(payment, "payment_hash", None),
        )
    else:
        logger.debug(
            "CyberHerd: payment %s missing zap request payload",
            getattr(payment, "checking_id", None) or getattr(payment, "payment_hash", None),
        )

    return zap_request


async def process_incoming_payment(payment: Any, app: Any | None = None) -> None:
    """Dispatcher invoked by invoice listener for LNbits payments.

    Note:
    - Payment-based zap detection is disabled (zaps are processed via Nostr kind 9735).
    - We still use payment events to trigger automatic splits when configured.
    """
    try:
        wallet_id = getattr(payment, "wallet_id", None)
        if not wallet_id:
            logger.debug("CyberHerd: skipping payment without wallet_id")
            return

        amount_msat = getattr(payment, "amount", 0) or 0
        amount_sats = amount_msat // 1000

        # Quiet by default: only log payment receipt at debug level
        logger.debug(
            "CyberHerd: PAYMENT EVENT RECEIVED | wallet=%s amount=%d msat (%d sats)",
            wallet_id,
            amount_msat,
            amount_sats,
        )

        try:
            wallet = await get_wallet(wallet_id)
        except Exception as exc:
            logger.error(f"CyberHerd: failed to load wallet {wallet_id}: {exc}")
            return

        if not wallet:
            logger.debug("CyberHerd: wallet %s not found, skipping payment", wallet_id)
            return

        user_id = getattr(wallet, "user", None)
        if not user_id:
            logger.debug("CyberHerd: wallet %s has no user, skipping payment", wallet_id)
            return

        # Wallet context log (debug level)
        try:
            wname = getattr(wallet, "name", None) or "<unnamed>"
            logger.debug("CyberHerd: wallet=%s user=%s", wname, str(user_id)[:8])
        except Exception:
            pass

        # Always trigger splits checks on any payment to the herd wallet
        monitor = get_zap_monitor(app=app, db=crud_module, user_id=user_id)
        try:
            await monitor._check_and_trigger_splits(payment)
            logger.debug("CyberHerd: splits check completed for user %s", str(user_id)[:8])
        except Exception as exc:
            logger.error(f"CyberHerd: error while checking/triggering splits: {exc}")
    except Exception as exc:
        logger.error(f"CyberHerd: process_incoming_payment error: {exc}")


def start_invoice_listener(app: Any | None = None) -> None:
    """Register background invoice listener when enabled."""
    enabled = parse_bool_env("CYBERHERD_USE_INVOICE_LISTENER", True)
    if not enabled:
        logger.info("CyberHerd: invoice listener disabled via CYBERHERD_USE_INVOICE_LISTENER")
        return

    async def _handler(payment: Any) -> None:
        await process_incoming_payment(payment, app=app)

    listener = wait_for_paid_invoices(INVOICE_LISTENER_NAME, _handler)
    create_permanent_unique_task(INVOICE_TASK_NAME, listener)
    logger.info("CyberHerd: invoice listener registered as %s", INVOICE_LISTENER_NAME)


def get_invoice_listener_status() -> dict[str, Any]:
    """Return registration status of the CyberHerd invoice listener."""
    from lnbits.tasks import invoice_listeners

    registered = INVOICE_LISTENER_NAME in invoice_listeners
    return {
        "registered": registered,
        "enabled": registered,
        "name": INVOICE_LISTENER_NAME,
    }


async def _restart_kind1_subscriptions_for_user(
    user_id: str | None, app: Any | None
) -> bool:
    """Ensure monitors refresh their kind 1/30311 subscriptions after reset."""
    if not user_id:
        return False

    uid_short = f"{user_id[:12]}..." if len(user_id) > 12 else user_id
    restarted = False

    try:
        await start_monitor_for_user(user_id, app=app)
    except Exception as exc:
        logger.warning(
            f"CyberHerd midnight reset: failed to ensure monitor for user {uid_short}: {exc}"
        )

    try:
        await trigger_immediate_refresh(user_id)
        restarted = True
        logger.info(
            f"CyberHerd midnight reset: restarted kind 1/30311 subscriptions for user {uid_short}"
        )
    except Exception as exc:
        logger.warning(
            f"CyberHerd midnight reset: failed to refresh kind 1/30311 subscriptions for user {uid_short}: {exc}"
        )

    return restarted


async def midnight_reset_job(app: Any | None = None):
    global _APP_REF
    if app is None:
        app = _APP_REF
    """Deactivate all members and reset splits for every tracking-enabled settings row.

    Previously only the first/global row was handled which could skip multi-tenant users
    and skip split resets when `source_wallet` wasn't present on the legacy row. Now we:
      1. Deactivate members for each user (iterate all settings).
      2. Iterate each settings row with zap tracking enabled and a source wallet and reset
         its splits to the predefined zap wallet (100%) or clear if none present.
    """
    start_time = utc_now()
    logger.info(f"🕛 CyberHerd midnight reset starting at {start_time.isoformat()}")
    
    total_users = 0
    total_members_deactivated = 0
    total_wallets_reset = 0
    total_processed_cleared = 0
    total_tracked_reset = 0
    total_subscriptions_restarted = 0
    errors = []
    
    try:
        # Get all settings rows to iterate through all users
        settings_rows = await list_settings_with_tracking(require_herd_wallet=False)
        if not settings_rows:
            logger.info("CyberHerd midnight reset: no tracking-enabled settings rows found")
            return
        
        total_users = len(settings_rows)
        logger.info(f"CyberHerd midnight reset: processing {total_users} user(s)")
        
        # Deactivate members for each user (only if they opted into midnight reset)
        for s in settings_rows:
            try:
                if not getattr(s, "midnight_reset_enabled", True):
                    logger.debug(f"CyberHerd midnight reset: skipping user {getattr(s,'user_id',None)[:12] + '...' if getattr(s,'user_id',None) else 'global'} - midnight_reset_enabled=False")
                    continue
            except Exception:
                # If attribute access fails, default to performing the reset
                pass
            user_id = getattr(s, "user_id", None)
            if user_id:
                try:
                    result = await deactivate_all_cyberherd_members(user_id)
                    # Count how many members were deactivated
                    # deactivate_all_cyberherd_members may return count or None
                    member_count = result if isinstance(result, int) else 0
                    total_members_deactivated += member_count
                    logger.info(f"CyberHerd: deactivated {member_count} member(s) for user {user_id[:12]}...")
                except Exception as e:
                    error_msg = f"Failed to deactivate members for user {user_id[:12]}...: {e}"
                    logger.warning(f"CyberHerd midnight reset: {error_msg}")
                    errors.append(error_msg)
                # Clear processed zap/event history (idempotency reset)
                try:
                    await clear_processed_events_for_user(user_id)
                    await clear_processed_zaps_for_user(user_id)
                    total_processed_cleared += 1
                except Exception as e:
                    error_msg = f"Failed to clear processed events for user {user_id[:12]}...: {e}"
                    logger.warning(f"CyberHerd midnight reset: {error_msg}")
                    errors.append(error_msg)
                # Reset tracked events/timestamps for fresh detection next day
                try:
                    setattr(s, "tracked_event_ids", [])
                    setattr(s, "tracked_event_addresses", {})
                    setattr(s, "tracked_event_timestamps", {})
                    await upsert_settings(s, user_id)
                    total_tracked_reset += 1
                except Exception as e:
                    error_msg = f"Failed to reset tracked events for user {user_id[:12]}...: {e}"
                    logger.warning(f"CyberHerd midnight reset: {error_msg}")
                    errors.append(error_msg)
                # Clear cached today notes similar to manual reset behaviour
                if app is not None:
                    try:
                        _clear_cached_notes_for_user(app, user_id=user_id)
                    except Exception as e:
                        logger.debug(f"CyberHerd midnight reset: note cache clear skipped for user {user_id[:12]}...: {e}")
                # Restart kind 1/30311 note subscriptions so the new day begins fresh
                try:
                    restarted = await _restart_kind1_subscriptions_for_user(user_id, app)
                    if restarted:
                        total_subscriptions_restarted += 1
                    else:
                        error_msg = f"Note subscriptions not restarted for user {user_id[:12]}..."
                        logger.warning(f"CyberHerd midnight reset: {error_msg}")
                        errors.append(error_msg)
                except Exception as e:
                    error_msg = f"Failed to restart note subscriptions for user {user_id[:12]}...: {e}"
                    logger.warning(f"CyberHerd midnight reset: {error_msg}")
                    errors.append(error_msg)
        
        # Reset splits for each source wallet (only for settings that opted in)
        for s in settings_rows:
            try:
                if not getattr(s, "midnight_reset_enabled", True):
                    logger.debug(f"CyberHerd midnight reset: skipping split reset for user {getattr(s,'user_id',None)[:12] + '...' if getattr(s,'user_id',None) else 'global'} - midnight_reset_enabled=False")
                    continue
            except Exception:
                pass
            sw = getattr(s, "source_wallet", None)
            if not sw:
                logger.debug("CyberHerd midnight reset: skipping row without source_wallet")
                continue
            try:
                uid = getattr(s, "user_id", None)
                result = await reset_splits_to_predefined_wallet(sw, user_id=uid)
                targets = result.get("targets", []) if isinstance(result, dict) else []
                logger.info(
                    f"CyberHerd midnight reset: splits reset for source_wallet={sw} targets={len(targets)}"
                )
                total_wallets_reset += 1
            except Exception as e:  # pragma: no cover
                error_msg = f"Split reset failed for source_wallet={sw}: {e}"
                logger.warning(f"CyberHerd midnight reset: {error_msg}")
                errors.append(error_msg)
    except Exception as e:  # pragma: no cover
        logger.error(f"CyberHerd midnight reset job failed: {e}")
    except Exception as e:  # pragma: no cover
        logger.error(f"CyberHerd midnight reset job failed: {e}")
        errors.append(f"Job failed: {e}")
    
    # Final summary log
    duration = (utc_now() - start_time).total_seconds()
    logger.success(
        f"✅ CyberHerd midnight reset completed in {duration:.2f}s | "
        f"Users: {total_users} | Members deactivated: {total_members_deactivated} | "
        f"Wallets reset: {total_wallets_reset} | Processed clears: {total_processed_cleared} | "
        f"Tracked resets: {total_tracked_reset} | Subscriptions restarted: {total_subscriptions_restarted} | "
        f"Errors: {len(errors)}"
    )
    
    if errors:
        logger.warning(f"CyberHerd midnight reset errors: {errors}")


async def wait_for_midnight():
    """Sleep until the next midnight in the system's local timezone.

    Uses the system's local timezone to calculate the next midnight.
    This ensures the reset occurs at midnight according to the server's
    configured timezone, automatically handling DST transitions.
    """
    from datetime import timedelta
    
    now_utc = utc_now()
    
    # Get system local time using datetime.now() without timezone
    # Then make it timezone-aware using astimezone()
    now_local = datetime.now().astimezone()
    local_tz = now_local.tzinfo
    local_tz_name = now_local.tzname() or "Local"
    
    # Build next midnight in local timezone
    # Start with today's midnight
    local_midnight = datetime.combine(now_local.date(), time(0, 0))
    local_midnight = local_midnight.replace(tzinfo=local_tz)
    
    # If we're already past midnight today, schedule for tomorrow's midnight
    if now_local >= local_midnight:
        local_midnight = local_midnight + timedelta(days=1)
    
    # Convert local midnight to UTC for sleep calculation
    target_utc = local_midnight.astimezone(timezone.utc)
    sleep_seconds = (target_utc - now_utc).total_seconds()
    if sleep_seconds < 0:
        sleep_seconds = 0
    
    logger.info(
        f"CyberHerd: waiting {sleep_seconds:.2f} seconds until next midnight reset "
        f"(target local={local_midnight.isoformat()} [{local_tz_name}] | target UTC={target_utc.isoformat()})"
    )
    await asyncio.sleep(sleep_seconds)


async def schedule_midnight_reset():
    """
    Schedules the midnight reset job to run daily.
    """
    global _APP_REF
    while True:
        await wait_for_midnight()
        await midnight_reset_job(app=_APP_REF)


def cyberherd_tasks(app: Any | None = None):
    global _APP_REF
    if app is not None:
        _APP_REF = app
    """
    Creates a permanent unique task to schedule the midnight reset job.
    """
    logger.info("🔧 CyberHerd: Initializing background tasks...")
    
    try:
        # Schedule midnight reset job
        create_permanent_unique_task("cyberherd_midnight_reset", schedule_midnight_reset)
        logger.success("✅ CyberHerd: Midnight reset task created successfully (task_id='cyberherd_midnight_reset')")
        
        # Log next scheduled run time using system local timezone
        now_local = datetime.now().astimezone()
        local_tz_name = now_local.tzname() or "Local"
        from datetime import timedelta
        
        # Calculate next midnight in local time
        next_midnight_local = datetime.combine(now_local.date(), time(0, 0))
        next_midnight_local = next_midnight_local.replace(tzinfo=now_local.tzinfo)
        if now_local >= next_midnight_local:
            next_midnight_local = next_midnight_local + timedelta(days=1)
        
        # Convert to UTC for accurate time calculation
        now_utc = utc_now()
        next_midnight_utc = next_midnight_local.astimezone(timezone.utc)
        seconds_until = (next_midnight_utc - now_utc).total_seconds()
        
        logger.info(
            f"⏰ CyberHerd: Next midnight reset scheduled in {seconds_until/3600:.1f} hours "
            f"(local: {next_midnight_local.isoformat()} [{local_tz_name}])"
        )
    except Exception as e:
        logger.error(f"❌ CyberHerd: Failed to create midnight reset task: {e}")
        raise
