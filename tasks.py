import asyncio
import json
from contextlib import nullcontext
from datetime import datetime, time, timezone
from typing import Any

from loguru import logger
from sqlalchemy import text

from lnbits.core.crud import get_wallet
from lnbits.tasks import create_permanent_unique_task, wait_for_paid_invoices

from .services.splits import reset_splits_to_predefined_wallet
from .services.payment_coordinator import (
    get_payment_coordinator as get_zap_monitor,
)
from .services.nostr_websocket_monitor import (
    start_monitor_for_user,
    trigger_immediate_refresh,
    clear_monitor_cache_for_user,
)
from . import crud as crud_module
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
    - Payment-based zap detection is the PRIMARY and AUTHORITATIVE path for zap processing.
    - Kind 9735 Nostr subscriptions have been disabled to prevent race conditions.
    - This ensures "at-most-once" processing by relying on the single source of truth (payments).
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

        # Get monitor to access zap processing
        monitor = get_zap_monitor(app=app, db=crud_module, user_id=user_id)
        
        # Process payment for zap (realtime detection)
        try:
            await monitor._process_payment_for_zap(payment)
            logger.debug("CyberHerd: payment zap processing completed for user %s", str(user_id)[:8])
        except Exception as exc:
            logger.debug(f"CyberHerd: payment zap processing skipped or failed: {exc}")

        # Always trigger splits checks on any payment to the herd wallet
        try:
            await monitor._check_and_trigger_splits(payment)
            logger.debug("CyberHerd: splits check completed for user %s", str(user_id)[:8])
        except Exception as exc:
            logger.error(f"CyberHerd: error while checking/triggering splits: {exc}")
    except Exception as exc:
        logger.error(f"CyberHerd: process_incoming_payment error: {exc}")


def start_invoice_listener(app: Any | None = None) -> Any | None:
    """Register background invoice listener when enabled.

    Returns the created task handle (or None if disabled) so the caller can
    track it for cancellation on extension shutdown/deactivate.
    """
    enabled = parse_bool_env("CYBERHERD_USE_INVOICE_LISTENER", True)
    if not enabled:
        logger.info("CyberHerd: invoice listener disabled via CYBERHERD_USE_INVOICE_LISTENER")
        return None

    async def _handler(payment: Any) -> None:
        await process_incoming_payment(payment, app=app)

    listener = wait_for_paid_invoices(INVOICE_LISTENER_NAME, _handler)
    task = create_permanent_unique_task(INVOICE_TASK_NAME, listener)
    logger.info(f"CyberHerd: invoice listener registered as {INVOICE_LISTENER_NAME}")
    return task


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

    # Clear in-memory deduplication cache to ensure notes from the new day
    # are processed even if they were just seen (preventing race conditions)
    try:
        await clear_monitor_cache_for_user(user_id)
    except Exception as exc:
        logger.warning(
            f"CyberHerd midnight reset: failed to clear monitor cache for user {uid_short}: {exc}"
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


_membership_day_lock = asyncio.Lock()


async def ensure_membership_day(
    user_id: str | None = None, app: Any | None = None, *, admission_lock_held: bool = False
) -> list[str]:
    """Catch up opted-in local days before admission, payout, or monitor startup.

    Missing markers initialize today without erasing legacy membership. Database
    reset and day advancement commit together; failed split resets remain pending
    and block intake until a retry succeeds. Idempotency ledgers survive rollover.
    """
    from .services.time_utils import get_day_boundaries_utc
    from .services.headbutt import _get_headbutt_lock

    reset_users = []
    # Admission callers hold this lock already and recheck the day after queuing.
    admission_lock = nullcontext() if admission_lock_held else _get_headbutt_lock()
    async with admission_lock, _membership_day_lock:
        database = crud_module.db
        schema = database.references_schema
        query = f"SELECT * FROM {schema}settings WHERE midnight_reset_enabled = 1"
        params = {}
        if user_id is not None:
            query += " AND user_id = :user_id"
            params["user_id"] = user_id
        # Do not fail open when storage is unavailable.
        settings_rows = await database.fetchall(query, params)
        today = get_day_boundaries_utc().local_day_str
        for row in settings_rows:
            uid = row.get("user_id")
            if not uid:
                continue
            values = {"user_id": uid, "local_day": today}
            source_wallet = row.get("source_wallet")
            async with database.connect() as conn:
                # Connection.execute commits automatically; use the underlying
                # transaction so membership and its durable day advance together.
                async def execute(query, values):
                    await conn.conn.execute(
                        text(conn.rewrite_query(query)), conn.rewrite_values(values)
                    )

                async with conn.conn.begin():
                    await execute(
                        f"""INSERT INTO {schema}membership_reset_days (user_id, local_day)
                        VALUES (:user_id, :local_day) ON CONFLICT (user_id) DO NOTHING""",
                        values,
                    )
                    marker = await conn.fetchone(
                        f"SELECT local_day, splits_pending FROM {schema}membership_reset_days WHERE user_id = :user_id"
                        + (" FOR UPDATE" if database.type != "SQLITE" else ""),
                        {"user_id": uid},
                    )
                    if marker["local_day"] < today:
                        await execute(
                            f"""UPDATE {schema}cyber_herd SET is_active = 0, amount = 0,
                            payouts = 0.0, event_id = NULL, note = NULL, kinds = NULL
                            WHERE user_id = :user_id""", {"user_id": uid},
                        )
                        await execute(
                            f"""UPDATE {schema}settings SET tracked_event_ids = '[]',
                            tracked_event_addresses = '{{}}', tracked_event_timestamps = '{{}}'
                            WHERE user_id = :user_id""", {"user_id": uid},
                        )
                        await execute(
                            f"""UPDATE {schema}membership_reset_days SET local_day = :local_day,
                            splits_pending = :pending WHERE user_id = :user_id""",
                            {**values, "pending": int(bool(source_wallet))},
                        )
                        reset_users.append(uid)
                        pending = bool(source_wallet)
                    else:
                        pending = bool(marker["splits_pending"])
            if uid in reset_users:
                await clear_monitor_cache_for_user(uid)
            if pending:
                if source_wallet:
                    await reset_splits_to_predefined_wallet(source_wallet, user_id=uid)
                await database.execute(
                    f"UPDATE {schema}membership_reset_days SET splits_pending = 0 WHERE user_id = :user_id",
                    {"user_id": uid},
                )
    return reset_users


async def midnight_reset_job(app: Any | None = None):
    """Apply due local-day resets, including social-only opted-in users."""
    app = app if app is not None else _APP_REF
    reset_users = await ensure_membership_day(app=app)
    # Intake can win the midnight race and advance the day first. Refresh every
    # opted-in user's subscriptions even if no database reset remains due.
    database = crud_module.db
    rows = await database.fetchall(
        f"SELECT user_id FROM {database.references_schema}settings WHERE midnight_reset_enabled = 1"
    )
    for row in rows:
        if row.get("user_id"):
            await _restart_kind1_subscriptions_for_user(row["user_id"], app)
    logger.info(f"CyberHerd daily reset: {len(reset_users)} herd(s) advanced")


async def wait_for_midnight():
    """Sleep until the next midnight in the server's local timezone.

    Compute the next local midnight and convert to UTC using timezone offsets
    to determine the sleep interval. Logs include both local and UTC targets.
    """
    from datetime import timedelta
    from .services.time_utils import _get_system_local_timezone

    now_utc = utc_now()

    # Determine server local timezone and current local time. Use a DST-aware
    # named zone (not a fixed offset) so the reset fires at real local midnight
    # on DST transition days.
    local_tz = _get_system_local_timezone()
    now_local = now_utc.astimezone(local_tz)
    local_tz_name = now_local.tzname() or "Local"

    # Build next local midnight using tz-aware time (handles DST correctly)
    local_midnight = datetime.combine(now_local.date(), time(0, 0), tzinfo=local_tz)
    if now_local >= local_midnight:
        next_date = (now_local + timedelta(days=1)).date()
        local_midnight = datetime.combine(next_date, time(0, 0), tzinfo=local_tz)

    # Convert local target to UTC and compute sleep interval
    target_utc = local_midnight.astimezone(timezone.utc)
    sleep_seconds = (target_utc - now_utc).total_seconds()
    if sleep_seconds < 0:
        sleep_seconds = 0

    logger.info(
        f"CyberHerd: waiting {sleep_seconds:.2f} seconds until next midnight reset "
        f"(local={local_midnight.isoformat()} [{local_tz_name}] | UTC={target_utc.isoformat()})"
    )
    await asyncio.sleep(sleep_seconds)


async def schedule_midnight_reset():
    """
    Schedules the midnight reset job to run daily.
    """
    while True:
        await midnight_reset_job(app=_APP_REF)
        await wait_for_midnight()


def cyberherd_tasks(app: Any | None = None):
    global _APP_REF
    if app is not None:
        _APP_REF = app
    """
    Creates a permanent unique task to schedule the midnight reset job.
    """
    logger.info("🔧 CyberHerd: Initializing background tasks...")

    reset_task = None
    try:
        # Schedule midnight reset job
        reset_task = create_permanent_unique_task("cyberherd_midnight_reset", schedule_midnight_reset)
        logger.success("✅ CyberHerd: Midnight reset task created successfully (task_id='cyberherd_midnight_reset')")
        
        # Log next scheduled run time using local midnight converted to UTC
        from datetime import timedelta

        now_utc = utc_now()
        from .services.time_utils import _get_system_local_timezone
        local_tz = _get_system_local_timezone()
        now_local = now_utc.astimezone(local_tz)
        local_tz_name = now_local.tzname() or "Local"

        next_midnight_local = datetime.combine(now_local.date(), time(0, 0, tzinfo=local_tz))
        if now_local >= next_midnight_local:
            next_date = (now_local + timedelta(days=1)).date()
            next_midnight_local = datetime.combine(next_date, time(0, 0, tzinfo=local_tz))

        next_midnight_utc = next_midnight_local.astimezone(timezone.utc)
        seconds_until = (next_midnight_utc - now_utc).total_seconds()

        logger.info(
            f"⏰ CyberHerd: Next midnight reset in {seconds_until/3600:.1f} hours "
            f"(local: {next_midnight_local.isoformat()} [{local_tz_name}] | UTC: {next_midnight_utc.isoformat()})"
        )
    except Exception as e:
        logger.error(f"❌ CyberHerd: Failed to create midnight reset task: {e}")
        raise

    return reset_task
