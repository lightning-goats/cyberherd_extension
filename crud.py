from __future__ import annotations

import json
import time
from datetime import datetime, timezone
import asyncio
from typing import Any
from contextlib import asynccontextmanager
from contextvars import ContextVar
from sqlalchemy.sql import text

from loguru import logger

from lnbits.db import Database # type: ignore
from .models import CyberherdSettings, LegacyCyberherdMember
from .services.shares import compute_member_share_percentages
from .utils.common import extract_t_tags_from_event, coerce_bool

db = Database("ext_cyberherd")
# Bind a non-autocommitting connection only to the task owning an admission.
# Child tasks inherit ContextVars but must never inherit this connection.
_admission_context: ContextVar[Any] = ContextVar("cyberherd_admission", default=None)


class _AdmissionConnection:
    def __init__(self, connection):
        self.connection = connection
        self.error = None

    async def _query(self, method, *args, **kwargs):
        if self.error is not None:
            raise self.error
        try:
            return await method(*args, **kwargs)
        except BaseException as exc:
            self.error = exc
            raise

    async def fetchone(self, *args, **kwargs):
        return await self._query(self.connection.fetchone, *args, **kwargs)

    async def fetchall(self, *args, **kwargs):
        return await self._query(self.connection.fetchall, *args, **kwargs)

    async def execute(self, query, values=None):
        conn = self.connection
        return await self._query(
            conn.conn.execute,
            text(conn.rewrite_query(query)), conn.rewrite_values(values or {})
        )

    def __getattr__(self, name):
        return getattr(self.connection, name)


def _current_admission():
    state = _admission_context.get()
    return state if state and state[0] is asyncio.current_task() else None


def in_admission_transaction() -> bool:
    return _current_admission() is not None


def _db():
    state = _current_admission()
    return state[1] if state else db


def defer_after_commit(callback) -> None:
    state = _current_admission()
    if state is None:
        raise RuntimeError("after-commit callback requires an admission transaction")
    state[2].append(callback)


async def _run_after_commit(callback, *args, **kwargs):
    if in_admission_transaction():
        defer_after_commit(lambda: callback(*args, **kwargs))
    else:
        await callback(*args, **kwargs)


@asynccontextmanager
async def admission_transaction(*, after_commit=None):
    """Commit membership, processed identity and totals together, or roll back all.

    LNbits Connection.execute normally commits each statement; the scoped wrapper
    bypasses that behavior while preserving LNbits SQL/value normalization.
    Deferred notifications run only after the connection and context are released.
    """
    if in_admission_transaction():
        yield
        return
    callbacks = []
    async with db.connect() as connection:
        scoped_connection = _AdmissionConnection(connection)
        token = _admission_context.set(
            (asyncio.current_task(), scoped_connection, callbacks)
        )
        try:
            yield
            if scoped_connection.error is not None:
                raise scoped_connection.error
            await connection.conn.commit()
        except BaseException:
            await connection.conn.rollback()
            raise
        finally:
            _admission_context.reset(token)
    if after_commit is not None:
        after_commit.extend(callbacks)
        return
    for callback in callbacks:
        try:
            await callback()
        except Exception as exc:
            logger.warning(f"CyberHerd after-commit notification failed: {exc}")


@asynccontextmanager
async def _admission_write_lock():
    # The transaction already serializes this DB connection. Acquiring the
    # ordinary CRUD lock here could deadlock with a writer waiting on db.lock.
    yield


# Serialize critical write operations in this module to reduce sqlite locking
_crud_write_lock: asyncio.Lock | None = None


def _get_crud_write_lock():
    if in_admission_transaction():
        return _admission_write_lock()
    global _crud_write_lock
    if _crud_write_lock is None:
        _crud_write_lock = asyncio.Lock()
    return _crud_write_lock


def get_utc_day_boundaries(days_ago: int = 0) -> tuple[datetime, datetime, int, int]:
    """Get UTC day boundaries for cyberherd calculations (UTC-first approach).
    
    DEPRECATED: Use services.time_utils.get_day_boundaries_utc() instead.
    This wrapper exists for backward compatibility only.
    
    All new code should import from time_utils:
        from .services.time_utils import get_day_boundaries_utc
        boundaries = get_day_boundaries_utc(days_ago=0)
    
    Args:
        days_ago: How many days back to calculate (0 = today, 1 = yesterday, etc.)
    
    Returns:
        Tuple of (midnight_utc, midnight_local, since_timestamp, until_timestamp)
        - midnight_utc: UTC midnight datetime (timezone-aware) - PRIMARY
        - midnight_local: Local midnight datetime (timezone-aware) - for display only
        - since_timestamp: Unix timestamp for start of day in UTC
        - until_timestamp: Unix timestamp for end of day in UTC
    """
    # Delegate to shared time_utils module
    from .services.time_utils import get_day_boundaries_utc as get_boundaries
    boundaries = get_boundaries(days_ago)
    
    # Return in legacy tuple format for backward compatibility
    return (
        boundaries.utc_midnight,
        boundaries.local_midnight,
        boundaries.utc_since_ts,
        boundaries.utc_until_ts
    )


def get_local_day_boundaries(days_ago: int = 0) -> tuple[datetime, datetime, int, int]:
    """DEPRECATED: Use get_utc_day_boundaries() instead.
    
    Legacy wrapper that returns values in old order for backward compatibility.
    Will be removed in future version.
    """
    midnight_utc, midnight_local, since_timestamp, until_timestamp = get_utc_day_boundaries(days_ago)
    # Return in old order: (midnight_local, midnight_utc, ...)
    return midnight_local, midnight_utc, since_timestamp, until_timestamp


def get_local_date_key(days_ago: int = 0) -> str:
    """Get local date key for caching.
    
    Returns the local date in ISO format for cache keys.
    This ensures cache keys are based on system's local date, not UTC date.
    """
    from datetime import timedelta
    
    # Use system's local timezone
    now_local = datetime.now().astimezone()
    target_date = now_local.date() - timedelta(days=days_ago)
    return target_date.isoformat()


async def get_settings(user_id: str | None = None) -> CyberherdSettings:
    try:
        # Attempt direct fetch by user first (multi-tenant row)
        row = None
        if user_id:
            row = await _db().fetchone(
                f"SELECT * FROM {db.references_schema}settings WHERE user_id = :user_id",
                {"user_id": user_id},
            )
            if row:
                try:
                    logger.debug(f"Cyberherd CRUD get_settings: user_id={user_id} row exists")
                except Exception:
                    pass
                return _row_to_settings(row)

        # Legacy/global reads must be explicit; unconfigured users must not
        # inherit another operator's wallets or private settings.
        if not row and not user_id:
            try:
                legacy_row = await _db().fetchone(
                    f"SELECT * FROM {db.references_schema}settings WHERE user_id IS NULL ORDER BY source_wallet LIMIT 1"
                )
            except Exception:
                legacy_row = None

            if legacy_row:
                try:
                    logger.debug("Cyberherd CRUD get_settings: using legacy/global settings row")
                except Exception:
                    pass
                return _row_to_settings(legacy_row)

        # No stored settings were found for the requested user or global scope;
        # return a default instance so callers can continue with safe defaults.
        return CyberherdSettings()
    except Exception as e:
        # Table doesn't exist - migrations haven't run yet
        # Don't try to self-heal, let migrations handle it
        msg = str(e).lower()
        if "undefinedtable" in msg or "no such table" in msg or "does not exist" in msg:
            logger.warning(f"Cyberherd: settings table doesn't exist yet (migrations not run?): {e}")
            return CyberherdSettings()
        # For other errors, re-raise
        raise


async def get_all_settings() -> list[CyberherdSettings]:
    """Get settings for all users.
    
    Returns:
        List of CyberherdSettings objects for all users
    """
    try:
        rows = await _db().fetchall(
            f"SELECT * FROM {db.references_schema}settings"
        )
        return [_row_to_settings(row) for row in rows]
    except Exception as e:
        # Table doesn't exist - migrations haven't run yet
        msg = str(e).lower()
        if "undefinedtable" in msg or "no such table" in msg or "does not exist" in msg:
            logger.warning(f"Cyberherd: settings table doesn't exist yet: {e}")
            return []
        # For other errors, re-raise
        raise


async def _bootstrap_settings_storage():
    """Idempotently create schema/table/columns/index for settings if missing.

    Safe to run at runtime; guards each DDL so it works on Postgres and SQLite.
    """
    if in_admission_transaction():
        raise RuntimeError("CyberHerd schema must be migrated before admission")
    table_name = f"{db.references_schema}settings"
    index_name = "idx_cyberherd_settings_user"
    
    stmts = [
        f"""CREATE TABLE IF NOT EXISTS {table_name} (
            source_wallet TEXT,
            max_members INTEGER DEFAULT 3,
            tracked_tags TEXT DEFAULT '[]'
        );""",
        f"ALTER TABLE {table_name} ADD COLUMN nostr_private_key TEXT;",
        f"ALTER TABLE {table_name} ADD COLUMN user_id TEXT;",
        f"ALTER TABLE {table_name} ADD COLUMN nostr_pubkey_override TEXT;",
        f"ALTER TABLE {table_name} ADD COLUMN zap_wallet TEXT;",
        f"ALTER TABLE {table_name} ADD COLUMN zap_wallet_alias TEXT;",
        f"ALTER TABLE {table_name} ADD COLUMN zap_tracking_enabled INTEGER DEFAULT 0;",
        f"ALTER TABLE {table_name} ADD COLUMN herd_wallet TEXT;",
        f"ALTER TABLE {table_name} ADD COLUMN zap_monitor_mode TEXT DEFAULT 'websocket';",
        f"ALTER TABLE {table_name} ADD COLUMN computed_effective_pubkey TEXT;",
        f"ALTER TABLE {table_name} ADD COLUMN repost_tracking_enabled INTEGER DEFAULT 0;",
        f"ALTER TABLE {table_name} ADD COLUMN likes_tracking_enabled INTEGER DEFAULT 0;",
        f"ALTER TABLE {table_name} ADD COLUMN minimum_sats INTEGER DEFAULT 10;",
        f"ALTER TABLE {table_name} ADD COLUMN nip05_verification_enabled INTEGER DEFAULT 1;",
        # legacy column `manual_event_ids` is handled via migration below if needed;
        # tracked_event_ids is used by newer code; ensure it's present
        f"ALTER TABLE {table_name} ADD COLUMN tracked_event_ids TEXT;",
        f"ALTER TABLE {table_name} ADD COLUMN tracked_event_timestamps TEXT;",  # JSON dict: note_id -> created_at
        f"ALTER TABLE {table_name} ADD COLUMN tracked_event_addresses TEXT;",  # JSON dict: note_id -> 30311 a-tag
        # Index last for performance
        f"CREATE UNIQUE INDEX IF NOT EXISTS {index_name} ON {table_name}(user_id) WHERE user_id IS NOT NULL;",
    ]
    for s in stmts:
        try:
            # For ALTER TABLE ADD COLUMN statements, check if column exists first
            if s.strip().startswith("ALTER TABLE") and "ADD COLUMN" in s:
                # Extract table and column name from ALTER TABLE statement
                parts = s.split()
                if len(parts) >= 6 and parts[2] == table_name and parts[3] == "ADD" and parts[4] == "COLUMN":
                    column_name = parts[5]
                    if await _column_exists(table_name, column_name):
                        try:
                            logger.debug(f"Cyberherd bootstrap skipping existing column: {column_name}")
                        except Exception:
                            pass
                        continue
            await _db().execute(s)
            try:
                logger.debug(f"Cyberherd bootstrap executed: {s[:50]}...")
            except Exception:
                pass
        except Exception as e:
            try:
                logger.warning(f"Cyberherd bootstrap statement failed: {s[:50]}... error: {e}")
            except Exception:
                pass
    # If a legacy `manual_event_ids` column exists but `tracked_event_ids` does not,
    # migrate the values into tracked_event_ids. This covers DBs where the rename
    # migration wasn't applied but data exists.
    try:
        has_manual = await _column_exists(table_name, "manual_event_ids")
        has_tracked = await _column_exists(table_name, "tracked_event_ids")
        if has_manual and not has_tracked:
            try:
                await _db().execute(f"ALTER TABLE {table_name} ADD COLUMN tracked_event_ids TEXT;")
            except Exception:
                # ignore if another process added it concurrently
                pass
            try:
                # Perform per-row migration so we can normalize legacy formats.
                rows = await _db().fetchall(f"SELECT user_id, manual_event_ids, tracked_event_ids FROM {table_name}")
                migrated = 0
                for r in rows or []:
                    try:
                        existing = r.get("tracked_event_ids")
                        if existing and str(existing).strip():
                            continue
                        manual_raw = r.get("manual_event_ids")
                        if not manual_raw or (isinstance(manual_raw, str) and not manual_raw.strip()):
                            continue

                        # Normalize manual_raw into a JSON array string
                        try:
                            if isinstance(manual_raw, str):
                                parsed = json.loads(manual_raw)
                            else:
                                parsed = manual_raw
                        except Exception:
                            # Not valid JSON; treat as comma-separated list or single id
                            parsed = [p.strip() for p in str(manual_raw).split(",") if p.strip()]

                        if not isinstance(parsed, list):
                            parsed = [parsed]

                        normalized = json.dumps([str(x) for x in parsed if x])

                        if r.get("user_id"):
                            await _db().execute(
                                f"UPDATE {table_name} SET tracked_event_ids = :te WHERE user_id = :uid AND (tracked_event_ids IS NULL OR tracked_event_ids = '')",
                                {"te": normalized, "uid": r.get("user_id")},
                            )
                        else:
                            # legacy single-row (no user_id)
                            await _db().execute(
                                f"UPDATE {table_name} SET tracked_event_ids = :te WHERE user_id IS NULL AND (tracked_event_ids IS NULL OR tracked_event_ids = '')",
                                {"te": normalized},
                            )
                        migrated += 1
                    except Exception:
                        continue

                try:
                    logger.info(f"Cyberherd bootstrap: migrated {migrated} manual_event_ids -> tracked_event_ids")
                except Exception:
                    pass
            except Exception:
                try:
                    logger.warning("Cyberherd bootstrap: failed to migrate manual_event_ids to tracked_event_ids")
                except Exception:
                    pass
    except Exception:
        pass
    try:
        logger.debug("Cyberherd settings storage bootstrap completed (runtime self-heal)")
    except Exception:
        pass


async def _column_exists(schema_table: str, column: str) -> bool:
    """Inspect only this backend and schema; a missing column is not an error."""
    if "." in schema_table:
        schema, table = schema_table.split(".", 1)
    else:
        schema, table = db.schema, schema_table
    if db.type != "SQLITE":
        row = await _db().fetchone(
            "SELECT 1 FROM information_schema.columns "
            "WHERE table_schema = :schema AND table_name = :table "
            "AND column_name = :col LIMIT 1",
            {"schema": schema or "public", "table": table, "col": column},
        )
        return row is not None
    rows = await _db().fetchall(f"PRAGMA table_info({table})")
    return any(row["name"] == column for row in rows)


async def get_settings_for_source_wallet(source_wallet: str) -> CyberherdSettings:
    """Resolve the explicitly configured source wallet on either backend."""
    row = await _db().fetchone(
        f"SELECT * FROM {db.references_schema}settings WHERE source_wallet = :wid",
        {"wid": source_wallet},
    )
    return _row_to_settings(row) if row else CyberherdSettings()


async def get_settings_for_herd_wallet(herd_wallet: str) -> CyberherdSettings:
    """Resolve only the explicitly configured herd wallet, never a global row."""
    row = await _db().fetchone(
        f"SELECT * FROM {db.references_schema}settings WHERE herd_wallet = :wid",
        {"wid": herd_wallet},
    )
    return _row_to_settings(row) if row else CyberherdSettings()


async def list_settings_with_tracking(require_herd_wallet: bool = True) -> list[CyberherdSettings]:
    """List settings rows with zap tracking enabled.

    If require_herd_wallet is True, only include rows that have a herd_wallet configured.
    """
    try:
        if require_herd_wallet:
            rows = await _db().fetchall(
                f"SELECT * FROM {db.references_schema}settings WHERE zap_tracking_enabled = 1 AND herd_wallet IS NOT NULL AND TRIM(herd_wallet) != ''"
            )
        else:
            rows = await _db().fetchall(
                f"SELECT * FROM {db.references_schema}settings WHERE zap_tracking_enabled = 1"
            )
    except Exception:
        raise
    return [_row_to_settings(r) for r in rows]


async def get_cyberherd_settings_for_all_users() -> list[CyberherdSettings]:
    """Get all cyberherd settings for all users."""
    try:
        rows = await _db().fetchall(f"SELECT * FROM {db.references_schema}settings")
    except Exception:
        raise
    return [_row_to_settings(r) for r in rows]


def _row_to_settings(row) -> CyberherdSettings:
    tracked = row.get("tracked_tags")
    try:
        if isinstance(tracked, str):
            tracked_list = json.loads(tracked)
        else:
            tracked_list = tracked or []
    except Exception:
        tracked_list = []

    tracked_events = row.get("tracked_event_ids")
    try:
        if isinstance(tracked_events, str):
            tracked_events_list = json.loads(tracked_events)
        else:
            tracked_events_list = tracked_events or []
    except Exception:
        tracked_events_list = []

    manual_events_raw = row.get("manual_event_ids")
    manual_events_list: list[str] = []
    if manual_events_raw:
        try:
            if isinstance(manual_events_raw, str):
                manual_events_list = json.loads(manual_events_raw)
                if isinstance(manual_events_list, str):
                    manual_events_list = [manual_events_list]
            elif isinstance(manual_events_raw, list):
                manual_events_list = manual_events_raw
        except Exception:
            # Legacy column may be a comma separated string
            if isinstance(manual_events_raw, str):
                manual_events_list = [eid.strip() for eid in manual_events_raw.split(',') if eid.strip()]


    tracked_timestamps = row.get("tracked_event_timestamps")
    try:
        if isinstance(tracked_timestamps, str):
            tracked_timestamps_dict = json.loads(tracked_timestamps)
        else:
            tracked_timestamps_dict = tracked_timestamps or {}
    except Exception:
        tracked_timestamps_dict = {}

    tracked_addresses = row.get("tracked_event_addresses")
    try:
        if isinstance(tracked_addresses, str):
            tracked_addresses_dict = json.loads(tracked_addresses)
        else:
            tracked_addresses_dict = tracked_addresses or {}
    except Exception:
        tracked_addresses_dict = {}

    tracked_events_set = {eid for eid in tracked_events_list if eid}
    tracked_events_set.update(eid for eid in manual_events_list if eid)
    tracked_events_set.update(tracked_addresses_dict.keys())
    combined_tracked_events = sorted(tracked_events_set)

    return CyberherdSettings(
        source_wallet=row.get("source_wallet"),
        zap_wallet=row.get("zap_wallet"),
        zap_wallet_alias=row.get("zap_wallet_alias"),
        herd_wallet=row.get("herd_wallet"),
        max_members=3 if row.get("max_members") is None else row["max_members"],
        tracked_tags=tracked_list,
        tracked_event_ids=combined_tracked_events,
        tracked_event_timestamps=tracked_timestamps_dict,
        tracked_event_addresses=tracked_addresses_dict,
        nostr_pubkey_override=row.get("nostr_pubkey_override"),
        computed_effective_pubkey=row.get("computed_effective_pubkey"),
        zap_tracking_enabled=coerce_bool(row.get("zap_tracking_enabled"), True),
        zap_monitor_mode=row.get("zap_monitor_mode", "websocket"),
        templates_owner_user=row.get("templates_owner_user"),  # legacy read-only; no longer set via API
        repost_tracking_enabled=coerce_bool(row.get("repost_tracking_enabled"), False),
        likes_tracking_enabled=coerce_bool(row.get("likes_tracking_enabled"), False),
        midnight_reset_enabled=coerce_bool(row.get("midnight_reset_enabled"), True),
        minimum_sats=10 if row.get("minimum_sats") is None else row["minimum_sats"],
        nip05_verification_enabled=coerce_bool(row.get("nip05_verification_enabled"), True),
        feeder_trigger_sats=row.get("feeder_trigger_sats"),
        member_allocation_percent=(10 if row.get("member_allocation_percent") is None else row["member_allocation_percent"]),
        send_splits_enabled=coerce_bool(row.get("send_splits_enabled"), False),
        user_id=row.get("user_id"),
    )


async def upsert_settings(settings: CyberherdSettings, user_id: str | None = None):
    if not user_id:
        raise RuntimeError("upsert_settings: user_id is required")
    if not in_admission_transaction():
        async with admission_transaction():
            return await upsert_settings(settings, user_id)
    _t0 = time.time()
    tracked_json = json.dumps(getattr(settings, "tracked_tags", []) or [])

    try:
        logger.debug(f"Cyberherd upsert_settings START user_id={user_id}")
    except Exception:
        pass

    # Detect optional column once
    has_templates_col = await _column_exists("settings", "templates_owner_user")
    has_feeder_col = await _column_exists("settings", "feeder_trigger_sats")

    existing = None
    if user_id:
        try:
            existing = await _db().fetchone(
                f"SELECT * FROM {db.references_schema}settings WHERE user_id = :user_id", {"user_id": user_id}
            )
        except Exception as e:
            logger.error(f"Cyberherd upsert_settings existing fetch error user_id={user_id} err={e}")
            existing = None

    # Multi-tenant (user-scoped) UPDATE path
    async def _execute_with_retry(sql: str, params: dict | None = None, retries: int = 8, delay: float = 0.05):
        """Execute a DB statement with a retry loop for transient locks (SQLite).

        Uses exponential backoff. Safe to call for both Postgres and SQLite; non-retryable
        exceptions are re-raised immediately.
        """
        last_exc = None
        for attempt in range(retries):
            try:
                if params is None:
                    await _db().execute(sql)
                else:
                    await _db().execute(sql, params)
                return
            except Exception as exc:
                last_exc = exc
                msg = str(exc).lower()
                # Retry for transient sqlite lock errors
                if "database is locked" in msg or "locked" in msg or "busy" in msg:
                    backoff = delay * (2 ** attempt)
                    try:
                        await asyncio.sleep(backoff)
                    except Exception:
                        pass
                    continue
                # non-retryable
                raise
        # if we exhausted retries, re-raise the last exception
        if last_exc:
            raise last_exc

    if user_id and existing:
        update_cols = [
            "source_wallet = :source_wallet",
            "zap_wallet = :zap_wallet",
            "zap_wallet_alias = :zap_wallet_alias",
            "herd_wallet = :herd_wallet",
            "max_members = :max_members",
            "tracked_tags = :tracked_tags",
            "tracked_event_ids = :tracked_event_ids",
            "tracked_event_timestamps = :tracked_event_timestamps",
            "tracked_event_addresses = :tracked_event_addresses",
            "nostr_pubkey_override = :nostr_pubkey_override",
            "computed_effective_pubkey = :computed_effective_pubkey",
            "zap_tracking_enabled = :zap_tracking_enabled",
            "zap_monitor_mode = :zap_monitor_mode",
            "repost_tracking_enabled = :repost_tracking_enabled",
            "likes_tracking_enabled = :likes_tracking_enabled",
            "minimum_sats = :minimum_sats",
        ]
        if has_feeder_col:
            update_cols.append("feeder_trigger_sats = :feeder_trigger_sats")
        # Check for send_splits_enabled column
        has_send_splits_col = await _column_exists("settings", "send_splits_enabled")
        if has_send_splits_col:
            update_cols.append("send_splits_enabled = :send_splits_enabled")
        # Only include midnight_reset_enabled if the column exists in this DB
        has_midnight_col = await _column_exists("settings", "midnight_reset_enabled")
        if has_midnight_col:
            update_cols.insert(update_cols.index("minimum_sats = :minimum_sats"), "midnight_reset_enabled = :midnight_reset_enabled")
        # Only include nip05_verification_enabled if the column exists in this DB
        has_nip05_col = await _column_exists("settings", "nip05_verification_enabled")
        if has_nip05_col:
            update_cols.insert(update_cols.index("minimum_sats = :minimum_sats") + 1, "nip05_verification_enabled = :nip05_verification_enabled")
        # Only include member_allocation_percent if the column exists in this DB
        has_member_alloc_col = await _column_exists("settings", "member_allocation_percent")
        if has_member_alloc_col:
            update_cols.append("member_allocation_percent = :member_allocation_percent")
        if has_templates_col:
            update_cols.append("templates_owner_user = :templates_owner_user")
        sql = f"UPDATE {db.references_schema}settings SET " + ", ".join(update_cols) + " WHERE user_id = :user_id"
        params = {
            "source_wallet": getattr(settings, "source_wallet", None),
            "zap_wallet": getattr(settings, "zap_wallet", None),
            "zap_wallet_alias": getattr(settings, "zap_wallet_alias", None),
            "herd_wallet": getattr(settings, "herd_wallet", None),
            "max_members": int(getattr(settings, "max_members", 3)),
            "tracked_tags": tracked_json,
            "tracked_event_ids": json.dumps(getattr(settings, "tracked_event_ids", []) or []),
            "tracked_event_timestamps": json.dumps(getattr(settings, "tracked_event_timestamps", {}) or {}),
            "tracked_event_addresses": json.dumps(getattr(settings, "tracked_event_addresses", {}) or {}),
            "nostr_pubkey_override": getattr(settings, "nostr_pubkey_override", None),
            "computed_effective_pubkey": getattr(settings, "computed_effective_pubkey", None),
            "zap_tracking_enabled": int(getattr(settings, "zap_tracking_enabled", False)),
            "zap_monitor_mode": getattr(settings, "zap_monitor_mode", "websocket"),
            "repost_tracking_enabled": int(getattr(settings, "repost_tracking_enabled", False)),
            "likes_tracking_enabled": int(getattr(settings, "likes_tracking_enabled", False)),
            "minimum_sats": int(getattr(settings, "minimum_sats", 10)),
            "templates_owner_user": getattr(settings, "templates_owner_user", None),
            "user_id": user_id,
        }
        if has_feeder_col:
            feeder_value = getattr(settings, "feeder_trigger_sats", None)
            params["feeder_trigger_sats"] = int(feeder_value) if feeder_value not in (None, "") else None
        # Only add send_splits_enabled param if column exists
        if has_send_splits_col:
            params["send_splits_enabled"] = int(getattr(settings, "send_splits_enabled", False))
        # Only add midnight_reset_enabled param if column exists
        if has_midnight_col:
            params["midnight_reset_enabled"] = int(getattr(settings, "midnight_reset_enabled", True))
        # Only add nip05_verification_enabled param if column exists
        if has_nip05_col:
            params["nip05_verification_enabled"] = int(getattr(settings, "nip05_verification_enabled", True))
        # Only add member_allocation_percent param if column exists
        if has_member_alloc_col:
            params["member_allocation_percent"] = int(getattr(settings, "member_allocation_percent", 10))
        try:
            async with _get_crud_write_lock():
                await _execute_with_retry(sql, params)
        except Exception as e:
            import traceback

            try:
                logger.error(
                    "Cyberherd upsert_settings UPDATE EXC user_id=%s err=%s stack=%s",
                    user_id,
                    e,
                    traceback.format_exc(),
                )
            except Exception:
                pass

            # Check if this is a missing column error
            err_msg = str(e).lower()
            logger.warning(f"Cyberherd upsert_settings UPDATE: checking error message: {err_msg}")
            if (
                "does not exist" in err_msg
                or "undefined column" in err_msg
                or "repost_tracking_enabled" in err_msg
                or "likes_tracking_enabled" in err_msg
                or "minimum_sats" in err_msg
            ):
                # DO NOT call bootstrap - let migrations handle table creation
                logger.error(f"Cyberherd upsert_settings UPDATE: missing column detected, migrations need to run: {err_msg}")
            raise
        try:
            logger.debug(
                "Cyberherd CRUD upsert (update) user_id=%s source_wallet=%s override_present=%s",
                user_id,
                getattr(settings, "source_wallet", None),
                bool(getattr(settings, "nostr_pubkey_override", None)),
            )
        except Exception:
            pass
        try:
            _dur = int((time.time() - _t0) * 1000)
            logger.debug(f"Cyberherd upsert_settings END user_id={user_id} dur_ms={_dur}")
        except Exception:
            pass
        if any(existing.get(field) != getattr(settings, field, None)
               for field in ("minimum_sats", "member_allocation_percent", "zap_wallet")):
            await recompute_member_payouts(user_id)
        # Ensure predefined zap wallet allocation if no active members yet, else schedule recompute.
        try:
            from .services.splits import (
                # On updates we should not perform a hard reset of splits because
                # toggling settings (including enabling zap tracking) must not
                # overwrite existing split targets. Always schedule a recompute
                # which respects existing members and targets.
                schedule_split_recompute as _sched_recompute,
            )  # type: ignore
            if getattr(settings, "source_wallet", None):
                # Schedule a recompute which will adjust splits based on current
                # members/weights but will not forcibly reset to the predefined
                # zap wallet. The insert path still sets defaults for new rows.
                await _run_after_commit(_sched_recompute, str(settings.source_wallet), user_id=user_id)
        except Exception as e:  # pragma: no cover
            try:
                logger.debug(f"Cyberherd upsert_settings post-update split ensure skipped: {e}")
            except Exception:
                pass
        return

    # Multi-tenant (user-scoped) INSERT path
    if user_id and not existing:
        insert_cols = [
            "user_id",
            "source_wallet",
            "zap_wallet",
            "zap_wallet_alias",
            "max_members",
            "tracked_tags",
            "tracked_event_ids",
            "tracked_event_timestamps",
            "tracked_event_addresses",
            "nostr_pubkey_override",
            "computed_effective_pubkey",
            "zap_tracking_enabled",
            "herd_wallet",
            "zap_monitor_mode",
            "repost_tracking_enabled",
            "likes_tracking_enabled",
            "minimum_sats",
        ]
        if has_templates_col:
            insert_cols.append("templates_owner_user")
        if has_feeder_col:
            insert_cols.append("feeder_trigger_sats")
        placeholders = [":" + c for c in insert_cols]
        sql = f"INSERT INTO {db.references_schema}settings ({', '.join(insert_cols)}) VALUES ({', '.join(placeholders)})"
        # Only include midnight_reset_enabled param if column exists
        has_midnight_col = await _column_exists("settings", "midnight_reset_enabled")
        # Only include nip05_verification_enabled param if column exists
        has_nip05_col = await _column_exists("settings", "nip05_verification_enabled")
        params = {
            "user_id": user_id,
            "source_wallet": getattr(settings, "source_wallet", None),
            "zap_wallet": getattr(settings, "zap_wallet", None),
            "zap_wallet_alias": getattr(settings, "zap_wallet_alias", None),
            "max_members": int(getattr(settings, "max_members", 3)),
            "tracked_tags": tracked_json,
            "tracked_event_ids": json.dumps(getattr(settings, "tracked_event_ids", []) or []),
            "tracked_event_timestamps": json.dumps(getattr(settings, "tracked_event_timestamps", {}) or {}),
            "tracked_event_addresses": json.dumps(getattr(settings, "tracked_event_addresses", {}) or {}),
            "nostr_pubkey_override": getattr(settings, "nostr_pubkey_override", None),
            "computed_effective_pubkey": getattr(settings, "computed_effective_pubkey", None),
            "zap_tracking_enabled": int(getattr(settings, "zap_tracking_enabled", False)),
            "herd_wallet": getattr(settings, "herd_wallet", None),
            "zap_monitor_mode": getattr(settings, "zap_monitor_mode", "websocket"),
            "repost_tracking_enabled": int(getattr(settings, "repost_tracking_enabled", False)),
            "likes_tracking_enabled": int(getattr(settings, "likes_tracking_enabled", False)),
            "minimum_sats": int(getattr(settings, "minimum_sats", 10)),
            "templates_owner_user": getattr(settings, "templates_owner_user", None),
        }
        if has_midnight_col:
            # Ensure column is present in insert list and params
            insert_cols.insert(insert_cols.index("minimum_sats"), "midnight_reset_enabled")
            # Rebuild placeholders and SQL to include the column
            placeholders = [":" + c for c in insert_cols]
            sql = f"INSERT INTO {db.references_schema}settings ({', '.join(insert_cols)}) VALUES ({', '.join(placeholders)})"
            params["midnight_reset_enabled"] = int(getattr(settings, "midnight_reset_enabled", True))
        if has_nip05_col:
            # Ensure column is present in insert list and params
            if "nip05_verification_enabled" not in insert_cols:
                insert_cols.insert(insert_cols.index("minimum_sats") + 1, "nip05_verification_enabled")
            # Rebuild placeholders and SQL to include the column
            placeholders = [":" + c for c in insert_cols]
            sql = f"INSERT INTO {db.references_schema}settings ({', '.join(insert_cols)}) VALUES ({', '.join(placeholders)})"
            params["nip05_verification_enabled"] = int(getattr(settings, "nip05_verification_enabled", True))
        if has_feeder_col:
            # Ensure column appears in insert order if midnight column was inserted earlier
            if "feeder_trigger_sats" not in insert_cols:
                insert_cols.append("feeder_trigger_sats")
            placeholders = [":" + c for c in insert_cols]
            sql = f"INSERT INTO {db.references_schema}settings ({', '.join(insert_cols)}) VALUES ({', '.join(placeholders)})"
            feeder_value = getattr(settings, "feeder_trigger_sats", None)
            params["feeder_trigger_sats"] = int(feeder_value) if feeder_value not in (None, "") else None
        # Check for member_allocation_percent column
        has_member_alloc_col = await _column_exists("settings", "member_allocation_percent")
        if has_member_alloc_col:
            if "member_allocation_percent" not in insert_cols:
                insert_cols.append("member_allocation_percent")
            placeholders = [":" + c for c in insert_cols]
            sql = f"INSERT INTO {db.references_schema}settings ({', '.join(insert_cols)}) VALUES ({', '.join(placeholders)})"
            params["member_allocation_percent"] = int(getattr(settings, "member_allocation_percent", 10))
        if await _column_exists("settings", "send_splits_enabled"):
            insert_cols.append("send_splits_enabled")
            params["send_splits_enabled"] = int(settings.send_splits_enabled)
        placeholders = [":" + column for column in insert_cols]
        updates = [f"{column} = excluded.{column}" for column in insert_cols if column != "user_id"]
        sql = (
            f"INSERT INTO {db.references_schema}settings ({', '.join(insert_cols)}) "
            f"VALUES ({', '.join(placeholders)}) "
            "ON CONFLICT (user_id) WHERE user_id IS NOT NULL DO UPDATE SET "
            + ", ".join(updates)
        )
        try:
            async with _get_crud_write_lock():
                await _execute_with_retry(sql, params)
        except Exception as e:
            import traceback

            try:
                logger.error(
                    "Cyberherd upsert_settings INSERT EXC user_id=%s err=%s stack=%s",
                    user_id,
                    e,
                    traceback.format_exc(),
                )
            except Exception:
                pass

            # Check if this is a missing column error and try to bootstrap
            err_msg = str(e).lower()
            logger.warning(f"Cyberherd upsert_settings INSERT: checking error message: {err_msg}")
            if (
                "does not exist" in err_msg
                or "undefined column" in err_msg
                or "repost_tracking_enabled" in err_msg
                or "likes_tracking_enabled" in err_msg
                or "minimum_sats" in err_msg
            ):
                # DO NOT call bootstrap - let migrations handle table creation
                logger.error(f"Cyberherd upsert_settings INSERT: missing table/column detected, migrations need to run: {err_msg}")
                raise
            else:
                raise
        try:
            logger.debug(
                "Cyberherd CRUD upsert (insert) user_id=%s source_wallet=%s override_present=%s",
                user_id,
                getattr(settings, "source_wallet", None),
                bool(getattr(settings, "nostr_pubkey_override", None)),
            )
        except Exception:
            pass
        try:
            _dur = int((time.time() - _t0) * 1000)
            logger.debug(f"Cyberherd upsert_settings END user_id={user_id} dur_ms={_dur}")
        except Exception:
            pass
        await recompute_member_payouts(user_id)
        # Post-insert: establish default splits where appropriate. For a new
        # settings row we still want sensible defaults, but avoid forcing a
        # hard reset if there are existing members associated (edge cases).
        try:
            from .services.splits import (
                reset_splits_to_predefined_wallet as _reset_splits,
                schedule_split_recompute as _sched_recompute,
            )  # type: ignore
            if getattr(settings, "source_wallet", None):
                members = await get_active_cyberherd_members(user_id)
                if len(members) == 0 and getattr(settings, "zap_wallet", None):
                    # Only perform a hard reset when there are truly no members.
                    await _run_after_commit(_reset_splits, str(settings.source_wallet), user_id=user_id)
                else:
                    # Otherwise schedule a recompute which is non-destructive.
                    await _run_after_commit(_sched_recompute, str(settings.source_wallet), user_id=user_id)
        except Exception as e:  # pragma: no cover
            try:
                logger.debug(f"Cyberherd upsert_settings post-insert split ensure skipped: {e}")
            except Exception:
                pass
        return

    # Writes must be user-scoped. Legacy single-row write path has been removed
    # to avoid accidental modifications to global tables in multi-tenant setups.
    # If callers pass no user_id, raise an error to catch incorrect usage.
    raise RuntimeError("upsert_settings: user_id is required for writes; legacy single-row support removed")


async def get_members() -> list[LegacyCyberherdMember]:
    return await _db().fetchall(
        f"SELECT * FROM {db.references_schema}members ORDER BY added_at DESC",
        model=LegacyCyberherdMember,
    )


async def add_member(member: LegacyCyberherdMember):
    # Ensure added_at timestamp
    if not getattr(member, "added_at", None):
        member.added_at = int(time.time())
    await _db().execute(
        f"INSERT INTO {db.references_schema}members (pubkey, alias, added_at) "
        "VALUES (:pubkey, :alias, :added_at)",
        member.dict(),
    )


async def update_member(pubkey: str, alias: str | None = None):
    # Only alias is editable for now
    await _db().execute(
    f"UPDATE {db.references_schema}members SET alias = :alias WHERE pubkey = :pubkey",
    {"alias": alias, "pubkey": pubkey},
    )


async def delete_member(pubkey: str):
    await _db().execute(
        f"DELETE FROM {db.references_schema}members WHERE pubkey = :pubkey",
        {"pubkey": pubkey},
    )


### Full cyber_herd CRUD (middleware parity)


async def _with_engagement_awards(rows: list[dict], user_id: str) -> list[dict]:
    if not rows:
        return rows
    awards = await _db().fetchall(
        f"SELECT pubkey,kind FROM {db.references_schema}engagement_awards WHERE user_id=:user_id",
        {"user_id": user_id},
    )
    by_pubkey: dict[str, list[int]] = {}
    for award in awards:
        by_pubkey.setdefault(award["pubkey"], []).append(int(award["kind"]))
    for row in rows:
        row["engagement_kinds"] = sorted(by_pubkey.get(row["pubkey"], []))
    return rows


async def _award_member_engagements(pubkey: str, kinds, user_id: str):
    from .services.shares import _parse_kinds

    for kind in sorted(_parse_kinds(kinds) & {6, 7}):
        await _db().execute(
            f"""INSERT INTO {db.references_schema}engagement_awards (user_id,pubkey,kind,awarded_at)
            VALUES (:user_id,:pubkey,:kind,:timestamp) ON CONFLICT DO NOTHING""",
            {"user_id": user_id, "pubkey": pubkey, "kind": kind, "timestamp": int(time.time())},
        )


async def get_engagement_kinds(pubkey: str, user_id: str) -> list[int]:
    """Read cycle awards even when the member row was deleted or evicted."""
    if not user_id or not pubkey:
        return []
    rows = await _db().fetchall(
        f"SELECT kind FROM {db.references_schema}engagement_awards "
        "WHERE user_id=:user_id AND pubkey=:pubkey ORDER BY kind",
        {"user_id": user_id, "pubkey": pubkey.strip().lower()},
    )
    return [int(row["kind"]) for row in rows]


async def get_all_cyberherd_members(user_id: str | None = None) -> list[dict]:
    """Get all cyberherd members for a specific user.
    
    Args:
        user_id: User ID to filter members. If None, returns empty list for security.
    
    Returns:
        List of member dictionaries for the specified user.
    """
    if not user_id:
        # For security, don't return any members if user_id is not specified
        logger.warning("get_all_cyberherd_members called without user_id, returning empty list")
        return []
    
    try:
        rows = await _db().fetchall(
            f"SELECT * FROM {db.references_schema}cyber_herd WHERE user_id = :user_id",
            {"user_id": user_id}
        )
        return await _with_engagement_awards([dict(r) for r in rows], user_id)
    except Exception as e:
        msg = str(e).lower()
        if "undefinedtable" in msg or "does not exist" in msg or "no such table" in msg:
            await _bootstrap_cyberherd_tables()
            try:
                rows = await _db().fetchall(
                    f"SELECT * FROM {db.references_schema}cyber_herd WHERE user_id = :user_id",
                    {"user_id": user_id}
                )
                return await _with_engagement_awards([dict(r) for r in rows], user_id)
            except Exception:
                return []
        raise



async def recompute_member_payouts(user_id: str | None = None) -> dict[str, float]:
    """Recalculate payout shares for all cyberherd members for the given user."""
    if not user_id:
        logger.warning("recompute_member_payouts called without user_id, skipping")
        return {}
    if not in_admission_transaction():
        async with admission_transaction():
            return await recompute_member_payouts(user_id)


    try:
        members = await get_all_cyberherd_members(user_id)
    except Exception as exc:
        if in_admission_transaction():
            raise
        logger.debug(f"cyberherd: recompute_member_payouts failed to load members: {exc}")
        return {}

    if not members:
        return {}

    try:
        settings = await get_settings(user_id)
        zap_wallet = getattr(settings, "zap_wallet", None)
    except Exception as exc:
        if in_admission_transaction():
            raise
        logger.debug(f"cyberherd: recompute_member_payouts could not load settings: {exc}")
        settings = None
        zap_wallet = None

    member_allocation_percent = getattr(settings, "member_allocation_percent", 10 if zap_wallet else 100)
    member_total = member_allocation_percent if zap_wallet else 100
    from .utils.common import is_valid_lightning_address
    eligible = [row for row in members if not row.get("banned") and is_valid_lightning_address(row.get("lud16"))]
    shares_percent = compute_member_share_percentages(eligible, member_total, getattr(settings, "minimum_sats", 10))

    results: dict[str, float] = {}
    async with _get_crud_write_lock():
        for row in members:
            pubkey = (row.get("pubkey") or "").strip().lower()
            if not pubkey:
                continue
            percent_value = float(shares_percent.get(pubkey, 0))
            payout_value = round(percent_value / 100.0, 8)
            results[pubkey] = payout_value
            try:
                await _db().execute(
                    f"UPDATE {db.references_schema}cyber_herd SET payouts = :payouts WHERE pubkey = :pubkey AND user_id = :user_id",
                    {"payouts": payout_value, "pubkey": pubkey, "user_id": user_id},
                )
            except Exception as exc:
                if in_admission_transaction():
                    raise
                logger.debug(
                    f"cyberherd: failed to update payouts for {pubkey[:8]}...: {exc}"
                )
    return results


async def get_cyberherd_member_by_pubkey(pubkey: str, user_id: str | None = None) -> dict | None:
    """Get a cyberherd member by pubkey for a specific user.
    
    Args:
        pubkey: The pubkey of the member to retrieve.
        user_id: User ID to filter by. If None, returns None for security.
    
    Returns:
        Member dictionary if found, None otherwise.
    """
    if not user_id:
        logger.warning("get_cyberherd_member_by_pubkey called without user_id, returning None")
        return None

    normalized_pubkey = (pubkey or "").strip().lower()
    if not normalized_pubkey:
        logger.warning("get_cyberherd_member_by_pubkey called without valid pubkey, returning None")
        return None

    try:
        # Only return user-scoped rows. Legacy global rows are no longer supported.
        row = await _db().fetchone(
            f"SELECT * FROM {db.references_schema}cyber_herd WHERE pubkey = :pubkey AND user_id = :user_id",
            {"pubkey": normalized_pubkey, "user_id": user_id},
        )
        return (await _with_engagement_awards([dict(row)], user_id))[0] if row else None
    except Exception as e:
        # If table is missing, attempt runtime bootstrap once then retry
        if any(t in str(e).lower() for t in ["undefinedtable", "does not exist", "no such table"]):
            await _bootstrap_cyberherd_tables()
            row = await _db().fetchone(
                f"SELECT * FROM {db.references_schema}cyber_herd WHERE pubkey = :pubkey AND user_id = :user_id",
                {"pubkey": normalized_pubkey, "user_id": user_id},
            )
            return (await _with_engagement_awards([dict(row)], user_id))[0] if row else None
        raise


async def get_active_cyberherd_members(user_id: str | None = None) -> list[dict]:
    """Get all active cyberherd members for a specific user.
    
    Args:
        user_id: User ID to filter members. If None, returns empty list for security.
    
    Returns:
        List of active member dictionaries for the specified user.
    """
    if not user_id:
        logger.warning("get_active_cyberherd_members called without user_id, returning empty list")
        return []
    
    try:
        rows = await _db().fetchall(
            f"SELECT * FROM {db.references_schema}cyber_herd WHERE is_active = 1 AND COALESCE(banned, 0) = 0 AND user_id = :user_id",
            {"user_id": user_id}
        )
        return await _with_engagement_awards([dict(r) for r in rows], user_id)
    except Exception as e:
        if any(t in str(e).lower() for t in ["undefinedtable", "does not exist", "no such table"]):
            await _bootstrap_cyberherd_tables()
            rows = await _db().fetchall(
                f"SELECT * FROM {db.references_schema}cyber_herd WHERE is_active = 1 AND COALESCE(banned, 0) = 0 AND user_id = :user_id",
                {"user_id": user_id}
            )
            return await _with_engagement_awards([dict(r) for r in rows], user_id)
        raise


async def get_active_cyberherd_size(user_id: str | None = None) -> int:
    """Get count of active cyberherd members for a specific user.
    
    Args:
        user_id: User ID to filter members. If None, returns 0 for security.
    
    Returns:
        Count of active members for the specified user.
    """
    if not user_id:
        logger.warning("get_active_cyberherd_size called without user_id, returning 0")
        return 0
    
    try:
        row = await _db().fetchone(
            f"SELECT COUNT(*) as count FROM {db.references_schema}cyber_herd WHERE is_active = 1 AND COALESCE(banned, 0) = 0 AND user_id = :user_id",
            {"user_id": user_id}
        )
        return int(row["count"]) if row and "count" in row else 0
    except Exception as e:
        if any(t in str(e).lower() for t in ["undefinedtable", "does not exist", "no such table"]):
            await _bootstrap_cyberherd_tables()
            row = await _db().fetchone(
                f"SELECT COUNT(*) as count FROM {db.references_schema}cyber_herd WHERE is_active = 1 AND COALESCE(banned, 0) = 0 AND user_id = :user_id",
                {"user_id": user_id}
            )
            return int(row["count"]) if row and "count" in row else 0
        raise


async def deactivate_cyberherd_member(pubkey: str, user_id: str | None = None):
    """Deactivate a cyberherd member for a specific user.
    
    Args:
        pubkey: The pubkey of the member to deactivate.
        user_id: User ID to filter by. If None, operation is skipped for security.
    """
    if not user_id:
        logger.warning("deactivate_cyberherd_member called without user_id, skipping")
        return
    pubkey = (pubkey or "").strip().lower()
    if not pubkey:
        return
    if not in_admission_transaction():
        async with admission_transaction():
            return await deactivate_cyberherd_member(pubkey, user_id)

    
    await _db().execute(
        f"""
        UPDATE {db.references_schema}cyber_herd
        SET is_active = 0, amount = 0, payouts = 0.0, event_id = NULL,
            note = NULL, kinds = NULL
        WHERE pubkey = :pubkey AND user_id = :user_id
        """,
        {"pubkey": pubkey, "user_id": user_id},
    )
    try:
        await recompute_member_payouts(user_id)
    except Exception as exc:
        if in_admission_transaction():
            raise
        logger.debug(f"cyberherd: recompute after deactivate failed: {exc}")
    # After deactivation, recompute and update split targets if a source wallet is configured
    try:
        from .services.splits import schedule_split_recompute  # type: ignore
        # Require explicit user_id to determine source_wallet and avoid global fallbacks
        s = await get_settings(user_id)
        sw = getattr(s, "source_wallet", None)
        if sw:
            if in_admission_transaction():
                defer_after_commit(lambda sw=sw, user_id=user_id: schedule_split_recompute(str(sw), user_id=user_id))
            else:
                await schedule_split_recompute(str(sw), user_id=user_id)
    except Exception as e:
        logger.warning(e)


async def deactivate_all_cyberherd_members(user_id: str | None = None):
    """Deactivate all cyberherd members for a specific user.
    
    Args:
        user_id: User ID to filter by. If None, operation is skipped for security.
        
    Returns:
        int: Number of members deactivated, or 0 if skipped.
    """
    if not user_id:
        logger.warning("deactivate_all_cyberherd_members called without user_id, skipping")
        return 0
    if not in_admission_transaction():
        async with admission_transaction():
            return await deactivate_all_cyberherd_members(user_id)

    
    # Now deactivate them
    result = await _db().execute(
        f"""
        UPDATE {db.references_schema}cyber_herd
        SET is_active = 0, amount = 0, payouts = 0.0, event_id = NULL,
            note = NULL, kinds = NULL
        WHERE is_active = 1 AND user_id = :user_id
        """,
        {"user_id": user_id}
    )
    
    member_count = result.rowcount

    try:
        await recompute_member_payouts(user_id)
    except Exception as exc:
        logger.debug(f"cyberherd: recompute after deactivate_all failed: {exc}")
    # Clear split targets since no members are active now
    try:
        from .services.splits import schedule_split_recompute  # type: ignore
        s = await get_settings(user_id)
        sw = getattr(s, "source_wallet", None)
        if sw:
            if in_admission_transaction():
                defer_after_commit(lambda sw=sw, user_id=user_id: schedule_split_recompute(str(sw), user_id=user_id))
            else:
                await schedule_split_recompute(str(sw), user_id=user_id)
    except Exception as e:
        logger.warning(e)
    
    return member_count

async def is_pubkey_banned(pubkey: str, user_id: str | None = None) -> bool:
    """Check if a pubkey is banned for a specific user.
    
    Args:
        pubkey: The pubkey to check (will be normalized to lowercase)
        user_id: User ID to filter by. If None, returns False.
        
    Returns:
        bool: True if the pubkey is banned, False otherwise.
    """
    if not user_id or not pubkey:
        return False
    
    normalized_pubkey = str(pubkey).strip().lower()
    
    try:
        result = await _db().fetchone(
            f"""
            SELECT banned FROM {db.references_schema}cyber_herd
            WHERE pubkey = :pubkey AND user_id = :user_id
            """,
            {"pubkey": normalized_pubkey, "user_id": user_id}
        )
        
        if result:
            return bool(result.get("banned", 0))
        return False
    except Exception as e:
        logger.warning(f"Error checking if pubkey {normalized_pubkey[:16]}... is banned: {e}")
        return False


async def set_member_ban_status(pubkey: str, banned: bool, user_id: str | None = None) -> bool:
    """Ban or unban a cyberherd member by pubkey.
    
    When banning a member, also deactivates them from the herd.
    
    Args:
        pubkey: The pubkey to ban/unban (will be normalized to lowercase)
        banned: True to ban, False to unban
        user_id: User ID to filter by. Required for multi-user support.
        
    Returns:
        bool: True if successful, False otherwise.
    """
    if not user_id or not pubkey:
        logger.error("set_member_ban_status called without user_id or pubkey")
        return False
    if not in_admission_transaction():
        async with admission_transaction():
            return await set_member_ban_status(pubkey, banned, user_id)

    
    normalized_pubkey = str(pubkey).strip().lower()
    banned_value = 1 if banned else 0
    
    try:
        # If banning, also deactivate the member
        if banned:
            result = await _db().execute(
                f"""
                UPDATE {db.references_schema}cyber_herd
                SET banned = :banned, is_active = 0
                WHERE pubkey = :pubkey AND user_id = :user_id
                """,
                {"banned": banned_value, "pubkey": normalized_pubkey, "user_id": user_id}
            )
            logger.info(f"Banned and deactivated member {normalized_pubkey[:16]}... for user {user_id}")
        else:
            result = await _db().execute(
                f"""
                UPDATE {db.references_schema}cyber_herd
                SET banned = :banned
                WHERE pubkey = :pubkey AND user_id = :user_id
                """,
                {"banned": banned_value, "pubkey": normalized_pubkey, "user_id": user_id}
            )
            logger.info(f"Unbanned member {normalized_pubkey[:16]}... for user {user_id}")
        
        if result.rowcount == 0:
            return False

        # Recompute payouts and splits after ban status change
        try:
            await recompute_member_payouts(user_id)
        except Exception as exc:
            logger.debug(f"cyberherd: recompute after ban status change failed: {exc}")
        
        try:
            from .services.splits import schedule_split_recompute
            s = await get_settings(user_id)
            sw = getattr(s, "source_wallet", None)
            if sw:
                if in_admission_transaction():
                    defer_after_commit(lambda sw=sw, user_id=user_id: schedule_split_recompute(str(sw), user_id=user_id))
                else:
                    await schedule_split_recompute(str(sw), user_id=user_id)
        except Exception as e:
            logger.warning(f"Failed to schedule split recompute after ban status change: {e}")
        
        return True
    except Exception as e:
        logger.error(f"Error setting ban status for pubkey {normalized_pubkey[:16]}...: {e}")
        return False


async def ensure_inactive_cyberherd_member(
    pubkey: str,
    user_id: str | None = None,
    *,
    refresh_metadata: bool = False,
) -> None:
    """Ensure a cyberherd member row exists as inactive for the given user.

    This is a lightweight helper used by historical backfill tasks (e.g. zap
    totals from payments). It creates a minimal member row without triggering
    messaging or split recomputation. Existing rows (for this user) are left
    unchanged.
    """
    if not user_id:
        logger.warning("ensure_inactive_cyberherd_member called without user_id, skipping")
        return

    normalized_pubkey = str(pubkey or "").strip().lower()
    if not normalized_pubkey:
        logger.warning("ensure_inactive_cyberherd_member called without valid pubkey, skipping")
        return

    # If a row already exists for this user, either respect its current state
    # or refresh its metadata depending on the caller's intent.
    existing = await get_cyberherd_member_by_pubkey(normalized_pubkey, user_id=user_id)
    if existing and not refresh_metadata:
        return

    table_name = f"{db.references_schema}cyber_herd"

    # Start with existing values so refresh_metadata does not wipe fields when
    # lookups fail or return partial data.
    display_name: str | None = (existing or {}).get("display_name") if existing else None
    lud16: str | None = (existing or {}).get("lud16") if existing else None
    picture: str | None = (existing or {}).get("picture") if existing else None
    nip05: str | None = (existing or {}).get("nip05") if existing else None
    nprofile: str | None = (existing or {}).get("nprofile") if existing else None
    relays_str: str | None = (existing or {}).get("relays") if existing else None
    metadata_ts: int | None = (existing or {}).get("metadata_last_checked_at") if existing else None

    if refresh_metadata:
        try:
            from .services.nostr_helpers import lookup_metadata, lookup_relays  # type: ignore

            try:
                md = await lookup_metadata(normalized_pubkey)
            except Exception as exc:
                md = None
                try:
                    logger.debug(
                        f"ensure_inactive_cyberherd_member: metadata lookup failed for {normalized_pubkey[:8]}...: {exc}"
                    )
                except Exception:
                    pass

            if md:
                try:
                    dn = md.get("display_name") or "Anon"
                    display_name = str(dn)
                except Exception:
                    display_name = display_name or "Anon"
                try:
                    lud16_val = md.get("lud16")
                    if isinstance(lud16_val, str):
                        lud16 = lud16_val.strip().lower() or lud16
                except Exception:
                    pass
                try:
                    picture_val = md.get("picture")
                    if isinstance(picture_val, str):
                        picture = picture_val
                except Exception:
                    pass
                try:
                    nip_val = md.get("nip05")
                    if isinstance(nip_val, str):
                        nip05_candidate = nip_val.strip().lower()
                        if nip05_candidate:
                            nip05 = nip05_candidate
                except Exception:
                    pass

            try:
                relay_list = await lookup_relays(normalized_pubkey)
            except Exception as exc:
                relay_list = []
                try:
                    logger.debug(
                        f"ensure_inactive_cyberherd_member: relay lookup failed for {normalized_pubkey[:8]}...: {exc}"
                    )
                except Exception:
                    pass

            if isinstance(relay_list, list) and relay_list:
                relays_str = ",".join(str(r) for r in relay_list if isinstance(r, str))

            try:
                from .utils.tlv import hex_to_nprofile  # type: ignore

                try:
                    relay_hints = relay_list if isinstance(relay_list, list) else None
                    nprofile = hex_to_nprofile(normalized_pubkey, relays=relay_hints)
                except Exception:
                    # Keep existing nprofile on failure
                    pass
            except Exception as exc:
                try:
                    logger.debug(
                        f"ensure_inactive_cyberherd_member: nprofile generation failed for {normalized_pubkey[:8]}...: {exc}"
                    )
                except Exception:
                    pass

            try:
                metadata_ts = int(time.time())
            except Exception:
                # Fall back to existing timestamp if conversion fails
                metadata_ts = metadata_ts
        except Exception as exc:
            try:
                logger.debug(
                    f"ensure_inactive_cyberherd_member: enrichment skipped for {normalized_pubkey[:8]}...: {exc}"
                )
            except Exception:
                pass

    values = {
        "pubkey": normalized_pubkey,
        "user_id": user_id,
        "display_name": display_name,
        "nprofile": nprofile,
        "lud16": lud16,
        "nip05": nip05,
        "picture": picture,
        "relays": relays_str,
        "metadata_last_checked_at": metadata_ts,
    }

    # For refresh_metadata we want to upsert all known metadata fields; for
    # first-time creation without refresh_metadata, we only ensure a stub row.
    if refresh_metadata:
        insert_sql = f"""
            INSERT INTO {table_name} (
                pubkey, user_id, display_name, nprofile, lud16, nip05,
                picture, relays, metadata_last_checked_at, is_active
            )
            VALUES (
                :pubkey, :user_id, :display_name, :nprofile, :lud16, :nip05,
                :picture, :relays, :metadata_last_checked_at, 0
            )
            ON CONFLICT(user_id, pubkey) DO UPDATE SET
                display_name = COALESCE(excluded.display_name, {table_name}.display_name),
                nprofile = COALESCE(excluded.nprofile, {table_name}.nprofile),
                lud16 = COALESCE(excluded.lud16, {table_name}.lud16),
                nip05 = COALESCE(excluded.nip05, {table_name}.nip05),
                picture = COALESCE(excluded.picture, {table_name}.picture),
                relays = COALESCE(excluded.relays, {table_name}.relays),
                metadata_last_checked_at = COALESCE(excluded.metadata_last_checked_at, {table_name}.metadata_last_checked_at)
        """
    else:
        insert_sql = f"""
            INSERT INTO {table_name} (pubkey, user_id, is_active)
            VALUES (:pubkey, :user_id, 0)
            ON CONFLICT(user_id, pubkey) DO NOTHING
        """

    for attempt in range(2):
        try:
            await _db().execute(insert_sql, values)
            break
        except Exception as e:
            err_msg = str(e).lower()
            if attempt == 0 and any(
                t in err_msg
                for t in [
                    "undefinedtable",
                    "does not exist",
                    "no such table",
                    "no such column",
                    "undefined column",
                ]
            ):
                await _bootstrap_cyberherd_tables()
                continue
            logger.debug(f"ensure_inactive_cyberherd_member failed: {e}")
            raise


async def add_new_active_member(member_data: dict, user_id: str | None = None):
    """Add or update a cyberherd member for a specific user.
    
    Args:
        member_data: Dictionary containing member information.
        user_id: User ID to associate with this member. Required for multi-user support.
    """
    if not user_id:
        logger.error("add_new_active_member called without user_id, skipping")
        return
    if not in_admission_transaction():
        async with admission_transaction():
            return await add_new_active_member(member_data, user_id)

    
    raw_pubkey = member_data.get("pubkey")
    normalized_pubkey = str(raw_pubkey).strip().lower() if raw_pubkey else ""
    if not normalized_pubkey:
        logger.error("add_new_active_member called without valid pubkey, skipping")
        return

    logger.info(f"cyberherd: add_new_active_member called for pubkey: {normalized_pubkey[:8]}... user_id: {user_id}")
    # Normalize values — callers are expected to pass amounts in satoshis.
    raw_amount = member_data.get("amount") or 0
    try:
        amt_val = int(raw_amount)
    except Exception:
        amt_val = 0
    if amt_val >= 21_000_000:
        logger.warning(
            "cyberherd: add_new_active_member received unusually large amount "
            "%s for pubkey %s — verify caller passes satoshis, not millisats",
            amt_val, normalized_pubkey[:8],
        )

    # Normalize lud16: strip and lowercase for consistent storage
    raw_lud16 = member_data.get("lud16") or ""
    normalized_lud16 = raw_lud16.strip().lower() if raw_lud16 else ""

    raw_nip05 = member_data.get("nip05") or ""
    normalized_nip05 = raw_nip05.strip().lower() if raw_nip05 else None

    table_name = f"{db.references_schema}cyber_herd"
    existing_nip05_expr = f"{table_name}.nip05"

    values = {
        "pubkey": normalized_pubkey,
        "user_id": user_id,  # Add user_id to values
        "display_name": member_data.get("display_name"),
        "event_id": member_data.get("event_id"),
        "note": member_data.get("note"),
    # Normalize kinds storage: store as comma-separated ints (e.g. "6,9735")
    "kinds": None,
        "nprofile": member_data.get("nprofile"),
    "lud16": normalized_lud16,
        "nip05": normalized_nip05,
        "payouts": float(member_data.get("payouts") or 0.0),
        "amount": int(amt_val),
        "picture": member_data.get("picture"),
        "relays": member_data.get("relays"),
        "metadata_last_checked_at": int(
            member_data.get("metadata_last_checked_at") or 0
        ),
    }
    # Enrich missing relays/nprofile using nostrclient and utils.nostr
    try:
        # Generate nprofile if missing (without relay hints - faster)
        if not values.get("nprofile"):
            pubkey = values.get('pubkey', '')
            logger.debug(f"cyberherd: generating nprofile for {pubkey[:8]}...")
            try:
                from .utils.tlv import hex_to_nprofile  # type: ignore
                values["nprofile"] = hex_to_nprofile(str(values["pubkey"]))
            except Exception as e:
                logger.debug(f"Cyberherd CRUD: hex_to_nprofile failed: {e}")
    except Exception as e:
        logger.debug(f"Cyberherd CRUD: outer exception in enrichment: {e}")
        # Never block insertion due to enrichment
        pass

    # Normalize kinds into canonical comma-separated string
    try:
        kinds = member_data.get("kinds")
        if kinds is None:
            values["kinds"] = None
        elif isinstance(kinds, (list, tuple, set)):
            ints = []
            for k in kinds:
                try:
                    ints.append(str(int(k)))
                except Exception:
                    # skip non-convertible
                    pass
            values["kinds"] = ",".join(ints) if ints else None
        elif isinstance(kinds, str):
            # Clean up whitespace and ensure only comma-separated ints remain
            parts = [p.strip() for p in kinds.split(",") if p and p.strip()]
            ints = []
            for p in parts:
                try:
                    ints.append(str(int(p)))
                except Exception:
                    # Try to handle strings like "[6]" or similar by stripping non-digits
                    s = ''.join(c for c in p if c.isdigit())
                    try:
                        if s:
                            ints.append(str(int(s)))
                    except Exception:
                        pass
            values["kinds"] = ",".join(ints) if ints else None
        else:
            # Try to coerce single int-like values
            try:
                values["kinds"] = str(int(kinds))
            except Exception:
                values["kinds"] = None
    except Exception:
        values["kinds"] = member_data.get("kinds")
    
    # Merge sanitized kinds with any existing stored kinds so we never overwrite
    # previous engagement categories (e.g. keep both zap=9735 and reaction=7)
    existing_kinds_row = None
    try:
        existing_kinds_row = await _db().fetchone(
            f"SELECT kinds, banned FROM {db.references_schema}cyber_herd WHERE pubkey = :pubkey AND user_id = :user_id",
            {"pubkey": normalized_pubkey, "user_id": user_id},
        )
    except Exception as e:
        try:
            logger.debug(f"cyberherd: add_new_active_member kinds lookup skipped: {e}")
        except Exception:
            pass

    if existing_kinds_row and existing_kinds_row.get("banned"):
        raise ValueError("banned member cannot be activated")
    if existing_kinds_row:
        existing_raw = None
        try:
            existing_raw = existing_kinds_row.get("kinds")
        except Exception:
            try:
                existing_raw = existing_kinds_row["kinds"]  # type: ignore[index]
            except Exception:
                existing_raw = None

        existing_set = {k for k in (existing_raw or "").split(",") if k}
        new_set = {k for k in (values.get("kinds") or "").split(",") if k}

        merged_set = existing_set.union(new_set) if new_set else existing_set

        if merged_set:
            try:
                merged_list = sorted(merged_set, key=lambda x: int(x))
            except Exception:
                merged_list = sorted(merged_set)
            values["kinds"] = ",".join(merged_list)
        else:
            values["kinds"] = None

    insert_sql = f"""INSERT INTO {table_name} (
                pubkey, user_id, display_name, event_id, note, kinds, nprofile, lud16,
                nip05, payouts, amount, picture, relays, metadata_last_checked_at, is_active
            ) VALUES (
                :pubkey, :user_id, :display_name, :event_id, :note, :kinds, :nprofile, :lud16,
                :nip05, :payouts, :amount, :picture, :relays, :metadata_last_checked_at, 1
            )
            ON CONFLICT(user_id, pubkey) DO UPDATE SET
                display_name = excluded.display_name,
                event_id = excluded.event_id,
                note = excluded.note,
                kinds = excluded.kinds,
                nprofile = excluded.nprofile,
                lud16 = excluded.lud16,
                nip05 = COALESCE(excluded.nip05, {existing_nip05_expr}),
                payouts = excluded.payouts,
                amount = excluded.amount,
                picture = excluded.picture,
                relays = excluded.relays,
                metadata_last_checked_at = excluded.metadata_last_checked_at,
                is_active = 1
            WHERE COALESCE({table_name}.banned, 0) = 0
            """
    for attempt in range(2):
        try:
            result = await _db().execute(insert_sql, values)
            if result.rowcount != 1:
                raise ValueError("member activation was rejected")
            break
        except Exception as e:
            err_msg = str(e).lower()
            if attempt == 0 and any(
                t in err_msg for t in ["undefinedtable", "does not exist", "no such table", "no such column", "undefined column"]
            ):
                await _bootstrap_cyberherd_tables()
                continue
            if any(t in err_msg for t in ["no such column", "undefined column"]):
                logger.error(f"cyberherd: add_new_active_member failed due to missing column: {e}")
                raise RuntimeError(
                    "cyberherd: add_new_active_member failed because the cyber_herd schema is missing required columns; run migrations"
                ) from e
            raise
    
    await _award_member_engagements(normalized_pubkey, values["kinds"], user_id)

    try:
        await recompute_member_payouts(user_id)
    except Exception as exc:
        if in_admission_transaction():
            raise
        logger.debug(f"cyberherd: recompute after add member skipped: {exc}")

    # After successful insert/update, recompute split targets (idempotent) so the
    # predefined zap wallet (if configured) and new member weights are reflected promptly.
    try:
        # Only recompute splits if the newly added member is eligible (has a lud16).
        # Ensure lud16 is stripped and lowercased before checking eligibility
        new_lud16 = (values.get("lud16") or "").strip().lower()
        if new_lud16:
            from .services.splits import schedule_split_recompute  # type: ignore
            # Require explicit user_id here to avoid running recompute on global row
            s = await get_settings(user_id)
            sw = getattr(s, "source_wallet", None)
            if sw:
                if in_admission_transaction():
                    defer_after_commit(lambda sw=sw, user_id=user_id: schedule_split_recompute(str(sw), user_id=user_id))
                else:
                    await schedule_split_recompute(str(sw), user_id=user_id)
        else:
            try:
                logger.debug("cyberherd: add_new_active_member: new member has no lud16; skipping split recompute")
            except Exception:
                pass
    except Exception as e:
        try:
            logger.debug(f"cyberherd: split recompute after add member skipped: {e}")
        except Exception:
            pass


async def update_and_activate_member(
    pubkey: str,
    amount_increase: int,
    user_id: str | None = None,
    event_id: str | None = None,
    kinds: str | list | tuple | set | None = None,
    nip05: str | None = None,
    *,
    amount_total: int | None = None,
):
    """Update and activate a cyberherd member for a specific user.

    Args:
        pubkey: The pubkey of the member to update.
        amount_increase: Amount to add to the member's total.
        user_id: User ID to filter by. If None, operation is skipped for security.
        event_id: Optional tracked event id (nostr e-tag) for threading.
    """
    if not user_id:
        logger.warning("update_and_activate_member called without user_id, skipping")
        return
    if not in_admission_transaction():
        async with admission_transaction():
            return await update_and_activate_member(pubkey, amount_increase, user_id, event_id, kinds, nip05, amount_total=amount_total)

    
    normalized_pubkey = (pubkey or "").strip().lower()
    if not normalized_pubkey:
        logger.warning("update_and_activate_member called without valid pubkey, skipping")
        return
    if amount_total is not None and amount_total < 0:
        raise ValueError("member amount cannot be negative")
    if await is_pubkey_banned(normalized_pubkey, user_id):
        raise ValueError("banned member cannot be activated")
    
    # Best-effort enrichment if the row is missing nprofile
    try:
        row = await _db().fetchone(
            f"SELECT nprofile FROM {db.references_schema}cyber_herd WHERE pubkey = :pubkey AND user_id = :user_id",
            {"pubkey": normalized_pubkey, "user_id": user_id},
        )
        need_nprofile = not row or not row.get("nprofile")
        if need_nprofile:
            try:
                from .utils.tlv import hex_to_nprofile  # type: ignore

                nprof = None
                try:
                    nprof = hex_to_nprofile(normalized_pubkey)
                except Exception as e:
                    logger.debug(f"Cyberherd CRUD (update) hex_to_nprofile failed: {e}")
                if nprof:
                    await _db().execute(
                        f"""
                        UPDATE {db.references_schema}cyber_herd
                        SET nprofile = :nprofile
                        WHERE pubkey = :pubkey AND user_id = :user_id
                        """,
                        {
                            "nprofile": nprof,
                            "pubkey": normalized_pubkey,
                            "user_id": user_id,
                        },
                    )
            except Exception as e:
                logger.debug(f"Cyberherd CRUD (update) enrichment skipped: {e}")
    except Exception:
        pass

    # Callers are expected to pass amount_increase in satoshis.
    try:
        ai = int(amount_increase or 0)
    except Exception:
        ai = 0
    if ai >= 21_000_000:
        logger.warning(
            "cyberherd: update_and_activate_member received unusually large "
            "amount_increase %s for pubkey %s — verify caller passes satoshis",
            ai, normalized_pubkey[:8],
        )

    nip05_normalized: str | None = None
    if nip05:
        try:
            nip05_normalized = str(nip05).strip().lower() or None
        except Exception:
            nip05_normalized = None

    table_name = f"{db.references_schema}cyber_herd"
    existing_nip05_expr = f"{table_name}.nip05"

    update_sql = f"""
        UPDATE {table_name}
        SET amount = COALESCE(:amount_total, COALESCE(amount, 0) + :amount_increase),
            is_active = 1,
            metadata_last_checked_at = :ts,
            event_id = COALESCE(:event_id, event_id),
            nip05 = COALESCE(:nip05, {existing_nip05_expr})
        WHERE pubkey = :pubkey AND user_id = :user_id
        """
    params = {
        "amount_increase": ai,
        "amount_total": amount_total,
        "ts": int(time.time()),
        "pubkey": normalized_pubkey,
        "user_id": user_id,
        "event_id": event_id,
        "nip05": nip05_normalized,
    }
    result = None
    for attempt in range(2):
        try:
            result = await _db().execute(update_sql, params)
            break
        except Exception as e:
            err_msg = str(e).lower()
            if attempt == 0 and any(
                t in err_msg for t in ["undefinedtable", "does not exist", "no such table", "no such column", "undefined column"]
            ):
                await _bootstrap_cyberherd_tables()
                continue
            if any(t in err_msg for t in ["no such column", "undefined column"]):
                logger.error(f"cyberherd: update_and_activate_member failed due to missing column: {e}")
                raise RuntimeError(
                    "cyberherd: update_and_activate_member failed because the cyber_herd schema is missing required columns; run migrations"
                ) from e
            raise
    # If no rows were updated, target row doesn't exist for this user. Fail fast
    # to avoid mutating legacy/global rows.
    if result.rowcount == 0:
        logger.info(f"cyberherd: update_and_activate_member: targeted update affected 0 rows for pubkey={normalized_pubkey[:8]} (user_id scoped)")
        return
    
    # Optionally update kinds column: merge any provided kinds into the stored set
    if kinds is not None:
        try:
            # Normalize new kinds into a set of strings
            new_kinds_set = set()
            if isinstance(kinds, (list, tuple, set)):
                for k in kinds:
                    try:
                        new_kinds_set.add(str(int(k)))
                    except Exception:
                        continue
            elif isinstance(kinds, str):
                parts = [p.strip() for p in kinds.split(",") if p and p.strip()]
                for p in parts:
                    try:
                        new_kinds_set.add(str(int(p)))
                    except Exception:
                        # try to extract digits
                        s = ''.join(c for c in p if c.isdigit())
                        try:
                            if s:
                                new_kinds_set.add(str(int(s)))
                        except Exception:
                            continue
            else:
                try:
                    new_kinds_set.add(str(int(kinds)))
                except Exception:
                    pass

            if new_kinds_set:
                # Fetch existing kinds
                row = await _db().fetchone(
                    f"SELECT kinds FROM {db.references_schema}cyber_herd WHERE pubkey = :pubkey AND user_id = :user_id",
                    {"pubkey": normalized_pubkey, "user_id": user_id},
                )
                existing = (row.get("kinds") if row else None) or ""
                existing_set = set([s for s in (existing or "").split(",") if s])
                merged = existing_set.union(new_kinds_set)
                if merged:
                    try:
                        # Sort numerically for stable representation
                        merged_list = sorted(list(merged), key=lambda x: int(x))
                    except Exception:
                        merged_list = sorted(list(merged))
                    merged_str = ",".join(merged_list)
                else:
                    merged_str = None

                await _db().execute(
                    f"UPDATE {db.references_schema}cyber_herd SET kinds = :kinds WHERE pubkey = :pubkey AND user_id = :user_id",
                    {"kinds": merged_str, "pubkey": normalized_pubkey, "user_id": user_id},
                )
        except Exception as e:
            if in_admission_transaction():
                raise
            try:
                logger.debug(f"cyberherd: failed to merge kinds for {normalized_pubkey[:8]}...: {e}")
            except Exception:
                pass

    await _award_member_engagements(normalized_pubkey, kinds, user_id)

    try:
        await recompute_member_payouts(user_id)
    except Exception as exc:
        if in_admission_transaction():
            raise
        logger.debug(f"cyberherd: recompute after update member skipped: {exc}")

    # After successful update, recompute split targets (idempotent) so the
    # predefined zap wallet (if configured) and updated member weights are reflected promptly.
    try:
        # Only recompute if this update affects split eligibility.
        # If the member has a lud16, they will affect targets. If they don't
        # have a lud16 but there are currently no eligible members, we should
        # still recompute to ensure zap-only/clear targets are applied.
        from .services.splits import schedule_split_recompute  # type: ignore
        # Determine whether the updated member has a lud16
        row = await get_member_by_pubkey(user_id, normalized_pubkey) if user_id else None
        has_lud16 = False
        try:
            if row and (row.get("lud16") or "").strip():
                has_lud16 = True
        except Exception:
            has_lud16 = False

        # Check if there are any currently eligible members (with lud16)
        eligible_members = []
        try:
            eligible_members = await get_active_cyberherd_members(user_id)
        except Exception:
            eligible_members = []
        any_eligible = any((m.get("lud16") or "").strip() for m in (eligible_members or []))

        if has_lud16 or not any_eligible:
                s = await get_settings(user_id)
                sw = getattr(s, "source_wallet", None)
                if sw:
                    if in_admission_transaction():
                        defer_after_commit(lambda sw=sw, user_id=user_id: schedule_split_recompute(str(sw), user_id=user_id))
                    else:
                        await schedule_split_recompute(str(sw), user_id=user_id)
        else:
            try:
                logger.debug("cyberherd: update_and_activate_member: member has no lud16 and eligible members exist; skipping split recompute")
            except Exception:
                pass
    except Exception as e:
        try:
            logger.debug(f"cyberherd: split recompute after update member skipped: {e}")
        except Exception:
            pass



async def delete_cyberherd_member_by_pubkey(pubkey: str, user_id: str | None = None):
    """Delete a cyberherd member for a specific user.
    
    Args:
        pubkey: The pubkey of the member to delete.
        user_id: User ID to filter by. If None, operation is skipped for security.
    """
    if not user_id:
        logger.warning("delete_cyberherd_member_by_pubkey called without user_id, skipping")
        return
    pubkey = (pubkey or "").strip().lower()
    if not pubkey:
        return
    if not in_admission_transaction():
        async with admission_transaction():
            return await delete_cyberherd_member_by_pubkey(pubkey, user_id)

    
    await _db().execute(
        f"DELETE FROM {db.references_schema}cyber_herd WHERE pubkey = :pubkey AND user_id = :user_id",
        {"pubkey": pubkey, "user_id": user_id},
    )
    try:
        await recompute_member_payouts(user_id)
    except Exception as exc:
        logger.debug(f"cyberherd: recompute after delete member skipped: {exc}")
    
    # After deletion, recompute and update split targets if a source wallet is configured
    try:
        from .services.splits import schedule_split_recompute
        s = await get_settings(user_id)
        sw = getattr(s, "source_wallet", None)
        if sw:
            if in_admission_transaction():
                defer_after_commit(lambda sw=sw, user_id=user_id: schedule_split_recompute(str(sw), user_id=user_id))
            else:
                await schedule_split_recompute(str(sw), user_id=user_id)
    except Exception as e:
        logger.warning(f"Failed to schedule split recompute after member deletion: {e}")


async def delete_cyberherd_members_without_lud16(user_id: str | None = None) -> int:
    """Delete cyberherd members for a user who have no lud16 configured.

    This is intended as a cleanup/maintenance helper. It removes members
    where lud16 is NULL or empty (regardless of display_name), then recomputes
    payouts and splits. Since lud16 is required for payouts, these rows are
    not eligible for distribution.

    Args:
        user_id: User ID to scope deletions. If None, operation is skipped.

    Returns:
        Number of members deleted.
    """
    if not user_id:
        logger.warning("delete_cyberherd_members_without_lud16 called without user_id, skipping")
        return 0
    if not in_admission_transaction():
        async with admission_transaction():
            return await delete_cyberherd_members_without_lud16(user_id)


    table = f"{db.references_schema}cyber_herd"
    # Use the DELETE result for the number of rows actually removed.
    delete_sql = f"""
        DELETE FROM {table}
        WHERE user_id = :user_id
          AND (
              lud16 IS NULL
              OR TRIM(lud16) = ''
              OR TRIM(lud16) = '-'
          )
    """
    params = {"user_id": user_id}

    result = await _db().execute(delete_sql, params)
    deleted = result.rowcount

    # Recompute payouts and splits after deletions
    try:
        await recompute_member_payouts(user_id)
    except Exception as exc:
        logger.debug(f"cyberherd: recompute after delete_members_without_lud16 skipped: {exc}")

    try:
        from .services.splits import schedule_split_recompute  # type: ignore
        s = await get_settings(user_id)
        sw = getattr(s, "source_wallet", None)
        if sw:
            if in_admission_transaction():
                defer_after_commit(lambda sw=sw, user_id=user_id: schedule_split_recompute(str(sw), user_id=user_id))
            else:
                await schedule_split_recompute(str(sw), user_id=user_id)
    except Exception as e:
        logger.warning(f"Failed to schedule split recompute after delete_members_without_lud16: {e}")

    return deleted


async def update_cyberherd_member_allocation(pubkey: str, allocation_percentage: float, user_id: str | None = None):
    """Set a member's allocation percentage for a specific user.
    
    Args:
        pubkey: The pubkey of the member.
        allocation_percentage: The allocation percentage to set.
        user_id: User ID to filter by. If None, operation is skipped for security.
    """
    if not user_id:
        logger.warning("update_cyberherd_member_allocation called without user_id, skipping")
        return
    pubkey = (pubkey or "").strip().lower()
    if not pubkey:
        return
    
    try:
        await _db().execute(
            f"""
            UPDATE {db.references_schema}cyber_herd
            SET allocation_percentage = :allocation
            WHERE pubkey = :pubkey AND user_id = :user_id
            """,
            {"allocation": float(allocation_percentage), "pubkey": pubkey, "user_id": user_id},
        )
        # Trigger split recompute to propagate allocation changes, but only if it will affect
        # split eligibility (member has lud16) or if there are currently no eligible members.
        try:
            # Check member lud16
            member_row = await get_member_by_pubkey(user_id, pubkey) if user_id else None
            has_lud16 = False
            try:
                if member_row and (member_row.get("lud16") or "").strip():
                    has_lud16 = True
            except Exception:
                has_lud16 = False

            # Check if any eligible members exist
            eligible_members = await get_active_cyberherd_members(user_id)
            any_eligible = any((m.get("lud16") or "").strip() for m in (eligible_members or []))

            if has_lud16 or not any_eligible:
                from .services.splits import update_split_targets_proportional
                s = await get_settings(user_id)
                sw = getattr(s, "source_wallet", None)
                if sw:
                    await _run_after_commit(update_split_targets_proportional, str(sw), user_id=user_id)
            else:
                try:
                    logger.debug("cyberherd: update_cyberherd_member_allocation: skipping split recompute (member not eligible and eligible members exist)")
                except Exception:
                    pass
        except Exception:
            pass
    except Exception as e:
        logger.debug(f"Error updating member allocation for {pubkey[:8]}...: {e}")
        raise


async def _bootstrap_cyberherd_tables():
    """Create cyberherd.cyber_herd table if missing (runtime self-heal).

    Mirrors middleware structure; safe, idempotent. Called on UndefinedTableError.
    """
    if in_admission_transaction():
        raise RuntimeError("CyberHerd schema must be migrated before admission")
    schema_prefix = db.references_schema or ""
    table_name = f"{schema_prefix}cyber_herd"
    if schema_prefix.endswith("."):
        schema_table = f"{schema_prefix[:-1]}.cyber_herd"
    else:
        schema_table = table_name

    stmts = [
        f"""CREATE TABLE IF NOT EXISTS {table_name} (
            pubkey TEXT NOT NULL,
            user_id TEXT NOT NULL DEFAULT '',
            display_name TEXT,
            event_id TEXT,
            note TEXT,
            kinds TEXT,
            nprofile TEXT,
            lud16 TEXT,
            nip05 TEXT,
            notified INTEGER DEFAULT 0,
            payouts DOUBLE PRECISION DEFAULT 0,
            amount INTEGER DEFAULT 0,
            picture TEXT,
            relays TEXT,
            metadata_last_checked_at INTEGER,
            is_active INTEGER DEFAULT 0,
            banned INTEGER DEFAULT 0,
            PRIMARY KEY (user_id, pubkey)
        );""",
        # Basic helpful indexes
        f"CREATE INDEX IF NOT EXISTS idx_cyberherd_active ON {table_name}(is_active);",
        f"CREATE INDEX IF NOT EXISTS idx_cyber_herd_user_id ON {table_name}(user_id);",
        f"CREATE INDEX IF NOT EXISTS idx_cyber_herd_user_active ON {table_name}(user_id, is_active);",
    ]
    # Ensure processed_events table exists for idempotent runtime self-heal
    # This table tracks processed payments to prevent duplicate member_increase posts
    # NOTE: The unique constraint is on (user_id, event_hash, note_id) to allow
    # the same event to be tracked for multiple different notes (e.g., a repost
    # that mentions two tracked notes should create two separate rows)
    stmts.append(
        f"""
        CREATE TABLE IF NOT EXISTS {db.references_schema}processed_events (
            user_id TEXT NOT NULL,
            event_hash TEXT NOT NULL,
            note_id TEXT DEFAULT '',
            pubkey TEXT,
            processed_at INTEGER NOT NULL,
            event_type TEXT
        );
        """
    )
    # Add unique index on the triple (user_id, event_hash, note_id)
    # Using COALESCE to handle NULL note_ids as empty strings
    stmts.append(
        f"""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_processed_events_unique_triple 
        ON {db.references_schema}processed_events(user_id, event_hash, COALESCE(note_id, ''));
        """
    )
    # Legacy processed_zaps table (kept for backward compatibility)
    stmts.append(
        f"""
        CREATE TABLE IF NOT EXISTS {db.references_schema}processed_zaps (
            user_id TEXT,
            event_id TEXT,
            note_id TEXT,
            zapper_pubkey TEXT,
            amount INTEGER,
            processed_at TEXT,
            zap_receipt_id TEXT,
            payment_hash TEXT,
            amount_sats INTEGER
        );
        """
    )
    stmts.append(
        f"""
        CREATE TABLE IF NOT EXISTS {db.references_schema}zap_totals (
            user_id TEXT NOT NULL,
            zapper_pubkey TEXT NOT NULL,
            total_sats INTEGER NOT NULL DEFAULT 0,
            event_count INTEGER NOT NULL DEFAULT 0,
            first_event_at INTEGER,
            last_event_at INTEGER,
            last_event_ids TEXT,
            last_updated_at REAL NOT NULL,
            target_pubkey TEXT,
            PRIMARY KEY (user_id, zapper_pubkey)
        );
        """
    )
    for s in stmts:
        try:
            await _db().execute(s)
        except Exception:
            raise
    if not await _column_exists(f"{db.references_schema}processed_events", "event_type"):
        await _db().execute(
            f"ALTER TABLE {db.references_schema}processed_events ADD COLUMN event_type TEXT"
        )
    processed_zaps_table = f"{db.references_schema}processed_zaps"
    if db.references_schema.endswith("."):
        processed_zaps_schema_table = f"{db.references_schema[:-1]}.processed_zaps"
    else:
        processed_zaps_schema_table = processed_zaps_table
    processed_zap_columns = {
        "event_id": "TEXT",
        "note_id": "TEXT",
        "zapper_pubkey": "TEXT",
        "amount": "INTEGER",
        "processed_at": "TEXT",
        "zap_receipt_id": "TEXT",
        "payment_hash": "TEXT",
        "amount_sats": "INTEGER",
    }
    for column, column_type in processed_zap_columns.items():
        try:
            has_column = await _column_exists(processed_zaps_schema_table, column)
        except Exception:
            raise
        if not has_column:
            try:
                await _db().execute(
                    f"ALTER TABLE {processed_zaps_table} ADD COLUMN {column} {column_type};"
                )
            except Exception:
                raise
    index_statements = [
        f"CREATE INDEX IF NOT EXISTS idx_processed_zaps_user_event ON {processed_zaps_table}(user_id, event_id);",
        f"CREATE INDEX IF NOT EXISTS idx_processed_zaps_user_note ON {processed_zaps_table}(user_id, note_id);",
        f"CREATE INDEX IF NOT EXISTS idx_processed_zaps_user_zapper ON {processed_zaps_table}(user_id, zapper_pubkey);",
    ]
    for stmt in index_statements:
        try:
            await _db().execute(stmt)
        except Exception:
            raise
    unique_statements = [
        f"CREATE UNIQUE INDEX IF NOT EXISTS uq_processed_zaps_user_event ON {processed_zaps_table}(user_id, event_id) WHERE event_id IS NOT NULL;",
        f"CREATE UNIQUE INDEX IF NOT EXISTS uq_processed_zaps_user_receipt ON {processed_zaps_table}(user_id, zap_receipt_id) WHERE zap_receipt_id IS NOT NULL;",
    ]
    for stmt in unique_statements:
        try:
            await _db().execute(stmt)
        except Exception:
            raise
    if db.type != "SQLITE":
        await _db().execute(
            f"ALTER TABLE {processed_zaps_table} ALTER COLUMN processed_at TYPE TEXT USING processed_at::text;"
        )
    try:
        if not await _column_exists(schema_table, "nip05"):
            await _db().execute(f"ALTER TABLE {table_name} ADD COLUMN nip05 TEXT;")
    except Exception:
        pass
    try:
        logger.info("Cyberherd runtime bootstrap: ensured cyber_herd table with user_id")
    except Exception:
        pass


def _parse_timestamp_field(value: Any) -> int | None:
    """Best-effort coercion of timestamps stored as ints, floats, or ISO strings."""
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
        if candidate.isdigit():
            try:
                return int(candidate)
            except Exception:
                return None
        if candidate.startswith(("+", "-")) and candidate[1:].isdigit():
            try:
                return int(candidate)
            except Exception:
                return None
        try:
            return int(float(candidate))
        except Exception:
            pass
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


def _normalize_hex(value: Any) -> str | None:
    """Normalize hex strings (zap IDs, note IDs, pubkeys) to lowercase."""
    if not value:
        return None
    try:
        candidate = str(value).strip().lower()
    except Exception:
        return None
    return candidate or None


async def get_zap_totals_row(user_id: str, zapper_pubkey: str) -> dict | None:
    """Fetch persisted zap totals for a zapper."""
    if not user_id:
        return None
    zapper = zapper_pubkey.strip().lower()
    try:
        row = await _db().fetchone(
            f"""
            SELECT * FROM {db.references_schema}zap_totals
            WHERE user_id = :user_id AND zapper_pubkey = :zapper_pubkey
            """,
            {"user_id": user_id, "zapper_pubkey": zapper},
        )
        return dict(row) if row else None
    except Exception as e:
        if any(t in str(e).lower() for t in ["undefinedtable", "does not exist", "no such table"]):
            await _bootstrap_cyberherd_tables()
            row = await _db().fetchone(
                f"""
                SELECT * FROM {db.references_schema}zap_totals
                WHERE user_id = :user_id AND zapper_pubkey = :zapper_pubkey
                """,
                {"user_id": user_id, "zapper_pubkey": zapper},
            )
            return dict(row) if row else None
        raise


async def upsert_zap_totals_row(
    *,
    user_id: str,
    zapper_pubkey: str,
    total_sats: int,
    event_count: int,
    first_event_at: int | None,
    last_event_at: int | None,
    last_event_ids: list[str],
    last_updated_at: float,
    target_pubkey: str,
) -> None:
    """Persist zap totals snapshot for a zapper."""
    zapper = zapper_pubkey.strip().lower()
    payload = {
        "user_id": user_id,
        "zapper_pubkey": zapper,
        "total_sats": int(total_sats),
        "event_count": int(event_count),
        "first_event_at": first_event_at,
        "last_event_at": last_event_at,
        "last_event_ids": json.dumps(last_event_ids or []),
        "last_updated_at": float(last_updated_at),
        "target_pubkey": target_pubkey.strip().lower() if target_pubkey else None,
    }
    try:
        await _db().execute(
            f"""
            INSERT INTO {db.references_schema}zap_totals (
                user_id, zapper_pubkey, total_sats, event_count,
                first_event_at, last_event_at, last_event_ids,
                last_updated_at, target_pubkey
            ) VALUES (
                :user_id, :zapper_pubkey, :total_sats, :event_count,
                :first_event_at, :last_event_at, :last_event_ids,
                :last_updated_at, :target_pubkey
            )
            ON CONFLICT(user_id, zapper_pubkey) DO UPDATE SET
                total_sats = excluded.total_sats,
                event_count = excluded.event_count,
                first_event_at = excluded.first_event_at,
                last_event_at = excluded.last_event_at,
                last_event_ids = excluded.last_event_ids,
                last_updated_at = excluded.last_updated_at,
                target_pubkey = excluded.target_pubkey;
            """,
            payload,
        )
    except Exception as e:
        if any(t in str(e).lower() for t in ["undefinedtable", "does not exist", "no such table"]):
            await _bootstrap_cyberherd_tables()
            await _db().execute(
                f"""
                INSERT INTO {db.references_schema}zap_totals (
                    user_id, zapper_pubkey, total_sats, event_count,
                    first_event_at, last_event_at, last_event_ids,
                    last_updated_at, target_pubkey
                ) VALUES (
                    :user_id, :zapper_pubkey, :total_sats, :event_count,
                    :first_event_at, :last_event_at, :last_event_ids,
                    :last_updated_at, :target_pubkey
                )
                ON CONFLICT(user_id, zapper_pubkey) DO UPDATE SET
                    total_sats = excluded.total_sats,
                    event_count = excluded.event_count,
                    first_event_at = excluded.first_event_at,
                    last_event_at = excluded.last_event_at,
                    last_event_ids = excluded.last_event_ids,
                    last_updated_at = excluded.last_updated_at,
                    target_pubkey = excluded.target_pubkey;
                """,
                payload,
            )
        else:
            raise


async def set_member_notified(pubkey: str, notified_ts: int, user_id: str | None = None) -> None:
    """Set notified timestamp for a member.
    
    Args:
        pubkey: The pubkey of the member.
        notified_ts: Timestamp when member was notified.
        user_id: User ID to filter by. If None, operation is skipped for security.
    """
    if not user_id:
        logger.warning("set_member_notified called without user_id, skipping")
        return
    pubkey = (pubkey or "").strip().lower()
    if not pubkey:
        return
    
    await _db().execute(
        f"""
        UPDATE {db.references_schema}cyber_herd
        SET notified = :ts
        WHERE pubkey = :pubkey AND user_id = :user_id
        """,
        {"ts": int(notified_ts), "pubkey": pubkey, "user_id": user_id},
    )


async def clear_member_notified(pubkey: str, user_id: str | None = None) -> None:
    """Clear notified timestamp for a member.
    
    Args:
        pubkey: The pubkey of the member.
        user_id: User ID to filter by. If None, operation is skipped for security.
    """
    if not user_id:
        logger.warning("clear_member_notified called without user_id, skipping")
        return
    pubkey = (pubkey or "").strip().lower()
    if not pubkey:
        return
    
    await _db().execute(
        f"""
        UPDATE {db.references_schema}cyber_herd
        SET notified = 0
        WHERE pubkey = :pubkey AND user_id = :user_id
        """,
        {"pubkey": pubkey, "user_id": user_id},
    )


async def get_processed_zap(user_id: str, event_id: str):
    """Get a processed zap by event ID."""
    try:
        row = await _db().fetchone(
            f"SELECT * FROM {db.references_schema}processed_zaps WHERE user_id = :user_id AND event_id = :event_id",
            {"user_id": user_id, "event_id": event_id},
        )
        return row
    except Exception as e:
        msg = str(e).lower()
        # If the table is missing in Postgres/SQLite, attempt runtime bootstrap then retry once
        if any(
            t in msg
            for t in (
                "undefinedtable",
                "does not exist",
                "no such table",
                "undefinedcolumn",
                "undefined column",
                "no such column",
            )
        ):
            try:
                await _bootstrap_cyberherd_tables()
                row = await _db().fetchone(
                    f"SELECT * FROM {db.references_schema}processed_zaps WHERE user_id = :user_id AND event_id = :event_id",
                    {"user_id": user_id, "event_id": event_id},
                )
                return row
            except Exception as e2:
                logger.error(f"Error getting processed zap after bootstrap: {e2}")
                return None
        logger.error(f"Error getting processed zap: {e}")
        return None

async def account_zap_payment(*, user_id: str, payment_hash: str,
                              zapper_pubkey: str, amount_sats: int,
                              note_id: str | None = None, processed_at: int | None = None) -> bool:
    """Record the settled invoice identity included in historical totals.

    This ledger is separate from the admission claim: a history refresh may
    include a settled invoice whose membership update has not run yet.
    """
    result = await _db().execute(
        f"""INSERT INTO {db.references_schema}processed_zaps
        (user_id,event_id,note_id,zapper_pubkey,amount,processed_at)
        VALUES (:user_id,:hash,:note,:zapper,:amount,:timestamp)
        ON CONFLICT DO NOTHING""",
        {"user_id": user_id, "hash": payment_hash, "note": note_id,
         "zapper": zapper_pubkey, "amount": amount_sats,
         "timestamp": str(int(time.time()) if processed_at is None else processed_at)},
    )
    return result.rowcount == 1


async def create_processed_zap(processed_zap_data: dict):
    """Create a processed zap record."""
    if not processed_zap_data:
        return None

    payload = dict(processed_zap_data)
    user_id = payload.get("user_id")
    if not user_id:
        logger.debug("create_processed_zap: missing user_id, skipping insert")
        return None

    payload["user_id"] = str(user_id)
    payload["event_id"] = _normalize_hex(payload.get("event_id")) or payload.get(
        "event_id"
    )
    payload["note_id"] = _normalize_hex(payload.get("note_id")) or payload.get(
        "note_id"
    )
    payload["zapper_pubkey"] = (
        _normalize_hex(payload.get("zapper_pubkey")) or payload.get("zapper_pubkey")
    )

    amount_raw = payload.get("amount")
    if amount_raw is None:
        amount_raw = payload.get("amount_sats")
    try:
        payload["amount"] = int(amount_raw) if amount_raw is not None else None
    except Exception:
        payload["amount"] = None

    processed_at = _parse_timestamp_field(payload.get("processed_at"))
    if processed_at is None:
        processed_at = int(time.time())
    payload["processed_at"] = str(processed_at)

    try:
        result = await _db().execute(
            f"""INSERT INTO {db.references_schema}processed_zaps (user_id, event_id, note_id, zapper_pubkey, amount, processed_at)
            VALUES (:user_id, :event_id, :note_id, :zapper_pubkey, :amount, :processed_at)
            ON CONFLICT DO NOTHING
            """,
            payload,
        )
        return payload if result.rowcount == 1 else None
    except Exception as e:
        msg = str(e).lower()
        if any(
            t in msg
            for t in (
                "undefinedtable",
                "does not exist",
                "no such table",
                "undefinedcolumn",
                "undefined column",
                "no such column",
            )
        ):
            try:
                await _bootstrap_cyberherd_tables()
                await _db().execute(
                    f"""INSERT INTO {db.references_schema}processed_zaps (user_id, event_id, note_id, zapper_pubkey, amount, processed_at)
                    VALUES (:user_id, :event_id, :note_id, :zapper_pubkey, :amount, :processed_at)
            ON CONFLICT DO NOTHING
                    """,
                    payload,
                )
                return payload
            except Exception as e2:
                logger.error(f"Error creating processed zap after bootstrap: {e2}")
                return None


async def fetch_processed_zaps(
    user_id: str,
    since: int | None = None,
    limit: int | None = 100,
    note_id: str | None = None,
    zapper_pubkey: str | None = None,
    *,
    fetch_multiplier: int = 4,
) -> tuple[list[dict], bool]:
    """Fetch processed zap records for diagnostics or debugging.

    Returns:
        (rows, has_more) tuple ordered by newest first.
    """
    if not user_id:
        return [], False

    clauses = ["user_id = :user_id"]
    params: dict[str, object] = {"user_id": user_id}

    if note_id:
        params["note_id"] = note_id.strip()
        clauses.append("note_id = :note_id")

    if zapper_pubkey:
        zp = zapper_pubkey.strip().lower()
        params["zapper_pubkey"] = zp
        clauses.append(
            "(LOWER(zapper_pubkey) = :zapper_pubkey OR zapper_pubkey = :zapper_pubkey)"
        )

    where_sql = " AND ".join(clauses)
    sql = (
        f"SELECT * FROM {db.references_schema}processed_zaps "
        f"WHERE {where_sql}"
    )

    try:
        base_limit = max(1, min(int(limit) if limit else 100, 500))
    except Exception:
        base_limit = 100

    try:
        batch_size = min(max(base_limit * max(1, int(fetch_multiplier)), 128), 2000)
    except (TypeError, ValueError):
        batch_size = 400
    # Legacy rows mix ISO timestamps and epoch strings. Lexical SQL ordering
    # cannot select the newest rows correctly. Scan in bounded batches and
    # retain only the newest page plus one lookahead record.
    sql += " ORDER BY processed_at DESC, event_id DESC, zap_receipt_id DESC LIMIT :limit OFFSET :offset"
    params["limit"] = batch_size
    params["offset"] = 0
    since_value = _parse_timestamp_field(since)

    try:
        candidate_rows: list[dict[str, Any]] = []
        while True:
            rows = await _db().fetchall(sql, params)
            for row in rows:
                data = dict(row)
                parsed_ts = _parse_timestamp_field(data.get("processed_at"))
                if since_value is not None and (parsed_ts is None or parsed_ts < since_value):
                    continue
                data["_processed_ts"] = parsed_ts
                if parsed_ts is not None:
                    data["processed_at_ts"] = parsed_ts
                candidate_rows.append(data)
            candidate_rows.sort(key=lambda row: row.get("_processed_ts") or 0, reverse=True)
            candidate_rows = candidate_rows[:base_limit + 1]
            if len(rows) < batch_size:
                break
            params["offset"] += len(rows)

        has_more = len(candidate_rows) > base_limit
        trimmed = candidate_rows[:base_limit]
        for r in trimmed:
            r.pop("_processed_ts", None)
        return trimmed, has_more
    except Exception as e:
        msg = str(e).lower()
        if any(
            keyword in msg
            for keyword in (
                "undefinedtable",
                "does not exist",
                "no such table",
                "no such column",
                "undefined column",
            )
        ):
            try:
                await _bootstrap_cyberherd_tables()
                return await fetch_processed_zaps(
                    user_id=user_id,
                    since=since,
                    limit=limit,
                    note_id=note_id,
                    zapper_pubkey=zapper_pubkey,
                    fetch_multiplier=fetch_multiplier,
                )
            except Exception:
                return [], False
        logger.warning(f"Failed fetching processed zaps for user {user_id}: {e}")
        return [], False


async def count_processed_zaps(user_id: str) -> int:
    """Return the number of persisted zaps processed for a user."""
    if not user_id:
        return 0

    sql = f"SELECT COUNT(*) AS count FROM {db.references_schema}processed_zaps WHERE user_id = :user_id"
    params = {"user_id": user_id}

    async def _fetch() -> int:
        row = await _db().fetchone(sql, params)
        if not row:
            return 0
        try:
            return int(row.get("count") or 0)
        except Exception:
            try:
                return int(row["count"])  # type: ignore[index]
            except Exception:
                return 0

    try:
        return await _fetch()
    except Exception as e:
        msg = str(e).lower()
        if any(
            keyword in msg
            for keyword in (
                "undefinedtable",
                "does not exist",
                "no such table",
                "no such column",
                "undefined column",
            )
        ):
            try:
                await _bootstrap_cyberherd_tables()
                return await _fetch()
            except Exception:
                return 0
        logger.debug(f"Failed counting processed zaps for user {user_id}: {e}")
        return 0


async def get_monitoring_totals(user_id: str) -> dict[str, int]:
    """Return persisted monitoring totals for zap/repost/reaction events."""
    totals = {"zap": 0, "repost": 0, "reaction": 0}
    if not user_id:
        return totals

    totals["zap"] = await count_processed_zaps(user_id)

    sql = (
        f"SELECT event_type, COUNT(*) AS count "
        f"FROM {db.references_schema}processed_events "
        f"WHERE user_id = :user_id AND event_type IS NOT NULL "
        f"GROUP BY event_type"
    )
    params = {"user_id": user_id}

    async def _fetch() -> None:
        rows = await _db().fetchall(sql, params)
        for row in rows or []:
            event_type_raw = row.get("event_type")
            try:
                event_type = str(event_type_raw).strip().lower()
            except Exception:
                continue
            try:
                count = int(row.get("count") or 0)
            except Exception:
                try:
                    count = int(row["count"])  # type: ignore[index]
                except Exception:
                    count = 0

            if event_type in ("repost", "reaction"):
                totals[event_type] = count
            elif event_type == "zap":
                totals["zap"] = max(totals["zap"], count)

    try:
        await _fetch()
    except Exception as e:
        msg = str(e).lower()
        if any(
            keyword in msg
            for keyword in (
                "undefinedtable",
                "does not exist",
                "no such table",
                "no such column",
                "undefined column",
            )
        ):
            try:
                await _bootstrap_cyberherd_tables()
                await _fetch()
            except Exception:
                return totals
        else:
            logger.debug(f"Failed fetching monitoring totals for user {user_id}: {e}")

    return totals


async def is_payment_processed(
    user_id: str, payment_hash: str, note_id: str | None = None, *, strict: bool = False
) -> bool:
    """Check if a payment/event has already been processed.
    
    Args:
        user_id: User ID to scope the check.
        payment_hash: Payment hash or event ID to check.
        note_id: Optional note ID. If provided, checks for the specific (event, note) pair.
                 If not provided, checks if event was processed for ANY note.
    
    ``strict=True`` propagates lookup failures so social admission cannot treat
    unavailable deduplication storage as an unprocessed event.

    Returns:
        True if payment was already processed, False otherwise.
    """
    if not user_id or not payment_hash:
        return False
    
    try:
        if note_id is not None:
            # Check for specific (event, note) pair
            # Use COALESCE to handle both NULL and empty string note_id values
            row = await _db().fetchone(
                f"SELECT 1 FROM {db.references_schema}processed_events WHERE user_id = :user_id AND event_hash = :event_hash AND COALESCE(note_id, '') = :note_id",
                {"user_id": user_id, "event_hash": payment_hash, "note_id": note_id or ''},
            )
        else:
            # Check if event was processed for any note (backward compatible behavior)
            row = await _db().fetchone(
                f"SELECT 1 FROM {db.references_schema}processed_events WHERE user_id = :user_id AND event_hash = :event_hash",
                {"user_id": user_id, "event_hash": payment_hash},
            )
        return row is not None
    except Exception as e:
        if strict:
            raise
        msg = str(e).lower()
        if any(t in msg for t in ("undefinedtable", "does not exist", "no such table")):
            try:
                await _bootstrap_cyberherd_tables()
                if note_id is not None:
                    row = await _db().fetchone(
                        f"SELECT 1 FROM {db.references_schema}processed_events WHERE user_id = :user_id AND event_hash = :event_hash AND COALESCE(note_id, '') = :note_id",
                        {"user_id": user_id, "event_hash": payment_hash, "note_id": note_id or ''},
                    )
                else:
                    row = await _db().fetchone(
                        f"SELECT 1 FROM {db.references_schema}processed_events WHERE user_id = :user_id AND event_hash = :event_hash",
                        {"user_id": user_id, "event_hash": payment_hash},
                    )
                return row is not None
            except Exception:
                return False
        return False


async def register_processed_payment(
    user_id: str,
    payment_hash: str,
    note_id: str | None = None,
    pubkey: str | None = None,
    event_type: str | None = None,
) -> bool:
    """Register a payment as processed to prevent duplicate handling.
    
    Args:
        user_id: User ID to scope the record.
        payment_hash: Payment hash to register.
        note_id: Optional note ID for reference. Used in unique constraint to allow
                 the same event to be processed for multiple different notes.
        pubkey: Optional pubkey for reference.
        event_type: Optional classification for the processed event (e.g., "zap").
    
    Returns:
        True if successfully registered (new), False if already existed (ON CONFLICT).
    """
    if not user_id or not payment_hash:
        return False

    # Normalize note_id: convert None to empty string for consistent unique constraint handling
    normalized_note_id = note_id or ''

    # One statement arbitrates concurrent claims without aborting a PostgreSQL
    # transaction on a duplicate. A read/check followed by INSERT is not atomic.
    async with _get_crud_write_lock():
        result = await _db().execute(
            f"""INSERT INTO {db.references_schema}processed_events
            (user_id, event_hash, note_id, pubkey, processed_at, event_type)
            VALUES (:user_id, :event_hash, :note_id, :pubkey, :processed_at, :event_type)
            ON CONFLICT DO NOTHING""",
            {"user_id": user_id, "event_hash": payment_hash,
             "note_id": normalized_note_id, "pubkey": _normalize_hex(pubkey) or pubkey,
             "processed_at": int(time.time()), "event_type": event_type},
        )
        return result.rowcount == 1


async def clear_processed_zaps_for_user(user_id: str | None):
    """Delete processed_zaps rows for a given user_id. If user_id is None, no-op.

    Used by manual reset to allow a hard reset of processed zap history for the
    requesting user so warm-start can reprocess payments or zaps.
    """
    if not user_id:
        return
    try:
        await _db().execute(
            f"DELETE FROM {db.references_schema}processed_zaps WHERE user_id = :user_id",
            {"user_id": user_id},
        )
    except Exception as e:
        msg = str(e).lower()
        if any(t in msg for t in ("undefinedtable", "does not exist", "no such table")):
            try:
                await _bootstrap_cyberherd_tables()
                await _db().execute(
                    f"DELETE FROM {db.references_schema}processed_zaps WHERE user_id = :user_id",
                    {"user_id": user_id},
                )
            except Exception as ie:
                logger.warning(f"Failed clearing processed_zaps after bootstrap: {ie}")
        else:
            logger.warning(f"Failed clearing processed_zaps for user {user_id}: {e}")


async def clear_processed_events_for_user(user_id: str | None):
    """Delete processed_events rows for a given user_id. If user_id is None, no-op.

    This is useful for tests and for administrative cleanup.
    """
    if not user_id:
        return
    try:
        await _db().execute(
            f"DELETE FROM {db.references_schema}processed_events WHERE user_id = :user_id",
            {"user_id": user_id},
        )
    except Exception as e:
        msg = str(e).lower()
        if any(t in msg for t in ("undefinedtable", "does not exist", "no such table")):
            try:
                await _bootstrap_cyberherd_tables()
                await _db().execute(
                    f"DELETE FROM {db.references_schema}processed_events WHERE user_id = :user_id",
                    {"user_id": user_id},
                )
            except Exception as ie:
                logger.warning(f"Failed clearing processed_events after bootstrap: {ie}")
        else:
            logger.warning(f"Failed clearing processed_events for user {user_id}: {e}")


async def is_event_processed(
    user_id: str, event_hash: str, note_id: str | None = None, *, strict: bool = False
) -> bool:
    """Alias for is_payment_processed with clearer naming for nostr events.

    This exists to make calling code explicit about processing Nostr events
    (reposts/reactions) vs invoice payments while reusing the same persisted
    processed_events table.
    
    Args:
        user_id: User ID to scope the check.
        event_hash: Event ID to check.
        strict: Propagate database lookup errors instead of returning False.
        note_id: Optional note ID. If provided, checks for the specific (event, note) pair.
                 If not provided, checks if event was processed for ANY note.
    """
    return await is_payment_processed(user_id, event_hash, note_id=note_id, strict=strict)


async def register_processed_event(
    user_id: str,
    event_hash: str,
    note_id: str | None = None,
    pubkey: str | None = None,
    event_type: str | None = None,
) -> bool:
    """Alias for register_processed_payment with clearer naming for nostr events.

    Returns True if a new record was inserted, False if it already existed or on failure.
    """
    return await register_processed_payment(
        user_id,
        event_hash,
        note_id=note_id,
        pubkey=pubkey,
        event_type=event_type,
    )


async def delete_processed_event(
    user_id: str,
    event_hash: str,
    note_id: str | None = None,
) -> None:
    """Remove a processed event marker, used when admission fails after claim."""
    if not user_id or not event_hash:
        return

    try:
        if note_id is not None:
            await _db().execute(
                f"""
                DELETE FROM {db.references_schema}processed_events
                WHERE user_id = :user_id
                  AND event_hash = :event_hash
                  AND COALESCE(note_id, '') = :note_id
                """,
                {
                    "user_id": user_id,
                    "event_hash": event_hash,
                    "note_id": note_id or "",
                },
            )
        else:
            await _db().execute(
                f"""
                DELETE FROM {db.references_schema}processed_events
                WHERE user_id = :user_id AND event_hash = :event_hash
                """,
                {"user_id": user_id, "event_hash": event_hash},
            )
    except Exception as exc:
        msg = str(exc).lower()
        if any(t in msg for t in ("undefinedtable", "does not exist", "no such table")):
            return
        logger.warning(
            f"Failed deleting processed event marker for user {user_id}: {exc}"
        )


async def get_member_by_pubkey(user_id: str, pubkey: str):
    """Get a member by pubkey."""
    if not user_id:
        logger.warning("get_member_by_pubkey called without user_id, returning None")
        return None
    normalized_pubkey = (pubkey or "").strip().lower()
    if not normalized_pubkey:
        logger.warning("get_member_by_pubkey called without valid pubkey, returning None")
        return None
    try:
        row = await _db().fetchone(
            f"SELECT * FROM {db.references_schema}cyber_herd WHERE pubkey = :pubkey AND user_id = :user_id",
            {"pubkey": normalized_pubkey, "user_id": user_id}
        )
        return (await _with_engagement_awards([dict(row)], user_id))[0] if row else None
    except Exception as e:
        logger.error(f"Error getting member by pubkey: {e}")
        return None


async def get_cyberherd_notes_for_settings(settings, since_days_ago: int = 0) -> list[str]:
    """Get CyberHerd note IDs for given settings using nostr relay queries.
    
    This returns notes detected by querying Nostr relays (fresh check).
    For the complete list of tracked notes (including auto-detected and manually added),
    use settings.tracked_event_ids instead.
    
    Args:
        settings: CyberherdSettings object
        since_days_ago: How many days back to look (0 = today only, 1 = yesterday+today, etc.)
    
    Returns:
        List of note IDs matching the criteria from Nostr query
    """
    # Get automatically detected notes
    detected_notes = await _get_detected_cyberherd_notes_for_settings(settings, since_days_ago)
    
    return detected_notes


async def _get_detected_cyberherd_notes_for_settings(settings, since_days_ago: int = 0) -> list[str]:
    # Preserve original tag case (minus a leading '#') and build a case-insensitive
    # filter set for Nostr. We send BOTH the original and lowercase variants in the
    # subscription/query filters because relay matching is exact; then we perform
    # all internal comparisons using the lowercase set.
    original_tags = [t.lstrip("#") for t in (settings.tracked_tags or []) if t]
    # Normalized lowercase list for internal matching
    tags_lower = [t.lower() for t in original_tags]
    # Allow empty tags - if not set, any note from author will count
    # Build expanded filter list (original order preserved; then lowercase variants)
    filter_tags: list[str] = []
    seen: set[str] = set()
    for t in original_tags + tags_lower:
        if t and t not in seen:
            filter_tags.append(t)
            seen.add(t)

    # Get effective pubkey - try multiple sources
    eff_pub = getattr(settings, "computed_effective_pubkey", None)
    # Treat non-string or obviously placeholder/mock values as missing so we can fall back
    try:
        if eff_pub and (not isinstance(eff_pub, str) or len(eff_pub) < 32):  # nostr hex pubkeys are 64 chars
            eff_pub = None
    except Exception:
        eff_pub = None
    if not eff_pub:
        # Try to compute from private key or override
        try:
            # Import locally to avoid circular imports
            import sys
            views_module = sys.modules.get('lnbits.extensions.cyberherd.views_api')
            if views_module:
                eff_pub = views_module._get_cached_effective_pubkey(settings)
                if not eff_pub:
                    eff_pub = views_module._get_effective_pubkey(settings)
        except Exception:
            eff_pub = None
    
    if not eff_pub:
        logger.warning("No effective pubkey available for note detection")
        return []

    # Use UTC-first time calculation (authoritative)
    midnight_utc, midnight_local, since_timestamp, until_timestamp = get_utc_day_boundaries(since_days_ago)
    
    # CRITICAL: Query strategy for note detection
    # We query by author + time + kind, then filter locally by hashtags
    # This is because:
    # 1. Relays may not index hashtags in content, only t-tags
    # 2. Users may use #hashtag in content without adding t-tag
    # 3. We need to catch both cases for comprehensive tracking
    #
    # For ENGAGEMENTS (reposts/reactions), we use #e (event reference) not #t
    # This is handled separately in nostr_event_monitor.py
    
    base_filter = {
        "kinds": [1, 30311],
        "since": since_timestamp,
        "authors": [eff_pub] if eff_pub else [],
    }

    filter_list = []
    if filter_tags:
        filter_with_tags = dict(base_filter)
        filter_with_tags["#t"] = filter_tags
        filter_list.append(filter_with_tags)
    filter_list.append(dict(base_filter))  # author-only fallback (catches content-only hashtags)

    logger.info(
        "\U0001F50D Querying notes: author=%s..., since=%s, filters=%d (tags=%s)",
        eff_pub[:16] if eff_pub else "none",
        since_timestamp,
        len(filter_list),
        filter_tags[:3],
    )

    try:
        from .services import nostr_helpers

        raw_events = []
        for idx, filt in enumerate(filter_list, start=1):
            try:
                fetched = await nostr_helpers.query_events(filt, limit=100, timeout=6.0)
                logger.info(
                    "get_cyberherd_notes_for_settings: filter %d returned %d events (has_t=%s)",
                    idx,
                    len(fetched or []),
                    "#t" in filt,
                )
                raw_events.extend(fetched or [])
            except Exception as sub_exc:
                logger.warning(
                    "get_cyberherd_notes_for_settings: filter %d failed: %s", idx, sub_exc
                )

        # Deduplicate events by id while preserving order of appearance (tagged-first preferred)
        deduped_events = []
        seen_ids: set[str] = set()
        for ev in raw_events:
            ev_id = ev.get("id") if isinstance(ev, dict) else None
            if isinstance(ev_id, str) and ev_id not in seen_ids:
                seen_ids.add(ev_id)
                deduped_events.append(ev)

        note_ids: list[str] = []
        debug_reasons: list[str] = []
        
        # Use the until_timestamp from the authoritative calculation
        for ev in deduped_events:
            try:
                ca = int(ev.get("created_at") or 0)
            except Exception:
                ca = 0
            # Strict: only accept events within the current local day window
            if not (since_timestamp <= ca < until_timestamp):
                if len(debug_reasons) < 5:
                    debug_reasons.append(f"skip:not_in_range ts={ca}")
                continue
            
            # Enhanced hashtag matching: check BOTH t-tags AND content
            content_l = (ev.get("content") or "").lower()
            tag_values = extract_t_tags_from_event(ev)
            
            # If no tags configured, accept all notes from author
            # Otherwise, match if: (1) any tag appears in t-tags, OR (2) any tag appears as #tag in content
            # Initialise before the branch: with no tags configured these stay
            # False, and the debug log further down would otherwise raise
            # NameError — silently making every no-tag config detect zero notes.
            has_t_tag = False
            has_content_hashtag = False
            if not tags_lower:
                # No tags set - accept any note from author
                pass
            else:
                has_t_tag = bool(set(tags_lower) & tag_values)
                has_content_hashtag = any(f"#{t}" in content_l for t in tags_lower)
                
                if not (has_t_tag or has_content_hashtag):
                    if len(debug_reasons) < 5:
                        ev_id_short = (ev.get('id') or '')[:16]
                        debug_reasons.append(f"skip:tag_miss id={ev_id_short}... t-tags={list(tag_values)[:3]} content_match={has_content_hashtag}")
                    continue
            
            # Author verification
            if ev.get("pubkey") != eff_pub:
                if len(debug_reasons) < 5:
                    ep = str(ev.get('pubkey'))
                    debug_reasons.append(f"skip:pubkey_mismatch ev={ep[:8]} eff={eff_pub[:8]}")
                continue
            
            if isinstance(ev.get("id"), str):
                note_ids.append(ev["id"])
                if len(note_ids) <= 5:  # Log first few for debugging
                    logger.debug(f"✓ Matched note {ev['id'][:16]}...: t-tag={has_t_tag}, content={has_content_hashtag}")
        
        # Log debug information if no events were found
        if not note_ids and debug_reasons:
            try:
                logger.warning(f"❌ No notes matched filters. Reasons: {', '.join(debug_reasons[:10])}")
            except Exception:
                pass
        elif note_ids:
            logger.info(f"✅ Matched {len(note_ids)} notes: {[n[:16]+'...' for n in note_ids[:5]]}")
        
        return note_ids
        
    except Exception as e:
        logger.error(f"Error in get_cyberherd_notes_for_settings: {e}", exc_info=True)
        return []
