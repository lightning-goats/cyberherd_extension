from __future__ import annotations

import json
import string
import time
from datetime import datetime, timezone
import asyncio
from typing import Any

from loguru import logger

from lnbits.db import Database # type: ignore
from lnbits.helpers import decrypt_internal_message, encrypt_internal_message # type: ignore

from .models import CyberherdSettings, LegacyCyberherdMember
from .services.shares import compute_member_share_percentages
from .utils.common import extract_t_tags_from_event, utc_now

db = Database("ext_cyberherd")
# Serialize critical write operations in this module to reduce sqlite locking
_crud_write_lock = asyncio.Lock()


def _sanitize_private_key(raw_value) -> tuple[str | None, int]:
    """Normalize decrypted nostr keys to 64 lower-case hex characters."""
    if raw_value is None:
        return None, 0

    candidate = raw_value
    if isinstance(candidate, bytes):
        candidate = candidate.decode("utf-8", errors="ignore")

    if not isinstance(candidate, str):
        return None, 0

    cleaned = "".join(ch for ch in candidate if ch in string.hexdigits).lower()
    if cleaned.startswith("0x"):
        cleaned = cleaned[2:]

    if len(cleaned) >= 64:
        return cleaned[:64], len(cleaned)

    return None, len(cleaned)


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
    This ensures cache keys are based on local date, not UTC date.
    """
    from datetime import timedelta
    
    now_local = datetime.now()
    target_date = now_local.date() - timedelta(days=days_ago)
    return target_date.isoformat()


async def get_settings(user_id: str | None = None) -> CyberherdSettings:
    try:
        # Attempt direct fetch by user first (multi-tenant row)
        row = None
        if user_id:
            row = await db.fetchone(
                f"SELECT * FROM {db.references_schema}settings WHERE user_id = :user_id",
                {"user_id": user_id},
            )
            if row:
                try:
                    logger.debug(f"Cyberherd CRUD get_settings: user_id={user_id} row exists")
                except Exception:
                    pass
                return _row_to_settings(row)

        # Fallback to legacy/global settings row when no user-specific record exists.
        # This ensures existing single-tenant deployments continue to surface their
        # configured wallets/settings instead of returning an empty defaults object.
        if not row:
            try:
                legacy_row = await db.fetchone(
                    f"SELECT * FROM {db.references_schema}settings WHERE user_id IS NULL ORDER BY rowid LIMIT 1"
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


async def _bootstrap_settings_storage():
    """Idempotently create schema/table/columns/index for settings if missing.

    Safe to run at runtime; guards each DDL so it works on Postgres and SQLite.
    """
    # Detect if we're on PostgreSQL (has schema support)
    is_postgres = False
    try:
        # Try a PostgreSQL-specific query
        await db.execute("SELECT 1 FROM information_schema.schemata LIMIT 1")
        is_postgres = True
    except Exception:
        pass
    
    table_name = "settings"
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
                if len(parts) >= 6 and parts[1] == table_name and parts[3] == "ADD" and parts[4] == "COLUMN":
                    column_name = parts[5]
                    if await _column_exists(table_name, column_name):
                        try:
                            logger.debug(f"Cyberherd bootstrap skipping existing column: {column_name}")
                        except Exception:
                            pass
                        continue
            await db.execute(s)
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
                await db.execute(f"ALTER TABLE {table_name} ADD COLUMN tracked_event_ids TEXT;")
            except Exception:
                # ignore if another process added it concurrently
                pass
            try:
                # Perform per-row migration so we can normalize legacy formats.
                rows = await db.fetchall(f"SELECT user_id, manual_event_ids, tracked_event_ids FROM {table_name}")
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
                            await db.execute(
                                f"UPDATE {table_name} SET tracked_event_ids = :te WHERE user_id = :uid AND (tracked_event_ids IS NULL OR tracked_event_ids = '')",
                                {"te": normalized, "uid": r.get("user_id")},
                            )
                        else:
                            # legacy single-row (no user_id)
                            await db.execute(
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
    """Return True if the given column exists. Works for Postgres; falls back to False on errors.

    We avoid deliberately triggering a failing statement inside an open transaction because that
    would put the transaction in an aborted state on Postgres. Instead we query information_schema.
    """
    try:
        if "." in schema_table:
            schema, table = schema_table.split(".", 1)
        else:
            schema, table = None, schema_table
        
        # Try PostgreSQL information_schema first (works for both cases)
        try:
            if schema:
                row = await db.fetchone(
                    """
                    SELECT 1 FROM information_schema.columns
                    WHERE table_schema = :schema AND table_name = :table AND column_name = :col
                    LIMIT 1
                    """,
                    {"schema": schema, "table": table, "col": column},
                )
            else:
                # No schema specified - check cyberherd schema first, then public for PostgreSQL
                row = await db.fetchone(
                    """
                    SELECT 1 FROM information_schema.columns
                    WHERE table_schema = 'cyberherd' AND table_name = :table AND column_name = :col
                    LIMIT 1
                    """,
                    {"table": table, "col": column},
                )
                if not row:
                    # Fallback to public schema
                    row = await db.fetchone(
                        """
                        SELECT 1 FROM information_schema.columns
                        WHERE table_schema = 'public' AND table_name = :table AND column_name = :col
                        LIMIT 1
                        """,
                        {"table": table, "col": column},
                    )
            if row:
                return True
        except Exception:
            # Not PostgreSQL or information_schema not available
            pass
        
        # Fallback to SQLite PRAGMA
        try:
            rows = await db.fetchall(f"PRAGMA table_info({table})")
            for r in rows:
                try:
                    if (r.get("name") or r.get("Name")) == column:
                        return True
                except Exception:
                    continue
        except Exception:
            pass
            
    except Exception:
        pass
    return False


async def get_settings_for_source_wallet(source_wallet: str) -> CyberherdSettings:
    """Attempt to find the settings row (user-scoped) that owns the given source_wallet.

    Falls back to global settings if no user mapping found.
    """
    try:
        row = await db.fetchone(
            f"SELECT * FROM {db.references_schema}settings WHERE user_id IN (SELECT user FROM wallets WHERE id = :wid)",
            {"wid": source_wallet},
        )
        if row:
            return _row_to_settings(row)
    except Exception as e:
        # In some test/dev DBs the core 'wallets' table may not exist on this connection.
        try:
            logger.debug(f"get_settings_for_source_wallet fallback (no wallets table?): {e}")
        except Exception:
            pass
    # If the above lookup didn't succeed (no wallets table or no mapping), try direct match
    try:
        row = await db.fetchone(
            f"SELECT * FROM {db.references_schema}settings WHERE source_wallet = :wid",
            {"wid": source_wallet},
        )
        if row:
            return _row_to_settings(row)
    except Exception:
        # Ignore and fall back to global
        pass
    # fallback to global
    return await get_settings(None)


async def get_settings_for_herd_wallet(herd_wallet: str) -> CyberherdSettings:
    """Return settings row that owns the given herd_wallet, falling back to global settings.

    Useful for mapping incoming payments to the correct user context.
    """
    try:
        row = await db.fetchone(
            f"SELECT * FROM {db.references_schema}settings WHERE herd_wallet = :wid",
            {"wid": herd_wallet},
        )
        if row:
            return _row_to_settings(row)
    except Exception:
        pass
    return await get_settings(None)


async def list_settings_with_tracking(require_herd_wallet: bool = True) -> list[CyberherdSettings]:
    """List settings rows with zap tracking enabled.

    If require_herd_wallet is True, only include rows that have a herd_wallet configured.
    """
    try:
        if require_herd_wallet:
            rows = await db.fetchall(
                f"SELECT * FROM {db.references_schema}settings WHERE zap_tracking_enabled = 1 AND herd_wallet IS NOT NULL AND TRIM(herd_wallet) != ''"
            )
        else:
            rows = await db.fetchall(
                f"SELECT * FROM {db.references_schema}settings WHERE zap_tracking_enabled = 1"
            )
    except Exception:
        rows = []
    return [_row_to_settings(r) for r in rows]


async def get_cyberherd_settings_for_all_users() -> list[CyberherdSettings]:
    """Get all cyberherd settings for all users."""
    try:
        rows = await db.fetchall(f"SELECT * FROM {db.references_schema}settings")
    except Exception:
        rows = []
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

    stored_key = row.get("nostr_private_key")
    nostr_key: str | None = None
    sanitized_len = 0
    if stored_key:
        decrypted = None
        try:
            decrypted = decrypt_internal_message(stored_key)
        except Exception as exc:
            logger.warning(f"Cyberherd: Failed to decrypt nostr_private_key: {type(exc).__name__}")
            if isinstance(stored_key, (bytes, str)):
                decrypted = stored_key

        nostr_key, sanitized_len = _sanitize_private_key(decrypted)
        if decrypted is not None and nostr_key is None and sanitized_len not in (0, 64):
            try:
                logger.warning(
                    f"Cyberherd: sanitized nostr key invalid length {sanitized_len} (expected >=64)"
                )
            except Exception:
                pass
    
    # Log key status (NEVER log the actual key value - security risk!)
    try:
        logger.debug(
            f"Cyberherd CRUD _row_to_settings: user_id={row.get('user_id')}, source_wallet={row.get('source_wallet')}, "
            f"zap_wallet={row.get('zap_wallet')}, nostr_key_present={bool(nostr_key)}, "
            f"nostr_key_length={len(nostr_key) if nostr_key else sanitized_len}, override_present={bool(row.get('nostr_pubkey_override'))}"
        )
    except Exception:
        pass

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
        max_members=row.get("max_members") or 3,
        tracked_tags=tracked_list,
        tracked_event_ids=combined_tracked_events,
        tracked_event_timestamps=tracked_timestamps_dict,
        tracked_event_addresses=tracked_addresses_dict,
        nostr_private_key=nostr_key,
        nostr_pubkey_override=row.get("nostr_pubkey_override"),
        computed_effective_pubkey=row.get("computed_effective_pubkey"),
        zap_tracking_enabled=bool(row.get("zap_tracking_enabled", True)),  # Default to True
        zap_monitor_mode=row.get("zap_monitor_mode", "websocket"),
        templates_owner_user=row.get("templates_owner_user"),  # legacy read-only; no longer set via API
        repost_tracking_enabled=bool(row.get("repost_tracking_enabled", False)),
        likes_tracking_enabled=bool(row.get("likes_tracking_enabled", False)),
        midnight_reset_enabled=bool(row.get("midnight_reset_enabled", True)),
        minimum_sats=row.get("minimum_sats") or 10,
        nip05_verification_enabled=bool(row.get("nip05_verification_enabled", True)),
        feeder_trigger_sats=row.get("feeder_trigger_sats"),
        user_id=row.get("user_id"),
    )


async def upsert_settings(settings: CyberherdSettings, user_id: str | None = None):
    _t0 = time.time()
    stored_key = (
        None
        if getattr(settings, "nostr_private_key", None) is None
        else encrypt_internal_message(settings.nostr_private_key)
    )
    try:
        # NEVER log stored_key prefix - it contains encrypted secret! Only log metadata
        logger.debug(
            f"Cyberherd upsert_settings: stored_key has_value={bool(stored_key)} len={len(stored_key) if stored_key else 0}"
        )
    except Exception:
        pass
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
            existing = await db.fetchone(
                f"SELECT 1 FROM {db.references_schema}settings WHERE user_id = :user_id", {"user_id": user_id}
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
                    await db.execute(sql)
                else:
                    await db.execute(sql, params)
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
            "nostr_private_key = :nostr_private_key",
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
        # Only include midnight_reset_enabled if the column exists in this DB
        has_midnight_col = await _column_exists("settings", "midnight_reset_enabled")
        if has_midnight_col:
            update_cols.insert(update_cols.index("minimum_sats = :minimum_sats"), "midnight_reset_enabled = :midnight_reset_enabled")
        # Only include nip05_verification_enabled if the column exists in this DB
        has_nip05_col = await _column_exists("settings", "nip05_verification_enabled")
        if has_nip05_col:
            update_cols.insert(update_cols.index("minimum_sats = :minimum_sats") + 1, "nip05_verification_enabled = :nip05_verification_enabled")
        if has_templates_col:
            update_cols.append("templates_owner_user = :templates_owner_user")
        sql = f"UPDATE {db.references_schema}settings SET " + ", ".join(update_cols) + " WHERE user_id = :user_id"
        params = {
            "source_wallet": getattr(settings, "source_wallet", None),
            "zap_wallet": getattr(settings, "zap_wallet", None),
            "zap_wallet_alias": getattr(settings, "zap_wallet_alias", None),
            "herd_wallet": getattr(settings, "herd_wallet", None),
            "max_members": int(getattr(settings, "max_members", 3) or 3),
            "tracked_tags": tracked_json,
            "tracked_event_ids": json.dumps(getattr(settings, "tracked_event_ids", []) or []),
            "tracked_event_timestamps": json.dumps(getattr(settings, "tracked_event_timestamps", {}) or {}),
            "tracked_event_addresses": json.dumps(getattr(settings, "tracked_event_addresses", {}) or {}),
            "nostr_private_key": stored_key,
            "nostr_pubkey_override": getattr(settings, "nostr_pubkey_override", None),
            "computed_effective_pubkey": getattr(settings, "computed_effective_pubkey", None),
            "zap_tracking_enabled": int(getattr(settings, "zap_tracking_enabled", False)),
            "zap_monitor_mode": getattr(settings, "zap_monitor_mode", "websocket"),
            "repost_tracking_enabled": int(getattr(settings, "repost_tracking_enabled", False)),
            "likes_tracking_enabled": int(getattr(settings, "likes_tracking_enabled", False)),
            "minimum_sats": int(getattr(settings, "minimum_sats", 10) or 10),
            "templates_owner_user": getattr(settings, "templates_owner_user", None),
            "user_id": user_id,
        }
        if has_feeder_col:
            feeder_value = getattr(settings, "feeder_trigger_sats", None)
            params["feeder_trigger_sats"] = int(feeder_value) if feeder_value not in (None, "") else None
        # Only add midnight_reset_enabled param if column exists
        if has_midnight_col:
            params["midnight_reset_enabled"] = int(getattr(settings, "midnight_reset_enabled", True))
        # Only add nip05_verification_enabled param if column exists
        if has_nip05_col:
            params["nip05_verification_enabled"] = int(getattr(settings, "nip05_verification_enabled", True))
        try:
            async with _crud_write_lock:
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
                "Cyberherd CRUD upsert (update) user_id=%s source_wallet=%s nostr_key_present=%s override_present=%s",
                user_id,
                getattr(settings, "source_wallet", None),
                bool(getattr(settings, "nostr_private_key", None)),
                bool(getattr(settings, "nostr_pubkey_override", None)),
            )
        except Exception:
            pass
        try:
            _dur = int((time.time() - _t0) * 1000)
            logger.debug(f"Cyberherd upsert_settings END user_id={user_id} dur_ms={_dur}")
        except Exception:
            pass
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
                await _sched_recompute(str(getattr(settings, "source_wallet")))
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
            "nostr_private_key",
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
            "max_members": int(getattr(settings, "max_members", 3) or 3),
            "tracked_tags": tracked_json,
            "tracked_event_ids": json.dumps(getattr(settings, "tracked_event_ids", []) or []),
            "tracked_event_timestamps": json.dumps(getattr(settings, "tracked_event_timestamps", {}) or {}),
            "tracked_event_addresses": json.dumps(getattr(settings, "tracked_event_addresses", {}) or {}),
            "nostr_private_key": stored_key,
            "nostr_pubkey_override": getattr(settings, "nostr_pubkey_override", None),
            "computed_effective_pubkey": getattr(settings, "computed_effective_pubkey", None),
            "zap_tracking_enabled": int(getattr(settings, "zap_tracking_enabled", False)),
            "herd_wallet": getattr(settings, "herd_wallet", None),
            "zap_monitor_mode": getattr(settings, "zap_monitor_mode", "websocket"),
            "repost_tracking_enabled": int(getattr(settings, "repost_tracking_enabled", False)),
            "likes_tracking_enabled": int(getattr(settings, "likes_tracking_enabled", False)),
            "minimum_sats": int(getattr(settings, "minimum_sats", 10) or 10),
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
        try:
            async with _crud_write_lock:
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
                "Cyberherd CRUD upsert (insert) user_id=%s source_wallet=%s nostr_key_present=%s override_present=%s",
                user_id,
                getattr(settings, "source_wallet", None),
                bool(getattr(settings, "nostr_private_key", None)),
                bool(getattr(settings, "nostr_pubkey_override", None)),
            )
        except Exception:
            pass
        try:
            _dur = int((time.time() - _t0) * 1000)
            logger.debug(f"Cyberherd upsert_settings END user_id={user_id} dur_ms={_dur}")
        except Exception:
            pass
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
                    await _reset_splits(str(getattr(settings, "source_wallet")))
                else:
                    # Otherwise schedule a recompute which is non-destructive.
                    await _sched_recompute(str(getattr(settings, "source_wallet")))
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
    return await db.fetchall(
        f"SELECT * FROM {db.references_schema}members ORDER BY added_at DESC",
        model=LegacyCyberherdMember,
    )


async def add_member(member: LegacyCyberherdMember):
    # Ensure added_at timestamp
    if not getattr(member, "added_at", None):
        member.added_at = int(time.time())
    await db.insert("members", member)


async def update_member(pubkey: str, alias: str | None = None):
    # Only alias is editable for now
    await db.execute(
    f"UPDATE {db.references_schema}members SET alias = :alias WHERE pubkey = :pubkey",
    {"alias": alias, "pubkey": pubkey},
    )


async def delete_member(pubkey: str):
    await db.execute(
        f"DELETE FROM {db.references_schema}members WHERE pubkey = :pubkey",
        {"pubkey": pubkey},
    )


### Full cyber_herd CRUD (middleware parity)


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
        rows = await db.fetchall(
            f"SELECT * FROM {db.references_schema}cyber_herd WHERE user_id = :user_id",
            {"user_id": user_id}
        )
        return [dict(r) for r in rows]
    except Exception as e:
        msg = str(e).lower()
        if "undefinedtable" in msg or "does not exist" in msg or "no such table" in msg:
            await _bootstrap_cyberherd_tables()
            try:
                rows = await db.fetchall(
                    f"SELECT * FROM {db.references_schema}cyber_herd WHERE user_id = :user_id",
                    {"user_id": user_id}
                )
                return [dict(r) for r in rows]
            except Exception:
                return []
        raise



async def recompute_member_payouts(user_id: str | None = None) -> dict[str, float]:
    """Recalculate payout shares for all cyberherd members for the given user."""
    if not user_id:
        logger.warning("recompute_member_payouts called without user_id, skipping")
        return {}

    try:
        members = await get_all_cyberherd_members(user_id)
    except Exception as exc:
        logger.debug(f"cyberherd: recompute_member_payouts failed to load members: {exc}")
        return {}

    if not members:
        return {}

    try:
        settings = await get_settings(user_id)
        zap_wallet = getattr(settings, "zap_wallet", None)
    except Exception as exc:
        logger.debug(f"cyberherd: recompute_member_payouts could not load settings: {exc}")
        zap_wallet = None

    member_total = 10 if zap_wallet else 100
    shares_percent = compute_member_share_percentages(members, member_total)

    results: dict[str, float] = {}
    async with _crud_write_lock:
        for row in members:
            pubkey = (row.get("pubkey") or "").strip().lower()
            if not pubkey:
                continue
            percent_value = int(shares_percent.get(pubkey, 0))
            payout_value = round(percent_value / 100.0, 6)
            results[pubkey] = payout_value
            try:
                await db.execute(
                    f"UPDATE {db.references_schema}cyber_herd SET payouts = :payouts WHERE pubkey = :pubkey AND user_id = :user_id",
                    {"payouts": payout_value, "pubkey": pubkey, "user_id": user_id},
                )
            except Exception as exc:
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
        row = await db.fetchone(
            f"SELECT * FROM {db.references_schema}cyber_herd WHERE pubkey = :pubkey AND user_id = :user_id",
            {"pubkey": normalized_pubkey, "user_id": user_id},
        )
        return dict(row) if row else None
    except Exception as e:
        # If table is missing, attempt runtime bootstrap once then retry
        if any(t in str(e).lower() for t in ["undefinedtable", "does not exist", "no such table"]):
            await _bootstrap_cyberherd_tables()
            row = await db.fetchone(
                f"SELECT * FROM {db.references_schema}cyber_herd WHERE pubkey = :pubkey AND user_id = :user_id",
                {"pubkey": normalized_pubkey, "user_id": user_id},
            )
            return dict(row) if row else None
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
        rows = await db.fetchall(
            f"SELECT * FROM {db.references_schema}cyber_herd WHERE is_active = 1 AND user_id = :user_id",
            {"user_id": user_id}
        )
        return [dict(r) for r in rows]
    except Exception as e:
        if any(t in str(e).lower() for t in ["undefinedtable", "does not exist", "no such table"]):
            await _bootstrap_cyberherd_tables()
            rows = await db.fetchall(
                f"SELECT * FROM {db.references_schema}cyber_herd WHERE is_active = 1 AND user_id = :user_id",
                {"user_id": user_id}
            )
            return [dict(r) for r in rows]
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
        row = await db.fetchone(
            f"SELECT COUNT(*) as count FROM {db.references_schema}cyber_herd WHERE is_active = 1 AND user_id = :user_id",
            {"user_id": user_id}
        )
        return int(row["count"]) if row and "count" in row else 0
    except Exception as e:
        if any(t in str(e).lower() for t in ["undefinedtable", "does not exist", "no such table"]):
            await _bootstrap_cyberherd_tables()
            row = await db.fetchone(
                f"SELECT COUNT(*) as count FROM {db.references_schema}cyber_herd WHERE is_active = 1 AND user_id = :user_id",
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
    
    await db.execute(
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
        logger.debug(f"cyberherd: recompute after deactivate failed: {exc}")
    # After deactivation, recompute and update split targets if a source wallet is configured
    try:
        from .services.splits import schedule_split_recompute  # type: ignore
        # Require explicit user_id to determine source_wallet and avoid global fallbacks
        s = await get_settings(user_id)
        sw = getattr(s, "source_wallet", None)
        if sw:
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
    
    # First, count how many members will be deactivated
    count_result = await db.fetchone(
        f"""
        SELECT COUNT(*) as count FROM {db.references_schema}cyber_herd
        WHERE is_active = 1 AND user_id = :user_id
        """,
        {"user_id": user_id}
    )
    member_count = count_result.get("count", 0) if count_result else 0
    
    # Now deactivate them
    await db.execute(
        f"""
        UPDATE {db.references_schema}cyber_herd
        SET is_active = 0, amount = 0, payouts = 0.0, event_id = NULL,
            note = NULL, kinds = NULL
        WHERE is_active = 1 AND user_id = :user_id
        """,
        {"user_id": user_id}
    )
    
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
        result = await db.fetchone(
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
    
    normalized_pubkey = str(pubkey).strip().lower()
    banned_value = 1 if banned else 0
    
    try:
        # If banning, also deactivate the member
        if banned:
            await db.execute(
                f"""
                UPDATE {db.references_schema}cyber_herd
                SET banned = :banned, is_active = 0
                WHERE pubkey = :pubkey AND user_id = :user_id
                """,
                {"banned": banned_value, "pubkey": normalized_pubkey, "user_id": user_id}
            )
            logger.info(f"Banned and deactivated member {normalized_pubkey[:16]}... for user {user_id}")
        else:
            await db.execute(
                f"""
                UPDATE {db.references_schema}cyber_herd
                SET banned = :banned
                WHERE pubkey = :pubkey AND user_id = :user_id
                """,
                {"banned": banned_value, "pubkey": normalized_pubkey, "user_id": user_id}
            )
            logger.info(f"Unbanned member {normalized_pubkey[:16]}... for user {user_id}")
        
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
                await schedule_split_recompute(str(sw), user_id=user_id)
        except Exception as e:
            logger.warning(f"Failed to schedule split recompute after ban status change: {e}")
        
        return True
    except Exception as e:
        logger.error(f"Error setting ban status for pubkey {normalized_pubkey[:16]}...: {e}")
        return False


async def add_new_active_member(member_data: dict, user_id: str | None = None):
    """Add or update a cyberherd member for a specific user.
    
    Args:
        member_data: Dictionary containing member information.
        user_id: User ID to associate with this member. Required for multi-user support.
    """
    if not user_id:
        logger.error("add_new_active_member called without user_id, skipping")
        return
    
    raw_pubkey = member_data.get("pubkey")
    normalized_pubkey = str(raw_pubkey).strip().lower() if raw_pubkey else ""
    if not normalized_pubkey:
        logger.error("add_new_active_member called without valid pubkey, skipping")
        return

    logger.info(f"cyberherd: add_new_active_member called for pubkey: {normalized_pubkey[:8]}... user_id: {user_id}")
    # Normalize values
    # Ensure amounts are stored in satoshis. If callers pass millisatoshis
    # (values >= 1_000_000), convert to sats by dividing by 1000.
    raw_amount = member_data.get("amount") or 0
    try:
        amt_val = int(raw_amount)
    except Exception:
        amt_val = 0
    if amt_val >= 1_000_000:
        # Likely millisatoshis -> convert to sats
        amt_val = amt_val // 1000

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
        existing_kinds_row = await db.fetchone(
            f"SELECT kinds FROM {db.references_schema}cyber_herd WHERE pubkey = :pubkey AND user_id = :user_id",
            {"pubkey": normalized_pubkey, "user_id": user_id},
        )
    except Exception as e:
        try:
            logger.debug(f"cyberherd: add_new_active_member kinds lookup skipped: {e}")
        except Exception:
            pass

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
            ON CONFLICT(pubkey) DO UPDATE SET
                user_id = excluded.user_id,
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
            """
    for attempt in range(2):
        try:
            await db.execute(insert_sql, values)
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
    
    try:
        await recompute_member_payouts(user_id)
    except Exception as exc:
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
    payouts_increase: float,
    user_id: str | None = None,
    event_id: str | None = None,
    kinds: str | list | tuple | set | None = None,
    nip05: str | None = None,
):
    """Update and activate a cyberherd member for a specific user.
    
    Args:
        pubkey: The pubkey of the member to update.
        amount_increase: Amount to add to the member's total.
        payouts_increase: Payout amount to add.
        user_id: User ID to filter by. If None, operation is skipped for security.
        event_id: Optional tracked event id (nostr e-tag) for threading.
    """
    if not user_id:
        logger.warning("update_and_activate_member called without user_id, skipping")
        return
    
    normalized_pubkey = (pubkey or "").strip().lower()
    if not normalized_pubkey:
        logger.warning("update_and_activate_member called without valid pubkey, skipping")
        return
    
    # Best-effort enrichment if the row is missing nprofile
    try:
        row = await db.fetchone(
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
                    await db.execute(
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

    # Normalize amount_increase to satoshis (accept msat input defensively)
    try:
        ai = int(amount_increase or 0)
    except Exception:
        ai = 0
    if ai >= 1_000_000:
        ai = ai // 1000

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
        SET amount = amount + :amount_increase,
            payouts = payouts + :payouts_increase,
            is_active = 1,
            metadata_last_checked_at = :ts,
            event_id = COALESCE(:event_id, event_id),
            nip05 = COALESCE(:nip05, {existing_nip05_expr})
        WHERE pubkey = :pubkey AND user_id = :user_id
        """
    params = {
        "amount_increase": ai,
        "payouts_increase": payouts_increase,
        "ts": int(time.time()),
        "pubkey": normalized_pubkey,
        "user_id": user_id,
        "event_id": event_id,
        "nip05": nip05_normalized,
    }
    result = None
    for attempt in range(2):
        try:
            result = await db.execute(update_sql, params)
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
    
    try:
        await recompute_member_payouts(user_id)
    except Exception as exc:
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
                row = await db.fetchone(
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

                await db.execute(
                    f"UPDATE {db.references_schema}cyber_herd SET kinds = :kinds WHERE pubkey = :pubkey AND user_id = :user_id",
                    {"kinds": merged_str, "pubkey": normalized_pubkey, "user_id": user_id},
                )
        except Exception as e:
            try:
                logger.debug(f"cyberherd: failed to merge kinds for {normalized_pubkey[:8]}...: {e}")
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
    
    await db.execute(
        f"DELETE FROM {db.references_schema}cyber_herd WHERE pubkey = :pubkey AND user_id = :user_id",
        {"pubkey": pubkey, "user_id": user_id},
    )
    try:
        await recompute_member_payouts(user_id)
    except Exception as exc:
        logger.debug(f"cyberherd: recompute after delete member skipped: {exc}")


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
    
    try:
        await db.execute(
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
                    await update_split_targets_proportional(str(sw))
            else:
                try:
                    logger.debug("cyberherd: update_cyberherd_member_allocation: skipping split recompute (member not eligible and eligible members exist)")
                except Exception:
                    pass
        except Exception:
            pass
    except Exception as e:
        logger.debug(f"Error updating member allocation for {pubkey[:8]}...: {e}")


async def _bootstrap_cyberherd_tables():
    """Create cyberherd.cyber_herd table if missing (runtime self-heal).

    Mirrors middleware structure; safe, idempotent. Called on UndefinedTableError.
    """
    schema_prefix = db.references_schema or ""
    table_name = f"{schema_prefix}cyber_herd"
    if schema_prefix.endswith("."):
        schema_table = f"{schema_prefix[:-1]}.cyber_herd"
    else:
        schema_table = table_name

    stmts = [
        f"""CREATE TABLE IF NOT EXISTS {table_name} (
            pubkey TEXT PRIMARY KEY,
            user_id TEXT,
            display_name TEXT,
            event_id TEXT,
            note TEXT,
            kinds TEXT,
            nprofile TEXT,
            lud16 TEXT,
            nip05 TEXT,
            notified INTEGER DEFAULT 0,
            payouts REAL DEFAULT 0,
            amount INTEGER DEFAULT 0,
            picture TEXT,
            relays TEXT,
            metadata_last_checked_at INTEGER,
            is_active INTEGER DEFAULT 0
        );""",
        # Basic helpful indexes
        f"CREATE INDEX IF NOT EXISTS idx_cyberherd_active ON {table_name}(is_active);",
        f"CREATE INDEX IF NOT EXISTS idx_cyber_herd_user_id ON {table_name}(user_id);",
        f"CREATE INDEX IF NOT EXISTS idx_cyber_herd_user_active ON {table_name}(user_id, is_active);",
    ]
    # Ensure processed_events table exists for idempotent runtime self-heal
    # This table tracks processed payments to prevent duplicate member_increase posts
    stmts.append(
        f"""
        CREATE TABLE IF NOT EXISTS {db.references_schema}processed_events (
            user_id TEXT NOT NULL,
            event_hash TEXT NOT NULL,
            note_id TEXT,
            pubkey TEXT,
            processed_at INTEGER NOT NULL,
            event_type TEXT,
            UNIQUE(user_id, event_hash)
        );
        """
    )
    stmts.append(
        f"ALTER TABLE {db.references_schema}processed_events ADD COLUMN event_type TEXT;"
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
            await db.execute(s)
        except Exception:
            pass
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
            has_column = False
        if not has_column:
            try:
                await db.execute(
                    f"ALTER TABLE {processed_zaps_table} ADD COLUMN {column} {column_type};"
                )
            except Exception:
                pass
    index_statements = [
        f"CREATE INDEX IF NOT EXISTS idx_processed_zaps_user_event ON {processed_zaps_table}(user_id, event_id);",
        f"CREATE INDEX IF NOT EXISTS idx_processed_zaps_user_note ON {processed_zaps_table}(user_id, note_id);",
        f"CREATE INDEX IF NOT EXISTS idx_processed_zaps_user_zapper ON {processed_zaps_table}(user_id, zapper_pubkey);",
    ]
    for stmt in index_statements:
        try:
            await db.execute(stmt)
        except Exception:
            pass
    unique_statements = [
        f"CREATE UNIQUE INDEX IF NOT EXISTS uq_processed_zaps_user_event ON {processed_zaps_table}(user_id, event_id) WHERE event_id IS NOT NULL;",
        f"CREATE UNIQUE INDEX IF NOT EXISTS uq_processed_zaps_user_receipt ON {processed_zaps_table}(user_id, zap_receipt_id) WHERE zap_receipt_id IS NOT NULL;",
    ]
    for stmt in unique_statements:
        try:
            await db.execute(stmt)
        except Exception:
            pass
    try:
        await db.execute(
            f"ALTER TABLE {processed_zaps_table} ALTER COLUMN processed_at TYPE TEXT USING processed_at::text;"
        )
    except Exception:
        # SQLite and other backends may not support ALTER COLUMN; ignore
        pass
    try:
        if not await _column_exists(schema_table, "nip05"):
            await db.execute(f"ALTER TABLE {table_name} ADD COLUMN nip05 TEXT;")
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
        row = await db.fetchone(
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
            row = await db.fetchone(
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
        await db.execute(
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
            await db.execute(
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
    
    await db.execute(
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
    
    await db.execute(
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
        row = await db.fetchone(
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
                row = await db.fetchone(
                    f"SELECT * FROM {db.references_schema}processed_zaps WHERE user_id = :user_id AND event_id = :event_id",
                    {"user_id": user_id, "event_id": event_id},
                )
                return row
            except Exception as e2:
                logger.error(f"Error getting processed zap after bootstrap: {e2}")
                return None
        logger.error(f"Error getting processed zap: {e}")
        return None

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
    payload["processed_at"] = processed_at

    try:
        await db.execute(
            f"""INSERT INTO {db.references_schema}processed_zaps (user_id, event_id, note_id, zapper_pubkey, amount, processed_at)
            VALUES (:user_id, :event_id, :note_id, :zapper_pubkey, :amount, :processed_at)
            """,
            payload,
        )
        return payload
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
                await db.execute(
                    f"""INSERT INTO {db.references_schema}processed_zaps (user_id, event_id, note_id, zapper_pubkey, amount, processed_at)
                    VALUES (:user_id, :event_id, :note_id, :zapper_pubkey, :amount, :processed_at)
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

    if since is not None:
        try:
            multiplier = max(1, int(fetch_multiplier))
        except Exception:
            multiplier = 4
        fetch_limit = min(base_limit * multiplier, 2000)
    else:
        fetch_limit = base_limit

    sql += " ORDER BY processed_at DESC"
    params["limit"] = fetch_limit
    sql += " LIMIT :limit"

    try:
        rows = await db.fetchall(sql, params)
        candidate_rows: list[dict[str, Any]] = []
        for row in rows or []:
            data = dict(row)
            parsed_ts = _parse_timestamp_field(data.get("processed_at"))
            data["_processed_ts"] = parsed_ts
            if parsed_ts is not None:
                data.setdefault("processed_at_ts", parsed_ts)
            candidate_rows.append(data)

        if since is not None:
            try:
                since_value = int(since)
            except Exception:
                since_value = None
            if since_value is not None:
                candidate_rows = [
                    r for r in candidate_rows if (r.get("_processed_ts") or 0) >= since_value
                ]

        candidate_rows.sort(
            key=lambda r: r.get("_processed_ts")
            or _parse_timestamp_field(r.get("processed_at"))
            or 0,
            reverse=True,
        )

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
        row = await db.fetchone(sql, params)
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
        rows = await db.fetchall(sql, params)
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


async def is_payment_processed(user_id: str, payment_hash: str) -> bool:
    """Check if a payment has already been processed.
    
    Args:
        user_id: User ID to scope the check.
        payment_hash: Payment hash to check.
    
    Returns:
        True if payment was already processed, False otherwise.
    """
    if not user_id or not payment_hash:
        return False
    
    try:
        row = await db.fetchone(
            f"SELECT 1 FROM {db.references_schema}processed_events WHERE user_id = :user_id AND event_hash = :event_hash",
            {"user_id": user_id, "event_hash": payment_hash},
        )
        return row is not None
    except Exception as e:
        msg = str(e).lower()
        if any(t in msg for t in ("undefinedtable", "does not exist", "no such table")):
            try:
                await _bootstrap_cyberherd_tables()
                row = await db.fetchone(
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
        note_id: Optional note ID for reference.
        pubkey: Optional pubkey for reference.
        event_type: Optional classification for the processed event (e.g., "zap").
    
    Returns:
        True if successfully registered (new), False if already existed (ON CONFLICT).
    """
    if not user_id or not payment_hash:
        return False

    # Use a write lock to serialize the check-and-insert to reliably determine
    # whether we actually inserted a new row or the row already existed.
    try:
        async with _crud_write_lock:
            try:
                # Check existence first
                row = await db.fetchone(
                    f"SELECT 1 FROM {db.references_schema}processed_events WHERE user_id = :user_id AND event_hash = :event_hash",
                    {"user_id": user_id, "event_hash": payment_hash},
                )
                if row:
                    return False  # Already existed

                # Not present — insert and report True
                await db.execute(
                    f"""INSERT INTO {db.references_schema}processed_events (user_id, event_hash, note_id, pubkey, processed_at, event_type)
                    VALUES (:user_id, :event_hash, :note_id, :pubkey, :processed_at, :event_type)
                    ON CONFLICT(user_id, event_hash) DO NOTHING
                    """,
                    {
                        "user_id": user_id,
                        "event_hash": payment_hash,
                        "note_id": note_id,
                        "pubkey": _normalize_hex(pubkey) or pubkey,
                        "processed_at": int(time.time()),
                        "event_type": event_type,
                    },
                )
                return True
            except Exception as e:
                msg = str(e).lower()
                if any(
                    t in msg
                    for t in (
                        "undefinedtable",
                        "does not exist",
                        "no such table",
                        "no such column",
                        "undefined column",
                    )
                ):
                    # Try to bootstrap tables and retry once
                    try:
                        await _bootstrap_cyberherd_tables()
                        # After bootstrap, perform the check-insert again
                        row = await db.fetchone(
                            f"SELECT 1 FROM {db.references_schema}processed_events WHERE user_id = :user_id AND event_hash = :event_hash",
                            {"user_id": user_id, "event_hash": payment_hash},
                        )
                        if row:
                            return False
                        await db.execute(
                            f"""INSERT INTO {db.references_schema}processed_events (user_id, event_hash, note_id, pubkey, processed_at, event_type)
                            VALUES (:user_id, :event_hash, :note_id, :pubkey, :processed_at, :event_type)
                            ON CONFLICT(user_id, event_hash) DO NOTHING
                            """,
                            {
                                "user_id": user_id,
                                "event_hash": payment_hash,
                                "note_id": note_id,
                                "pubkey": _normalize_hex(pubkey) or pubkey,
                                "processed_at": int(time.time()),
                                "event_type": event_type,
                            },
                        )
                        return True
                    except Exception:
                        return False
                # If other DB errors occurred, return False to indicate not newly registered
                return False
    except Exception:
        # In case lock acquisition or unexpected errors occur, fail safe as not-new
        return False


async def clear_processed_zaps_for_user(user_id: str | None):
    """Delete processed_zaps rows for a given user_id. If user_id is None, no-op.

    Used by manual reset to allow a hard reset of processed zap history for the
    requesting user so warm-start can reprocess payments or zaps.
    """
    if not user_id:
        return
    try:
        await db.execute(
            f"DELETE FROM {db.references_schema}processed_zaps WHERE user_id = :user_id",
            {"user_id": user_id},
        )
    except Exception as e:
        msg = str(e).lower()
        if any(t in msg for t in ("undefinedtable", "does not exist", "no such table")):
            try:
                await _bootstrap_cyberherd_tables()
                await db.execute(
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
        await db.execute(
            f"DELETE FROM {db.references_schema}processed_events WHERE user_id = :user_id",
            {"user_id": user_id},
        )
    except Exception as e:
        msg = str(e).lower()
        if any(t in msg for t in ("undefinedtable", "does not exist", "no such table")):
            try:
                await _bootstrap_cyberherd_tables()
                await db.execute(
                    f"DELETE FROM {db.references_schema}processed_events WHERE user_id = :user_id",
                    {"user_id": user_id},
                )
            except Exception as ie:
                logger.warning(f"Failed clearing processed_events after bootstrap: {ie}")
        else:
            logger.warning(f"Failed clearing processed_events for user {user_id}: {e}")


async def is_event_processed(user_id: str, event_hash: str) -> bool:
    """Alias for is_payment_processed with clearer naming for nostr events.

    This exists to make calling code explicit about processing Nostr events
    (reposts/reactions) vs invoice payments while reusing the same persisted
    processed_events table.
    """
    return await is_payment_processed(user_id, event_hash)


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
        row = await db.fetchone(
            f"SELECT * FROM {db.references_schema}cyber_herd WHERE pubkey = :pubkey AND user_id = :user_id",
            {"pubkey": normalized_pubkey, "user_id": user_id}
        )
        return dict(row) if row else None
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
