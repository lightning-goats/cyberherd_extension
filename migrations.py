"""Consolidated CyberHerd migrations.

This file contains a clean, consolidated initial migration that creates
all tables with their current schema. This replaces the fragmented historical
migrations for new installations.

For existing installations, the old migrations (m001-m020) remain to ensure
proper upgrade paths. New installations can use just m001_consolidated.
"""

from lnbits.db import COCKROACH, POSTGRES, SQLITE, Database
import logging
from contextlib import asynccontextmanager

from sqlalchemy.sql import text

logger = logging.getLogger(__name__)


class _MigrationConnection:
    """Execute DDL within one transaction instead of committing each statement."""

    def __init__(self, connection):
        self.connection = connection

    async def execute(self, query, values=None):
        conn = self.connection
        return await conn.conn.execute(
            text(conn.rewrite_query(query)), conn.rewrite_values(values or {})
        )

    def __getattr__(self, name):
        return getattr(self.connection, name)


@asynccontextmanager
async def _migration_transaction(database):
    if isinstance(database, Database):
        async with database.connect() as connection:
            async with _migration_transaction(connection) as transaction:
                yield transaction
        return
    connection = database.conn
    already_started = connection.in_transaction()
    transaction = connection.begin_nested() if already_started else connection.begin()
    async with transaction:
        if database.type == SQLITE and not already_started:
            # sqlite's legacy transaction mode does not BEGIN for DDL alone.
            await connection.exec_driver_sql("BEGIN")
        yield _MigrationConnection(database)


async def _column_exists(db: Database, schema_table: str, column: str) -> bool:
    """Return True if ``column`` exists on ``schema_table``.

    Checking existence up front avoids issuing a failing ``ALTER TABLE ... ADD
    COLUMN`` inside the shared migration transaction. On Postgres a failed
    statement aborts the whole transaction, which would silently break every
    subsequent migration; catching the error is not enough because the
    transaction is already poisoned. Works on Postgres (information_schema) and
    SQLite (PRAGMA), propagating storage errors.
    """
    if "." in schema_table:
        schema, table = schema_table.split(".", 1)
    else:
        schema, table = None, schema_table

    if db.type in (POSTGRES, COCKROACH):
        params = {"table": table, "col": column}
        if schema:
            query = (
                "SELECT 1 FROM information_schema.columns "
                "WHERE table_schema = :schema AND table_name = :table "
                "AND column_name = :col LIMIT 1"
            )
            params["schema"] = schema
        else:
            query = (
                "SELECT 1 FROM information_schema.columns "
                "WHERE table_name = :table AND column_name = :col LIMIT 1"
            )
        row = await db.fetchone(query, params)
        return row is not None

    # SQLite: PRAGMA table_info.
    if db.type == SQLITE:
        rows = await db.fetchall(f"PRAGMA table_info({table})")
        for r in rows:
            try:
                name = r.get("name") if hasattr(r, "get") else r["name"]
            except Exception:
                name = None
            if name == column:
                return True

    return False


async def _add_column_if_missing(
    db: Database, schema_table: str, column: str, column_def: str
) -> None:
    """ALTER TABLE to add ``column`` only when it does not already exist.

    ``column_def`` is the SQL type/default fragment (e.g. ``INTEGER DEFAULT 0``).
    Portable and safe to re-run: on a fresh install where the consolidated m001
    schema already defines the column, this is a no-op that never issues a
    failing statement.
    """
    try:
        if await _column_exists(db, schema_table, column):
            return
        await db.execute(
            f"ALTER TABLE {schema_table} ADD COLUMN {column} {column_def};"
        )
        logger.info(f"CyberHerd migration: added column {schema_table}.{column}")
    except Exception as e:
        # Propagate failures so the runner cannot record a false success.
        logger.warning(f"CyberHerd migration: could not add {schema_table}.{column}: {e}")
        raise

# Expose ordered list of migration function names
__all__ = [
    'm001_consolidated_schema',  # Changed from m000 to m001
    'm018_noop',  # Placeholder for old migration gap
    'm019_noop',  # Placeholder for old migration gap
    'm020_noop',  # Placeholder for old migration gap
    'm021_add_likes_tracking',  # Missing column not in old migrations
    'm022_add_user_id_to_cyber_herd',  # Add user_id column for multi-user support
    'm023_add_tracked_event_addresses',  # Store 30311 addresses for replies
    'm024_add_midnight_reset_toggle',
    'm025_migrate_legacy_userids',
    'm026_add_zap_totals_table',
    'm027_add_feeder_trigger_sats',
    'm028_sync_processed_zaps_schema',
    'm029_add_nip05_verification_toggle',
    'm030_add_banned_column',  # Add banned column for member banning
    'm031_add_send_splits_enabled',  # Add send_splits_enabled toggle for automatic splits
    'm032_enable_interaction_tracking',  # Enable repost/reaction tracking by default for existing users
    'm033_add_member_allocation_percent',  # Add member_allocation_percent for configurable split percentage
    'm034_fix_processed_events_unique_constraint',  # Fix unique constraint to include note_id for multi-note events
    'm035_fix_cyber_herd_user_pubkey_key',  # Scope member identity by user_id + pubkey
    'm036_repair_postgres_member_key',
    'm037_add_membership_reset_days',
    'm038_preserve_payout_precision',
    'm039_repair_processed_zap_timestamp',
    'm040_repair_processed_zap_nullability',
    'm041_repair_database_contract',
    'm042_add_engagement_awards',
    'm043_add_treat_delivery_records',
    'm044_add_prejoin_engagement_evidence',
]

logger.info(f"CYBERHERD MIGRATIONS: Registered {len(__all__)} migrations: {__all__}")


async def m001_consolidated_schema(db: Database):
    """Create all CyberHerd tables with complete current schema.
    
    This consolidated migration creates all tables in their final form,
    avoiding the complexity of incremental ALTER TABLE migrations.
    Safe to run on fresh installs or existing databases (uses IF NOT EXISTS).
    """
    logger.info("=" * 60)
    logger.info("CYBERHERD MIGRATION m001: STARTING")
    logger.info("=" * 60)
    
    # Create schema for Postgres/Cockroach (ignored on SQLite)
    if db.type in (POSTGRES, COCKROACH):
        logger.info("CYBERHERD m001: Creating schema...")
        await db.execute(f"CREATE SCHEMA IF NOT EXISTS {db.schema};")
        logger.info("CYBERHERD m001: Schema created successfully")
    
    # -------------------------------------------------------------------------
    # Settings Table - User Configuration
    # -------------------------------------------------------------------------
    await db.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {db.references_schema}settings (
            -- Core settings
            source_wallet TEXT,
            max_members INTEGER DEFAULT 3,
            tracked_tags TEXT DEFAULT '[]',
            
            -- Nostr configuration
            nostr_private_key TEXT,
            nostr_pubkey_override TEXT,
            computed_effective_pubkey TEXT,
            
            -- Wallet configuration
            zap_wallet TEXT,
            zap_wallet_alias TEXT,
            herd_wallet TEXT,
            
            -- Tracking configuration
            tracked_event_ids TEXT,
            tracked_event_timestamps TEXT DEFAULT '{{}}',
            tracked_event_addresses TEXT DEFAULT '{{}}',
            zap_tracking_enabled INTEGER DEFAULT 1,
            zap_monitor_mode TEXT DEFAULT 'websocket',
            repost_tracking_enabled INTEGER DEFAULT 0,
            likes_tracking_enabled INTEGER DEFAULT 0,
        midnight_reset_enabled INTEGER DEFAULT 1,
            minimum_sats INTEGER DEFAULT 10,
            nip05_verification_enabled INTEGER DEFAULT 1,
            
            -- Multi-user support
            user_id TEXT,
            
            -- Template ownership
            templates_owner_user TEXT
        );
        """
    )
    
    # Index for multi-user queries
    try:
        await db.execute(
            f"CREATE UNIQUE INDEX IF NOT EXISTS idx_settings_user ON {db.references_schema}settings(user_id) WHERE user_id IS NOT NULL;"
        )
    except Exception:
        raise
    
    # -------------------------------------------------------------------------
    # Cyber Herd Table - Active Members (Legacy, kept for compatibility)
    # -------------------------------------------------------------------------
    await db.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {db.references_schema}cyber_herd (
            pubkey TEXT NOT NULL,
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
            is_active INTEGER DEFAULT 0,
            user_id TEXT NOT NULL DEFAULT '',
            banned INTEGER DEFAULT 0,
            PRIMARY KEY (user_id, pubkey)
        );
        """
    )
    
    # Indices for performance
    try:
        await db.execute(
            f"CREATE INDEX IF NOT EXISTS idx_cyber_herd_user_id ON {db.references_schema}cyber_herd(user_id);"
        )
        await db.execute(
            f"CREATE INDEX IF NOT EXISTS idx_cyber_herd_user_active ON {db.references_schema}cyber_herd(user_id, is_active);"
        )
        await db.execute(
            f"CREATE INDEX IF NOT EXISTS idx_cyber_herd_user_banned ON {db.references_schema}cyber_herd(user_id, banned);"
        )
    except Exception:
        raise
    
    # -------------------------------------------------------------------------
    # Members Table - New member tracking system
    # -------------------------------------------------------------------------
    await db.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {db.references_schema}members (
            pubkey TEXT PRIMARY KEY,
            amount INTEGER NOT NULL DEFAULT 0,
            allocation_percentage REAL NOT NULL DEFAULT 10.0,
            note_id TEXT,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            is_active BOOLEAN NOT NULL DEFAULT TRUE,
            display_name TEXT,
            lud16 TEXT,
            picture TEXT,
            relays TEXT,
            metadata_last_checked_at TIMESTAMP,
            payouts REAL NOT NULL DEFAULT 0.0
        );
        """
    )
    
    # -------------------------------------------------------------------------
    # Processed Zaps Table - Prevent duplicate zap processing
    # -------------------------------------------------------------------------
    await db.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {db.references_schema}processed_zaps (
            id {db.serial_primary_key},
            user_id TEXT NOT NULL,
            event_id TEXT,
            note_id TEXT,
            zapper_pubkey TEXT,
            amount INTEGER,
            processed_at TEXT,
            zap_receipt_id TEXT,
            payment_hash TEXT,
            amount_sats INTEGER,
            UNIQUE(user_id, event_id),
            UNIQUE(user_id, zap_receipt_id)
        );
        """
    )

    # Indices for processed_zaps
    try:
        await db.execute(
            f"CREATE INDEX IF NOT EXISTS idx_processed_zaps_user_event ON {db.references_schema}processed_zaps(user_id, event_id);"
        )
        await db.execute(
            f"CREATE INDEX IF NOT EXISTS idx_processed_zaps_user_note ON {db.references_schema}processed_zaps(user_id, note_id);"
        )
        await db.execute(
            f"CREATE INDEX IF NOT EXISTS idx_processed_zaps_user_zapper ON {db.references_schema}processed_zaps(user_id, zapper_pubkey);"
        )
        try:
            await db.execute(
                f"CREATE INDEX IF NOT EXISTS idx_processed_zaps_user_receipt ON {db.references_schema}processed_zaps(user_id, zap_receipt_id) WHERE zap_receipt_id IS NOT NULL;"
            )
        except Exception:
            raise
    except Exception:
        raise

    # -------------------------------------------------------------------------
    # Zap Totals Table - Persist aggregated zap receipt summaries
    # -------------------------------------------------------------------------
    await db.execute(
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

    # -------------------------------------------------------------------------
    # Processed Events Table - idempotency for payment/engagement processing.
    # Previously only created lazily by the runtime bootstrap, which left a
    # window on fresh installs where dedup checks hit a missing table and
    # returned "not processed", allowing double-processing. Create it here.
    # -------------------------------------------------------------------------
    await db.execute(
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
    try:
        await db.execute(
            f"""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_processed_events_unique_triple
            ON {db.references_schema}processed_events(user_id, event_hash, COALESCE(note_id, ''));
            """
        )
    except Exception as e:
        logger.info(f"CyberHerd m001: processed_events unique index: {e}")
        raise

    logger.info("CyberHerd: consolidated schema migration completed")


async def m018_noop(db: Database):
    """No-op migration to fill gap from removed historical migrations."""
    logger.info("CYBERHERD MIGRATION m018: No-op (gap filler)")
    pass


async def m019_noop(db: Database):
    """No-op migration to fill gap from removed historical migrations."""
    logger.info("CYBERHERD MIGRATION m019: No-op (gap filler)")
    pass


async def m020_noop(db: Database):
    """No-op migration to fill gap from removed historical migrations."""
    logger.info("CYBERHERD MIGRATION m020: No-op (gap filler)")
    pass


async def m021_add_likes_tracking(db: Database):
    """Add likes_tracking_enabled column if missing.
    
    This column was in the model but missing from historical migrations.
    Safe to run multiple times (ignores error if column exists).
    Works for both SQLite and PostgreSQL.
    """
    logger.info("CYBERHERD MIGRATION m021: Adding likes_tracking_enabled column")
    # Build table name with schema prefix for PostgreSQL, without for SQLite
    table_name = f"{db.references_schema}settings"
    
    await _add_column_if_missing(db, table_name, "likes_tracking_enabled", "INTEGER DEFAULT 0")


async def m022_add_user_id_to_cyber_herd(db: Database):
    """Add user_id column to cyber_herd table for multi-user support.

    This column is needed to associate herd members with specific users.
    Safe to run multiple times (ignores error if column exists).
    Works for both SQLite and PostgreSQL.
    """
    logger.info("CYBERHERD MIGRATION m022: Adding user_id column to cyber_herd table")
    # Build table name with schema prefix for PostgreSQL, without for SQLite
    table_name = f"{db.references_schema}cyber_herd"
    
    await _add_column_if_missing(db, table_name, "user_id", "TEXT")

    # Add indices for performance
    try:
        await db.execute(
            f"CREATE INDEX IF NOT EXISTS idx_cyber_herd_user_id ON {table_name}(user_id);"
        )
        await db.execute(
            f"CREATE INDEX IF NOT EXISTS idx_cyber_herd_user_active ON {table_name}(user_id, is_active);"
        )
        logger.info("CyberHerd m022: added indices for user_id column")
    except Exception as e:
        logger.info(f"CyberHerd m022 indices: {e}")
        raise


async def m023_add_tracked_event_addresses(db: Database):
    """Add tracked_event_addresses column for threaded 30311 replies."""
    logger.info("CYBERHERD MIGRATION m023: Adding tracked_event_addresses column")
    table_name = f"{db.references_schema}settings"

    await _add_column_if_missing(db, table_name, "tracked_event_addresses", "TEXT DEFAULT '{}'")


async def m024_add_midnight_reset_toggle(db: Database):
    """Add midnight_reset_enabled column to settings to opt-in nightly reset per-user.

    Default value is 1 (enabled) to preserve existing behavior for upgrades.
    """
    logger.info("CYBERHERD MIGRATION m024: Adding midnight_reset_enabled column (default 1)")
    table_name = f"{db.references_schema}settings"
    await _add_column_if_missing(db, table_name, "midnight_reset_enabled", "INTEGER DEFAULT 1")


async def m025_migrate_legacy_userids(db: Database):
    """Migrate legacy settings rows (user_id NULL/empty) to explicit user_id using wallet ownership.

    Heuristic used:
    - For settings rows where `source_wallet` is set but `user_id` is NULL/empty,
      attempt to resolve the owning user via core `wallets` table: `SELECT user FROM wallets WHERE id = :source_wallet`.
    - If a user is found and that user does NOT already have a settings row, update the legacy settings row to set `user_id`.

    This migration is intentionally conservative and idempotent. It only migrates settings rows where the
    wallet -> user mapping is clear and will not overwrite any existing user-scoped settings rows.
    We avoid automatically assigning `cyber_herd` member rows because there is no reliable automatic
    cross-reference; use the diagnostics tool for manual/more aggressive migration of member rows.
    """
    from lnbits.core.crud.wallets import get_wallet

    table = f"{db.references_schema}settings"
    rows = await db.fetchall(
        f"SELECT source_wallet FROM {table} "
        "WHERE (user_id IS NULL OR TRIM(user_id) = '') "
        "AND source_wallet IS NOT NULL AND TRIM(source_wallet) != '' "
        "GROUP BY source_wallet HAVING COUNT(*) = 1"
    )
    for row in rows:
        wallet = await get_wallet(row["source_wallet"])
        if not wallet:
            continue
        existing = await db.fetchone(
            f"SELECT 1 FROM {table} WHERE user_id = :uid",
            {"uid": wallet.user},
        )
        if not existing:
            await db.execute(
                f"UPDATE {table} SET user_id = :uid WHERE source_wallet = :wallet "
                "AND (user_id IS NULL OR TRIM(user_id) = '')",
                {"uid": wallet.user, "wallet": row["source_wallet"]},
            )


async def m026_add_zap_totals_table(db: Database):
    """Ensure zap_totals table exists for persisted zap aggregations."""
    logger.info("CYBERHERD MIGRATION m026: Ensuring zap_totals table exists")
    table_name = f"{db.references_schema}zap_totals"
    try:
        await db.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
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
        logger.info("CyberHerd m026: zap_totals table ready")
    except Exception as e:
        logger.warning(f"CyberHerd m026: failed ensuring zap_totals table: {e}")
        raise


async def m027_add_feeder_trigger_sats(db: Database):
    """Add feeder_trigger_sats column to settings for external feeder logic."""
    table_name = f"{db.references_schema}settings"
    logger.info("CYBERHERD m027: Adding feeder_trigger_sats column to settings")
    await _add_column_if_missing(db, table_name, "feeder_trigger_sats", "INTEGER DEFAULT 0")


async def m028_sync_processed_zaps_schema(db: Database):
    """Ensure processed_zaps table has the unified legacy/new columns."""
    logger.info("CYBERHERD m028: syncing processed_zaps table schema")
    table_name = f"{db.references_schema}processed_zaps"
    try:
        await db.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id {db.serial_primary_key},
                user_id TEXT NOT NULL,
                event_id TEXT,
                note_id TEXT,
                zapper_pubkey TEXT,
                amount INTEGER,
                processed_at TEXT,
                zap_receipt_id TEXT,
                payment_hash TEXT,
                amount_sats INTEGER,
                UNIQUE(user_id, event_id),
                UNIQUE(user_id, zap_receipt_id)
            );
            """
        )
    except Exception as e:
        logger.debug(f"CYBERHERD m028: base table ensure failed (may already exist): {e}")
        raise
    columns_to_add = [
        ("event_id", "TEXT"),
        ("note_id", "TEXT"),
        ("zapper_pubkey", "TEXT"),
        ("amount", "INTEGER"),
        ("processed_at", "TEXT"),
        ("zap_receipt_id", "TEXT"),
        ("payment_hash", "TEXT"),
        ("amount_sats", "INTEGER"),
    ]
    for column, column_def in columns_to_add:
        await _add_column_if_missing(db, table_name, column, column_def)

    index_statements = [
        f"CREATE INDEX IF NOT EXISTS idx_processed_zaps_user_event ON {db.references_schema}processed_zaps(user_id, event_id);",
        f"CREATE INDEX IF NOT EXISTS idx_processed_zaps_user_note ON {db.references_schema}processed_zaps(user_id, note_id);",
        f"CREATE INDEX IF NOT EXISTS idx_processed_zaps_user_zapper ON {db.references_schema}processed_zaps(user_id, zapper_pubkey);",
    ]
    for stmt in index_statements:
        try:
            await db.execute(stmt)
        except Exception:
            raise
    try:
        await db.execute(
            f"CREATE UNIQUE INDEX IF NOT EXISTS uq_processed_zaps_user_event ON {db.references_schema}processed_zaps(user_id, event_id) WHERE event_id IS NOT NULL;"
        )
    except Exception:
        raise
    try:
        await db.execute(
            f"CREATE UNIQUE INDEX IF NOT EXISTS uq_processed_zaps_user_receipt ON {db.references_schema}processed_zaps(user_id, zap_receipt_id) WHERE zap_receipt_id IS NOT NULL;"
        )
    except Exception:
        raise
    if db.type != SQLITE:
        await db.execute(
            f"ALTER TABLE {db.references_schema}processed_zaps ALTER COLUMN processed_at TYPE TEXT USING processed_at::text;"
        )
    logger.info("CYBERHERD m028: processed_zaps schema sync complete")


async def m029_add_nip05_verification_toggle(db: Database):
    """Add nip05_verification_enabled column to settings table."""
    logger.info("CYBERHERD m029: adding nip05_verification_enabled column")
    await _add_column_if_missing(
        db, f"{db.references_schema}settings", "nip05_verification_enabled", "INTEGER DEFAULT 1"
    )


async def m030_add_banned_column(db: Database):
    """Add banned column to cyber_herd table for member banning."""
    logger.info("CYBERHERD m030: adding banned column to cyber_herd table")
    table_name = f"{db.references_schema}cyber_herd"
    await _add_column_if_missing(db, table_name, "banned", "INTEGER DEFAULT 0")

    # Add index for banned status queries
    try:
        await db.execute(
            f"CREATE INDEX IF NOT EXISTS idx_cyber_herd_user_banned ON {table_name}(user_id, banned);"
        )
        logger.info("CYBERHERD m030: added index for banned column")
    except Exception as e:
        logger.info(f"CYBERHERD m030 index: {e}")
        raise


async def m031_add_send_splits_enabled(db: Database):
    """Add send_splits_enabled column to settings table for automatic splits trigger."""
    logger.info("CYBERHERD m031: adding send_splits_enabled column")
    await _add_column_if_missing(
        db, f"{db.references_schema}settings", "send_splits_enabled", "INTEGER DEFAULT 0"
    )


async def m032_enable_interaction_tracking(db: Database):
    """Enable repost_tracking_enabled and likes_tracking_enabled by default for existing users.
    
    This migration fixes Bug #8 where interaction tracking was disabled by default,
    preventing real-time detection of reposts and reactions on existing tracked notes.
    New users will get these enabled via the model defaults.
    """
    logger.info("CYBERHERD m032: enabling interaction tracking for existing users")
    try:
        # Enable repost tracking for all users where it's currently disabled (or NULL)
        await db.execute(
            f"""
            UPDATE {db.references_schema}settings
            SET repost_tracking_enabled = 1 
            WHERE repost_tracking_enabled = 0 OR repost_tracking_enabled IS NULL;
            """
        )
        logger.info("CYBERHERD m032: enabled repost_tracking for existing users")
        
        # Enable likes tracking for all users where it's currently disabled (or NULL)
        await db.execute(
            f"""
            UPDATE {db.references_schema}settings
            SET likes_tracking_enabled = 1 
            WHERE likes_tracking_enabled = 0 OR likes_tracking_enabled IS NULL;
            """
        )
        logger.info("CYBERHERD m032: enabled likes_tracking for existing users")
        
        logger.info("CYBERHERD m032: interaction tracking enabled successfully")
    except Exception as e:
        logger.warning(f"CYBERHERD m032: failed to enable interaction tracking: {e}")
        raise


async def m033_add_member_allocation_percent(db: Database):
    """Add member_allocation_percent column to settings table for configurable split percentage."""
    logger.info("CYBERHERD m033: adding member_allocation_percent column")
    await _add_column_if_missing(
        db, f"{db.references_schema}settings", "member_allocation_percent", "INTEGER DEFAULT 10"
    )


async def m034_fix_processed_events_unique_constraint(db: Database):
    """Fix the processed_events unique constraint to include note_id.
    
    The original unique constraint was UNIQUE(user_id, event_hash), but this prevents
    proper tracking of events that reference multiple tracked notes (e.g., a repost
    that mentions two different tracked notes should create two processed_events rows).
    
    The new constraint is UNIQUE(user_id, event_hash, note_id) with note_id defaulting
    to empty string for NULL values to maintain uniqueness semantics.
    
    PostgreSQL is repaired in place. SQLite swaps tables only when the old
    two-column uniqueness exists. Every statement in a swap is transactional;
    duplicate identities fail the migration rather than discarding history.
    """
    async with _migration_transaction(db) as transaction:
        table = f"{db.references_schema}processed_events"
        if db.type != SQLITE:
            constraints = await transaction.fetchall(
                "SELECT c.conname, array_agg(a.attname ORDER BY k.ordinality) AS columns "
                "FROM pg_constraint c "
                "CROSS JOIN LATERAL unnest(c.conkey) WITH ORDINALITY AS k(attnum, ordinality) "
                "JOIN pg_attribute a ON a.attrelid=c.conrelid AND a.attnum=k.attnum "
                "WHERE c.contype='u' AND c.conrelid=CAST(:table AS regclass) GROUP BY c.conname",
                {"table": table},
            )
            for constraint in constraints:
                if set(constraint["columns"]) == {"user_id", "event_hash"}:
                    name = constraint["conname"].replace('"', '""')
                    await transaction.execute(f'ALTER TABLE {table} DROP CONSTRAINT "{name}"')
        else:
            indexes = await transaction.fetchall("PRAGMA index_list(processed_events)")
            pair_key = False
            for index in indexes:
                name = index["name"].replace('"', '""')
                columns = await transaction.fetchall(f'PRAGMA index_info("{name}")')
                if index["unique"] and {row["name"] for row in columns} == {"user_id", "event_hash"}:
                    pair_key = True
            if pair_key:
                await transaction.execute("""CREATE TABLE processed_events_new (
                    user_id TEXT NOT NULL, event_hash TEXT NOT NULL, note_id TEXT DEFAULT '',
                    pubkey TEXT, processed_at INTEGER NOT NULL, event_type TEXT)""")
                await transaction.execute("""INSERT INTO processed_events_new
                    SELECT user_id,event_hash,COALESCE(note_id,''),pubkey,processed_at,event_type
                    FROM processed_events""")
                await transaction.execute("DROP TABLE processed_events")
                await transaction.execute("ALTER TABLE processed_events_new RENAME TO processed_events")
        await transaction.execute(
            f"CREATE UNIQUE INDEX IF NOT EXISTS idx_processed_events_unique_triple "
            f"ON {table}(user_id,event_hash,COALESCE(note_id,''))"
        )



async def m035_fix_cyber_herd_user_pubkey_key(db: Database):
    """Rebuild cyber_herd so member rows are scoped by (user_id, pubkey).

    Older installs used pubkey as the primary key, which lets one user's member
    row overwrite another user's row when the same Nostr pubkey participates in
    more than one CyberHerd instance.
    """
    if db.type == POSTGRES:
        await m036_repair_postgres_member_key(db)
        return
    columns = await db.fetchall("PRAGMA table_info(cyber_herd)")
    if {row["name"] for row in columns if row["pk"]} == {"user_id", "pubkey"}:
        return
    logger.info("CYBERHERD m035: rebuilding cyber_herd with user-scoped primary key")
    table_name = f"{db.references_schema}cyber_herd"
    temp_table = f"{db.references_schema}cyber_herd_new"

    async with _migration_transaction(db) as transaction:
        await transaction.execute(
            f"""
            CREATE TABLE {temp_table} (
                pubkey TEXT NOT NULL,
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
                is_active INTEGER DEFAULT 0,
                user_id TEXT NOT NULL DEFAULT '',
                banned INTEGER DEFAULT 0,
                PRIMARY KEY (user_id, pubkey)
            );
            """
        )

        for column, column_def in (
            ("nip05", "TEXT"),
            ("banned", "INTEGER DEFAULT 0"),
            ("user_id", "TEXT"),
        ):
            await _add_column_if_missing(transaction, table_name, column, column_def)

        await transaction.execute(
            f"""
            INSERT INTO {temp_table} (
                pubkey, display_name, event_id, note, kinds, nprofile, lud16,
                nip05, notified, payouts, amount, picture, relays,
                metadata_last_checked_at, is_active, user_id, banned
            )
            SELECT
                LOWER(TRIM(pubkey)),
                display_name,
                event_id,
                note,
                kinds,
                nprofile,
                lud16,
                nip05,
                COALESCE(notified, 0),
                COALESCE(payouts, 0),
                COALESCE(amount, 0),
                picture,
                relays,
                metadata_last_checked_at,
                COALESCE(is_active, 0),
                COALESCE(NULLIF(TRIM(user_id), ''), ''),
                COALESCE(banned, 0)
            FROM {table_name}
            ;
            """
        )

        await transaction.execute(f"DROP TABLE IF EXISTS {table_name};")
        await transaction.execute(f"ALTER TABLE {temp_table} RENAME TO cyber_herd;")

        rebuilt_table = table_name
        await transaction.execute(
            f"CREATE INDEX IF NOT EXISTS idx_cyber_herd_user_id ON {rebuilt_table}(user_id);"
        )
        await transaction.execute(
            f"CREATE INDEX IF NOT EXISTS idx_cyber_herd_user_active ON {rebuilt_table}(user_id, is_active);"
        )
        await transaction.execute(
            f"CREATE INDEX IF NOT EXISTS idx_cyber_herd_user_banned ON {rebuilt_table}(user_id, banned);"
        )
        logger.info("CYBERHERD m035: cyber_herd user-scoped primary key ready")


async def m036_repair_postgres_member_key(db: Database):
    """Repair PostgreSQL installs where m035 failed but was recorded as complete.

    m035 catches its errors, so version 35 alone does not guarantee that the
    member key matches CRUD's ON CONFLICT(user_id, pubkey). Change the legacy
    key in place to preserve every row, column and index. A single ALTER keeps
    the old key intact if adding the new key fails; propagate that failure so
    the migration runner does not record another false success.
    """
    if db.type != POSTGRES:
        return

    table_name = f"{db.references_schema}cyber_herd"
    primary_key = await db.fetchone(
        """
        SELECT c.conname, array_agg(a.attname ORDER BY k.ordinality) AS columns
        FROM pg_constraint c
        CROSS JOIN LATERAL unnest(c.conkey) WITH ORDINALITY AS k(attnum, ordinality)
        JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = k.attnum
        WHERE c.contype = 'p' AND c.conrelid = CAST(:table_name AS regclass)
        GROUP BY c.conname
        """,
        {"table_name": table_name},
    )
    columns = list(primary_key["columns"]) if primary_key else []
    if set(columns) == {"user_id", "pubkey"}:
        return
    if columns != ["pubkey"]:
        raise RuntimeError(f"CyberHerd m036: unexpected member primary key: {columns}")

    constraint = primary_key["conname"].replace('"', '""')
    await db.execute(
        f'ALTER TABLE {table_name} DROP CONSTRAINT "{constraint}", '
        'ADD PRIMARY KEY (user_id, pubkey)'
    )
    logger.info("CYBERHERD m036: repaired PostgreSQL member key to (user_id, pubkey)")


async def m037_add_membership_reset_days(db: Database):
    """Baseline legacy herds today without guessing or deleting existing membership.

    The pre-upgrade reset history is unknown. A baseline is not a performed reset;
    subsequent local-day changes trigger durable catch-up before event intake.
    """
    from .services.time_utils import get_day_boundaries_utc

    await db.execute(f"""CREATE TABLE IF NOT EXISTS {db.references_schema}membership_reset_days (
        user_id TEXT PRIMARY KEY,
        local_day TEXT NOT NULL,
        splits_pending INTEGER NOT NULL DEFAULT 0
    )""")
    await db.execute(
        f"""INSERT INTO {db.references_schema}membership_reset_days (user_id, local_day)
        SELECT user_id, :local_day FROM {db.references_schema}settings
        WHERE user_id IS NOT NULL AND user_id != ''
        ON CONFLICT (user_id) DO NOTHING""",
        {"local_day": get_day_boundaries_utc().local_day_str},
    )


async def m038_preserve_payout_precision(db: Database):
    """Store fractional payout shares with the same precision as split targets."""
    if db.type == POSTGRES:
        await db.execute(
            f"ALTER TABLE {db.references_schema}cyber_herd "
            "ALTER COLUMN payouts TYPE DOUBLE PRECISION"
        )


async def m039_repair_processed_zap_timestamp(db: Database):
    """Repair legacy timestamp columns that reject the accounting ledger's text."""
    if db.type != POSTGRES:
        return
    table = f"{db.references_schema}processed_zaps"
    # Do not suppress errors: a recorded version must guarantee this repair.
    # Preserve historical values and the legacy insertion-time default.
    await db.execute(
        f"ALTER TABLE {table} ALTER COLUMN processed_at TYPE TEXT "
        "USING processed_at::text"
    )


async def m040_repair_processed_zap_nullability(db: Database):
    """Allow payment-ledger rows without legacy Nostr receipt fields."""
    if db.type != POSTGRES:
        return
    table = f"{db.references_schema}processed_zaps"
    # Match the consolidated schema: settled invoices need no relay receipt,
    # and profile zaps may have no note. Keep user_id and deduplication intact.
    for column in (
        "zap_receipt_id", "payment_hash", "note_id", "zapper_pubkey", "amount_sats"
    ):
        await db.execute(f"ALTER TABLE {table} ALTER COLUMN {column} DROP NOT NULL")
    await db.execute(
        f"CREATE UNIQUE INDEX IF NOT EXISTS uq_processed_zaps_user_event "
        f"ON {table}(user_id, event_id) WHERE event_id IS NOT NULL"
    )


async def m041_repair_database_contract(db: Database):
    """Repair recorded historical migrations without replacing member data."""
    await m034_fix_processed_events_unique_constraint(db)
    await _add_column_if_missing(db, f"{db.references_schema}members", "alias", "TEXT")
    await _add_column_if_missing(db, f"{db.references_schema}members", "added_at", "INTEGER")
    await _add_column_if_missing(
        db, f"{db.references_schema}cyber_herd", "allocation_percentage", "REAL"
    )
    if db.type == POSTGRES:
        # Satoshi totals and Unix seconds are not limited to signed 32 bits.
        for table, columns in (
            ("cyber_herd", ("amount", "notified", "metadata_last_checked_at")),
            ("members", ("amount",)),
            ("processed_zaps", ("amount", "amount_sats")),
            ("processed_events", ("processed_at",)),
            ("zap_totals", ("total_sats", "first_event_at", "last_event_at")),
        ):
            for column in columns:
                await db.execute(
                    f"ALTER TABLE {db.references_schema}{table} ALTER COLUMN {column} TYPE BIGINT"
                )
        # float4 rounds epoch seconds by tens of seconds, breaking cache TTLs.
        await db.execute(
            f"ALTER TABLE {db.references_schema}zap_totals "
            "ALTER COLUMN last_updated_at TYPE DOUBLE PRECISION"
        )


async def _create_engagement_awards(db):
    await db.execute(f"""CREATE TABLE IF NOT EXISTS {db.references_schema}engagement_awards (
        user_id TEXT NOT NULL, pubkey TEXT NOT NULL,
        kind INTEGER NOT NULL CHECK (kind IN (6, 7)),
        awarded_at BIGINT,
        PRIMARY KEY (user_id, pubkey, kind)
    )""")


async def m042_add_engagement_awards(db: Database):
    """Preserve evidenced social awards for the current cycle, including evictions."""
    from datetime import date
    from .services.time_utils import get_day_boundaries_utc

    async with _migration_transaction(db) as transaction:
        await _create_engagement_awards(transaction)
        schema = db.references_schema
        for kind in (6, 7):
            # Current membership kinds prove the award, but not its original time.
            await transaction.execute(
                f"""INSERT INTO {schema}engagement_awards (user_id,pubkey,kind)
                SELECT user_id,LOWER(TRIM(pubkey)),:kind FROM {schema}cyber_herd
                WHERE user_id IS NOT NULL AND TRIM(user_id) != ''
                AND pubkey IS NOT NULL AND TRIM(pubkey) != ''
                AND (',' || COALESCE(kinds,'') || ',') LIKE :pattern
                ON CONFLICT DO NOTHING""",
                {"kind": kind, "pattern": f"%,{kind},%"},
            )
        today = get_day_boundaries_utc()
        owners = await transaction.fetchall(
            f"""SELECT s.user_id,s.midnight_reset_enabled,r.local_day
            FROM {schema}settings s LEFT JOIN {schema}membership_reset_days r
            ON s.user_id=r.user_id WHERE s.user_id IS NOT NULL AND s.user_id != ''"""
        )
        for owner in owners:
            if owner["midnight_reset_enabled"]:
                since = today.local_since_ts
            elif owner["local_day"]:
                days = (date.fromisoformat(today.local_day_str) - date.fromisoformat(owner["local_day"])).days
                since = get_day_boundaries_utc(days_ago=days).local_since_ts
            else:
                since = 0  # no known reset; retain documented successful awards
            await transaction.execute(
                f"""INSERT INTO {schema}engagement_awards (user_id,pubkey,kind,awarded_at)
                SELECT user_id,LOWER(TRIM(pubkey)),
                    CASE WHEN event_type='repost' THEN 6 ELSE 7 END,MIN(processed_at)
                FROM {schema}processed_events WHERE user_id=:user_id
                AND event_type IN ('repost','reaction') AND processed_at >= :since
                AND pubkey IS NOT NULL AND TRIM(pubkey) != ''
                GROUP BY user_id,LOWER(TRIM(pubkey)),event_type
                ON CONFLICT DO NOTHING""",
                {"user_id": owner["user_id"], "since": int(since)},
            )


async def m043_add_treat_delivery_records(db: Database):
    """Durable member branches, distribution snapshots and signed-message outbox."""
    async with _migration_transaction(db) as transaction:
        await transaction.execute(f"""CREATE TABLE IF NOT EXISTS {db.references_schema}delivery_records (
            id TEXT PRIMARY KEY, user_id TEXT NOT NULL, kind TEXT NOT NULL,
            state TEXT NOT NULL, payload TEXT NOT NULL, updated_at BIGINT NOT NULL
        )""")
        await transaction.execute(f"""CREATE INDEX IF NOT EXISTS delivery_records_work
            ON {db.references_schema}delivery_records (kind,state,updated_at)""")
        # An independent cycle token also distinguishes multiple manual resets today.
        await transaction.execute(f"""CREATE TABLE IF NOT EXISTS {db.references_schema}member_cycles (
            user_id TEXT PRIMARY KEY, cycle_id TEXT NOT NULL
        )""")


async def m044_add_prejoin_engagement_evidence(db: Database):
    """Keep qualifying social actions separate from awarded membership points."""
    # LNbits attaches the SQLite file under a second name. Explicit main-schema
    # DDL avoids taking incompatible locks on both aliases during ALTER TABLE.
    schema = "main." if db.type == SQLITE else db.references_schema
    async with _migration_transaction(db) as transaction:
        if db.type == SQLITE:
            columns = await transaction.fetchall(
                "PRAGMA main.table_info(member_cycles)"
            )
            has_start = any(row["name"] == "started_at" for row in columns)
        else:
            has_start = await _column_exists(
                transaction, f"{schema}member_cycles", "started_at"
            )
        if not has_start:
            await transaction.execute(
                f"ALTER TABLE {schema}member_cycles ADD COLUMN started_at BIGINT NOT NULL DEFAULT 0"
            )
        await transaction.execute(
            f"""CREATE TABLE IF NOT EXISTS {schema}engagement_observations (
            user_id TEXT NOT NULL, pubkey TEXT NOT NULL, kind INTEGER NOT NULL CHECK (kind IN (6,7)),
            cycle_id TEXT NOT NULL, event_id TEXT NOT NULL, note_id TEXT NOT NULL,
            created_at BIGINT NOT NULL,
            PRIMARY KEY (user_id,pubkey,kind,cycle_id)
        )"""
        )
