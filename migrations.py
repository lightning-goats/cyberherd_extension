"""Consolidated CyberHerd migrations.

This file contains a clean, consolidated initial migration that creates
all tables with their current schema. This replaces the fragmented historical
migrations for new installations.

For existing installations, the old migrations (m001-m020) remain to ensure
proper upgrade paths. New installations can use just m001_consolidated.
"""

from lnbits.db import Database
import logging

logger = logging.getLogger(__name__)


async def _column_exists(db: Database, schema_table: str, column: str) -> bool:
    """Return True if ``column`` exists on ``schema_table``.

    Checking existence up front avoids issuing a failing ``ALTER TABLE ... ADD
    COLUMN`` inside the shared migration transaction. On Postgres a failed
    statement aborts the whole transaction, which would silently break every
    subsequent migration; catching the error is not enough because the
    transaction is already poisoned. Works on Postgres (information_schema) and
    SQLite (PRAGMA), returning False on any error.
    """
    if "." in schema_table:
        schema, table = schema_table.split(".", 1)
    else:
        schema, table = None, schema_table

    # Postgres / Cockroach: information_schema.
    try:
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
        if row:
            return True
    except Exception:
        # Not Postgres, or information_schema unavailable — fall through.
        pass

    # SQLite: PRAGMA table_info.
    try:
        rows = await db.fetchall(f"PRAGMA table_info({table})")
        for r in rows:
            try:
                name = r.get("name") if hasattr(r, "get") else r["name"]
            except Exception:
                name = None
            if name == column:
                return True
    except Exception:
        pass

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
        # Tolerate a concurrent add / benign duplicate race; anything else is
        # logged for visibility but not fatal.
        msg = str(e).lower()
        if any(t in msg for t in ("duplicate", "already exists")):
            return
        logger.warning(f"CyberHerd migration: could not add {schema_table}.{column}: {e}")

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
    try:
        logger.info("CYBERHERD m001: Creating schema...")
        await db.execute("CREATE SCHEMA IF NOT EXISTS cyberherd;")
        logger.info("CYBERHERD m001: Schema created successfully")
    except Exception as e:
        logger.warning(f"CYBERHERD m001: Schema creation error (may be SQLite): {e}")
    
    # -------------------------------------------------------------------------
    # Settings Table - User Configuration
    # -------------------------------------------------------------------------
    await db.execute(
        """
        CREATE TABLE IF NOT EXISTS cyberherd.settings (
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
            tracked_event_timestamps TEXT DEFAULT '{}',
            tracked_event_addresses TEXT DEFAULT '{}',
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
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_settings_user ON cyberherd.settings(user_id) WHERE user_id IS NOT NULL;"
        )
    except Exception:
        pass
    
    # -------------------------------------------------------------------------
    # Cyber Herd Table - Active Members (Legacy, kept for compatibility)
    # -------------------------------------------------------------------------
    await db.execute(
        """
        CREATE TABLE IF NOT EXISTS cyberherd.cyber_herd (
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
            "CREATE INDEX IF NOT EXISTS idx_cyber_herd_user_id ON cyberherd.cyber_herd(user_id);"
        )
        await db.execute(
            "CREATE INDEX IF NOT EXISTS idx_cyber_herd_user_active ON cyberherd.cyber_herd(user_id, is_active);"
        )
        await db.execute(
            "CREATE INDEX IF NOT EXISTS idx_cyber_herd_user_banned ON cyberherd.cyber_herd(user_id, banned);"
        )
    except Exception:
        pass
    
    # -------------------------------------------------------------------------
    # Members Table - New member tracking system
    # -------------------------------------------------------------------------
    await db.execute(
        """
        CREATE TABLE IF NOT EXISTS cyberherd.members (
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
        CREATE TABLE IF NOT EXISTS cyberherd.processed_zaps (
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
            "CREATE INDEX IF NOT EXISTS idx_processed_zaps_user_event ON cyberherd.processed_zaps(user_id, event_id);"
        )
        await db.execute(
            "CREATE INDEX IF NOT EXISTS idx_processed_zaps_user_note ON cyberherd.processed_zaps(user_id, note_id);"
        )
        await db.execute(
            "CREATE INDEX IF NOT EXISTS idx_processed_zaps_user_zapper ON cyberherd.processed_zaps(user_id, zapper_pubkey);"
        )
        try:
            await db.execute(
                "CREATE INDEX IF NOT EXISTS idx_processed_zaps_user_receipt ON cyberherd.processed_zaps(user_id, zap_receipt_id) WHERE zap_receipt_id IS NOT NULL;"
            )
        except Exception:
            pass
    except Exception:
        pass

    # -------------------------------------------------------------------------
    # Zap Totals Table - Persist aggregated zap receipt summaries
    # -------------------------------------------------------------------------
    await db.execute(
        """
        CREATE TABLE IF NOT EXISTS cyberherd.zap_totals (
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
    logger.info("CYBERHERD MIGRATION m025: migrating legacy settings rows to explicit user_id where possible")
    settings_table = f"{db.references_schema}settings"
    try:
        # Find legacy settings rows with a source_wallet and no user_id
        rows = await db.fetchall(
            f"SELECT rowid AS rid, source_wallet FROM {settings_table} WHERE (user_id IS NULL OR TRIM(user_id) = '') AND source_wallet IS NOT NULL AND TRIM(source_wallet) != ''"
        )
    except Exception:
        # Fallback for SQLite or other connections without schema prefix
        try:
            rows = await db.fetchall(
                "SELECT rowid AS rid, source_wallet FROM settings WHERE (user_id IS NULL OR TRIM(user_id) = '') AND source_wallet IS NOT NULL AND TRIM(source_wallet) != ''"
            )
        except Exception as e:
            logger.info(f"CyberHerd m025: could not enumerate legacy settings rows: {e}")
            return

    if not rows:
        logger.info("CyberHerd m025: no legacy settings rows to migrate")
        return

    migrated = 0
    for row in rows:
        source_wallet = row.get("source_wallet") if isinstance(row, dict) else row[1]
        rid = row.get("rid") if isinstance(row, dict) else row[0]
        if not source_wallet:
            continue
        # Resolve owning user via core wallets table
        try:
            user_rows = await db.fetchall(
                'SELECT "user" FROM wallets WHERE id = :wallet_id',
                {"wallet_id": source_wallet},
            )
        except Exception:
            try:
                user_rows = await db.fetchall(
                    'SELECT "user" FROM public.wallets WHERE id = :wallet_id',
                    {"wallet_id": source_wallet},
                )
            except Exception:
                continue
        if not user_rows:
            continue
        user_id = user_rows[0].get("user") if isinstance(user_rows[0], dict) else user_rows[0][0]
        if not user_id:
            continue
        # Check if user already has a settings row
        try:
            existing = await db.fetchall(
                f"SELECT 1 FROM {settings_table} WHERE user_id = :uid LIMIT 1",
                {"uid": user_id},
            )
        except Exception:
            existing = []
        if existing:
            continue
        # Update the legacy row
        try:
            await db.execute(
                f"UPDATE {settings_table} SET user_id = :uid WHERE rowid = :rid",
                {"uid": user_id, "rid": rid},
            )
            migrated += 1
        except Exception as e:
            logger.warning(f"CyberHerd m025: failed to migrate row {rid}: {e}")

    logger.info(f"CyberHerd m025: migrated {migrated} legacy settings rows")


async def m027_add_feeder_trigger_sats(db: Database):
    """Add feeder_trigger_sats column to settings for external feeder logic."""
    table_name = f"{db.references_schema}settings"
    logger.info("CYBERHERD m027: Adding feeder_trigger_sats column to settings")
    await _add_column_if_missing(db, table_name, "feeder_trigger_sats", "INTEGER DEFAULT 0")


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


async def m028_sync_processed_zaps_schema(db: Database):
    """Ensure processed_zaps table has the unified legacy/new columns."""
    logger.info("CYBERHERD m028: syncing processed_zaps table schema")
    table_name = "cyberherd.processed_zaps"
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
        "CREATE INDEX IF NOT EXISTS idx_processed_zaps_user_event ON cyberherd.processed_zaps(user_id, event_id);",
        "CREATE INDEX IF NOT EXISTS idx_processed_zaps_user_note ON cyberherd.processed_zaps(user_id, note_id);",
        "CREATE INDEX IF NOT EXISTS idx_processed_zaps_user_zapper ON cyberherd.processed_zaps(user_id, zapper_pubkey);",
    ]
    for stmt in index_statements:
        try:
            await db.execute(stmt)
        except Exception:
            pass
    try:
        await db.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS uq_processed_zaps_user_event ON cyberherd.processed_zaps(user_id, event_id) WHERE event_id IS NOT NULL;"
        )
    except Exception:
        pass
    try:
        await db.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS uq_processed_zaps_user_receipt ON cyberherd.processed_zaps(user_id, zap_receipt_id) WHERE zap_receipt_id IS NOT NULL;"
        )
    except Exception:
        pass
    try:
        await db.execute(
            "ALTER TABLE cyberherd.processed_zaps ALTER COLUMN processed_at TYPE TEXT USING processed_at::text;"
        )
    except Exception:
        pass
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
            """
            UPDATE cyberherd.settings 
            SET repost_tracking_enabled = 1 
            WHERE repost_tracking_enabled = 0 OR repost_tracking_enabled IS NULL;
            """
        )
        logger.info("CYBERHERD m032: enabled repost_tracking for existing users")
        
        # Enable likes tracking for all users where it's currently disabled (or NULL)
        await db.execute(
            """
            UPDATE cyberherd.settings 
            SET likes_tracking_enabled = 1 
            WHERE likes_tracking_enabled = 0 OR likes_tracking_enabled IS NULL;
            """
        )
        logger.info("CYBERHERD m032: enabled likes_tracking for existing users")
        
        logger.info("CYBERHERD m032: interaction tracking enabled successfully")
    except Exception as e:
        logger.warning(f"CYBERHERD m032: failed to enable interaction tracking: {e}")


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
    
    This migration:
    1. Creates a new table with the correct constraint
    2. Migrates existing data (handling NULL note_ids)
    3. Drops the old table and renames the new one
    """
    logger.info("CYBERHERD m034: fixing processed_events unique constraint to include note_id")
    table_name = f"{db.references_schema}processed_events"
    temp_table = f"{db.references_schema}processed_events_new"
    
    try:
        # Step 1: Create new table with correct unique constraint
        # Using COALESCE pattern in the unique index to handle NULL note_ids
        await db.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {temp_table} (
                user_id TEXT NOT NULL,
                event_hash TEXT NOT NULL,
                note_id TEXT DEFAULT '',
                pubkey TEXT,
                processed_at INTEGER NOT NULL,
                event_type TEXT
            );
            """
        )
        
        # Step 2: Create the unique index that handles the (user_id, event_hash, note_id) triple
        # Using COALESCE to ensure NULL note_ids are treated as empty strings
        try:
            await db.execute(
                f"""
                CREATE UNIQUE INDEX IF NOT EXISTS idx_processed_events_unique_triple 
                ON {temp_table}(user_id, event_hash, COALESCE(note_id, ''));
                """
            )
        except Exception as idx_e:
            # Some databases may not support COALESCE in index - fall back to simpler approach
            logger.debug(f"CYBERHERD m034: COALESCE index failed, trying simple index: {idx_e}")
            try:
                await db.execute(
                    f"""
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_processed_events_unique_triple 
                    ON {temp_table}(user_id, event_hash, note_id);
                    """
                )
            except Exception:
                pass
        
        # Step 3: Migrate existing data, converting NULL note_ids to empty string
        # and handling duplicates by keeping the earliest processed_at
        try:
            await db.execute(
                f"""
                INSERT INTO {temp_table} (user_id, event_hash, note_id, pubkey, processed_at, event_type)
                SELECT 
                    user_id, 
                    event_hash, 
                    COALESCE(note_id, '') as note_id,
                    pubkey, 
                    processed_at, 
                    event_type
                FROM {table_name}
                WHERE user_id IS NOT NULL AND event_hash IS NOT NULL
                ON CONFLICT DO NOTHING;
                """
            )
            logger.info("CYBERHERD m034: migrated existing data to new table")
        except Exception as migrate_e:
            # If migration fails, the old table structure is still valid
            # Just log and continue - the code will work with the old structure
            logger.warning(f"CYBERHERD m034: data migration failed, keeping old table: {migrate_e}")
            try:
                await db.execute(f"DROP TABLE IF EXISTS {temp_table};")
            except Exception:
                pass
            return
        
        # Step 4: Drop old table and rename new one
        try:
            await db.execute(f"DROP TABLE IF EXISTS {table_name};")
            await db.execute(f"ALTER TABLE {temp_table} RENAME TO processed_events;")
            logger.info("CYBERHERD m034: replaced old table with new schema")
        except Exception as rename_e:
            logger.warning(f"CYBERHERD m034: table swap failed: {rename_e}")
            # Try to recover by keeping old table
            try:
                await db.execute(f"DROP TABLE IF EXISTS {temp_table};")
            except Exception:
                pass
            return
        
        logger.info("CYBERHERD m034: processed_events unique constraint fixed successfully")
    except Exception as e:
        logger.warning(f"CYBERHERD m034: migration failed: {e}")


async def m035_fix_cyber_herd_user_pubkey_key(db: Database):
    """Rebuild cyber_herd so member rows are scoped by (user_id, pubkey).

    Older installs used pubkey as the primary key, which lets one user's member
    row overwrite another user's row when the same Nostr pubkey participates in
    more than one CyberHerd instance.
    """
    logger.info("CYBERHERD m035: rebuilding cyber_herd with user-scoped primary key")
    table_name = f"{db.references_schema}cyber_herd"
    temp_table = f"{db.references_schema}cyber_herd_new"

    try:
        await db.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {temp_table} (
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
            await _add_column_if_missing(db, table_name, column, column_def)

        await db.execute(
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
            WHERE pubkey IS NOT NULL AND TRIM(pubkey) != ''
            ON CONFLICT DO NOTHING;
            """
        )

        await db.execute(f"DROP TABLE IF EXISTS {table_name};")
        await db.execute(f"ALTER TABLE {temp_table} RENAME TO cyber_herd;")

        rebuilt_table = table_name
        await db.execute(
            f"CREATE INDEX IF NOT EXISTS idx_cyber_herd_user_id ON {rebuilt_table}(user_id);"
        )
        await db.execute(
            f"CREATE INDEX IF NOT EXISTS idx_cyber_herd_user_active ON {rebuilt_table}(user_id, is_active);"
        )
        await db.execute(
            f"CREATE INDEX IF NOT EXISTS idx_cyber_herd_user_banned ON {rebuilt_table}(user_id, banned);"
        )
        logger.info("CYBERHERD m035: cyber_herd user-scoped primary key ready")
    except Exception as e:
        logger.warning(f"CYBERHERD m035: migration failed: {e}")
        try:
            await db.execute(f"DROP TABLE IF EXISTS {temp_table};")
        except Exception:
            pass
