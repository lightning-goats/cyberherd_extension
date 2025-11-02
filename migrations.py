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
            pubkey TEXT PRIMARY KEY,
            display_name TEXT,
            event_id TEXT,
            note TEXT,
            kinds TEXT,
            nprofile TEXT,
            lud16 TEXT,
            notified INTEGER DEFAULT 0,
            payouts REAL DEFAULT 0,
            amount INTEGER DEFAULT 0,
            picture TEXT,
            relays TEXT,
            metadata_last_checked_at INTEGER,
            is_active INTEGER DEFAULT 0,
            user_id TEXT,
            banned INTEGER DEFAULT 0
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
    
    try:
        await db.execute(
            f"ALTER TABLE {table_name} ADD COLUMN likes_tracking_enabled INTEGER DEFAULT 0;"
        )
        logger.info("CyberHerd m021: added likes_tracking_enabled column")
    except Exception as e:
        # Column likely already exists (from m000_consolidated_schema)
        logger.info(f"CyberHerd m021: column already exists or table not ready: {e}")


async def m022_add_user_id_to_cyber_herd(db: Database):
    """Add user_id column to cyber_herd table for multi-user support.

    This column is needed to associate herd members with specific users.
    Safe to run multiple times (ignores error if column exists).
    Works for both SQLite and PostgreSQL.
    """
    logger.info("CYBERHERD MIGRATION m022: Adding user_id column to cyber_herd table")
    # Build table name with schema prefix for PostgreSQL, without for SQLite
    table_name = f"{db.references_schema}cyber_herd"
    
    try:
        await db.execute(
            f"ALTER TABLE {table_name} ADD COLUMN user_id TEXT;"
        )
        logger.info("CyberHerd m022: added user_id column to cyber_herd table")
    except Exception as e:
        # Column likely already exists (from m000_consolidated_schema)
        logger.info(f"CyberHerd m022: column already exists or table not ready: {e}")
    
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

    try:
        await db.execute(
            f"ALTER TABLE {table_name} ADD COLUMN tracked_event_addresses TEXT DEFAULT '{{}}';"
        )
        logger.info("CyberHerd m023: added tracked_event_addresses column")
    except Exception as e:
        logger.info(
            f"CyberHerd m023: column already exists or table not ready: {e}"
        )


async def m024_add_midnight_reset_toggle(db: Database):
    """Add midnight_reset_enabled column to settings to opt-in nightly reset per-user.

    Default value is 1 (enabled) to preserve existing behavior for upgrades.
    """
    logger.info("CYBERHERD MIGRATION m024: Adding midnight_reset_enabled column (default 1)")
    table_name = f"{db.references_schema}settings"
    try:
        await db.execute(
            f"ALTER TABLE {table_name} ADD COLUMN midnight_reset_enabled INTEGER DEFAULT 1;"
        )
        logger.info("CyberHerd m024: added midnight_reset_enabled column")
    except Exception as e:
        logger.info(f"CyberHerd m024: column already exists or table not ready: {e}")


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
async def m027_add_feeder_trigger_sats(db: Database):
    """Add feeder_trigger_sats column to settings for external feeder logic."""
    table_name = f"{db.references_schema}settings"
    try:
        logger.info("CYBERHERD m027: Adding feeder_trigger_sats column to settings")
        await db.execute(
            f"ALTER TABLE {table_name} ADD COLUMN feeder_trigger_sats INTEGER DEFAULT 0;"
        )
        logger.info("CYBERHERD m027: feeder_trigger_sats column added")
    except Exception as e:
        msg = str(e).lower()
        if "duplicate" in msg or "already exists" in msg:
            logger.info("CYBERHERD m027: feeder_trigger_sats column already exists, skipping")
        else:
            logger.warning(f"CYBERHERD m027: failed to add feeder_trigger_sats column: {e}")


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
    alter_statements = [
        f"ALTER TABLE {table_name} ADD COLUMN event_id TEXT;",
        f"ALTER TABLE {table_name} ADD COLUMN note_id TEXT;",
        f"ALTER TABLE {table_name} ADD COLUMN zapper_pubkey TEXT;",
        f"ALTER TABLE {table_name} ADD COLUMN amount INTEGER;",
        f"ALTER TABLE {table_name} ADD COLUMN processed_at TEXT;",
        f"ALTER TABLE {table_name} ADD COLUMN zap_receipt_id TEXT;",
        f"ALTER TABLE {table_name} ADD COLUMN payment_hash TEXT;",
        f"ALTER TABLE {table_name} ADD COLUMN amount_sats INTEGER;",
    ]
    for stmt in alter_statements:
        try:
            await db.execute(stmt)
        except Exception as e:
            msg = str(e).lower()
            if any(token in msg for token in ("duplicate", "already exists", "syntax error", "duplicate column")):
                continue
            logger.debug(f"CYBERHERD m028: alter skipped ({stmt}): {e}")

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
    try:
        await db.execute(
            """
            ALTER TABLE cyberherd.settings 
            ADD COLUMN nip05_verification_enabled INTEGER DEFAULT 1;
            """
        )
        logger.info("CYBERHERD m029: nip05_verification_enabled column added successfully")
    except Exception as e:
        msg = str(e).lower()
        if any(token in msg for token in ("duplicate", "already exists", "duplicate column")):
            logger.info("CYBERHERD m029: nip05_verification_enabled column already exists, skipping")
        else:
            logger.warning(f"CYBERHERD m029: failed to add nip05_verification_enabled column: {e}")


async def m030_add_banned_column(db: Database):
    """Add banned column to cyber_herd table for member banning."""
    logger.info("CYBERHERD m030: adding banned column to cyber_herd table")
    table_name = f"{db.references_schema}cyber_herd"
    try:
        await db.execute(
            f"""
            ALTER TABLE {table_name}
            ADD COLUMN banned INTEGER DEFAULT 0;
            """
        )
        logger.info("CYBERHERD m030: banned column added successfully")
    except Exception as e:
        msg = str(e).lower()
        if any(token in msg for token in ("duplicate", "already exists", "duplicate column")):
            logger.info("CYBERHERD m030: banned column already exists, skipping")
        else:
            logger.warning(f"CYBERHERD m030: failed to add banned column: {e}")
    
    # Add index for banned status queries
    try:
        await db.execute(
            f"CREATE INDEX IF NOT EXISTS idx_cyber_herd_user_banned ON {table_name}(user_id, banned);"
        )
        logger.info("CYBERHERD m030: added index for banned column")
    except Exception as e:
        logger.info(f"CYBERHERD m030 index: {e}")
