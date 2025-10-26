from lnbits.db import Database
import logging

# Expose ordered list of migration function names so LNbits core can determine
# the latest version even if regex ordering changes; optional but helpful.
__all__ = [
    'm000_create_schema',
    'm001_initial',
    'm002_create_cyberherd_table',
    'm003_add_nostr_private_key',
    'm004_add_user_id',
    'm005_add_pubkey_override',
    'm006_add_zap_wallet_columns',
    'm007_add_zap_tracking_enabled',
    'm008_add_herd_wallet',
    'm009_add_zap_monitor_mode',
    'm011_add_computed_effective_pubkey',
    'm012_create_members_table',
    'm013_settings_user_unique_index',
    'm014_drop_legacy_processed_zap_events',
    'm007_add_processed_zaps_table',
    'm015_ensure_processed_zaps',
    'm016_add_templates_owner_user',
    'm017_add_repost_tracking_enabled',
    'm018_add_manual_event_ids',
    'm019_add_minimum_sats',
    'm020_add_user_id_to_cyber_herd',
]

logger = logging.getLogger(__name__)


async def m001_initial(db: Database):
    """Create initial cyberherd tables: settings (single row) and members."""
    # Ensure schema exists for Postgres/Cockroach (SQLite ignores schemas)
    try:
        if getattr(db, 'schema', None) or True:
            # We defensively create schema even if db.schema is set; harmless on SQLite.
            await db.execute("CREATE SCHEMA IF NOT EXISTS cyberherd;")
    except Exception:
        # Ignore if not supported
        pass

    await db.execute(
        """
        CREATE TABLE IF NOT EXISTS cyberherd.settings (
            source_wallet TEXT,
            max_members INTEGER DEFAULT 3,
            tracked_tags TEXT DEFAULT '[]'
        );
        """
    )

    await db.execute(
        """
        CREATE TABLE cyberherd.members (
            pubkey TEXT PRIMARY KEY,
            alias TEXT,
            added_at INTEGER
        );
        """
    )


async def m002_create_cyberherd_table(db: Database):
    """
    Create the main cyber_herd table matching middleware expectations.
    """
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
            is_active INTEGER DEFAULT 0
        );
        """
    )


async def m000_create_schema(db: Database):
    """Create cyberherd schema for Postgres/Cockroach before any tables.

    Placed after function definitions to satisfy ordering by name (m000).
    Safe to run multiple times; ignored on SQLite.
    """
    try:
        await db.execute("CREATE SCHEMA IF NOT EXISTS cyberherd;")
    except Exception:
        # Non-fatal: likely SQLite or already exists
        pass


async def m003_add_nostr_private_key(db: Database):
    """Add column for storing a hex-encoded nostr private key."""
    await _ensure_settings_table(db)
    await db.execute("ALTER TABLE cyberherd.settings ADD COLUMN nostr_private_key TEXT;")


async def m004_add_user_id(db: Database):
    """Add a nullable user_id column to support per-user settings.

    This is additive and compatible with sqlite's ALTER TABLE ADD COLUMN.
    Existing installations will keep their single-row legacy settings; new
    per-user rows can be created after this migration runs.
    """
    await _ensure_settings_table(db)
    await db.execute("ALTER TABLE cyberherd.settings ADD COLUMN user_id TEXT;")


async def m005_add_pubkey_override(db: Database):
    """Add a column to store an optional hex pubkey override for the listener."""
    await _ensure_settings_table(db)
    await db.execute("ALTER TABLE cyberherd.settings ADD COLUMN nostr_pubkey_override TEXT;")


async def m006_add_zap_wallet_columns(db: Database):
    """Add zap_wallet and zap_wallet_alias columns to settings.

    Backwards-compatible additive migration.
    """
    try:
        await _ensure_settings_table(db)
        await db.execute("ALTER TABLE cyberherd.settings ADD COLUMN zap_wallet TEXT;")
    except Exception:
        # Column may already exist; ignore
        pass
    try:
        await db.execute(
            "ALTER TABLE cyberherd.settings ADD COLUMN zap_wallet_alias TEXT;"
        )
    except Exception:
        # Column may already exist; ignore
        pass


async def m007_add_zap_tracking_enabled(db: Database):
    """Add zap_tracking_enabled column to settings table.

    Backwards-compatible additive migration.
    """
    try:
        await _ensure_settings_table(db)
        await db.execute("ALTER TABLE cyberherd.settings ADD COLUMN zap_tracking_enabled INTEGER DEFAULT 0;")
    except Exception:
        # Column may already exist; ignore
        pass


async def m008_add_herd_wallet(db: Database):
    """Add herd_wallet column to settings table.

    Backwards-compatible additive migration.
    """
    try:
        await _ensure_settings_table(db)
        await db.execute("ALTER TABLE cyberherd.settings ADD COLUMN herd_wallet TEXT;")
    except Exception:
        # Column may already exist; ignore
        pass


async def m009_add_zap_monitor_mode(db: Database):
    """Add zap_monitor_mode column to settings table.

    Values: 'payment' (default) or 'nostr'. Backwards-compatible additive migration.
    """
    try:
        await _ensure_settings_table(db)
        await db.execute("ALTER TABLE cyberherd.settings ADD COLUMN zap_monitor_mode TEXT DEFAULT 'payment';")
    except Exception:
        # Column may already exist; ignore
        pass


async def m011_add_computed_effective_pubkey(db: Database):
    """Add computed_effective_pubkey column to settings table.

    Stores the computed effective pubkey for better performance and consistency.
    Backwards-compatible additive migration.
    """
    try:
        await _ensure_settings_table(db)
        await db.execute("ALTER TABLE cyberherd.settings ADD COLUMN computed_effective_pubkey TEXT;")
    except Exception:
        # Column may already exist; ignore
        pass
    """Add processed_zap_events table to track processed zap events.

    Prevents duplicate processing of zap events during recovery and monitoring.
    """
    try:
        await db.execute(
            """
            CREATE TABLE cyberherd.processed_zap_events (
                zap_event_id TEXT PRIMARY KEY,
                processed_at INTEGER NOT NULL,
                status TEXT DEFAULT 'completed',
                note_id TEXT,
                zapper_pubkey TEXT,
                amount_sats INTEGER,
                user_id TEXT
            );
            """
        )
        # Add index for faster lookups
        await db.execute(
            "CREATE INDEX idx_processed_zap_events_processed_at ON cyberherd.processed_zap_events(processed_at);"
        )
        await db.execute(
            "CREATE INDEX idx_processed_zap_events_note_id ON cyberherd.processed_zap_events(note_id);"
        )
    except Exception:
        # Table may already exist; ignore
        pass


async def m012_create_members_table(db: Database):
    """Create the cyberherd members table."""
    await db.execute(
        """
        CREATE TABLE IF NOT EXISTS cyberherd.members (
            pubkey TEXT PRIMARY KEY,
            amount INTEGER NOT NULL DEFAULT 0,
            allocation_percentage REAL NOT NULL DEFAULT 10.0,
            note_id TEXT,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            is_active BOOLEAN NOT NULL DEFAULT 1,
            display_name TEXT,
            lud16 TEXT,
            picture TEXT,
            relays TEXT,
            metadata_last_checked_at TIMESTAMP,
            payouts REAL NOT NULL DEFAULT 0.0
        );
        """
    )


async def m013_settings_user_unique_index(db: Database):
    """Ensure at most one settings row per user_id (ignoring NULLs).

    Adds a partial unique index compatible with SQLite (emulated by
    covering index with WHERE clause). Safe to run multiple times.
    """
    try:
        await _ensure_settings_table(db)
        await db.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_cyberherd_settings_user ON cyberherd.settings(user_id) WHERE user_id IS NOT NULL;")
    except Exception:
        # Best effort; not fatal
        pass


async def m014_drop_legacy_processed_zap_events(db: Database):
    """Drop legacy processed_zap_events table now superseded by processed_zaps.

    Safe: uses IF EXISTS pattern and ignores errors. Indices are dropped implicitly.
    """
    try:
        await db.execute("DROP TABLE IF EXISTS cyberherd.processed_zap_events;")
        logger.info("Cyberherd migration: dropped legacy processed_zap_events table")
    except Exception as e:
        try:
            logger.debug(f"Cyberherd migration: could not drop processed_zap_events: {e}")
        except Exception:
            pass

async def m007_add_processed_zaps_table(db: Database):
    """Add table to track processed zap events."""
    try:
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS cyberherd.processed_zaps (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT NOT NULL,
                event_id TEXT NOT NULL,
                note_id TEXT,
                zapper_pubkey TEXT,
                amount INTEGER DEFAULT 0,
                processed_at TEXT NOT NULL,
                UNIQUE(user_id, event_id)
            );
            """
        )

        # Add index for faster lookups
        await db.execute(
            "CREATE INDEX IF NOT EXISTS idx_processed_zaps_user_event ON cyberherd.processed_zaps (user_id, event_id);"
        )

        await db.execute(
            "CREATE INDEX IF NOT EXISTS idx_processed_zaps_user_zapper ON cyberherd.processed_zaps (user_id, zapper_pubkey);"
        )
    except Exception as e:
        # Log the error but don't fail the migration completely
        import logging

        logger = logging.getLogger(__name__)
        logger.warning(f"Error creating processed_zaps table: {e}")


async def m015_ensure_processed_zaps(db: Database):
    """Final safeguard migration ensuring processed_zaps exists with indices.

    Some deployments may have missed earlier idempotent creation due to
    numbering confusion. This repeats the DDL with IF NOT EXISTS guards.
    """
    try:
        await m007_add_processed_zaps_table(db)
    except Exception as e:
        try:
            logger.debug(f"Cyberherd migration m015 ensure failed: {e}")
        except Exception:
            pass


async def m016_add_templates_owner_user(db: Database):
    """Add templates_owner_user column to settings (optional feature ownership linkage).

    Idempotent: ignores error if column already exists. Ensures settings table first.
    """
    try:
        await _ensure_settings_table(db)
        await db.execute("ALTER TABLE cyberherd.settings ADD COLUMN templates_owner_user TEXT;")
        try:
            logger.info("Cyberherd migration: added templates_owner_user column")
        except Exception:
            pass
    except Exception:
        # Likely column already exists; ignore
        pass


async def _ensure_settings_table(db: Database):
    """Idempotently create schema and base settings table if missing.

    Handles cases where earlier migrations failed and later ones re-run first.
    """
    try:
        await db.execute("CREATE SCHEMA IF NOT EXISTS cyberherd;")
    except Exception:
        pass
    try:
        await db.execute(
            """CREATE TABLE IF NOT EXISTS cyberherd.settings (
                source_wallet TEXT,
                max_members INTEGER DEFAULT 3,
                tracked_tags TEXT DEFAULT '[]'
            );"""
        )
    except Exception:
        pass


async def m017_add_repost_tracking_enabled(db: Database):
    """Add repost_tracking_enabled column to settings table."""
    await db.execute("ALTER TABLE cyberherd.settings ADD COLUMN repost_tracking_enabled INTEGER DEFAULT 0;")


async def m018_add_manual_event_ids(db: Database):
    """Add manual_event_ids column to settings table."""
    await db.execute("ALTER TABLE cyberherd.settings ADD COLUMN manual_event_ids TEXT;")


async def m019_add_minimum_sats(db: Database):
    """Add minimum_sats column to settings table."""
    await db.execute("ALTER TABLE cyberherd.settings ADD COLUMN minimum_sats INTEGER DEFAULT 10;")


async def m020_add_user_id_to_cyber_herd(db: Database):
    """Add user_id column to cyber_herd table for multi-user support.
    
    This enables proper user isolation so each user has their own set of members.
    Existing data will have NULL user_id and won't be visible until users 
    reconfigure their herds.
    """
    try:
        await db.execute("ALTER TABLE cyberherd.cyber_herd ADD COLUMN user_id TEXT;")
        logger.info("Cyberherd migration: added user_id column to cyber_herd table")
    except Exception as e:
        # Column may already exist; ignore
        logger.debug(f"Cyberherd migration m020: {e}")
    
    # Add index for performance on user_id queries
    try:
        await db.execute(
            "CREATE INDEX IF NOT EXISTS idx_cyber_herd_user_id ON cyberherd.cyber_herd(user_id);"
        )
        logger.info("Cyberherd migration: added index on cyber_herd.user_id")
    except Exception as e:
        logger.debug(f"Cyberherd migration m020 index: {e}")
    
    # Add index for common query pattern: active members for a user
    try:
        await db.execute(
            "CREATE INDEX IF NOT EXISTS idx_cyber_herd_user_active ON cyberherd.cyber_herd(user_id, is_active);"
        )
        logger.info("Cyberherd migration: added index on cyber_herd(user_id, is_active)")
    except Exception as e:
        logger.debug(f"Cyberherd migration m020 active index: {e}")
