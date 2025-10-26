"""Diagnostics and safe migration helpers for CyberHerd legacy rows.

Usage:
    # Report-only (default)
    python -m lnbits.extensions.cyberherd.diagnostics_migrate_legacy --report

    # Migrate legacy rows to a target user_id (requires confirmation)
    python -m lnbits.extensions.cyberherd.diagnostics_migrate_legacy --migrate --target-user <user_id> [--yes]

Notes:
- This script is intentionally conservative: report-only by default and requires
  explicit --migrate + --target-user to perform changes.
- Works for both SQLite and PostgreSQL via the project's Database wrapper.
- It only migrates rows that have NULL or empty user_id. It will not overwrite
  existing user-scoped rows.
- Always back up your DB before running migrations in production.
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import List

from lnbits.db import Database
from loguru import logger

DB = Database("ext_cyberherd")


async def _fetch_legacy_settings_rows():
    # Rows where user_id IS NULL or empty string
    try:
        rows = await DB.fetchall(
            f"SELECT rowid AS id, * FROM {DB.references_schema}settings WHERE user_id IS NULL OR TRIM(user_id) = ''"
        )
    except Exception:
        # SQLite/Postgres differences: try without schema prefix
        try:
            rows = await DB.fetchall("SELECT rowid AS id, * FROM settings WHERE user_id IS NULL OR TRIM(user_id) = ''")
        except Exception as e:
            logger.error(f"Error fetching legacy settings rows: {e}")
            return []
    return rows or []


async def _fetch_legacy_herd_rows():
    try:
        rows = await DB.fetchall(
            f"SELECT rowid AS id, * FROM {DB.references_schema}cyber_herd WHERE user_id IS NULL OR TRIM(user_id) = ''"
        )
    except Exception:
        try:
            rows = await DB.fetchall("SELECT rowid AS id, * FROM cyber_herd WHERE user_id IS NULL OR TRIM(user_id) = ''")
        except Exception as e:
            logger.error(f"Error fetching legacy cyber_herd rows: {e}")
            return []
    return rows or []


async def report():
    settings_rows = await _fetch_legacy_settings_rows()
    herd_rows = await _fetch_legacy_herd_rows()

    print("\nCyberHerd legacy diagnostics report:\n")
    print(f"Legacy settings rows (user_id NULL/empty): {len(settings_rows)}")
    for r in settings_rows[:20]:
        print(json.dumps({k: r.get(k) for k in r.keys() if k != 'nostr_private_key'}, default=str))
    if len(settings_rows) > 20:
        print(f"... and {len(settings_rows)-20} more")

    print(f"\nLegacy cyber_herd rows (user_id NULL/empty): {len(herd_rows)}")
    for r in herd_rows[:50]:
        print(json.dumps(r, default=str))
    if len(herd_rows) > 50:
        print(f"... and {len(herd_rows)-50} more")

    if not settings_rows and not herd_rows:
        print("\nNo legacy rows detected.")
    else:
        print("\nRecommendation: If these rows belong to a specific user, run with --migrate --target-user <user_id> to migrate them. Always backup DB first.")


async def migrate(target_user: str, yes: bool = False):
    settings_rows = await _fetch_legacy_settings_rows()
    herd_rows = await _fetch_legacy_herd_rows()

    total = len(settings_rows) + len(herd_rows)
    if total == 0:
        print("No legacy rows detected; nothing to migrate.")
        return

    print(f"About to migrate {len(settings_rows)} settings rows and {len(herd_rows)} herd rows to user_id={target_user}")
    if not yes:
        resp = input("Proceed? Type 'yes' to continue: ")
        if resp.strip().lower() != "yes":
            print("Aborting.")
            return

    # Migrate settings rows
    migrated_settings = 0
    for r in settings_rows:
        # For safety, don't migrate if any non-empty user_id already exists for that identifying source_wallet
        sw = r.get("source_wallet")
        if sw:
            existing = await DB.fetchone(f"SELECT 1 FROM {DB.references_schema}settings WHERE user_id = :uid", {"uid": target_user})
            # don't overwrite other user's settings; if target_user already has a row, skip
            if existing:
                print(f"Skipping settings row id={r.get('id')} because target user already has a settings row")
                continue
        try:
            await DB.execute(
                f"UPDATE {DB.references_schema}settings SET user_id = :uid WHERE (user_id IS NULL OR TRIM(user_id) = '') AND rowid = :rid",
                {"uid": target_user, "rid": r.get("id")},
            )
            migrated_settings += 1
        except Exception:
            # try without schema
            try:
                await DB.execute(
                    "UPDATE settings SET user_id = :uid WHERE (user_id IS NULL OR TRIM(user_id) = '') AND rowid = :rid",
                    {"uid": target_user, "rid": r.get("id")},
                )
                migrated_settings += 1
            except Exception as e:
                logger.error(f"Failed migrating settings row id={r.get('id')}: {e}")

    migrated_herd = 0
    for r in herd_rows:
        try:
            await DB.execute(
                f"UPDATE {DB.references_schema}cyber_herd SET user_id = :uid WHERE (user_id IS NULL OR TRIM(user_id) = '') AND rowid = :rid",
                {"uid": target_user, "rid": r.get("id")},
            )
            migrated_herd += 1
        except Exception:
            try:
                await DB.execute(
                    "UPDATE cyber_herd SET user_id = :uid WHERE (user_id IS NULL OR TRIM(user_id) = '') AND rowid = :rid",
                    {"uid": target_user, "rid": r.get("id")},
                )
                migrated_herd += 1
            except Exception as e:
                logger.error(f"Failed migrating cyber_herd row id={r.get('id')}: {e}")

    print(f"Migration complete: settings migrated={migrated_settings}, herd migrated={migrated_herd}")
    print("Note: you may need to run extension migrations or restart services for application-level changes to be visible.")


def _parse_args():
    p = argparse.ArgumentParser(description="CyberHerd diagnostics and legacy migration helper")
    p.add_argument("--report", action="store_true", help="Report legacy rows (default if no action)")
    p.add_argument("--migrate", action="store_true", help="Migrate legacy rows to a target user_id")
    p.add_argument("--target-user", type=str, help="User ID to migrate legacy rows to")
    p.add_argument("--yes", action="store_true", help="Assume yes for migrations")
    return p.parse_args()


async def main(argv: List[str] | None = None):
    args = _parse_args()
    if args.migrate and not args.target_user:
        print("--migrate requires --target-user <user_id>")
        return
    if args.migrate:
        await migrate(args.target_user, yes=args.yes)
    else:
        await report()


if __name__ == "__main__":
    import asyncio

    asyncio.get_event_loop().run_until_complete(main())
