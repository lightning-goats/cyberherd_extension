"""Membership regressions; enable PG schemas with CYBERHERD_TEST_POSTGRES=1."""

# Only generated test schema identifiers are interpolated.
# ruff: noqa: S608

import os
from types import SimpleNamespace
from uuid import uuid4

import pytest

import lnbits.db as db_module
from lnbits.core import helpers
from lnbits.db import POSTGRES, SQLITE, Database
from lnbits.extensions.cyberherd import crud, migrations
from lnbits.settings import settings


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture(params=[SQLITE, POSTGRES])
async def member_db(request, monkeypatch, tmp_path):
    backend = request.param
    if backend == POSTGRES and not os.environ.get("CYBERHERD_TEST_POSTGRES"):
        pytest.skip(
            "set CYBERHERD_TEST_POSTGRES=1 to use disposable PostgreSQL schemas"
        )
    postgres_url = settings.lnbits_database_url
    if backend == POSTGRES and not postgres_url:
        pytest.skip("PostgreSQL URL is not configured")
    monkeypatch.setattr(db_module, "DB_TYPE", backend)
    monkeypatch.setattr(
        settings, "lnbits_database_url", postgres_url if backend == POSTGRES else None
    )
    monkeypatch.setattr(settings, "lnbits_data_folder", str(tmp_path))
    database = Database(f"ext_cyberherd_test_{uuid4().hex}")
    monkeypatch.setattr(crud, "db", database)
    monkeypatch.setattr(
        helpers, "core_db", SimpleNamespace(connect=lambda: database.reuse_conn(None))
    )
    try:
        yield database
    finally:
        if backend == POSTGRES:
            await database.execute(f"DROP SCHEMA IF EXISTS {database.schema} CASCADE")
        await database.engine.dispose()


async def create_members(database, primary_key):
    await database.execute(
        f"CREATE TABLE {database.references_schema}settings (user_id TEXT PRIMARY KEY)"
    )
    await database.execute(f"""CREATE TABLE {database.references_schema}cyber_herd (
            pubkey TEXT NOT NULL, user_id TEXT, display_name TEXT,
            event_id TEXT, note TEXT, kinds TEXT, nprofile TEXT, lud16 TEXT,
            nip05 TEXT, notified INTEGER DEFAULT 0, payouts REAL DEFAULT 0,
            amount INTEGER DEFAULT 0, picture TEXT, relays TEXT,
            metadata_last_checked_at INTEGER, is_active INTEGER DEFAULT 0,
            banned INTEGER DEFAULT 0, PRIMARY KEY ({primary_key})
        )""")
    await database.execute(f"""INSERT INTO {database.references_schema}cyber_herd
            (pubkey, user_id, display_name, kinds, amount, payouts, notified, banned)
            VALUES ('existing', 'owner', 'Existing member', '9735', 42, 1.5, 1, 1)""")


async def migrate_from_35(database, monkeypatch):
    versions = []

    async def record_version(conn, name, version):
        assert name == "cyberherd"
        versions.append(version)

    monkeypatch.setattr(helpers, "update_migration_version", record_version)
    async with database.connect() as connection:
        await helpers.run_migration(
            connection, migrations, "cyberherd", type("Version", (), {"version": 35})()
        )
    # Column type upgrades invalidate asyncpg prepared SELECT metadata. A real
    # extension upgrade starts with fresh connections; mirror that boundary.
    await database.engine.dispose()
    return versions


@pytest.mark.anyio
@pytest.mark.parametrize("kind", [6, 7])
async def test_members_join_after_version_35_key_repair(member_db, monkeypatch, kind):
    # SQLite installs already completed m035; the broken upgrade is PostgreSQL.
    key = "pubkey" if member_db.type == POSTGRES else "user_id, pubkey"
    await create_members(member_db, key)
    before = dict(
        await member_db.fetchone(
            f"SELECT * FROM {member_db.references_schema}cyber_herd"
        )
    )

    versions = await migrate_from_35(member_db, monkeypatch)
    after = dict(
        await member_db.fetchone(
            f"SELECT * FROM {member_db.references_schema}cyber_herd"
        )
    )
    assert after == before

    member = {"pubkey": "a" * 64, "kinds": [kind], "nprofile": "test-profile"}
    await crud.add_new_active_member(member, user_id="owner")
    await crud.add_new_active_member(member, user_id="other-owner")
    # Replay updates the same row; the same pubkey can join another user's herd.
    await crud.add_new_active_member(member, user_id="owner")
    rows = await member_db.fetchall(
        "SELECT user_id, kinds, is_active "
        f"FROM {member_db.references_schema}cyber_herd "
        "WHERE pubkey = :pubkey ORDER BY user_id",
        {"pubkey": "a" * 64},
    )
    assert [dict(r) for r in rows] == [
        {"user_id": "other-owner", "kinds": str(kind), "is_active": 1},
        {"user_id": "owner", "kinds": str(kind), "is_active": 1},
    ]
    assert versions == [36, 37, 38]


@pytest.mark.anyio
async def test_key_repair_is_idempotent(member_db, monkeypatch):
    await create_members(member_db, "user_id, pubkey")
    before = dict(
        await member_db.fetchone(
            f"SELECT * FROM {member_db.references_schema}cyber_herd"
        )
    )
    for _ in range(2):
        assert await migrate_from_35(member_db, monkeypatch) == [36, 37, 38]
        assert (
            dict(
                await member_db.fetchone(
                    f"SELECT * FROM {member_db.references_schema}cyber_herd"
                )
            )
            == before
        )


@pytest.mark.anyio
async def test_key_repair_failure_keeps_old_key_and_does_not_advance_version(
    member_db, monkeypatch
):
    if member_db.type != POSTGRES:
        pytest.skip("PostgreSQL repair only")
    await create_members(member_db, "pubkey")
    await member_db.execute(
        f"UPDATE {member_db.references_schema}cyber_herd SET user_id = NULL"
    )
    recorded = []

    async def record_version(*args):
        recorded.append(args)

    monkeypatch.setattr(helpers, "update_migration_version", record_version)
    with pytest.raises(Exception, match="null"):
        async with member_db.connect() as connection:
            await helpers.run_migration(
                connection,
                migrations,
                "cyberherd",
                type("Version", (), {"version": 35})(),
            )
    assert recorded == []
    row = await member_db.fetchone(
        "SELECT pg_get_constraintdef(oid) AS definition FROM pg_constraint "
        "WHERE conrelid = CAST(:table_name AS regclass) AND contype = 'p'",
        {"table_name": f"{member_db.references_schema}cyber_herd"},
    )
    assert row["definition"] == "PRIMARY KEY (pubkey)"
    assert (
        await member_db.fetchone(
            f"SELECT user_id FROM {member_db.references_schema}cyber_herd"
        )
    )["user_id"] is None
