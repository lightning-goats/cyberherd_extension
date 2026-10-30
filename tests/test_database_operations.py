"""Database contracts against disposable schemas/files, never live herd rows."""

import asyncio
import os
from types import SimpleNamespace
from uuid import uuid4

import pytest

import lnbits.db as db_module
from lnbits.db import POSTGRES, SQLITE, Database
from lnbits.extensions.cyberherd import crud, migrations
from lnbits.extensions.cyberherd.models import CyberherdSettings, LegacyCyberherdMember
from lnbits.extensions.cyberherd.services import splits
from lnbits.settings import settings

# Identifiers below are generated test schema/table names.
# ruff: noqa: S608


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture(params=[SQLITE, POSTGRES])
async def audit_db(request, monkeypatch, tmp_path):
    backend = request.param
    postgres_url = settings.lnbits_database_url
    if backend == POSTGRES and not (
        os.environ.get("CYBERHERD_TEST_POSTGRES") and postgres_url
    ):
        pytest.skip(
            "enable disposable PostgreSQL schemas with CYBERHERD_TEST_POSTGRES=1"
        )
    monkeypatch.setattr(db_module, "DB_TYPE", backend)
    monkeypatch.setattr(
        settings, "lnbits_database_url", postgres_url if backend == POSTGRES else None
    )
    monkeypatch.setattr(settings, "lnbits_data_folder", str(tmp_path))
    database = Database(f"ext_db_audit_{uuid4().hex}")
    monkeypatch.setattr(crud, "db", database)
    monkeypatch.setattr(crud, "_crud_write_lock", None)

    async def no_external_writes(*args, **kwargs):
        return None

    for name in (
        "schedule_split_recompute",
        "reset_splits_to_predefined_wallet",
        "update_split_targets_proportional",
    ):
        monkeypatch.setattr(splits, name, no_external_writes)
    try:
        for name in migrations.__all__:
            await getattr(migrations, name)(database)
        yield database
    finally:
        if backend == POSTGRES:
            await database.execute(f"DROP SCHEMA IF EXISTS {database.schema} CASCADE")
        await database.engine.dispose()


async def save_settings(owner="owner", **overrides):
    config = CyberherdSettings(user_id=owner, **overrides)
    await crud.upsert_settings(config, owner)
    return await crud.get_settings(owner)


async def add_member(owner="owner", pubkey="a" * 64, **overrides):
    await crud.add_new_active_member(
        {
            "pubkey": pubkey,
            "lud16": "member@example.test",
            "kinds": [9735],
            "amount": 21,
            **overrides,
        },
        owner,
    )
    return await crud.get_cyberherd_member_by_pubkey(pubkey, owner)


@pytest.mark.anyio
async def test_settings_round_trip_and_scope(audit_db):
    first = await save_settings(
        send_splits_enabled=True,
        member_allocation_percent=0,
        midnight_reset_enabled=False,
        nip05_verification_enabled=False,
        tracked_event_ids=["e" * 64],
        tracked_event_timestamps={"e" * 64: 123},
        tracked_event_addresses={"e" * 64: "30311:test:note"},
    )
    assert first.send_splits_enabled is True
    assert first.member_allocation_percent == 0
    assert first.midnight_reset_enabled is False
    assert first.nip05_verification_enabled is False
    assert first.tracked_event_ids == ["e" * 64]
    assert first.tracked_event_timestamps == {"e" * 64: 123}
    await save_settings("other", herd_wallet="other-herd", source_wallet="other-source")
    assert (
        await crud.get_settings_for_source_wallet("other-source")
    ).user_id == "other"
    assert (await crud.get_settings_for_herd_wallet("other-herd")).user_id == "other"
    assert (await crud.get_settings("owner")).herd_wallet is None
    assert len(await crud.get_all_settings()) == 2
    assert len(await crud.list_settings_with_tracking()) == 1
    await save_settings(send_splits_enabled=False, member_allocation_percent=100)
    assert (await crud.get_settings("owner")).send_splits_enabled is False


@pytest.mark.anyio
async def test_settings_concurrent_first_write(audit_db):
    await asyncio.gather(*(save_settings(max_members=i + 1) for i in range(5)))
    assert len(await crud.get_all_settings()) == 1


@pytest.mark.anyio
async def test_column_checks_and_missing_settings_do_not_abort_transaction(audit_db):
    async with crud.admission_transaction():
        assert await crud._column_exists("settings", "send_splits_enabled")
        assert not await crud._column_exists("settings", "missing_audit_column")
        assert (await crud.get_settings("missing-owner")).user_id is None
        await crud.ensure_inactive_cyberherd_member("a" * 64, "owner")
    assert await crud.get_member_by_pubkey("owner", "a" * 64)


@pytest.mark.anyio
async def test_legacy_member_helpers(audit_db):
    await crud.add_member(LegacyCyberherdMember(pubkey="legacy", alias="Original"))
    await crud.update_member("legacy", "Updated")
    rows = await crud.get_members()
    assert len(rows) == 1 and rows[0].alias == "Updated" and rows[0].added_at
    await crud.delete_member("legacy")
    assert await crud.get_members() == []


@pytest.mark.anyio
async def test_membership_mutations_are_user_scoped(audit_db):
    await save_settings()
    await save_settings("other")
    await add_member()
    await add_member("other")
    assert await crud.get_active_cyberherd_size("owner") == 1
    await crud.set_member_notified("a" * 64, 123, "owner")
    assert (await crud.get_member_by_pubkey("owner", "a" * 64))["notified"] == 123
    await crud.clear_member_notified("a" * 64, "owner")
    assert await crud.set_member_ban_status("a" * 64, True, "owner")
    assert await crud.is_pubkey_banned("a" * 64, "owner")
    assert await crud.get_active_cyberherd_size("owner") == 0
    assert await crud.get_active_cyberherd_size("other") == 1
    await crud.set_member_ban_status("a" * 64, False, "owner")
    await crud.update_and_activate_member("a" * 64, 2, "owner", kinds=[6])
    member = await crud.get_member_by_pubkey("owner", "a" * 64)
    assert member["amount"] == 23 and member["kinds"] == "6,9735"
    await crud.deactivate_cyberherd_member("a" * 64, "owner")
    assert (await crud.get_member_by_pubkey("owner", "a" * 64))["amount"] == 0
    await crud.delete_cyberherd_member_by_pubkey("a" * 64, "owner")
    assert await crud.get_all_cyberherd_members("owner") == []
    assert len(await crud.get_all_cyberherd_members("other")) == 1


@pytest.mark.anyio
async def test_payout_recompute_uses_updated_kinds(audit_db):
    await save_settings(zap_wallet="goats", member_allocation_percent=10)
    await add_member(amount=100)
    await add_member(pubkey="b" * 64, amount=100)
    await crud.update_and_activate_member("a" * 64, 0, "owner", kinds=[6])
    alice = await crud.get_member_by_pubkey("owner", "a" * 64)
    bob = await crud.get_member_by_pubkey("owner", "b" * 64)
    assert alice["payouts"] == pytest.approx(0.05073892)
    assert bob["payouts"] == pytest.approx(0.04926108)


@pytest.mark.anyio
async def test_swallowed_database_error_cannot_commit_partial_membership(audit_db):
    await save_settings()
    callbacks = []
    with pytest.raises(Exception, match="missing_audit_column"):
        async with crud.admission_transaction():
            await add_member()
            crud.defer_after_commit(lambda: callbacks.append("published"))
            try:
                await crud._db().fetchone(
                    f"SELECT missing_audit_column FROM {audit_db.references_schema}settings"
                )
            except Exception:
                pass
    assert await crud.get_active_cyberherd_size("owner") == 0
    assert callbacks == []


@pytest.mark.anyio
async def test_processed_claim_scope_and_rollback(audit_db):
    assert await crud.register_processed_event("owner", "event", note_id=None)
    async with crud.admission_transaction():
        assert not await crud.register_processed_event("owner", "event", note_id="")
        assert await crud.register_processed_event(
            "owner", "event", note_id="other-note"
        )
        assert await crud.register_processed_event("other", "event", note_id="")
    assert await crud.is_event_processed(
        "owner", "event", note_id="other-note", strict=True
    )
    await crud.delete_processed_event("owner", "event", note_id="other-note")
    assert not await crud.is_event_processed(
        "owner", "event", note_id="other-note", strict=True
    )
    with pytest.raises(RuntimeError, match="rollback"):
        async with crud.admission_transaction():
            assert await crud.register_processed_event("owner", "retry")
            raise RuntimeError("rollback")
    assert not await crud.is_event_processed("owner", "retry", strict=True)
    assert await crud.register_processed_event("owner", "retry")


@pytest.mark.anyio
async def test_processed_zap_creation_and_exact_pagination(audit_db):
    await crud.create_processed_zap(
        {
            "user_id": "owner",
            "event_id": "old",
            "processed_at": "2026-01-01T00:00:00Z",
            "amount": 2,
        }
    )
    assert await crud.get_processed_zap("owner", "old")
    for i in range(5):
        assert await crud.account_zap_payment(
            user_id="owner",
            payment_hash=f"hash-{i}",
            zapper_pubkey="a" * 64,
            amount_sats=21,
            processed_at=1793370000 + i,
        )
    assert not await crud.account_zap_payment(
        user_id="owner", payment_hash="hash-0", zapper_pubkey="a" * 64, amount_sats=21
    )
    rows, more = await crud.fetch_processed_zaps("owner", limit=2)
    assert [row["event_id"] for row in rows] == ["hash-4", "hash-3"]
    assert more is True
    assert await crud.count_processed_zaps("owner") == 6
    await crud.clear_processed_zaps_for_user("other")
    assert await crud.count_processed_zaps("owner") == 6


@pytest.mark.anyio
async def test_claims_from_independent_connections_are_atomic(audit_db):
    other = Database(audit_db.name)
    query = f"""INSERT INTO {audit_db.references_schema}processed_events
        (user_id,event_hash,note_id,processed_at)
        VALUES ('owner','concurrent','',123) ON CONFLICT DO NOTHING"""
    try:
        results = await asyncio.gather(audit_db.execute(query), other.execute(query))
        assert sorted(result.rowcount for result in results) == [0, 1]
    finally:
        await other.engine.dispose()


@pytest.mark.anyio
async def test_legacy_pair_key_upgrade_preserves_rows(audit_db):
    table = f"{audit_db.references_schema}processed_events"
    await audit_db.execute(f"DROP TABLE {table}")
    await audit_db.execute(f"""CREATE TABLE {table} (
        user_id TEXT NOT NULL,event_hash TEXT NOT NULL,note_id TEXT,
        pubkey TEXT,processed_at INTEGER NOT NULL,event_type TEXT,
        UNIQUE(user_id,event_hash))""")
    await audit_db.execute(
        f"INSERT INTO {table} VALUES ('owner','old',NULL,'alice',123,'repost')"
    )
    await migrations.m034_fix_processed_events_unique_constraint(audit_db)
    assert await crud.is_event_processed("owner", "old", note_id="", strict=True)
    assert await crud.register_processed_event("owner", "old", note_id="new-note")
    assert not await crud.register_processed_event("owner", "old", note_id="")
    assert await crud.get_monitoring_totals("owner") == {
        "zap": 0,
        "repost": 1,
        "reaction": 0,
    }


@pytest.mark.anyio
async def test_migration_failure_preserves_old_table_and_is_retryable(
    audit_db, monkeypatch
):
    if audit_db.type != SQLITE:
        pytest.skip("SQLite table-swap path")
    await audit_db.execute("DROP TABLE processed_events")
    await audit_db.execute("""CREATE TABLE processed_events (
        user_id TEXT,event_hash TEXT,note_id TEXT,pubkey TEXT,
        processed_at INTEGER,event_type TEXT,UNIQUE(user_id,event_hash))""")
    await audit_db.execute(
        "INSERT INTO processed_events VALUES ('owner','old','','alice',123,'repost')"
    )
    original = migrations._MigrationConnection.execute

    async def fail_rename(self, query, values=None):
        if "RENAME TO processed_events" in query:
            raise RuntimeError("injected rename failure")
        return await original(self, query, values)

    with monkeypatch.context() as patch:
        patch.setattr(migrations._MigrationConnection, "execute", fail_rename)
        with pytest.raises(RuntimeError, match="injected"):
            await migrations.m034_fix_processed_events_unique_constraint(audit_db)
    assert await crud.is_event_processed("owner", "old", strict=True)
    await migrations.m034_fix_processed_events_unique_constraint(audit_db)
    assert await crud.register_processed_event("owner", "old", note_id="new-note")


@pytest.mark.anyio
async def test_legacy_wallet_owner_migration_uses_core_wallet_lookup(
    audit_db, monkeypatch
):
    table = f"{audit_db.references_schema}settings"
    await audit_db.execute(
        f"INSERT INTO {table} (source_wallet) VALUES ('legacy-source')"
    )

    async def wallet(wallet_id):
        assert wallet_id == "legacy-source"
        return SimpleNamespace(user="legacy-owner")

    monkeypatch.setattr("lnbits.core.crud.wallets.get_wallet", wallet)
    await migrations.m025_migrate_legacy_userids(audit_db)
    await migrations.m025_migrate_legacy_userids(audit_db)
    assert (await crud.get_settings("legacy-owner")).source_wallet == "legacy-source"


@pytest.mark.anyio
async def test_normalized_pubkey_mutations_match_reads(audit_db):
    await save_settings()
    await add_member()
    await crud.set_member_notified(" A" + "A" * 63 + " ", 123, "owner")
    assert (await crud.get_member_by_pubkey("owner", "a" * 64))["notified"] == 123
    await crud.clear_member_notified("A" * 64, "owner")
    await crud.deactivate_cyberherd_member("A" * 64, "owner")
    assert await crud.get_active_cyberherd_size("owner") == 0
    await crud.delete_cyberherd_member_by_pubkey("A" * 64, "owner")
    assert await crud.get_all_cyberherd_members("owner") == []


@pytest.mark.anyio
async def test_prune_and_bulk_deactivation_preserve_other_tenants(audit_db):
    await save_settings()
    await save_settings("other")
    await add_member(lud16="")
    await add_member("other", lud16="")
    assert await crud.delete_cyberherd_members_without_lud16("owner") == 1
    await add_member()
    assert await crud.deactivate_all_cyberherd_members("owner") == 1
    assert await crud.get_active_cyberherd_size("other") == 1
    assert await crud.get_active_cyberherd_size("owner") == 0


@pytest.mark.anyio
async def test_child_task_does_not_share_transaction(audit_db):
    await save_settings()
    child = None
    with pytest.raises(RuntimeError, match="rollback"):
        async with crud.admission_transaction():
            await add_member()
            child = asyncio.create_task(crud.get_active_cyberherd_size("owner"))
            await asyncio.sleep(0)
            assert not child.done()
            raise RuntimeError("rollback")
    assert await asyncio.wait_for(child, timeout=5) == 0


@pytest.mark.anyio
async def test_unconfigured_tenant_cannot_inherit_legacy_wallets(audit_db):
    await audit_db.execute(
        f"INSERT INTO {audit_db.references_schema}settings (source_wallet) VALUES ('legacy-source')"
    )
    assert (await crud.get_settings()).source_wallet == "legacy-source"
    assert (await crud.get_settings("unconfigured-owner")).source_wallet is None


@pytest.mark.anyio
async def test_settings_split_work_is_deferred_until_commit(audit_db, monkeypatch):
    calls = []

    async def record(*args, **kwargs):
        calls.append(kwargs.get("user_id"))

    monkeypatch.setattr(splits, "reset_splits_to_predefined_wallet", record)
    with pytest.raises(RuntimeError, match="rollback"):
        async with crud.admission_transaction():
            await save_settings(source_wallet="source", zap_wallet="goats")
            assert calls == []
            raise RuntimeError("rollback")
    assert calls == []
    async with crud.admission_transaction():
        await save_settings(source_wallet="source", zap_wallet="goats")
        assert calls == []
    assert calls == ["owner"]


@pytest.mark.anyio
async def test_manual_reset_is_atomic_and_clears_totals_with_the_ledger(
    audit_db, monkeypatch
):
    from lnbits.extensions.cyberherd import tasks
    from lnbits.extensions.cyberherd.services import headbutt

    monkeypatch.setattr(headbutt, "_HEADBUTT_LOCK", None)
    monkeypatch.setattr(tasks, "_membership_day_lock", asyncio.Lock())

    async def no_external_work(*args, **kwargs):
        return None

    monkeypatch.setattr(tasks, "clear_monitor_cache_for_user", no_external_work)
    await save_settings(tracked_event_ids=["note"])
    await save_settings("other")
    await add_member()
    await add_member("other")
    await crud.register_processed_event("owner", "hash")
    await crud.account_zap_payment(
        user_id="owner", payment_hash="hash", zapper_pubkey="a" * 64, amount_sats=21
    )
    await crud.upsert_zap_totals_row(
        user_id="owner",
        zapper_pubkey="a" * 64,
        total_sats=21,
        event_count=1,
        first_event_at=123,
        last_event_at=123,
        last_event_ids=["hash"],
        last_updated_at=123,
        target_pubkey="f" * 64,
    )
    original = crud._AdmissionConnection.execute

    async def fail_settings(self, query, values=None):
        if "UPDATE" in query and "settings SET tracked_event_ids" in query:
            raise RuntimeError("injected reset failure")
        return await original(self, query, values)

    with monkeypatch.context() as patch:
        patch.setattr(crud._AdmissionConnection, "execute", fail_settings)
        with pytest.raises(RuntimeError, match="injected reset"):
            await tasks.manual_reset_membership("owner")
    assert await crud.get_active_cyberherd_size("owner") == 1
    assert await crud.is_payment_processed("owner", "hash", strict=True)
    assert await crud.count_processed_zaps("owner") == 1
    assert (await crud.get_zap_totals_row("owner", "a" * 64))["total_sats"] == 21
    await tasks.manual_reset_membership("owner")
    assert await crud.get_active_cyberherd_size("owner") == 0
    assert await crud.get_active_cyberherd_size("other") == 1
    assert not await crud.is_payment_processed("owner", "hash", strict=True)
    assert await crud.count_processed_zaps("owner") == 0
    assert await crud.get_zap_totals_row("owner", "a" * 64) is None
    assert (await crud.get_settings("owner")).tracked_event_ids == []


@pytest.mark.anyio
async def test_daily_reset_preserves_accounting_and_retries_pending_splits(
    audit_db, monkeypatch
):
    from lnbits.extensions.cyberherd import tasks
    from lnbits.extensions.cyberherd.services import headbutt
    from lnbits.extensions.cyberherd.services.time_utils import get_day_boundaries_utc

    monkeypatch.setattr(headbutt, "_HEADBUTT_LOCK", None)
    monkeypatch.setattr(tasks, "_membership_day_lock", asyncio.Lock())
    calls = []

    async def no_external_work(*args, **kwargs):
        return None

    async def reset_splits(*args, **kwargs):
        calls.append(kwargs.get("user_id"))
        if len(calls) == 1:
            raise RuntimeError("split storage unavailable")

    monkeypatch.setattr(tasks, "clear_monitor_cache_for_user", no_external_work)
    monkeypatch.setattr(tasks, "reset_splits_to_predefined_wallet", reset_splits)
    await save_settings(source_wallet="source", midnight_reset_enabled=True)
    await add_member()
    await crud.register_processed_event("owner", "hash")
    table = f"{audit_db.references_schema}membership_reset_days"
    await audit_db.execute(
        f"INSERT INTO {table} (user_id,local_day) VALUES ('owner','2000-01-01')"
    )
    with pytest.raises(RuntimeError, match="split storage unavailable"):
        await tasks.ensure_membership_day("owner")
    assert await crud.get_active_cyberherd_size("owner") == 0
    assert await crud.is_event_processed("owner", "hash", strict=True)
    marker = await audit_db.fetchone(f"SELECT * FROM {table} WHERE user_id='owner'")
    assert marker["local_day"] == get_day_boundaries_utc().local_day_str
    assert marker["splits_pending"] == 1
    await tasks.ensure_membership_day("owner")
    marker = await audit_db.fetchone(f"SELECT * FROM {table} WHERE user_id='owner'")
    assert marker["splits_pending"] == 0
    assert calls == ["owner", "owner"]


@pytest.mark.anyio
async def test_satoshi_and_timestamp_storage_does_not_truncate(audit_db):
    await save_settings()
    await add_member(amount=3_000_000_000)
    member = await crud.get_member_by_pubkey("owner", "a" * 64)
    assert member["amount"] == 3_000_000_000
    await crud.set_member_notified("a" * 64, 3_000_000_001, "owner")
    assert await crud.account_zap_payment(
        user_id="owner",
        payment_hash="large",
        zapper_pubkey="a" * 64,
        amount_sats=3_000_000_000,
        processed_at=3_000_000_001,
    )
    await crud.upsert_zap_totals_row(
        user_id="owner",
        zapper_pubkey="a" * 64,
        total_sats=3_000_000_000,
        event_count=1,
        first_event_at=3_000_000_001,
        last_event_at=3_000_000_001,
        last_event_ids=["large"],
        last_updated_at=1793370000.123456,
        target_pubkey="f" * 64,
    )
    row = await crud.get_zap_totals_row("owner", "a" * 64)
    assert row["total_sats"] == 3_000_000_000
    assert abs(row["last_updated_at"] - 1793370000.123456) < 0.00001


@pytest.mark.anyio
async def test_scoped_payment_reader_and_websocket_members(audit_db, monkeypatch):
    from lnbits.extensions.cyberherd.services import messaging
    from lnbits.extensions.cyberherd.services.payment import PaymentService

    await save_settings()
    await save_settings("other")
    await add_member()
    await add_member("other", pubkey="b" * 64)
    assert await PaymentService(db=crud)._get_active_members() == []
    rows = await PaymentService(db=crud, user_id="owner")._get_active_members()
    assert [row["pubkey"] for row in rows] == ["a" * 64]
    payloads = []

    async def broadcast(kind, payload, **kwargs):
        payloads.append(payload)

    monkeypatch.setattr(messaging, "send_to_websocket_clients", broadcast)
    await messaging.send_cyberherd_update(db=crud, user_id="owner")
    assert [row["pubkey"] for row in payloads[0]["members"]] == ["a" * 64]


@pytest.mark.anyio
async def test_runtime_bootstrap_is_idempotent_and_never_runs_in_admission(audit_db):
    await save_settings()
    await add_member()
    before = await crud.get_member_by_pubkey("owner", "a" * 64)
    await crud._bootstrap_settings_storage()
    await crud._bootstrap_cyberherd_tables()
    assert await crud.get_member_by_pubkey("owner", "a" * 64) == before
    with pytest.raises(RuntimeError, match="migrated before admission"):
        async with crud.admission_transaction():
            await crud._bootstrap_cyberherd_tables()


@pytest.mark.anyio
async def test_banned_members_cannot_be_reactivated_by_crud(audit_db):
    await save_settings()
    await add_member()
    assert await crud.set_member_ban_status("a" * 64, True, "owner")
    with pytest.raises(ValueError, match="banned"):
        await crud.update_and_activate_member("a" * 64, 1, "owner")
    with pytest.raises(ValueError, match="banned"):
        await add_member()
    assert await crud.get_active_cyberherd_size("owner") == 0
    assert not await crud.set_member_ban_status("missing", True, "owner")


@pytest.mark.anyio
async def test_incremental_zap_history_keeps_the_target_note(audit_db):
    from lnbits.extensions.cyberherd.services import zap_totals

    await save_settings()
    await add_member()
    await zap_totals.record_incremental_zap_total(
        user_id="owner",
        zapper_pubkey="a" * 64,
        target_pubkey="f" * 64,
        amount_sats=21,
        event_id="e" * 64,
        note_id="note",
        event_timestamp=123,
    )
    rows, more = await crud.fetch_processed_zaps("owner", note_id="note")
    assert len(rows) == 1 and rows[0]["amount"] == 21 and not more


@pytest.mark.anyio
async def test_manual_amount_updates_retry_and_deduplicate_per_tenant(
    audit_db, monkeypatch
):
    from fastapi import HTTPException
    from lnbits.extensions.cyberherd import views_api
    from lnbits.extensions.cyberherd.models import MemberPut
    from lnbits.extensions.cyberherd.services import messaging

    await save_settings()
    await save_settings("other")
    await add_member()
    await add_member("other")
    await crud.deactivate_cyberherd_member("a" * 64, "owner")
    monkeypatch.setattr(
        views_api.api_put_member, "_processed_zaps", set(), raising=False
    )

    async def no_messages(*args, **kwargs):
        return True

    async def no_feeder_query(*args, **kwargs):
        return 0

    monkeypatch.setattr(messaging, "publish_event_message", no_messages)
    monkeypatch.setattr(
        views_api, "_compute_feeder_difference_for_user", no_feeder_query
    )
    request = SimpleNamespace(app=SimpleNamespace())
    payload = MemberPut(amount=80, zap_request={"id": "e" * 64})
    owner_wallet = SimpleNamespace(wallet=SimpleNamespace(user="owner"))
    other_wallet = SimpleNamespace(wallet=SimpleNamespace(user="other"))
    original = crud._AdmissionConnection.execute

    async def fail_amount(self, query, values=None):
        if "SET amount =" in query:
            raise RuntimeError("injected amount failure")
        return await original(self, query, values)

    with monkeypatch.context() as patch:
        patch.setattr(crud._AdmissionConnection, "execute", fail_amount)
        with pytest.raises(HTTPException):
            await views_api.api_put_member(request, "a" * 64, payload, owner_wallet)
    member = await crud.get_member_by_pubkey("owner", "a" * 64)
    assert member["is_active"] == 0 and member["amount"] == 0
    assert not views_api.api_put_member._processed_zaps
    await views_api.api_put_member(request, "a" * 64, payload, owner_wallet)
    await views_api.api_put_member(request, "a" * 64, payload, other_wallet)
    assert (await crud.get_member_by_pubkey("owner", "a" * 64))["amount"] == 80
    assert (await crud.get_member_by_pubkey("other", "a" * 64))["amount"] == 80
    # A manual total is an assignment, independent of any earlier increment.
    await crud.update_and_activate_member("a" * 64, 21, "owner")
    await crud.update_and_activate_member("a" * 64, 0, "owner", amount_total=50)
    assert (await crud.get_member_by_pubkey("owner", "a" * 64))["amount"] == 50


@pytest.mark.anyio
async def test_real_migration_runner_records_versions_in_order(audit_db, monkeypatch):
    from lnbits.core import helpers

    recorded = []

    async def record(conn, name, version):
        recorded.append(version)

    monkeypatch.setattr(
        helpers, "core_db", SimpleNamespace(connect=lambda: audit_db.reuse_conn(None))
    )
    monkeypatch.setattr(helpers, "update_migration_version", record)
    async with audit_db.connect() as connection:
        await helpers.run_migration(connection, migrations, "cyberherd")
    assert recorded == sorted(recorded)
    assert recorded[-1] == 44
    assert len(recorded) == len(migrations.__all__)


@pytest.mark.anyio
async def test_failed_schema_change_does_not_advance_migration_version(
    audit_db, monkeypatch
):
    from lnbits.core import helpers
    from sqlalchemy.exc import DBAPIError

    recorded = []

    async def record(*args):
        recorded.append(args)

    async def fail(conn):
        await migrations._add_column_if_missing(
            conn,
            f"{conn.references_schema}settings",
            "audit_bad_column",
            "INTEGER DEFAULT (",
        )

    monkeypatch.setattr(helpers, "update_migration_version", record)
    async with audit_db.connect() as connection:
        with pytest.raises(DBAPIError):
            await helpers.run_migration(
                connection, SimpleNamespace(m042_failed=fail), "cyberherd"
            )
    assert recorded == []
    assert not await crud._column_exists("settings", "audit_bad_column")


@pytest.mark.anyio
async def test_payment_history_rebuild_and_increment_commit_once(audit_db, monkeypatch):
    import hashlib
    import json
    from datetime import datetime, timezone

    import coincurve
    from lnbits.extensions.cyberherd.services import nostr_helpers, zap_totals

    sender = coincurve.PrivateKey()
    sender_pubkey = sender.public_key.format(compressed=True)[1:].hex()
    target_pubkey = coincurve.PrivateKey().public_key.format(compressed=True)[1:].hex()
    note = "e" * 64
    event = {
        "pubkey": sender_pubkey,
        "created_at": 123,
        "kind": 9734,
        "tags": [["p", target_pubkey], ["e", note]],
        "content": "",
    }
    digest = hashlib.sha256(
        json.dumps(
            [
                0,
                event["pubkey"],
                event["created_at"],
                event["kind"],
                event["tags"],
                event["content"],
            ],
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode()
    ).digest()
    event.update(id=digest.hex(), sig=sender.sign_schnorr(digest).hex())
    assert nostr_helpers.verify_event_signature(event)
    await save_settings(
        herd_wallet="herd",
        nostr_pubkey_override=target_pubkey,
        tracked_event_ids=[note],
    )
    await add_member(pubkey=sender_pubkey)

    def payment(number, sats):
        return SimpleNamespace(
            success=True,
            amount=sats * 1000,
            wallet_id="herd",
            extra={"nostr": event},
            payment_hash=f"{number:064x}",
            time=datetime.fromtimestamp(123 + number, timezone.utc),
        )

    first, second = payment(1, 21), payment(2, 42)
    payments = [first, first, second]

    async def history(wallet_id, incoming, complete, filters):
        assert wallet_id == "herd" and incoming and complete
        return payments[filters.offset : filters.offset + filters.limit]

    async def metadata(*args, **kwargs):
        return None

    async def relays(*args, **kwargs):
        return []

    monkeypatch.setattr(zap_totals, "get_payments", history)
    monkeypatch.setattr(nostr_helpers, "lookup_metadata", metadata)
    monkeypatch.setattr(nostr_helpers, "lookup_relays", relays)
    result = await zap_totals.rebuild_zap_totals_from_payments(
        user_id="owner", batch_size=1, all_zaps=True
    )
    assert result["payments_scanned"] == 3 and result["zap_candidates"] == 2
    for identity, sats in (
        (first.payment_hash, 21),
        (f"{3:064x}", 7),
        (f"{3:064x}", 7),
    ):
        await zap_totals.record_incremental_zap_total(
            user_id="owner",
            zapper_pubkey=sender_pubkey,
            target_pubkey=target_pubkey,
            amount_sats=sats,
            event_id=identity,
            note_id=note,
            event_timestamp=126,
        )
    row = await crud.get_zap_totals_row("owner", sender_pubkey)
    assert (row["total_sats"], row["event_count"]) == (70, 3)
    payments.extend([payment(3, 7), payment(4, 5)])

    async def fail_totals(**kwargs):
        raise RuntimeError("injected totals failure")

    with monkeypatch.context() as patch:
        patch.setattr(crud, "upsert_zap_totals_row", fail_totals)
        with pytest.raises(RuntimeError, match="injected totals"):
            await zap_totals.rebuild_zap_totals_from_payments(
                user_id="owner", all_zaps=True
            )
    assert await crud.count_processed_zaps("owner") == 3
    row = await crud.get_zap_totals_row("owner", sender_pubkey)
    assert (row["total_sats"], row["event_count"]) == (70, 3)
    await zap_totals.rebuild_zap_totals_from_payments(user_id="owner", all_zaps=True)
    row = await crud.get_zap_totals_row("owner", sender_pubkey)
    assert (row["total_sats"], row["event_count"]) == (75, 4)


@pytest.mark.anyio
async def test_cleanup_helpers_and_allocation_storage_stay_user_scoped(audit_db):
    await save_settings()
    await save_settings("other")
    await add_member()
    await add_member("other")
    await crud.update_cyberherd_member_allocation("a" * 64, 1.0, "owner")
    assert (await crud.get_member_by_pubkey("owner", "a" * 64))[
        "allocation_percentage"
    ] == 1.0
    assert (await crud.get_member_by_pubkey("other", "a" * 64))[
        "allocation_percentage"
    ] is None
    await crud.register_processed_event("owner", "event")
    await crud.register_processed_event("other", "event")
    await crud.clear_processed_events_for_user("owner")
    assert not await crud.is_event_processed("owner", "event", strict=True)
    assert await crud.is_event_processed("other", "event", strict=True)
    assert len(await crud.get_cyberherd_settings_for_all_users()) == 2


@pytest.mark.anyio
async def test_allocation_split_writes_wait_for_commit(audit_db, monkeypatch):
    await save_settings(source_wallet="source")
    await add_member()
    calls = []

    async def targets(*args, **kwargs):
        calls.append(kwargs.get("user_id"))

    monkeypatch.setattr(splits, "update_split_targets_proportional", targets)
    with pytest.raises(RuntimeError, match="rollback"):
        async with crud.admission_transaction():
            await crud.update_cyberherd_member_allocation("a" * 64, 1.0, "owner")
            assert calls == []
            raise RuntimeError("rollback")
    assert calls == []
    assert (await crud.get_member_by_pubkey("owner", "a" * 64))[
        "allocation_percentage"
    ] is None
    async with crud.admission_transaction():
        await crud.update_cyberherd_member_allocation("a" * 64, 1.0, "owner")
        assert calls == []
    assert calls == ["owner"]


@pytest.mark.anyio
async def test_runtime_schema_failure_is_not_silently_accepted(audit_db, monkeypatch):
    async def denied(*args, **kwargs):
        raise PermissionError("schema writes denied")

    with monkeypatch.context() as patch:
        patch.setattr(audit_db, "execute", denied)
        with pytest.raises(PermissionError, match="schema writes denied"):
            await crud._bootstrap_cyberherd_tables()
