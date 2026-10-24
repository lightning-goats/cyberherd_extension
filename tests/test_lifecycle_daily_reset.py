"""Daily lifecycle regressions: disposable database and stubbed side effects only."""

import os
import re
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
import lnbits.db as db_module
from lnbits.db import Database, SQLITE, POSTGRES
from sqlalchemy import event
from lnbits.settings import settings
from lnbits.extensions import cyberherd
from lnbits.extensions.cyberherd import crud, migrations, tasks
from lnbits.extensions.cyberherd.services import messaging, time_utils


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture(params=[SQLITE, POSTGRES])
async def lifecycle_db(request, monkeypatch, tmp_path):
    backend = request.param
    if backend == POSTGRES and not os.environ.get("CYBERHERD_TEST_POSTGRES"):
        pytest.skip(
            "enable disposable PostgreSQL schemas with CYBERHERD_TEST_POSTGRES=1"
        )
    postgres_url = settings.lnbits_database_url
    if backend == POSTGRES and not postgres_url:
        pytest.skip("PostgreSQL URL is not configured")
    monkeypatch.setattr(db_module, "DB_TYPE", backend)
    monkeypatch.setattr(
        settings, "lnbits_database_url", postgres_url if backend == POSTGRES else None
    )
    monkeypatch.setattr(settings, "lnbits_data_folder", str(tmp_path))

    class ScopedDatabase(Database):
        # Qualify test SQL against the generated schema, never the public schema.
        def scope(self, query):
            return re.sub(
                r"(?<![\w.])(settings|cyber_herd|processed_zaps|membership_reset_days)\b",
                lambda m: self.references_schema + m.group(0),
                query,
            )

        async def execute(self, query, *args, **kwargs):
            return await super().execute(self.scope(query), *args, **kwargs)

        async def fetchone(self, query, *args, **kwargs):
            return await super().fetchone(self.scope(query), *args, **kwargs)

    database = ScopedDatabase("ext_lifecycle_" + uuid4().hex)
    monkeypatch.setattr(crud, "db", database)
    monkeypatch.setattr(tasks, "clear_monitor_cache_for_user", AsyncMock())
    monkeypatch.setattr(
        tasks,
        "reset_splits_to_predefined_wallet",
        AsyncMock(return_value={"targets": []}),
    )
    monkeypatch.setattr(
        tasks, "_restart_kind1_subscriptions_for_user", AsyncMock(return_value=True)
    )
    await database.execute("""CREATE TABLE settings (
        user_id TEXT PRIMARY KEY, midnight_reset_enabled INTEGER, zap_tracking_enabled INTEGER,
        source_wallet TEXT, tracked_event_ids TEXT, tracked_event_addresses TEXT,
        tracked_event_timestamps TEXT)""")
    await database.execute("""CREATE TABLE cyber_herd (
        user_id TEXT, pubkey TEXT, is_active INTEGER, amount INTEGER, payouts REAL,
        event_id TEXT, note TEXT, kinds TEXT, banned INTEGER)""")
    await database.execute(
        "CREATE TABLE processed_zaps (user_id TEXT, payment_hash TEXT)"
    )
    await database.execute(
        "INSERT INTO settings VALUES ('social',1,0,'source','[\"old\"]','{}','{}')"
    )
    await database.execute(
        "INSERT INTO settings VALUES ('disabled',0,1,NULL,'[]','{}','{}')"
    )
    await database.execute(
        "INSERT INTO cyber_herd VALUES ('social','member',1,42,1,'old','note','6,7',1)"
    )
    await database.execute(
        "INSERT INTO cyber_herd VALUES ('disabled','member',1,12,1,'old','note','9735',0)"
    )
    await database.execute(
        "INSERT INTO processed_zaps VALUES ('social','durable-payment')"
    )
    try:
        yield database
    finally:
        if backend == POSTGRES:
            await database.execute(f"DROP SCHEMA IF EXISTS {database.schema} CASCADE")
        await database.engine.dispose()


def freeze(monkeypatch, instant):
    class Frozen(datetime):
        @classmethod
        def now(cls, tz=None):
            return instant.astimezone(tz) if tz else instant

    monkeypatch.setattr(time_utils, "datetime", Frozen)
    monkeypatch.setenv("TZ", "America/Denver")


@pytest.mark.anyio
async def test_upgrade_baseline_preserves_members_then_next_day_catches_up_once(
    lifecycle_db, monkeypatch
):
    freeze(monkeypatch, datetime(2026, 10, 24, 18, tzinfo=timezone.utc))
    await migrations.m037_add_membership_reset_days(lifecycle_db)
    assert await tasks.ensure_membership_day() == []
    assert (
        await lifecycle_db.fetchone(
            "SELECT amount FROM cyber_herd WHERE user_id='social'"
        )
    )["amount"] == 42
    # Restart several days later must catch up before any subsequent intake.
    freeze(monkeypatch, datetime(2026, 10, 28, 18, tzinfo=timezone.utc))
    assert await tasks.ensure_membership_day() == ["social"]
    row = await lifecycle_db.fetchone("SELECT * FROM cyber_herd WHERE user_id='social'")
    assert (
        row["is_active"],
        row["amount"],
        row["payouts"],
        row["kinds"],
        row["banned"],
    ) == (0, 0, 0, None, 1)
    assert (
        await lifecycle_db.fetchone(
            "SELECT tracked_event_ids FROM settings WHERE user_id='social'"
        )
    )["tracked_event_ids"] == "[]"
    assert (await lifecycle_db.fetchone("SELECT COUNT(*) AS n FROM processed_zaps"))[
        "n"
    ] == 1
    assert (
        await lifecycle_db.fetchone(
            "SELECT amount FROM cyber_herd WHERE user_id='disabled'"
        )
    )["amount"] == 12
    await lifecycle_db.execute(
        "UPDATE cyber_herd SET amount=9,is_active=1 WHERE user_id='social'"
    )
    assert await tasks.ensure_membership_day() == []
    assert (
        await lifecycle_db.fetchone(
            "SELECT amount FROM cyber_herd WHERE user_id='social'"
        )
    )["amount"] == 9


@pytest.mark.anyio
async def test_failed_reset_does_not_advance_marker(lifecycle_db, monkeypatch):
    freeze(monkeypatch, datetime(2026, 10, 24, 18, tzinfo=timezone.utc))
    await migrations.m037_add_membership_reset_days(lifecycle_db)
    freeze(monkeypatch, datetime(2026, 10, 25, 18, tzinfo=timezone.utc))
    await lifecycle_db.execute("DROP TABLE cyber_herd")
    with pytest.raises(Exception):
        await tasks.ensure_membership_day()
    row = await lifecycle_db.fetchone(
        "SELECT local_day FROM membership_reset_days WHERE user_id='social'"
    )
    assert row["local_day"] == "2026-10-24"


@pytest.mark.parametrize(
    "instant,hours,end",
    [
        (
            datetime(2026, 11, 1, 6, 30, tzinfo=timezone.utc),
            25,
            datetime(2026, 11, 2, 7, tzinfo=timezone.utc),
        ),
        (
            datetime(2026, 3, 8, 7, 30, tzinfo=timezone.utc),
            23,
            datetime(2026, 3, 9, 6, tzinfo=timezone.utc),
        ),
    ],
)
def test_system_localtime_without_tz_preserves_dst(monkeypatch, instant, hours, end):
    freeze(monkeypatch, instant)
    monkeypatch.delenv("TZ")
    monkeypatch.setattr(
        time_utils,
        "Path",
        lambda filename: SimpleNamespace(
            resolve=lambda: Path("/usr/share/zoneinfo/America/Denver"),
            read_text=lambda: "America/Denver",
        ),
    )
    zone = time_utils._get_system_local_timezone()
    assert zone.key == "America/Denver"
    boundaries = time_utils.get_day_boundaries_utc()
    assert boundaries.local_until_ts - boundaries.local_since_ts == hours * 3600
    assert boundaries.local_until_ts == int(end.timestamp())


@pytest.mark.anyio
@pytest.mark.parametrize("enabled", [False, True])
@pytest.mark.parametrize("event_type", ["new_member", "custom_event"])
async def test_fallback_notifications_preserve_owner_toggle(
    monkeypatch, enabled, event_type
):
    async def allowed(user_id=None):
        return enabled if user_id == "owner" else not enabled

    dependency = SimpleNamespace(
        is_nostr_publishing_enabled=AsyncMock(side_effect=allowed),
        publish_note=AsyncMock(return_value=True),
        send_to_websocket_clients=AsyncMock(return_value=True),
    )
    monkeypatch.setattr(messaging, "_msg", dependency)
    monkeypatch.setattr(messaging, "_messaging_available", True)
    monkeypatch.setattr(messaging, "_get_random_template", AsyncMock(return_value=None))
    monkeypatch.setattr(
        messaging,
        "make_messages",
        AsyncMock(return_value={"content": "Hello", "payload": None}),
    )
    monkeypatch.setattr(
        messaging,
        "_append_membership_extras",
        AsyncMock(side_effect=lambda content, *args, **kwargs: content),
    )
    assert await messaging.publish_event_message(
        event_type, owner_user_id="owner", wallet_id="wallet", values={}
    )
    dependency.is_nostr_publishing_enabled.assert_awaited_with(user_id="owner")
    assert dependency.publish_note.await_count == int(enabled)
    if enabled:
        assert dependency.publish_note.await_args.kwargs["user_id"] == "owner"
    assert dependency.send_to_websocket_clients.await_count == 1


@pytest.mark.anyio
async def test_startup_catchup_finishes_before_registering_intake(monkeypatch):
    order = []
    callbacks = []

    async def catchup(*args, **kwargs):
        order.append("catchup")
        return []

    def create(name, callback):
        order.append(name)
        callbacks.append((name, callback))
        return SimpleNamespace(cancel=lambda: None)

    monkeypatch.setattr(tasks, "ensure_membership_day", catchup)
    monkeypatch.setattr(cyberherd, "create_permanent_unique_task", create)
    monkeypatch.setattr(tasks, "create_permanent_unique_task", create)
    monkeypatch.setattr(
        cyberherd, "start_invoice_listener", lambda: order.append("invoice")
    )
    monkeypatch.setattr(cyberherd, "scheduled_tasks", [])
    cyberherd.cyberherd_start()
    assert len(callbacks) == 1
    await callbacks[0][1]()
    assert order.index("catchup") < order.index("ext_cyberherd_subscriptions")
    assert order.index("catchup") < order.index("invoice")


@pytest.mark.anyio
async def test_startup_storage_failure_does_not_start_intake(monkeypatch):
    callbacks = []

    def create(name, callback):
        callbacks.append((name, callback))
        return SimpleNamespace(cancel=lambda: None)

    monkeypatch.setattr(
        tasks,
        "ensure_membership_day",
        AsyncMock(side_effect=RuntimeError("storage unavailable")),
    )
    monkeypatch.setattr(cyberherd, "create_permanent_unique_task", create)
    monkeypatch.setattr(cyberherd, "scheduled_tasks", [])
    listener = SimpleNamespace(called=False)
    monkeypatch.setattr(tasks, "create_permanent_unique_task", create)
    monkeypatch.setattr(
        cyberherd, "start_invoice_listener", lambda: setattr(listener, "called", True)
    )
    cyberherd.cyberherd_start()
    assert len(callbacks) == 1
    with pytest.raises(RuntimeError, match="storage unavailable"):
        await callbacks[0][1]()
    assert len(callbacks) == 1
    assert not listener.called


@pytest.mark.anyio
async def test_split_failure_retries_without_resetting_new_members(
    lifecycle_db, monkeypatch
):
    freeze(monkeypatch, datetime(2026, 10, 24, 18, tzinfo=timezone.utc))
    await migrations.m037_add_membership_reset_days(lifecycle_db)
    freeze(monkeypatch, datetime(2026, 10, 25, 18, tzinfo=timezone.utc))
    reset = AsyncMock(side_effect=RuntimeError("split failure"))
    monkeypatch.setattr(tasks, "reset_splits_to_predefined_wallet", reset)
    with pytest.raises(RuntimeError, match="split failure"):
        await tasks.ensure_membership_day("social")
    marker = await lifecycle_db.fetchone(
        "SELECT * FROM membership_reset_days WHERE user_id='social'"
    )
    assert (marker["local_day"], marker["splits_pending"]) == ("2026-10-25", 1)
    await lifecycle_db.execute(
        "UPDATE cyber_herd SET is_active=1,amount=9 WHERE user_id='social'"
    )
    reset.side_effect = None
    assert await tasks.ensure_membership_day("social") == []
    assert (
        await lifecycle_db.fetchone(
            "SELECT amount FROM cyber_herd WHERE user_id='social'"
        )
    )["amount"] == 9
    assert (
        await lifecycle_db.fetchone(
            "SELECT splits_pending FROM membership_reset_days WHERE user_id='social'"
        )
    )["splits_pending"] == 0


@pytest.mark.anyio
async def test_settings_write_failure_rolls_back_member_reset(
    lifecycle_db, monkeypatch
):
    freeze(monkeypatch, datetime(2026, 10, 24, 18, tzinfo=timezone.utc))
    await migrations.m037_add_membership_reset_days(lifecycle_db)
    freeze(monkeypatch, datetime(2026, 10, 25, 18, tzinfo=timezone.utc))

    def fail_settings_write(conn, cursor, statement, parameters, context, executemany):
        if re.search(
            r"UPDATE (?:[a-z0-9_]+\.)?settings SET tracked_event_ids", statement
        ):
            raise RuntimeError("settings unavailable")

    event.listen(
        lifecycle_db.engine.sync_engine, "before_cursor_execute", fail_settings_write
    )
    with pytest.raises(Exception, match="settings unavailable"):
        await tasks.ensure_membership_day("social")
    row = await lifecycle_db.fetchone(
        "SELECT is_active,amount FROM cyber_herd WHERE user_id='social'"
    )
    assert (row["is_active"], row["amount"]) == (1, 42)
    assert (
        await lifecycle_db.fetchone(
            "SELECT local_day FROM membership_reset_days WHERE user_id='social'"
        )
    )["local_day"] == "2026-10-24"


@pytest.mark.anyio
async def test_rollover_waits_for_inflight_admission(lifecycle_db, monkeypatch):
    import asyncio
    from lnbits.extensions.cyberherd.services import headbutt

    monkeypatch.setattr(headbutt, "_HEADBUTT_LOCK", None)
    freeze(monkeypatch, datetime(2026, 10, 24, 18, tzinfo=timezone.utc))
    await migrations.m037_add_membership_reset_days(lifecycle_db)
    freeze(monkeypatch, datetime(2026, 10, 25, 18, tzinfo=timezone.utc))
    async with headbutt._get_headbutt_lock():
        reset = asyncio.create_task(tasks.ensure_membership_day("social"))
        try:
            await asyncio.sleep(0.03)
            row = await lifecycle_db.fetchone(
                "SELECT amount FROM cyber_herd WHERE user_id='social'"
            )
            assert row["amount"] == 42
            assert not reset.done()
        finally:
            if reset.done():
                await reset
    assert await reset == ["social"]


@pytest.mark.anyio
async def test_scheduled_refresh_runs_when_admission_already_advanced_day(
    lifecycle_db, monkeypatch
):
    freeze(monkeypatch, datetime(2026, 10, 24, 18, tzinfo=timezone.utc))
    await migrations.m037_add_membership_reset_days(lifecycle_db)
    freeze(monkeypatch, datetime(2026, 10, 25, 18, tzinfo=timezone.utc))
    assert await tasks.ensure_membership_day("social") == ["social"]
    tasks.clear_monitor_cache_for_user.assert_awaited_once_with("social")
    await tasks.midnight_reset_job()
    tasks._restart_kind1_subscriptions_for_user.assert_awaited_once_with(
        "social", tasks._APP_REF
    )
