"""Social deliveries must not replay admission after displacement or restart."""

import asyncio
import os
from types import SimpleNamespace
from uuid import uuid4

import pytest

import lnbits.db as db_module
from lnbits.db import POSTGRES, SQLITE, Database
from lnbits.extensions.cyberherd import crud, migrations
from lnbits.extensions.cyberherd.services import headbutt, messaging, subscriptions
from lnbits.settings import settings

# Only generated test schema identifiers are interpolated.
# ruff: noqa: S608

NOTE = "e" * 64
OWNER = "test-owner"
ALICE = "a" * 64
BOB = "b" * 64


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture(params=[SQLITE, POSTGRES])
async def social_db(request, monkeypatch, tmp_path):
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
    database = Database(f"ext_social_test_{uuid4().hex}")
    monkeypatch.setattr(crud, "db", database)
    monkeypatch.setattr(headbutt, "_HEADBUTT_LOCK", None)
    schema = database.references_schema
    try:
        await database.execute(f"""CREATE TABLE {schema}cyber_herd (
            pubkey TEXT NOT NULL, user_id TEXT NOT NULL, display_name TEXT,
            event_id TEXT, note TEXT, kinds TEXT, nprofile TEXT, lud16 TEXT,
            nip05 TEXT, notified INTEGER DEFAULT 0, payouts REAL DEFAULT 0,
            amount INTEGER DEFAULT 0, picture TEXT, relays TEXT,
            metadata_last_checked_at INTEGER, is_active INTEGER DEFAULT 0,
            banned INTEGER DEFAULT 0, PRIMARY KEY (user_id, pubkey)
        )""")
        await database.execute(f"""CREATE TABLE {schema}processed_events (
            user_id TEXT NOT NULL, event_hash TEXT NOT NULL, note_id TEXT DEFAULT '',
            pubkey TEXT, processed_at INTEGER NOT NULL, event_type TEXT,
            UNIQUE (user_id, event_hash, note_id)
        )""")
        await migrations._create_engagement_awards(database)
        await migrations.m043_add_treat_delivery_records(database)
        await migrations.m044_add_prejoin_engagement_evidence(database)
        for pubkey in (ALICE, BOB):
            await database.execute(
                f"INSERT INTO {schema}cyber_herd "
                "(pubkey,user_id,display_name,lud16,nprofile) "
                "VALUES (:pubkey,:owner,'Member','member@example.test','nprofile-test')",
                {"pubkey": pubkey, "owner": OWNER},
            )
        yield database
    finally:
        if backend == POSTGRES:
            await database.execute(f"DROP SCHEMA IF EXISTS {database.schema} CASCADE")
        await database.engine.dispose()


@pytest.fixture
async def scenario(social_db, monkeypatch):
    user_settings = SimpleNamespace(
        tracked_event_ids=[NOTE],
        tracked_tags=[],
        max_members=1,
        midnight_reset_enabled=False,
        repost_tracking_enabled=True,
        likes_tracking_enabled=True,
        minimum_sats=10,
        source_wallet=None,
        herd_wallet=None,
        nip05_verification_enabled=False,
        member_allocation_percent=10,
    )
    announcements = []

    async def get_settings(user_id=None):
        return user_settings

    async def today_notes(self):
        return [NOTE]

    async def no_post_admission(self, *args, **kwargs):
        return None

    async def publish(event_type, **kwargs):
        announcements.append(event_type)
        await asyncio.sleep(0)
        return True

    async def reply_params(self, *args, **kwargs):
        return None, None

    async def websocket_topic(self):
        return "test-topic"

    async def query_events(filters, **kwargs):
        if 1 in filters.get("kinds", []) or filters.get("ids") == [NOTE]:
            from lnbits.extensions.cyberherd.services.time_utils import get_day_boundaries_utc

            return [{"id": NOTE, "pubkey": "f" * 64, "kind": 1,
                     "created_at": int(get_day_boundaries_utc().local_since_ts) + 1,
                     "tags": [], "content": ""}]
        return []

    monkeypatch.setattr(crud, "get_settings", get_settings)
    monkeypatch.setattr(subscriptions.nostr_helpers, "query_events", query_events)
    monkeypatch.setattr(subscriptions.nostr_helpers, "check_availability", lambda: True)
    monkeypatch.setattr(subscriptions, "get_effective_pubkey", lambda s: "f" * 64)
    monkeypatch.setattr(
        subscriptions.nostr_helpers, "verify_event_signature", lambda event: True
    )
    monkeypatch.setattr(
        headbutt.EnhancedHeadbuttService, "_get_today_cyberherd_notes", today_notes
    )
    monkeypatch.setattr(
        headbutt.EnhancedHeadbuttService, "_post_admission_tasks", no_post_admission
    )
    monkeypatch.setattr(
        headbutt.EnhancedHeadbuttService, "_resolve_reply_params", reply_params
    )
    monkeypatch.setattr(
        headbutt.EnhancedHeadbuttService, "_get_websocket_topic", websocket_topic
    )
    monkeypatch.setattr(messaging, "publish_event_message", publish)
    return SimpleNamespace(
        db=social_db, settings=user_settings, announcements=announcements
    )


def social_event(pubkey=ALICE, kind=6, event_id="1" * 64):
    return {
        "id": event_id,
        "pubkey": pubkey,
        "kind": kind,
        "created_at": 1792856243,
        "tags": [["e", NOTE]],
        "content": "",
    }


async def deliver(scenario, event):
    return await subscriptions.process_event_for_user(
        OWNER, event, scenario.settings, SimpleNamespace(state=SimpleNamespace())
    )


async def active_members():
    return {row["pubkey"] for row in await crud.get_active_cyberherd_members(OWNER)}


@pytest.mark.anyio
@pytest.mark.parametrize("kind", [6, 7])
async def test_processed_event_cannot_readmit_displaced_member(scenario, kind):
    event = social_event(kind=kind)
    await deliver(scenario, event)
    await deliver(scenario, social_event(BOB, 6, "2" * 64))
    assert await active_members() == {BOB}
    if kind == 7:
        # An open slot must not make an already-counted reaction eligible again.
        await crud.deactivate_cyberherd_member(BOB, OWNER)
    before = await active_members()
    published = len(scenario.announcements)
    await deliver(scenario, event)
    assert await active_members() == before
    assert len(scenario.announcements) == published


@pytest.mark.anyio
async def test_persistent_marker_blocks_replay_inside_admission_after_restart(
    scenario, monkeypatch
):
    event = social_event()
    await deliver(scenario, event)
    await deliver(scenario, social_event(BOB, 6, "2" * 64))
    assert await crud.is_event_processed(OWNER, event["id"], note_id=NOTE)
    monkeypatch.setattr(headbutt, "_HEADBUTT_LOCK", None)
    # A fresh service instance bypasses all subscription/in-memory caches.
    service = headbutt.EnhancedHeadbuttService(
        db=crud, messaging_module=messaging, user_id=OWNER
    )
    published = len(scenario.announcements)
    await service.attempt_headbutt(
        headbutt._Attacker(ALICE, 0, kinds=[6], note_id=NOTE, event_id=event["id"])
    )
    assert await active_members() == {BOB}
    assert len(scenario.announcements) == published


@pytest.mark.anyio
async def test_concurrent_admissions_cannot_replay_after_intervening_eviction(scenario):
    def service():
        return headbutt.EnhancedHeadbuttService(
            db=crud, messaging_module=messaging, user_id=OWNER
        )

    def attacker(pubkey, eid):
        return headbutt._Attacker(pubkey, 0, kinds=[6], note_id=NOTE, event_id=eid)

    await asyncio.gather(
        service().attempt_headbutt(attacker(ALICE, "1" * 64)),
        service().attempt_headbutt(attacker(BOB, "2" * 64)),
        service().attempt_headbutt(attacker(ALICE, "1" * 64)),
    )
    assert await active_members() == {BOB}
    assert scenario.announcements.count("repost_displaces") == 1
    assert await crud.is_event_processed(OWNER, "1" * 64, note_id=NOTE)


@pytest.mark.anyio
async def test_processed_lookup_failure_cannot_authorize_admission(
    scenario, monkeypatch
):
    original = scenario.db.fetchone

    async def fail_processed_query(query, values=None, *args, **kwargs):
        if "processed_events" in query:
            raise RuntimeError("database unavailable")
        return await original(query, values, *args, **kwargs)

    monkeypatch.setattr(scenario.db, "fetchone", fail_processed_query)
    await deliver(scenario, social_event())
    assert await active_members() == set()
    assert scenario.announcements == []


@pytest.mark.anyio
async def test_marker_write_failure_does_not_publish_success(scenario, monkeypatch):
    async def unavailable(*args, **kwargs):
        raise RuntimeError("marker write failed")

    monkeypatch.setattr(crud, "register_processed_event", unavailable)
    assert await deliver(scenario, social_event()) is False
    assert scenario.announcements == []


@pytest.mark.anyio
async def test_other_user_or_note_marker_does_not_block_new_engagement(scenario):
    event = social_event()
    await crud.register_processed_event("other-owner", event["id"], note_id=NOTE)
    await crud.register_processed_event(OWNER, event["id"], note_id="f" * 64)
    await deliver(scenario, event)
    assert await active_members() == {ALICE}
    assert await crud.is_event_processed(OWNER, event["id"], note_id=NOTE)


@pytest.mark.anyio
async def test_legacy_payment_ledger_repair_commits_membership_once(scenario, monkeypatch):
    from lnbits.extensions.cyberherd import migrations
    from lnbits.extensions.cyberherd.services import zap_totals

    database = scenario.db
    if database.type != POSTGRES:
        pytest.skip("legacy PostgreSQL constraints")
    schema = database.references_schema
    await database.execute(f"""CREATE TABLE {schema}processed_zaps (
        id SERIAL PRIMARY KEY, user_id TEXT NOT NULL,
        zap_receipt_id TEXT NOT NULL, payment_hash TEXT NOT NULL,
        note_id TEXT NOT NULL, zapper_pubkey TEXT NOT NULL,
        amount_sats INTEGER NOT NULL, processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        event_id TEXT, amount INTEGER, UNIQUE(user_id, zap_receipt_id))""")
    await database.execute(f"""INSERT INTO {schema}processed_zaps
        (user_id,zap_receipt_id,payment_hash,note_id,zapper_pubkey,amount_sats)
        VALUES ('old-owner','receipt','old-hash','old-note','old-zapper',42)""")
    old_row = dict(await database.fetchone(f"SELECT * FROM {schema}processed_zaps"))
    await database.execute(f"""CREATE TABLE {schema}zap_totals (
        user_id TEXT NOT NULL, zapper_pubkey TEXT NOT NULL,
        total_sats INTEGER NOT NULL DEFAULT 0, event_count INTEGER NOT NULL DEFAULT 0,
        first_event_at INTEGER, last_event_at INTEGER, last_event_ids TEXT,
        last_updated_at REAL NOT NULL, target_pubkey TEXT,
        PRIMARY KEY (user_id, zapper_pubkey))""")

    async def metadata(self, attacker):
        return {}, None

    monkeypatch.setattr(headbutt.EnhancedHeadbuttService, "_resolve_attacker_metadata", metadata)
    payment_hash = "3" * 64

    async def claim():
        if await crud.is_payment_processed(OWNER, payment_hash, strict=True):
            return False
        return await crud.register_processed_event(
            OWNER, payment_hash, note_id=NOTE, pubkey=ALICE, event_type="payment_zap"
        )

    async def account():
        await zap_totals.record_incremental_zap_total(
            user_id=OWNER, zapper_pubkey=ALICE, target_pubkey="f" * 64,
            amount_sats=21, event_id=payment_hash, skip_member_check=True,
        )

    def service():
        return headbutt.EnhancedHeadbuttService(db=crud, messaging_module=messaging, user_id=OWNER)

    def attacker():
        return headbutt._Attacker(ALICE, 21, kinds=[9735], note_id=NOTE, event_id=payment_hash)

    await migrations.m039_repair_processed_zap_timestamp(database)
    await database.engine.dispose()
    with pytest.raises(Exception, match="zap_receipt_id"):
        await service().attempt_headbutt(attacker(), payment_effect=claim, payment_success=account)
    assert await active_members() == set()
    assert not await crud.is_payment_processed(OWNER, payment_hash, strict=True)
    assert not scenario.announcements

    for _ in range(2):
        await migrations.m040_repair_processed_zap_nullability(database)
    new_row = dict(await database.fetchone(f"SELECT * FROM {schema}processed_zaps"))
    from datetime import datetime
    # PostgreSQL's timestamp-to-text cast omits insignificant fractional zeros.
    assert datetime.fromisoformat(new_row["processed_at"]) == datetime.fromisoformat(str(old_row["processed_at"]))
    assert {k: v for k, v in new_row.items() if k != "processed_at"} == {
        k: v for k, v in old_row.items() if k != "processed_at"
    }
    result = await service().attempt_headbutt(attacker(), payment_effect=claim, payment_success=account)
    assert result["status"] == "reactivated"
    assert await active_members() == {ALICE}
    assert await crud.is_payment_processed(OWNER, payment_hash, strict=True)
    duplicate = await service().attempt_headbutt(attacker(), payment_effect=claim, payment_success=account)
    assert duplicate == {"status": "duplicate"}
    member = await crud.get_cyberherd_member_by_pubkey(ALICE, OWNER)
    assert member["amount"] == 21
    totals = await crud.get_zap_totals_row(OWNER, ALICE)
    assert (totals["total_sats"], totals["event_count"]) == (21, 1)
