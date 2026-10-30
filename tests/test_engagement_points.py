"""Contribution scores and durable cycle awards, with no external actions."""

import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from lnbits.extensions.cyberherd import crud, migrations, tasks, views_api
from lnbits.extensions.cyberherd.services import headbutt, shares, splits
from lnbits.extensions.cyberherd.services.time_utils import get_day_boundaries_utc
from lnbits.extensions.cyberherd.tests.test_database_operations import (
    audit_db as _audit_db,
    save_settings,
    add_member,
)
from lnbits.extensions.cyberherd.tests.test_social_event_replay import (
    social_db as _social_db,
    scenario as _scenario,
    OWNER,
    ALICE,
    BOB,
    NOTE,
    deliver,
    social_event,
    active_members,
)

# Reuse isolated fixtures without duplicating their database setup.
audit_db = _audit_db
social_db = _social_db
scenario = _scenario

# Identifiers come from the disposable DB fixture only.
# ruff: noqa: S608


@pytest.fixture
def anyio_backend():
    return "asyncio"


def member(pubkey, amount=0, kinds=(), **kwargs):
    return {
        "pubkey": pubkey,
        "amount": amount,
        "kinds": list(kinds),
        "is_active": 1,
        **kwargs,
    }


@pytest.mark.parametrize("minimum", [1, 10, 50, 100])
def test_zap_repost_like_order_and_cumulative_scores(minimum):
    assert shares.member_point_units(member("zap", minimum), minimum) == 10 * minimum
    assert (
        shares.member_point_units(member("repost", kinds=[6]), minimum) == 3 * minimum
    )
    assert shares.member_point_units(member("like", kinds=[7]), minimum) == minimum
    assert (
        shares.member_point_units(member("both", kinds=[6, 7, 6, 7]), minimum)
        == 4 * minimum
    )
    assert (
        shares.member_point_units(member("all", minimum, [6, 7]), minimum)
        == 14 * minimum
    )
    values = shares.compute_member_share_percentages(
        [
            member("zap", minimum),
            member("repost", kinds=[6]),
            member("like", kinds=[7]),
        ],
        10,
        minimum,
    )
    assert values == {"zap": 7.142857, "repost": 2.142857, "like": 0.714286}


def test_points_rounding_is_exact_and_stable_without_database_order():
    members = [member("a", 20, [6, 7]), member("b", 20)]
    assert shares.compute_member_share_percentages(members, 10) == {
        "a": 5.454545,
        "b": 4.545455,
    }
    equal = [member("c", kinds=[7]), member("b", kinds=[7]), member("a", kinds=[7])]
    assert shares.compute_member_share_percentages(equal, 10) == {
        "a": 3.333334,
        "b": 3.333333,
        "c": 3.333333,
    }
    assert shares.compute_member_share_percentages(
        equal[::-1], 10
    ) == shares.compute_member_share_percentages(equal, 10)
    assert shares.compute_member_share_percentages(equal, 0) == {"a": 0, "b": 0, "c": 0}


def test_inactive_banned_and_unawarded_kinds_do_not_score():
    rows = [
        member("inactive", 100, [6], is_active=0),
        member("banned", 100, [6], banned=1),
        member("unawarded", kinds=[6], engagement_kinds=[]),
        member("valid", 10),
    ]
    assert shares.compute_member_share_percentages(rows, 10) == {
        "inactive": 0,
        "banned": 0,
        "unawarded": 0,
        "valid": 10,
    }


@pytest.mark.anyio
async def test_first_repost_and_like_add_once_across_notes(scenario, monkeypatch):
    # Target selection is tested separately; this contract covers shared social awards.
    async def eligible(*args, **kwargs):
        return True
    monkeypatch.setattr("lnbits.extensions.cyberherd.services.subscriptions._is_tracked_event", eligible)
    other_note = "d" * 64
    scenario.settings.tracked_event_ids.append(other_note)

    async def query_events(filters, **kwargs):
        return [
            {
                "id": note,
                "pubkey": "f" * 64,
                "kind": 1,
                "created_at": int(get_day_boundaries_utc().local_since_ts) + 1,
                "tags": [],
                "content": "",
            }
            for note in filters.get("ids", [NOTE, other_note])
        ]

    monkeypatch.setattr(
        "lnbits.extensions.cyberherd.services.nostr_helpers.query_events", query_events
    )
    await deliver(scenario, social_event(ALICE, 6, "1" * 64))
    assert await crud.get_engagement_kinds(ALICE, OWNER) == [6]
    like = social_event(ALICE, 7, "2" * 64)
    like["tags"] = [["e", other_note]]
    await deliver(scenario, like)
    row = await crud.get_member_by_pubkey(OWNER, ALICE)
    assert row["engagement_kinds"] == [6, 7]
    assert shares.member_point_units(row) == 40
    before = len(scenario.announcements)
    repost = social_event(ALICE, 6, "3" * 64)
    repost["tags"] = [["e", other_note]]
    await deliver(scenario, repost)
    await deliver(scenario, social_event(ALICE, 7, "4" * 64))
    assert len(scenario.announcements) == before
    assert await crud.get_engagement_kinds(ALICE, OWNER) == [6, 7]
    assert (await crud.get_member_by_pubkey(OWNER, ALICE))["amount"] == 0


@pytest.mark.anyio
async def test_awards_survive_eviction_and_cannot_grant_free_readmission(scenario):
    await deliver(scenario, social_event(ALICE, 6, "1" * 64))
    await deliver(scenario, social_event(BOB, 6, "2" * 64))
    assert await active_members() == {BOB}
    await deliver(scenario, social_event(ALICE, 6, "3" * 64))
    assert await active_members() == {BOB}
    assert await crud.get_engagement_kinds(ALICE, OWNER) == [6]
    await crud.deactivate_cyberherd_member(BOB, OWNER)
    await deliver(scenario, social_event(ALICE, 7, "4" * 64))
    assert await active_members() == {ALICE}
    row = await crud.get_member_by_pubkey(OWNER, ALICE)
    assert row["engagement_kinds"] == [6, 7]
    assert shares.member_point_units(row) == 40
    await crud.delete_cyberherd_member_by_pubkey(ALICE, OWNER)
    await deliver(scenario, social_event(ALICE, 6, "5" * 64))
    assert await active_members() == set()
    assert await crud.get_engagement_kinds(ALICE, OWNER) == [6, 7]


@pytest.mark.anyio
async def test_awards_are_scoped_and_rollback_with_membership(audit_db, monkeypatch):
    await save_settings()
    await save_settings("other")
    original = crud.recompute_member_payouts

    async def fail(*args, **kwargs):
        raise RuntimeError("injected payout failure")

    with monkeypatch.context() as patch:
        patch.setattr(crud, "recompute_member_payouts", fail)
        with pytest.raises(RuntimeError):
            await add_member(amount=0, kinds=[6])
    assert await crud.get_engagement_kinds("a" * 64, "owner") == []
    assert await crud.get_member_by_pubkey("owner", "a" * 64) is None
    await add_member(amount=0, kinds=[6])
    await add_member("other", amount=0, kinds=[7])
    assert await crud.get_engagement_kinds("a" * 64, "owner") == [6]
    assert await crud.get_engagement_kinds("a" * 64, "other") == [7]
    await original("owner")


@pytest.mark.anyio
async def test_daily_and_manual_resets_clear_awards_but_disabled_days_do_not(
    audit_db, monkeypatch
):
    monkeypatch.setattr(headbutt, "_HEADBUTT_LOCK", None)
    monkeypatch.setattr(tasks, "_membership_day_lock", asyncio.Lock())

    async def no_work(*args, **kwargs):
        return None

    monkeypatch.setattr(tasks, "clear_monitor_cache_for_user", no_work)
    await save_settings(midnight_reset_enabled=True)
    await save_settings("other", midnight_reset_enabled=False)
    await add_member(amount=0, kinds=[6, 7])
    await add_member("other", amount=0, kinds=[6, 7])
    for owner in ("owner", "other"):
        await audit_db.execute(
            f"INSERT INTO {audit_db.references_schema}membership_reset_days (user_id,local_day) VALUES (:uid,'2000-01-01')",
            {"uid": owner},
        )
    await tasks.ensure_membership_day()
    assert await crud.get_engagement_kinds("a" * 64, "owner") == []
    assert await crud.get_engagement_kinds("a" * 64, "other") == [6, 7]
    await tasks.manual_reset_membership("other")
    assert await crud.get_engagement_kinds("a" * 64, "other") == []


@pytest.mark.anyio
async def test_share_api_payout_storage_and_split_targets_agree(audit_db, monkeypatch):
    from lnbits.extensions.cyberherd.services.splits import (
        _update_split_targets_proportional,
    )

    await save_settings(source_wallet="source", zap_wallet="goats", minimum_sats=10)
    await add_member(amount=20, kinds=[9735, 6, 7])
    await add_member(pubkey="b" * 64, amount=20, lud16="second@example.test")
    targets = []

    async def store(source_wallet, new_targets):
        assert source_wallet == "source"
        targets[:] = new_targets

    monkeypatch.setattr(splits, "split_set_targets", store)
    splits._split_signature_cache.clear()
    await _update_split_targets_proportional("source", "owner", strict=True)
    response = await views_api.api_get_shares(
        SimpleNamespace(),
        {
            "type": "wallet",
            "value": SimpleNamespace(wallet=SimpleNamespace(user="owner")),
        },
    )
    api = {
        row.get("lud16", row.get("zap_wallet")): row["percent"]
        for row in response["shares"]
    }
    applied = {row.wallet: row.percent for row in targets}
    assert (
        api
        == applied
        == {
            "goats": 90,
            "member@example.test": 5.454545,
            "second@example.test": 4.545455,
        }
    )
    stored = await crud.get_member_by_pubkey("owner", "a" * 64)
    assert stored["payouts"] * 100 == pytest.approx(5.454545)
    assert response["share_policy"] == "contribution_points"
    await save_settings(source_wallet="source", zap_wallet="goats", minimum_sats=50)
    stored = await crud.get_member_by_pubkey("owner", "a" * 64)
    assert stored["payouts"] * 100 == pytest.approx(6.666667)


@pytest.mark.anyio
async def test_migration_backfills_successes_in_current_cycle_only(
    audit_db, monkeypatch
):
    await save_settings(midnight_reset_enabled=True)
    await add_member(amount=0, kinds=[6])
    today = get_day_boundaries_utc()
    table = audit_db.references_schema
    await audit_db.execute(f"DELETE FROM {table}engagement_awards")
    for pubkey, event_type, timestamp in (
        ("b" * 64, "reaction", today.local_since_ts + 1),
        ("c" * 64, "headbutt_failure", today.local_since_ts + 2),
        ("d" * 64, "repost", today.local_since_ts - 1),
    ):
        await crud.ensure_inactive_cyberherd_member(pubkey, "owner")
        await audit_db.execute(
            f"INSERT INTO {table}processed_events (user_id,event_hash,note_id,pubkey,processed_at,event_type) "
            "VALUES ('owner',:hash,'note',:pubkey,:timestamp,:type)",
            {
                "hash": pubkey,
                "pubkey": pubkey,
                "timestamp": int(timestamp),
                "type": event_type,
            },
        )
    original = migrations._MigrationConnection.execute

    async def fail_history(self, query, values=None):
        if "CASE WHEN event_type='repost'" in query:
            raise RuntimeError("injected backfill failure")
        return await original(self, query, values)

    with monkeypatch.context() as patch:
        patch.setattr(migrations._MigrationConnection, "execute", fail_history)
        with pytest.raises(RuntimeError):
            await migrations.m042_add_engagement_awards(audit_db)
    assert await crud.get_engagement_kinds("a" * 64, "owner") == []
    await migrations.m042_add_engagement_awards(audit_db)
    await migrations.m042_add_engagement_awards(audit_db)
    assert await crud.get_engagement_kinds("a" * 64, "owner") == [6]
    assert await crud.get_engagement_kinds("b" * 64, "owner") == [7]
    assert await crud.get_engagement_kinds("c" * 64, "owner") == []
    assert await crud.get_engagement_kinds("d" * 64, "owner") == []
    assert await crud.get_engagement_kinds("a" * 64, "other") == []


@pytest.mark.anyio
async def test_distinct_paid_zaps_keep_accumulating_after_both_social_awards(
    scenario, monkeypatch
):
    from lnbits.extensions.cyberherd.services.payment_coordinator import (
        PaymentCoordinatorService,
    )

    scenario.settings.max_members = 3
    scenario.settings.herd_wallet = "herd"
    scenario.settings.zap_tracking_enabled = True
    scenario.settings.nostr_pubkey_override = "f" * 64
    await crud._bootstrap_cyberherd_tables()
    await deliver(scenario, social_event(ALICE, 6, "6" * 64))
    await deliver(scenario, social_event(ALICE, 7, "7" * 64))

    async def no_broadcast(*args, **kwargs):
        return None

    monkeypatch.setattr(
        "lnbits.extensions.cyberherd.services.leaderboard.broadcast_leaderboard_update",
        no_broadcast,
    )
    request = {"kind": 9734, "pubkey": ALICE, "tags": [["p", "f" * 64], ["e", NOTE]]}
    monitor = PaymentCoordinatorService(
        user_id=OWNER, db=crud, app=SimpleNamespace(state=SimpleNamespace())
    )
    for identity in ("1" * 64, "2" * 64, "1" * 64):
        payment = SimpleNamespace(
            wallet_id="herd",
            success=True,
            amount=10_000,
            payment_hash=identity,
            checking_id=identity,
            extra={"nostr": request},
            time=datetime.now(timezone.utc),
        )
        assert await monitor._process_payment_for_zap(
            payment, settings_override=scenario.settings
        )
    row = await crud.get_member_by_pubkey(OWNER, ALICE)
    assert row["amount"] == 20 and row["engagement_kinds"] == [6, 7]
    assert shares.member_point_units(row, 10) == 240
    assert await crud.count_processed_zaps(OWNER) == 2


@pytest.mark.anyio
async def test_concurrent_interactions_award_each_type_only_once(scenario):
    await asyncio.gather(
        deliver(scenario, social_event(ALICE, 6, "1" * 64)),
        deliver(scenario, social_event(ALICE, 6, "2" * 64)),
        deliver(scenario, social_event(ALICE, 7, "3" * 64)),
        deliver(scenario, social_event(ALICE, 7, "4" * 64)),
    )
    assert await crud.get_engagement_kinds(ALICE, OWNER) == [6, 7]
    row = await crud.get_member_by_pubkey(OWNER, ALICE)
    assert shares.member_point_units(row) == 40
    # A like observed before admission is promoted silently; a like delivered
    # after admission may have its ordinary existing-member notification.
    assert 1 <= len(scenario.announcements) <= 2
    assert sum(kind in ("kind_6_repost", "kind_7_reaction") for kind in scenario.announcements) == 1


@pytest.mark.anyio
async def test_failed_admission_does_not_consume_a_social_award(scenario):
    scenario.settings.nip05_verification_enabled = True
    await deliver(scenario, social_event(ALICE, 6, "1" * 64))
    assert await active_members() == set()
    assert await crud.get_engagement_kinds(ALICE, OWNER) == []
    scenario.settings.nip05_verification_enabled = False
    await deliver(scenario, social_event(ALICE, 6, "2" * 64))
    assert await active_members() == {ALICE}
    assert await crud.get_engagement_kinds(ALICE, OWNER) == [6]


@pytest.mark.anyio
async def test_award_reset_rolls_back_with_failed_manual_reset(audit_db, monkeypatch):
    monkeypatch.setattr(headbutt, "_HEADBUTT_LOCK", None)
    monkeypatch.setattr(tasks, "_membership_day_lock", asyncio.Lock())
    await save_settings()
    await add_member(amount=0, kinds=[6, 7])
    original = crud._AdmissionConnection.execute

    async def fail(self, query, values=None):
        if "settings SET tracked_event_ids" in query:
            raise RuntimeError("injected reset failure")
        return await original(self, query, values)

    with monkeypatch.context() as patch:
        patch.setattr(crud._AdmissionConnection, "execute", fail)
        with pytest.raises(RuntimeError):
            await tasks.manual_reset_membership("owner")
    assert await crud.get_engagement_kinds("a" * 64, "owner") == [6, 7]
    assert await crud.get_active_cyberherd_size("owner") == 1


@pytest.mark.anyio
async def test_disabled_daily_reset_backfills_awards_since_known_cycle_start(audit_db):
    await save_settings(midnight_reset_enabled=False)
    today = get_day_boundaries_utc()
    from datetime import timedelta, date

    yesterday = (
        date.fromisoformat(today.local_day_str) - timedelta(days=1)
    ).isoformat()
    await audit_db.execute(
        f"INSERT INTO {audit_db.references_schema}membership_reset_days (user_id,local_day) VALUES ('owner',:day)",
        {"day": yesterday},
    )
    await audit_db.execute(
        f"INSERT INTO {audit_db.references_schema}processed_events (user_id,event_hash,note_id,pubkey,processed_at,event_type) "
        "VALUES ('owner','prior-day','note',:pubkey,:timestamp,'repost')",
        {"pubkey": "a" * 64, "timestamp": int(today.local_since_ts) - 1},
    )
    await migrations.m042_add_engagement_awards(audit_db)
    assert await crud.get_engagement_kinds("a" * 64, "owner") == [6]


@pytest.mark.anyio
async def test_startup_refreshes_existing_shares_before_starting_intake(
    audit_db, monkeypatch
):
    import lnbits.extensions.cyberherd as extension

    await save_settings(source_wallet="source", zap_wallet="goats")
    await add_member(amount=20, kinds=[6, 7])
    await add_member(pubkey="b" * 64, amount=20)
    await audit_db.execute(
        f"UPDATE {audit_db.references_schema}cyber_herd SET payouts=0.01"
    )
    events = []
    captured = {}

    async def ensure_day(**kwargs):
        events.append("day")

    async def enabled(user_id):
        return True

    async def targets(source_wallet, user_id, strict=False):
        assert source_wallet == "source" and user_id == "owner" and strict
        events.append("targets")

    def capture(name, function):
        captured["startup"] = function
        return SimpleNamespace()

    monkeypatch.setattr(tasks, "ensure_membership_day", ensure_day)
    monkeypatch.setattr(
        "lnbits.extensions.cyberherd.utils.common.is_extension_enabled_for_user",
        enabled,
    )
    monkeypatch.setattr(splits, "update_split_targets_proportional", targets)
    monkeypatch.setattr(extension, "create_permanent_unique_task", capture)
    monkeypatch.setattr(extension, "scheduled_tasks", [])
    monkeypatch.setattr(
        extension,
        "_start_daily_ready_background_tasks",
        lambda: events.append("intake"),
    )
    extension.cyberherd_start()
    await captured["startup"]()
    assert events == ["day", "targets", "intake"]
    assert (await crud.get_member_by_pubkey("owner", "a" * 64))[
        "payouts"
    ] * 100 == pytest.approx(5.454545)


def test_fractional_targets_remain_below_the_backend_total_limit():
    import struct
    from lnbits.extensions.splitpayments.models import Target

    percentages = [90, 7.142857, 2.142857, 0.714286]
    targets = [
        Target(id=str(i), wallet=str(i), source="source", percent=p)
        for i, p in enumerate(percentages)
    ]
    as_float32 = lambda p: struct.unpack("!f", struct.pack("!f", p))[0]
    assert sum(as_float32(p) for p in percentages) > 100
    safe = splits._storage_safe_targets(targets)
    assert sum(t.percent for t in safe) <= 100
    assert sum(as_float32(t.percent) for t in safe) <= 100
    for before, after in zip(targets, safe):
        assert 0 <= after.percent <= before.percent
        assert before.percent - after.percent < 0.000001
    exact = [
        Target(id="a", wallet="a", source="source", percent=90),
        Target(id="b", wallet="b", source="source", percent=10),
    ]
    assert splits._storage_safe_targets(exact) is exact


@pytest.mark.anyio
async def test_rejected_member_upsert_cannot_award_social_points(audit_db, monkeypatch):
    await save_settings()
    original = crud._AdmissionConnection.execute

    async def rejected(self, query, values=None):
        if "INSERT INTO" in query and "cyber_herd (" in query:
            return SimpleNamespace(rowcount=0)
        return await original(self, query, values)

    with monkeypatch.context() as patch:
        patch.setattr(crud._AdmissionConnection, "execute", rejected)
        with pytest.raises(ValueError, match="activation was rejected"):
            await add_member(amount=0, kinds=[6])
    assert await crud.get_engagement_kinds("a" * 64, "owner") == []
    assert await crud.get_member_by_pubkey("owner", "a" * 64) is None
