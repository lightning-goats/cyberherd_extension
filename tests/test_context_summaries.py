"""Queued score summaries use isolated databases and never publish or pay."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from lnbits.extensions.cyberherd import crud
from lnbits.extensions.cyberherd.services import treats
from lnbits.extensions.cyberherd.tests.test_database_operations import (
    add_member,
    save_settings,
)
from lnbits.extensions.cyberherd.tests.test_database_operations import (
    audit_db as _audit_db,
)
from lnbits.extensions.cyberherd.tests.test_messaging_template_storage import (
    messaging_db as _messaging_db,
)
from lnbits.extensions.cyberherd.tests.test_treats import PUBKEY, ROOT, rows
from lnbits.extensions.cyberherd_messaging import crud as message_crud
from lnbits.extensions.cyberherd_messaging import migrations as message_migrations
from lnbits.extensions.cyberherd_messaging import services as messages

audit_db = _audit_db
messaging_db = _messaging_db
# Fixture-generated schema names only.


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture(autouse=True)
def choose_first_template(monkeypatch):
    # Exact wording checks deliberately select the first stored variant.
    monkeypatch.setattr("random.choice", lambda templates: templates[0])


async def setup_member():
    await save_settings(
        minimum_sats=21, zap_wallet="goats@example.test", member_allocation_percent=10
    )
    await add_member(pubkey=PUBKEY, amount=21, kinds=[6, 7])


@pytest.mark.anyio
async def test_member_queue_freezes_current_share_and_zap_gain(audit_db):
    await setup_member()
    await treats.queue_member(
        "member_increase",
        "owner",
        {
            "member_pubkey": PUBKEY,
            "source_event_id": "zap",
            "increase_amount": 1,
        },
        "herd",
        [ROOT],
        "cyberherd",
    )
    notice = (await rows("message"))[0]
    score = notice["data"]["values"]["current_score"]
    assert score["total_point_units"] / score["minimum_sats"] == 14
    assert score["share_percent"] == 10
    assert score["earned_points"] == pytest.approx(10 / 21)
    await add_member(pubkey="c" * 64, amount=210)
    await save_settings(minimum_sats=10)
    assert (await treats.record(notice["id"]))["data"]["values"][
        "current_score"
    ] == score


@pytest.mark.anyio
async def test_social_gain_first_observation_only(audit_db):
    await setup_member()
    await crud.observe_engagement(PUBKEY, 6, "first", ROOT, 0, "owner")
    for source, expected in [("first", 3), ("repeat", 0)]:
        summary = await treats.member_summary(
            "owner", PUBKEY, "existing_member_repost", {"source_event_id": source}
        )
        assert summary["earned_points"] == expected


@pytest.mark.anyio
@pytest.mark.parametrize("event_type", sorted(treats.FAILED_ADMISSIONS))
async def test_rejection_does_not_reserve_membership_thread(audit_db, event_type):
    await save_settings()
    await treats.queue_member(
        event_type,
        "owner",
        {
            "attacker_pubkey": PUBKEY,
            "victim_pubkey": "c" * 64,
            "event_id": "failed",
            "required_sats": 22,
        },
        "herd",
        [ROOT],
        "cyberherd",
    )
    assert not await rows("thread")
    notice = (await rows("message"))[0]["data"]
    assert notice["values"]["current_score"] == {"status": "not_admitted"}
    assert notice["thread"] is None
    assert notice["roots"] == [ROOT]
    assert notice["p_tags"] == [PUBKEY, "c" * 64]


@pytest.mark.anyio
async def test_feeder_queue_freezes_one_herd_summary(audit_db):
    await setup_member()
    await treats.queue_feeder(
        "owner",
        "herd",
        "deposit",
        {
            "new_amount": 2345,
            "feedings": 2,
            "payout_cycles": 2,
        },
        "cyberherd",
    )
    await add_member(pubkey="c" * 64, amount=210)
    await treats.queue_feeder(
        "owner",
        "herd",
        "deposit",
        {
            "new_amount": 2345,
            "feedings": 2,
            "payout_cycles": 2,
        },
        "cyberherd",
    )
    notices = await rows("message")
    assert len(notices) == 1
    assert notices[0]["data"]["values"]["herd_score"] == {
        "active_members": 1,
        "total_points": 14,
        "member_allocation": 10,
    }


@pytest.mark.anyio
@pytest.mark.parametrize(
    "event_type",
    [
        "new_member",
        "member_increase",
        "existing_member_repost",
        "existing_member_reaction",
        "headbutt_success",
        "kind_6_repost",
        "kind_7_reaction",
        "member_thread",
    ],
)
async def test_member_render_current_score_from_database(
    messaging_db, monkeypatch, event_type
):
    bundle = AsyncMock(
        return_value=SimpleNamespace(
            nostr_content="A member update\n\nhttps://lightning-goats.com/leaderboard",
            websocket_content="",
        )
    )
    monkeypatch.setattr(messages, "build_message_bundle", bundle)
    score = {
        "zap_sats": 21,
        "minimum_sats": 21,
        "repost_points": 3,
        "like_points": 1,
        "total_point_units": 294,
        "share_percent": 0.61,
        "earned_points": 3,
    }
    content = await messages.render_member_message(
        event_type,
        {
            "name": "Alice",
            "current_score": score,
        },
        PUBKEY,
        "owner",
    )
    assert "Current share: 0.61%" in content
    assert "Total: 14 points" in content
    assert "This interaction added 3 points" in content
    assert "Shares can change before distribution." in content
    assert "Treat share:" not in content
    if event_type != "member_thread":
        assert content.endswith("https://lightning-goats.com/leaderboard")


@pytest.mark.anyio
async def test_failed_and_feeder_render_distinct_database_summaries(
    messaging_db, monkeypatch
):
    monkeypatch.setattr(
        messages,
        "build_message_bundle",
        AsyncMock(
            return_value=SimpleNamespace(
                nostr_content="Original explanation", websocket_content=""
            )
        ),
    )
    content = await messages.render_member_message(
        "kind_7_headbutt_failure",
        {
            "attacker_display_name": "Alice",
            "current_score": {"status": "not_admitted"},
        },
        PUBKEY,
        "owner",
    )
    assert "did not earn membership points or a share" in content
    assert "Current share:" not in content and "Total:" not in content
    content = await messages.render_feeder_message(
        {
            "new_amount": 2345,
            "feedings": 2,
            "payout_cycles": 2,
            "herd_score": {
                "active_members": 3,
                "total_points": 65.62,
                "member_allocation": 10,
            },
        },
        "owner",
    )
    assert "3 active members · 65.62 total points" in content
    assert "Member allocation: 10%" in content
    assert "2 feeder activation(s) and 2 CyberHerd payout cycle(s)" in content
    assert "Your current points" not in content


@pytest.mark.anyio
async def test_context_template_migration_preserves_customisation(messaging_db):
    await message_crud.ensure_default_templates("owner")
    await message_crud.update_message_template(
        "owner", "cyber_herd_current_score", "0", "Custom score {total_points}"
    )
    await message_crud.delete_message_template("owner", "cyber_herd_feeder_score", "0")
    await message_migrations.m007_seed_context_summary_templates(messaging_db)
    await message_crud.ensure_default_templates("owner")
    assert (
        await message_crud.get_message_templates("owner", "cyber_herd_current_score")
    )[0].content == "Custom score {total_points}"
    assert not await message_crud.get_message_templates(
        "owner", "cyber_herd_feeder_score"
    )


@pytest.mark.anyio
@pytest.mark.parametrize(
    "event_type",
    [
        "headbutt_success",
        "headbutt_failure",
        "kind_6_headbutt_failure",
        "kind_7_headbutt_failure",
        "join_below_minimum",
        "repost_displaces",
        "zapper_displaces_kind_6",
        "zapper_displaces_kind_7",
    ],
)
async def test_actual_headbutt_templates_keep_context(
    messaging_db, monkeypatch, event_type
):
    monkeypatch.setattr("random.choice", lambda items: items[0])
    failure = event_type in treats.FAILED_ADMISSIONS
    summary = (
        {"status": "not_admitted"}
        if failure
        else {
            "zap_sats": 21,
            "minimum_sats": 21,
            "repost_points": 3,
            "like_points": 1,
            "total_point_units": 294,
            "share_percent": 10,
        }
    )
    content = await messages.render_member_message(
        event_type,
        {
            "attacker_name": "Alice",
            "attacker_display_name": "Alice",
            "attacker_pubkey": PUBKEY,
            "attacker_amount": 21,
            "victim_name": "Bob",
            "victim_display_name": "Bob",
            "victim_pubkey": "c" * 64,
            "victim_amount": 20,
            "required_amount": 22,
            "required_sats": 22,
            "difference": 1,
            "current_score": summary,
        },
        PUBKEY,
        "owner",
    )
    assert "Alice" in content and "nostr:npub" in content
    assert "{" not in content and "}" not in content
    assert ("Current share: 10%" in content) == (not failure)
    assert ("did not earn membership points" in content) == failure


@pytest.mark.anyio
async def test_distribution_anchor_and_failed_then_join_have_own_scores(
    audit_db, monkeypatch
):
    await setup_member()
    await treats.queue_member(
        "kind_7_headbutt_failure",
        "owner",
        {
            "attacker_pubkey": "d" * 64,
            "event_id": "failed",
        },
        "herd",
        [ROOT],
        "cyberherd",
    )
    assert not await rows("thread")
    await treats.queue_member(
        "new_member",
        "owner",
        {
            "member_pubkey": "d" * 64,
            "source_event_id": "joined",
        },
        "herd",
        [ROOT],
        "cyberherd",
    )
    assert len(await rows("thread")) == 1
    monkeypatch.setattr(
        "lnbits.extensions.cyberherd.utils.common.is_extension_enabled_for_user",
        AsyncMock(return_value=True),
    )
    await save_settings(minimum_sats=21, herd_wallet="herd", source_wallet="source")
    await treats.prepare_distribution(
        SimpleNamespace(
            wallet_id="source",
            payment_hash="funding",
            amount=1000000,
        ),
        [SimpleNamespace(wallet="member@example.test", percent=10, alias="Alice")],
    )
    anchor = next(
        r for r in await rows("message") if r["data"]["event_type"] == "member_thread"
    )
    assert anchor["data"]["values"]["current_score"]["total_point_units"] == 294


@pytest.mark.anyio
async def test_headbutt_dispatch_queues_attacker_without_live_publish(
    audit_db, monkeypatch
):
    from lnbits.extensions.cyberherd.services import messaging as dispatch

    queue = AsyncMock(return_value=True)
    monkeypatch.setattr(treats, "queue_member", queue)
    monkeypatch.setattr(dispatch, "_msg", None)
    assert await dispatch.publish_event_message(
        "kind_7_headbutt_failure",
        owner_user_id="owner",
        values={"attacker_pubkey": PUBKEY, "event_id": "failure"},
        e_tags=[ROOT],
        wallet_id="herd",
    )
    assert queue.await_args.args[2]["member_pubkey"] == PUBKEY
