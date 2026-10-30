"""Qualifying social actions become points only after successful admission."""

import asyncio
import time

import pytest

from lnbits.extensions.cyberherd import crud
from lnbits.extensions.cyberherd.services import headbutt, shares, treats
from lnbits.extensions.cyberherd.tests.test_social_event_replay import (
    ALICE,
    BOB,
    NOTE,
    OWNER,
    active_members,
    deliver,
    social_event,
)
from lnbits.extensions.cyberherd.tests.test_social_event_replay import (
    scenario as _scenario,
)
from lnbits.extensions.cyberherd.tests.test_social_event_replay import (
    social_db as _social_db,
)

social_db = _social_db
scenario = _scenario
# Queries use only the fixture's Database-owned schema.
# ruff: noqa: S608


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.mark.anyio
async def test_failed_like_then_repost_earns_four_points_without_second_like_message(
    scenario,
):
    await crud.update_and_activate_member(BOB, 0, OWNER, kinds=[7])
    like = social_event(ALICE, 7, "1" * 64)
    await deliver(scenario, like)
    assert await active_members() == {BOB}
    assert await crud.get_engagement_kinds(ALICE, OWNER) == []
    before = len(scenario.announcements)
    repost = social_event(ALICE, 6, "2" * 64)
    await deliver(scenario, repost)
    assert await active_members() == {ALICE}
    member = await crud.get_member_by_pubkey(OWNER, ALICE)
    assert member["engagement_kinds"] == [6, 7]
    assert shares.member_point_units(member, 10) == 40
    assert len(scenario.announcements) == before + 1
    assert "existing_member_reaction" not in scenario.announcements
    await deliver(scenario, like)
    assert len(scenario.announcements) == before + 1


@pytest.mark.anyio
async def test_failed_social_actions_then_zap_credit_all_types_once(scenario):
    await crud.update_and_activate_member(BOB, 100, OWNER, kinds=[9735])
    await deliver(scenario, social_event(ALICE, 7, "1" * 64))
    await deliver(scenario, social_event(ALICE, 6, "2" * 64))
    assert await active_members() == {BOB}
    assert await crud.get_engagement_kinds(ALICE, OWNER) == []
    before = len(scenario.announcements)
    service = headbutt.EnhancedHeadbuttService(db=crud, user_id=OWNER)
    result = await service.attempt_headbutt(
        headbutt._Attacker(ALICE, 101, kinds=[9735], note_id=NOTE, event_id="3" * 64)
    )
    assert result and await active_members() == {ALICE}
    member = await crud.get_member_by_pubkey(OWNER, ALICE)
    assert member["amount"] == 101 and member["engagement_kinds"] == [6, 7]
    assert shares.member_point_units(member, 10) == 1050
    assert len(scenario.announcements) == before + 1
    await asyncio.gather(
        *(deliver(scenario, social_event(ALICE, 7, str(i) * 64)) for i in (4, 5))
    )
    assert (
        shares.member_point_units(await crud.get_member_by_pubkey(OWNER, ALICE), 10)
        == 1050
    )
    assert len(scenario.announcements) == before + 1


@pytest.mark.anyio
async def test_old_observation_does_not_cross_reset(scenario):
    await crud.update_and_activate_member(BOB, 100, OWNER, kinds=[9735])
    await deliver(scenario, social_event(ALICE, 7, "1" * 64))
    await treats.cycle(OWNER, reset=True)
    await crud.update_and_activate_member(ALICE, 10, OWNER, kinds=[9735])
    assert await crud.get_engagement_kinds(ALICE, OWNER) == []
    # An old event cannot be entered into the new cycle after reset either.
    assert not await crud.observe_engagement(
        ALICE, 7, "4" * 64, NOTE, int(time.time()) - 60, OWNER
    )


@pytest.mark.anyio
async def test_recovered_processed_failure_silently_credits_active_member(scenario):
    await crud.update_and_activate_member(ALICE, 0, OWNER, kinds=[6])
    like = social_event(ALICE, 7, "1" * 64)
    await crud.register_processed_event(
        OWNER, like["id"], note_id=NOTE, pubkey=ALICE, event_type="headbutt_failure"
    )
    await deliver(scenario, like)
    assert await crud.get_engagement_kinds(ALICE, OWNER) == [6, 7]
    assert scenario.announcements == []


@pytest.mark.anyio
async def test_wrong_note_and_invalid_signature_do_not_leave_observations(
    scenario, monkeypatch
):
    from lnbits.extensions.cyberherd.services import subscriptions

    event = social_event(ALICE, 7, "1" * 64)
    monkeypatch.setattr(
        subscriptions.nostr_helpers, "verify_event_signature", lambda event: False
    )
    await deliver(scenario, event)
    rows = await crud.db.fetchall(
        f"SELECT * FROM {crud.db.references_schema}engagement_observations"
    )
    assert rows == []
    monkeypatch.setattr(
        subscriptions.nostr_helpers, "verify_event_signature", lambda event: True
    )
    event["tags"] = [["e", "d" * 64]]
    await deliver(scenario, event)
    rows = await crud.db.fetchall(
        f"SELECT * FROM {crud.db.references_schema}engagement_observations"
    )
    assert rows == []


@pytest.mark.anyio
async def test_failed_activation_cannot_promote_observations(scenario, monkeypatch):
    await crud.observe_engagement(ALICE, 7, "1" * 64, NOTE, int(time.time()), OWNER)
    original = crud._AdmissionConnection.execute

    async def fail(self, query, values=None):
        if "INSERT INTO" in query and "cyber_herd" in query:
            raise RuntimeError("activation failed")
        return await original(self, query, values)

    monkeypatch.setattr(crud._AdmissionConnection, "execute", fail)
    with pytest.raises(RuntimeError, match="activation failed"):
        await crud.add_new_active_member(
            {
                "pubkey": ALICE,
                "lud16": "alice@example.test",
                "amount": 10,
                "kinds": [9735],
            },
            OWNER,
        )
    assert await crud.get_engagement_kinds(ALICE, OWNER) == []
