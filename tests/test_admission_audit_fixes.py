"""Regression coverage for audited admission and API boundaries."""
from types import SimpleNamespace
from unittest.mock import AsyncMock
import pytest
from fastapi import HTTPException
from lnbits.extensions.cyberherd import crud, views_api
from lnbits.extensions.cyberherd.services import headbutt, subscriptions
from test_social_event_replay import OWNER, NOTE, ALICE, BOB, active_members, scenario, social_db, anyio_backend


REAL_TODAY_NOTES = headbutt.EnhancedHeadbuttService._get_today_cyberherd_notes


def service():
    return headbutt.EnhancedHeadbuttService(db=crud, messaging_module=SimpleNamespace(), user_id=OWNER)


def attacker(amount=0, kind=6):
    return headbutt._Attacker(ALICE, amount, kinds=[kind], note_id=NOTE, event_id="1"*64)


@pytest.mark.anyio
@pytest.mark.parametrize("kind,amount", [(6,0),(9735,101)])
async def test_inactive_placeholder_must_satisfy_fresh_identity(scenario, monkeypatch, kind, amount):
    scenario.settings.nip05_verification_enabled=True
    await crud.update_and_activate_member(BOB,100 if kind == 9735 else 0,OWNER,kinds=[9735] if kind == 9735 else [7])
    monkeypatch.setattr(headbutt.EnhancedHeadbuttService,"_resolve_attacker_metadata",AsyncMock(return_value=({},"test")))
    assert await service().attempt_headbutt(attacker(amount,kind)) is None
    assert await active_members()=={BOB}


@pytest.mark.anyio
@pytest.mark.parametrize("address", [None,"not-an-address","https://name@example.test","a@@example.test","a@localhost","a@exa mple.test"])
async def test_social_entry_rejects_missing_or_malformed_address(scenario, monkeypatch, address):
    await scenario.db.execute(f"UPDATE {scenario.db.references_schema}cyber_herd SET lud16=:address WHERE pubkey=:pubkey", {"address":address,"pubkey":ALICE})
    monkeypatch.setattr(headbutt.EnhancedHeadbuttService,"_resolve_attacker_metadata",AsyncMock(return_value=({},"test")))
    assert await service().attempt_headbutt(attacker()) is None
    assert await active_members()==set()


@pytest.mark.anyio
async def test_below_minimum_zap_cannot_displace_social_member(scenario):
    await crud.update_and_activate_member(BOB,0,OWNER,kinds=[7])
    assert await service().attempt_headbutt(attacker(1,9735)) is None
    assert await active_members()=={BOB}


@pytest.mark.anyio
async def test_active_member_can_add_one_sat(scenario):
    await crud.update_and_activate_member(ALICE,10,OWNER,kinds=[9735])
    assert (await service().attempt_headbutt(attacker(1,9735)))["status"]=="updated"
    assert (await crud.get_cyberherd_member_by_pubkey(ALICE,OWNER))["amount"]==11


@pytest.mark.anyio
async def test_query_miss_cannot_authorize_tracked_note_admission(scenario, monkeypatch):
    monkeypatch.setattr(headbutt.EnhancedHeadbuttService,"_get_today_cyberherd_notes",AsyncMock(return_value=[]))
    scenario.settings.tracked_event_timestamps={NOTE:1}
    assert await service().attempt_headbutt(attacker()) is None
    assert await active_members()==set()


@pytest.mark.anyio
async def test_explicit_id_is_not_proof_of_today_author_and_tags(scenario, monkeypatch):
    monkeypatch.setattr(subscriptions.nostr_helpers,"check_availability",lambda:True)
    monkeypatch.setattr(subscriptions.nostr_helpers,"query_events",AsyncMock(return_value=[]))
    scenario.settings.tracked_event_timestamps={NOTE:1}
    assert not await subscriptions._is_tracked_event(OWNER,NOTE,scenario.settings,SimpleNamespace(state=SimpleNamespace()))


@pytest.mark.anyio
async def test_negative_manual_amount_has_no_membership_effect(scenario):
    with pytest.raises(HTTPException) as exc:
        await views_api.api_put_member(SimpleNamespace(),ALICE,views_api.MemberPut(amount=-1),SimpleNamespace(wallet=SimpleNamespace(user=OWNER)))
    assert exc.value.status_code==400
    assert await active_members()==set()


def test_prune_endpoint_is_reachable_before_member_route():
    scope={"type":"http","method":"DELETE","path":"/api/v1/members/prune_without_lud16","root_path":""}
    matches=[r for r in views_api.cyberherd_api_router.routes if str(r.matches(scope)[0])=="Match.FULL"]
    assert matches[0].endpoint is views_api.api_delete_members_without_lud16


@pytest.mark.anyio
async def test_marker_failure_rolls_back_open_slot_admission(scenario, monkeypatch):
    monkeypatch.setattr(crud,"register_processed_event",AsyncMock(side_effect=RuntimeError("marker failure")))
    with pytest.raises(RuntimeError,match="marker failure"):
        await service().attempt_headbutt(attacker())
    assert await active_members()==set()


@pytest.mark.anyio
async def test_marker_failure_rolls_back_victim_and_attacker(scenario, monkeypatch):
    await crud.update_and_activate_member(BOB,0,OWNER,kinds=[7])
    monkeypatch.setattr(crud,"register_processed_event",AsyncMock(side_effect=RuntimeError("marker failure")))
    with pytest.raises(RuntimeError,match="marker failure"):
        await service().attempt_headbutt(attacker())
    assert await active_members()=={BOB}


@pytest.mark.anyio
async def test_shares_fallback_uses_configured_budget_and_engagement(scenario):
    scenario.settings.member_allocation_percent=20
    scenario.settings.zap_wallet="goats-wallet"
    await crud.update_and_activate_member(ALICE,100,OWNER,kinds=[9735])
    await crud.update_and_activate_member(BOB,0,OWNER,kinds=[7])
    result=await views_api.api_get_shares(SimpleNamespace(), {"type":"admin","value":SimpleNamespace(id=OWNER)})
    assert result["allocation_model"]=="zap+members"
    assert {s.get("pubkey") or "goats":s["percent"] for s in result["shares"]}=={ALICE:19.0, BOB:1.0,"goats":80.0}


@pytest.mark.anyio
async def test_settings_reject_budget_below_enabled_engagement_capacity(scenario, monkeypatch):
    scenario.settings.max_members=3
    scenario.settings.member_allocation_percent=10
    monkeypatch.setattr(crud,"upsert_settings",AsyncMock())
    with pytest.raises(HTTPException) as exc:
        await views_api.api_put_settings(SimpleNamespace(), {"member_allocation_percent":1}, SimpleNamespace(wallet=SimpleNamespace(user=OWNER)))
    assert exc.value.status_code==400


@pytest.mark.anyio
@pytest.mark.parametrize("defect", ["age", "author", "tag", "kind"])
async def test_daily_note_query_independently_checks_relay_metadata(scenario, monkeypatch, defect):
    from lnbits.extensions.cyberherd.services.time_utils import get_day_boundaries_utc
    scenario.settings.nostr_pubkey_override="f"*64
    scenario.settings.tracked_tags=["cyberherd"]
    note={"id":NOTE,"pubkey":"f"*64,"kind":1,"created_at":get_day_boundaries_utc().local_since_ts+1,"tags":[["t","cyberherd"]]}
    if defect=="age": note["created_at"]=1
    if defect=="author": note["pubkey"]="d"*64
    if defect=="tag": note["tags"]=[["t","other"]]
    if defect=="kind": note["kind"]=7
    monkeypatch.setattr(headbutt.nostr_helpers,"query_events",AsyncMock(return_value=[note]))
    # scenario replaces the method; exercise the implementation saved below.
    candidate=service()
    candidate.app=SimpleNamespace()
    assert await REAL_TODAY_NOTES(candidate)==[]


@pytest.mark.anyio
@pytest.mark.parametrize("payload", [{"max_members":11},{"repost_tracking_enabled":True,"member_allocation_percent":1}])
async def test_settings_validate_merged_capacity_not_only_allocation_field(scenario, payload):
    scenario.settings.max_members=3
    scenario.settings.member_allocation_percent=10
    with pytest.raises(HTTPException) as exc:
        await views_api.api_put_settings(SimpleNamespace(),payload,SimpleNamespace(wallet=SimpleNamespace(user=OWNER)))
    assert exc.value.status_code==400


@pytest.mark.anyio
async def test_after_commit_tasks_run_after_admission_lock_is_released(scenario, monkeypatch):
    observed = []

    async def observe_lock(self, *args, **kwargs):
        observed.append(headbutt._get_headbutt_lock().locked())

    monkeypatch.setattr(headbutt.EnhancedHeadbuttService, "_post_admission_tasks", observe_lock)
    assert (await service().attempt_headbutt(attacker()))["status"] == "reactivated"
    assert observed == [False]
