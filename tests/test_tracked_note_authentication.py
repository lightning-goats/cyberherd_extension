"""Relay metadata cannot authenticate a tracked daily note without its signature."""
import hashlib
import json
from types import SimpleNamespace

import coincurve
import pytest

from lnbits.extensions.cyberherd import crud
from lnbits.extensions.cyberherd.models import CyberherdSettings
from lnbits.extensions.cyberherd.services import headbutt, messaging, nostr_helpers, subscriptions
from lnbits.extensions.cyberherd.services.time_utils import get_day_boundaries_utc
from test_social_event_replay import OWNER, social_db, anyio_backend

PROJECT = coincurve.PrivateKey(bytes.fromhex("16" * 32))
ACTOR = coincurve.PrivateKey(bytes.fromhex("27" * 32))


def signed_event(key, kind=1, tags=None, created_at=None, content="daily note"):
    event = {
        "pubkey": key.public_key_xonly.format().hex(),
        "created_at": created_at if created_at is not None else get_day_boundaries_utc().local_since_ts + 1,
        "kind": kind,
        "tags": tags if tags is not None else [["t", "cyberherd"]],
        "content": content,
    }
    payload = [0, event["pubkey"], event["created_at"], kind, event["tags"], content]
    event["id"] = hashlib.sha256(json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode()).hexdigest()
    event["sig"] = key.sign_schnorr(bytes.fromhex(event["id"])).hex()
    assert nostr_helpers.verify_event_signature(event)
    return event


@pytest.fixture
async def authenticated_notes(social_db, monkeypatch):
    note = signed_event(PROJECT)
    settings = CyberherdSettings(
        user_id=OWNER, midnight_reset_enabled=False, nip05_verification_enabled=False,
        nostr_pubkey_override=note["pubkey"], tracked_event_ids=[note["id"]],
        tracked_tags=["cyberherd"],
    )
    relayed = [note]

    async def get_settings(*args, **kwargs):
        return settings

    async def query_events(*args, **kwargs):
        return relayed

    async def no_external_effects(*args, **kwargs):
        return True

    monkeypatch.setattr(crud, "get_settings", get_settings)
    monkeypatch.setattr(nostr_helpers, "check_availability", lambda: True)
    monkeypatch.setattr(nostr_helpers, "query_events", query_events)
    monkeypatch.setattr(headbutt.EnhancedHeadbuttService, "_post_admission_tasks", no_external_effects)
    monkeypatch.setattr(headbutt.EnhancedHeadbuttService, "_maybe_publish_new_member", no_external_effects)
    monkeypatch.setattr(messaging, "publish_event_message", no_external_effects)
    pubkey = ACTOR.public_key_xonly.format().hex()
    await social_db.execute(
        f"INSERT INTO {social_db.references_schema}cyber_herd (pubkey,user_id,lud16,display_name) VALUES (:pubkey,:user_id,'actor@example.test','Actor')",
        {"pubkey": pubkey, "user_id": OWNER},
    )
    return SimpleNamespace(note=note, settings=settings, relayed=relayed, actor=pubkey, app=SimpleNamespace(state=SimpleNamespace()))


@pytest.mark.anyio
@pytest.mark.parametrize("tamper", ["stale_to_today", "signature", "other_valid_id", "valid"])
async def test_real_signed_reaction_requires_authenticated_daily_note(authenticated_notes, tamper):
    fixture = authenticated_notes
    note = fixture.note
    if tamper == "stale_to_today":
        note = signed_event(PROJECT, created_at=get_day_boundaries_utc().local_since_ts - 86400)
        fixture.settings.tracked_event_ids = [note["id"]]
        forged = dict(note, created_at=get_day_boundaries_utc().local_since_ts + 1)
        assert not nostr_helpers.verify_event_signature(forged)
        fixture.relayed[:] = [forged]
    elif tamper == "signature":
        fixture.relayed[:] = [dict(note, sig="00" * 64)]
    elif tamper == "other_valid_id":
        fixture.relayed[:] = [signed_event(PROJECT, content="different signed note")]
    reaction = signed_event(ACTOR, kind=7, tags=[["e", note["id"]]], content="+")
    assert nostr_helpers.verify_event_signature(reaction)
    await subscriptions.process_event_for_user(OWNER, reaction, fixture.settings, fixture.app)
    active = await crud.get_active_cyberherd_members(OWNER)
    assert {member["pubkey"] for member in active} == ({fixture.actor} if tamper == "valid" else set())
    assert await crud.is_event_processed(OWNER, reaction["id"], note_id=note["id"]) is (tamper == "valid")


@pytest.mark.anyio
@pytest.mark.parametrize("tamper", ["stale_to_today", "signature", "other_valid_id"])
async def test_explicit_id_query_authenticates_signature_and_requested_id(authenticated_notes, tamper):
    fixture = authenticated_notes
    if tamper == "stale_to_today":
        old = signed_event(PROJECT, created_at=get_day_boundaries_utc().local_since_ts - 86400)
        fixture.note = old
        fixture.settings.tracked_event_ids = [old["id"]]
        fixture.relayed[:] = [dict(old, created_at=get_day_boundaries_utc().local_since_ts + 1)]
    elif tamper == "signature":
        fixture.relayed[:] = [dict(fixture.note, sig="00" * 64)]
    else:
        fixture.relayed[:] = [signed_event(PROJECT, content="different signed note")]
    assert not await subscriptions._is_tracked_event(OWNER, fixture.note["id"], fixture.settings, fixture.app)


@pytest.mark.anyio
@pytest.mark.parametrize("tamper", ["stale_to_today", "signature"])
async def test_daily_query_and_cache_ingestion_reject_forged_metadata(authenticated_notes, tamper):
    fixture = authenticated_notes
    note = fixture.note
    if tamper == "stale_to_today":
        note = signed_event(PROJECT, created_at=get_day_boundaries_utc().local_since_ts - 86400)
        note = dict(note, created_at=get_day_boundaries_utc().local_since_ts + 1)
    else:
        note = dict(note, sig="00" * 64)
    fixture.relayed[:] = [note]
    service = headbutt.EnhancedHeadbuttService(db=crud, messaging_module=messaging, user_id=OWNER, app=fixture.app)
    assert await service._get_today_cyberherd_notes() == []
    assert not subscriptions._would_event_be_tracked(note, fixture.settings.nostr_pubkey_override, ["cyberherd"])
    assert not await subscriptions._append_today(None, fixture.settings.nostr_pubkey_override, ["cyberherd"], note)


@pytest.mark.anyio
@pytest.mark.parametrize("mismatch", ["author", "kind", "d_tag", "signature", "valid"])
async def test_address_resolution_authenticates_requested_coordinate(authenticated_notes, mismatch):
    fixture = authenticated_notes
    key = ACTOR if mismatch == "author" else PROJECT
    kind = 1 if mismatch == "kind" else 30311
    d_tag = "different" if mismatch == "d_tag" else "daily"
    note = signed_event(key, kind=kind, tags=[["t", "cyberherd"], ["d", d_tag]])
    if mismatch == "signature":
        note = dict(note, sig="00" * 64)
    fixture.relayed[:] = [note]
    coordinate = "30311:" + PROJECT.public_key_xonly.format().hex() + ":daily"
    resolved, _ = await subscriptions._resolve_tracked_event(fixture.settings, coordinate, {}, fixture.app)
    assert resolved == (note["id"] if mismatch == "valid" else None)
