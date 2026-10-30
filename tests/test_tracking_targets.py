"""Canonical current-broadcast targets retain aliases without growing the UI list."""

import time
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from pynostr.event import Event
from pynostr.key import PrivateKey

from lnbits.extensions.cyberherd import crud
from lnbits.extensions.cyberherd.services import subscriptions, tracking_targets
from lnbits.extensions.cyberherd.tests.test_database_operations import (
    add_member,
    save_settings,
)
from lnbits.extensions.cyberherd.tests.test_database_operations import (
    audit_db as _audit_db,
)
from lnbits.extensions.cyberherd.tests.test_stream_integration import (
    HOST,
    HOST_KEY,
    stream_event,
)
from lnbits.extensions.cyberherd.tests.test_stream_integration import (
    provider_cache as _provider_cache,
)

audit_db = _audit_db
provider_cache = _provider_cache
# Database-owned disposable schema only.
# ruff: noqa: S608


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture(autouse=True)
def target_state(monkeypatch, provider_cache):
    monkeypatch.setattr(tracking_targets, "_known_goal_ids", set())
    monkeypatch.setattr(subscriptions, "_user_update_locks", {})
    monkeypatch.setattr(subscriptions, "_user_locks_mutex", None)


def sign(kind, tags, content="", created_at=None, key=HOST_KEY):
    event = Event(
        kind=kind,
        tags=tags,
        content=content,
        created_at=int(time.time()) if created_at is None else created_at,
    )
    event.sign(key)
    return event.to_dict()


async def broadcast(monkeypatch):
    await save_settings(
        nostr_pubkey_override=HOST,
        computed_effective_pubkey=HOST,
        herd_wallet="herd",
        source_wallet="source",
    )
    goal = sign(
        9041, [["amount", "10000000"], ["relays", "wss://nos.lol"]], "Feed goats"
    )
    note = sign(
        1,
        [["t", "cyberherd"], ["goal", goal["id"]]],
        "#CyberHerd",
        created_at=int(time.time()) - 10,
    )

    async def query(filters, **kwargs):
        return [goal] if filters.get("ids") == [goal["id"]] else []

    monkeypatch.setattr(subscriptions.nostr_helpers, "query_events", query)
    assert await subscriptions._append_today("owner", HOST, ["cyberherd"], note)
    events = []
    for i in range(10):
        event = stream_event(created_at=int(time.time()) + i)
        tags = [*event["tags"], ["goal", goal["id"]]]
        event = sign(30311, tags, created_at=event["created_at"], key="2" * 64)
        events.append(event)
        assert await subscriptions._append_today("owner", HOST, ["cyberherd"], event)
    return note, events, goal


@pytest.mark.anyio
async def test_refresh_versions_collapse_to_three_verified_targets(
    audit_db, monkeypatch
):
    note, versions, goal = await broadcast(monkeypatch)
    settings = await crud.get_settings("owner")
    assert set(settings.tracked_event_ids) == {
        note["id"],
        versions[-1]["id"],
        goal["id"],
    }
    assert len(settings.tracked_event_addresses) == 10
    resolved, metadata = await subscriptions._resolve_tracked_event(
        settings, versions[0]["id"], None, None
    )
    assert resolved == versions[-1]["id"] and metadata["id"] == resolved
    assert len((await crud.get_settings("owner")).tracked_event_ids) == 3


@pytest.mark.anyio
async def test_unrelated_tagged_note_does_not_replace_broadcast_announcement(
    audit_db, monkeypatch
):
    note, versions, goal = await broadcast(monkeypatch)
    unrelated = sign(
        1,
        [["t", "cyberherd"]],
        "Other #CyberHerd note",
        created_at=int(time.time()) + 20,
    )
    assert await subscriptions._append_today("owner", HOST, ["cyberherd"], unrelated)
    settings = await crud.get_settings("owner")
    assert set(settings.tracked_event_ids) == {
        note["id"],
        versions[-1]["id"],
        goal["id"],
    }
    monkeypatch.setattr(
        subscriptions.nostr_helpers, "query_events", AsyncMock(return_value=[unrelated])
    )
    monkeypatch.setattr(subscriptions.nostr_helpers, "check_availability", lambda: True)
    assert not await subscriptions._is_tracked_event(
        "owner", unrelated["id"], settings, None
    )


@pytest.mark.anyio
async def test_goal_requires_verified_link_and_project_signature(audit_db, monkeypatch):
    _note, _versions, goal = await broadcast(monkeypatch)
    settings = await crud.get_settings("owner")
    unrelated = sign(
        9041, [["amount", "1000"], ["relays", "wss://nos.lol"]], "Another goal"
    )
    assert not await tracking_targets.accept_goal("owner", settings, unrelated)
    assert await tracking_targets.current_goal("owner", settings, goal["id"])
    assert await subscriptions._is_tracked_event("owner", goal["id"], settings, None)


@pytest.mark.anyio
@pytest.mark.parametrize("existing", [False, True])
async def test_settled_goal_zaps_are_eligible(audit_db, monkeypatch, existing):
    from lnbits.extensions.cyberherd.services import payment_coordinator

    _note, _versions, goal = await broadcast(monkeypatch)
    actor = PrivateKey(bytes.fromhex("4" * 64)).public_key.hex()
    if existing:
        await add_member(pubkey=actor, amount=21)
    request = sign(
        9734, [["e", goal["id"]], ["p", HOST], ["amount", "21000"]], key="4" * 64
    )
    payment = SimpleNamespace(
        wallet_id="herd",
        success=True,
        amount=21000,
        payment_hash="d" * 64,
        extra={"nostr": request},
    )
    trigger = AsyncMock(return_value={"status": "new"})
    monkeypatch.setattr(payment_coordinator, "trigger_headbutt_from_zap", trigger)
    monkeypatch.setattr(
        "lnbits.extensions.cyberherd.services.leaderboard.broadcast_leaderboard_update",
        AsyncMock(),
    )
    monkeypatch.setattr(
        subscriptions.nostr_helpers, "query_events", AsyncMock(return_value=[goal])
    )
    monitor = payment_coordinator.PaymentCoordinatorService(user_id="owner")
    assert await monitor._process_payment_for_zap(
        payment, settings_override=await crud.get_settings("owner")
    )
    assert trigger.await_args.kwargs["note_id"] == goal["id"]


@pytest.mark.anyio
async def test_goal_reposts_and_likes_use_existing_social_awards(audit_db, monkeypatch):
    _, _, goal = await broadcast(monkeypatch)
    actor = PrivateKey(bytes.fromhex("4" * 64)).public_key.hex()
    await add_member(pubkey=actor, amount=21)
    monkeypatch.setattr(
        subscriptions,
        "_trigger_repost_headbutt",
        AsyncMock(return_value={"updated": actor}),
    )
    monkeypatch.setattr(
        subscriptions,
        "_trigger_reaction_headbutt",
        AsyncMock(return_value={"updated": actor}),
    )
    for kind in [16, 7]:
        event = sign(
            kind, [["e", goal["id"]]], content="+" if kind == 7 else "", key="4" * 64
        )
        assert await subscriptions.process_event_for_user(
            "owner", event, await crud.get_settings("owner"), None
        )
    observations = await crud.db.fetchall(
        f"SELECT kind FROM {crud.db.references_schema}engagement_observations "
        "WHERE pubkey=:pubkey",
        {"pubkey": actor},
    )
    assert sorted(r["kind"] for r in observations) == [6, 7]
