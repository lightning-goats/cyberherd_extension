"""Stream discovery, social credit and durable chat routing without live effects."""

import time
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from pynostr.event import Event
from pynostr.key import PrivateKey

from lnbits.extensions.cyberherd import crud
from lnbits.extensions.cyberherd.services import streams, subscriptions, treats
from lnbits.extensions.cyberherd.services.note_metadata import extract_30311_address
from lnbits.extensions.cyberherd.tests.test_database_operations import (
    add_member,
    save_settings,
)
from lnbits.extensions.cyberherd.tests.test_database_operations import (
    audit_db as _audit_db,
)
from lnbits.extensions.cyberherd.tests.test_treats import PUBKEY, ROOT, rows
from lnbits.extensions.cyberherd_messaging import services as messaging

audit_db = _audit_db
# Database-owned fixture schema only.
# ruff: noqa: S608
HOST_KEY = "1" * 64
PROVIDER_KEY = "2" * 64
HOST = PrivateKey(bytes.fromhex(HOST_KEY)).public_key.hex()
PROVIDER = PrivateKey(bytes.fromhex(PROVIDER_KEY)).public_key.hex()
SERVICE = "https://api-uk.zap.stream/api/v1"


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture(autouse=True)
def provider_cache(monkeypatch):
    monkeypatch.setattr(
        streams, "_provider_keys", {SERVICE: (PROVIDER, time.time() + 1800)}
    )


def stream_event(status="live", created_at=None, d="stream-one", key=PROVIDER_KEY):
    event = Event(
        kind=30311,
        content="",
        created_at=int(time.time()) if created_at is None else created_at,
        tags=[
            ["d", d],
            ["status", status],
            ["t", "cyberherd"],
            ["p", HOST, "", "host"],
            ["service", SERVICE],
        ],
    )
    event.sign(key)
    return event.to_dict()


async def tracked_stream():
    await save_settings(
        nostr_pubkey_override=HOST,
        computed_effective_pubkey=HOST,
        herd_wallet="herd",
        source_wallet="source",
    )
    event = stream_event()
    assert await subscriptions._append_today("owner", HOST, ["cyberherd"], event)
    return event, await crud.get_settings("owner")


@pytest.mark.anyio
async def test_provider_authorization_requires_official_key_or_host_proof(monkeypatch):
    assert await streams.authenticate(stream_event(), HOST)
    forged = stream_event(key="3" * 64)
    monkeypatch.setattr(streams, "_provider_keys", {})
    client = AsyncMock()
    client.get.return_value = SimpleNamespace(
        raise_for_status=lambda: None, json=lambda: {"nostrPubkey": PROVIDER}
    )
    manager = AsyncMock()
    manager.__aenter__.return_value = client
    monkeypatch.setattr(streams.httpx, "AsyncClient", lambda **kwargs: manager)
    assert not await streams.authenticate(forged, HOST)
    assert await streams.authenticate(stream_event(), HOST)
    assert client.get.await_args.args[0].startswith(
        "https://api-uk.zap.stream/.well-known/lnurlp/"
    )
    assert not await streams.authenticate(
        stream_event(created_at=int(time.time()) + 600), HOST
    )


@pytest.mark.anyio
async def test_address_refresh_does_not_regress_to_older_live_version(audit_db):
    event, settings = await tracked_stream()
    address = extract_30311_address(event)
    ended = stream_event("ended", event["created_at"] + 1)
    await subscriptions._append_today("owner", HOST, ["cyberherd"], ended)
    await subscriptions._append_today("owner", HOST, ["cyberherd"], event)
    assert (await streams.lookup("owner", address))["id"] == ended["id"]
    assert await streams.active("owner") is None
    assert not await subscriptions._is_tracked_event(
        "owner", event["id"], settings, None
    )


@pytest.mark.anyio
async def test_address_only_repost_uses_shared_repost_award(audit_db, monkeypatch):
    event, settings = await tracked_stream()
    address = extract_30311_address(event)
    await add_member(pubkey=PUBKEY, amount=21)
    action = Event(
        kind=16,
        content="",
        tags=[["a", address], ["k", "30311"]],
        created_at=int(time.time()),
    )
    action.sign("4" * 64)
    pubkey = action.pubkey
    await add_member(pubkey=pubkey, amount=21)
    trigger = AsyncMock(return_value={"updated": pubkey})
    monkeypatch.setattr(subscriptions, "_trigger_repost_headbutt", trigger)
    assert await subscriptions.process_event_for_user(
        "owner", action.to_dict(), settings, None
    )
    observed = await crud.db.fetchall(
        f"SELECT kind FROM {crud.db.references_schema}engagement_observations "
        "WHERE pubkey=:pubkey",
        {"pubkey": pubkey},
    )
    assert [r["kind"] for r in observed] == [6]
    assert (
        trigger.await_args.kwargs.get("reposted_event_id", event["id"]) == event["id"]
    )


@pytest.mark.anyio
async def test_filters_use_or_for_event_and_stable_address(audit_db):
    event, settings = await tracked_stream()
    filters = streams.engagement_filters(settings, [6, 7], 100, 200)
    assert filters[0]["kinds"] == [6, 7, 16]
    assert filters[0]["#e"] == [event["id"]] and "#a" not in filters[0]
    assert filters[1]["#a"] == [extract_30311_address(event)] and "#e" not in filters[1]


@pytest.mark.anyio
async def test_member_chat_branch_and_treat_keep_joining_conversation(
    audit_db, monkeypatch
):
    event, _settings = await tracked_stream()
    address = extract_30311_address(event)
    await add_member(pubkey=PUBKEY, lud16="alice@example.test", note=event["id"])
    await treats.queue_member(
        "new_member",
        "owner",
        {"member_pubkey": PUBKEY, "source_event_id": "join"},
        "herd",
        [event["id"]],
        "cyberherd",
    )
    first = (await rows("message"))[0]
    thread = await treats.record(first["data"]["thread"])
    assert thread["data"]["stream_address"] == address
    thread["data"]["event_id"] = ROOT
    await treats.put(thread["id"], "owner", "thread", "submitted", thread["data"])
    await treats.queue_member(
        "member_increase",
        "owner",
        {"member_pubkey": PUBKEY, "source_event_id": "note-zap"},
        "herd",
        ["b" * 64],
        "cyberherd",
    )
    assert len(await rows("thread")) == 2
    monkeypatch.setattr(
        "lnbits.extensions.cyberherd.utils.common.is_extension_enabled_for_user",
        AsyncMock(return_value=True),
    )
    await treats.prepare_distribution(
        SimpleNamespace(wallet_id="source", payment_hash="funding", amount=1000000),
        [SimpleNamespace(wallet="alice@example.test", percent=10, alias="Alice")],
    )
    recipient = (await rows("treat"))[0]
    assert recipient["data"]["thread"] == first["data"]["thread"]
    recipient["state"] = "paid"
    recipient["data"]["amount_msat"] = 10000
    await treats._queue_treat(recipient)
    notice = next(
        r
        for r in await rows("message")
        if r["data"]["event_type"] == "cyber_herd_treats"
    )
    monkeypatch.setattr(
        messaging, "render_member_message", AsyncMock(return_value="Treats")
    )
    publish = AsyncMock()
    monkeypatch.setattr(messaging, "publish_note", publish)
    await treats._publish(notice)
    assert publish.await_args.kwargs["reply_to_30311_a_tag"] == address
    assert publish.await_args.kwargs["reply_to_30311_event"] == ROOT


@pytest.mark.anyio
async def test_one_feeder_notice_fixes_chat_at_queue_time(audit_db):
    event, _ = await tracked_stream()
    await treats.queue_feeder(
        "owner",
        "herd",
        "deposit",
        {"new_amount": 2345, "feedings": 2, "payout_cycles": 2},
        "cyberherd",
    )
    await subscriptions._append_today(
        "owner", HOST, ["cyberherd"], stream_event(d="stream-two")
    )
    await treats.queue_feeder(
        "owner",
        "herd",
        "deposit",
        {"new_amount": 2345, "feedings": 2, "payout_cycles": 2},
        "cyberherd",
    )
    notices = await rows("message")
    assert len(notices) == 1
    assert notices[0]["data"]["stream_address"] == extract_30311_address(event)


@pytest.mark.anyio
async def test_publisher_emits_live_chat_kind_and_parent(monkeypatch):
    monkeypatch.setattr(
        messaging, "is_nostr_publishing_enabled", AsyncMock(return_value=True)
    )
    sign_publish = AsyncMock(return_value=True)
    monkeypatch.setattr(messaging, "_try_bunker_sign_and_publish", sign_publish)
    address = f"30311:{PROVIDER}:stream-one"
    await messaging.publish_note(
        "Chat",
        reply_to_30311_a_tag=address,
        reply_to_30311_event=ROOT,
        p_tags=[PUBKEY],
        wallet_id="herd",
        user_id="owner",
    )
    assert sign_publish.await_args.args[2] == 1311
    assert ["a", address, "", "root"] in sign_publish.await_args.args[3]
    assert ["e", ROOT, "", "reply"] in sign_publish.await_args.args[3]


@pytest.mark.anyio
async def test_host_proof_authenticates_other_provider_without_network(monkeypatch):
    import hashlib

    event = stream_event(key="3" * 64)
    address = extract_30311_address(event)
    proof = PrivateKey(bytes.fromhex(HOST_KEY)).sign(
        hashlib.sha256(address.encode()).digest()
    )
    if isinstance(proof, bytes):
        proof = proof.hex()
    unsigned = {k: event[k] for k in ["kind", "tags", "content", "created_at"]}
    unsigned["tags"] = [t for t in event["tags"] if t[0] not in ("p", "service")]
    unsigned["tags"].append(["p", HOST, "", "Host", proof])
    signed = Event(**unsigned)
    signed.sign("3" * 64)
    assert await streams.authenticate(signed.to_dict(), HOST)
    unsigned["tags"][-1][-1] = "0" * 128
    forged = Event(**unsigned)
    forged.sign("3" * 64)
    assert not await streams.authenticate(forged.to_dict(), HOST)


@pytest.mark.anyio
async def test_stream_refresh_and_note_actions_award_social_kinds_once(
    audit_db, monkeypatch
):
    from lnbits.extensions.cyberherd.services.shares import member_point_units

    event, _settings = await tracked_stream()
    actor_key = "4" * 64
    actor = PrivateKey(bytes.fromhex(actor_key)).public_key.hex()
    await add_member(pubkey=actor, amount=21)
    address = extract_30311_address(event)

    async def credit_repost(owner, pubkey, note_id, event_id, app, **kwargs):
        await crud.update_and_activate_member(pubkey, 0, owner, kinds=[6])
        return {"updated": pubkey}

    monkeypatch.setattr(subscriptions, "_trigger_repost_headbutt", credit_repost)
    for index in range(2):
        update = stream_event(created_at=event["created_at"] + index)
        await subscriptions._append_today("owner", HOST, ["cyberherd"], update)
        action = Event(
            kind=16,
            content="",
            tags=[["a", address]],
            created_at=int(time.time()) + index,
        )
        action.sign(actor_key)
        assert await subscriptions.process_event_for_user(
            "owner", action.to_dict(), await crud.get_settings("owner"), None
        )
    member = await crud.get_cyberherd_member_by_pubkey(actor, "owner")
    assert member["engagement_kinds"] == [6]
    assert member_point_units(member, 10) == 240  # 21 zap points plus 3 repost points.

    note = Event(
        kind=1,
        content="#CyberHerd",
        tags=[["t", "cyberherd"]],
        created_at=int(time.time()),
    )
    note.sign(HOST_KEY)
    await subscriptions._append_today("owner", HOST, ["cyberherd"], note.to_dict())

    async def query(filters, **kwargs):
        return [note.to_dict()] if filters.get("ids") == [note.id] else []

    monkeypatch.setattr(subscriptions.nostr_helpers, "query_events", query)
    monkeypatch.setattr(subscriptions.nostr_helpers, "check_availability", lambda: True)

    async def credit_like(owner, pubkey, note_id, event_id, app, **kwargs):
        await crud.update_and_activate_member(pubkey, 0, owner, kinds=[7])
        return {"updated": pubkey}

    monkeypatch.setattr(subscriptions, "_trigger_reaction_headbutt", credit_like)
    like = Event(
        kind=7, content="+", tags=[["e", note.id]], created_at=int(time.time())
    )
    like.sign(actor_key)
    assert await subscriptions.process_event_for_user(
        "owner", like.to_dict(), await crud.get_settings("owner"), None
    )
    member = await crud.get_cyberherd_member_by_pubkey(actor, "owner")
    assert member["engagement_kinds"] == [6, 7]
    assert member_point_units(member, 10) == 250


@pytest.mark.anyio
@pytest.mark.parametrize("existing", [False, True])
async def test_settled_address_only_stream_zap_uses_host_recipient(
    audit_db, monkeypatch, existing
):
    from lnbits.extensions.cyberherd.services import payment_coordinator

    event, settings = await tracked_stream()
    actor = PrivateKey(bytes.fromhex("4" * 64)).public_key.hex()
    if existing:
        await add_member(pubkey=actor, amount=21)
    request = Event(
        kind=9734,
        content="",
        tags=[["a", extract_30311_address(event)], ["p", HOST], ["amount", "21000"]],
        created_at=int(time.time()),
    )
    request.sign("4" * 64)
    payment = SimpleNamespace(
        wallet_id="herd",
        amount=21000,
        success=True,
        payment_hash="d" * 64,
        checking_id="d" * 64,
        extra={"nostr": request.to_dict()},
    )
    trigger = AsyncMock(return_value={"status": "new"})
    monkeypatch.setattr(payment_coordinator, "trigger_headbutt_from_zap", trigger)
    monkeypatch.setattr(
        "lnbits.extensions.cyberherd.services.leaderboard.broadcast_leaderboard_update",
        AsyncMock(),
    )
    monkeypatch.setattr(
        subscriptions.nostr_helpers, "query_events", AsyncMock(return_value=[event])
    )
    monitor = payment_coordinator.PaymentCoordinatorService(user_id="owner")
    assert await monitor._process_payment_for_zap(payment, settings_override=settings)
    assert trigger.await_args.kwargs["note_id"] == event["id"]
    assert trigger.await_args.kwargs["amount_sats"] == 21
    trigger.reset_mock()
    payment.success = False
    assert not await monitor._process_payment_for_zap(
        payment, settings_override=settings
    )
    trigger.assert_not_awaited()


@pytest.mark.anyio
async def test_provider_outage_keeps_settled_zap_retryable(audit_db, monkeypatch):
    import httpx

    from lnbits.extensions.cyberherd.services import payment_coordinator

    event, settings = await tracked_stream()
    monkeypatch.setattr(streams, "_provider_keys", {})
    client = AsyncMock()
    client.get.side_effect = httpx.ConnectError("unavailable")
    manager = AsyncMock()
    manager.__aenter__.return_value = client
    monkeypatch.setattr(streams.httpx, "AsyncClient", lambda **kwargs: manager)
    request = Event(
        kind=9734,
        content="",
        tags=[["a", extract_30311_address(event)], ["p", HOST]],
        created_at=int(time.time()),
    )
    request.sign("4" * 64)
    payment = SimpleNamespace(
        wallet_id="herd",
        amount=21000,
        success=True,
        payment_hash="d" * 64,
        extra={"nostr": request.to_dict()},
    )
    trigger = AsyncMock()
    monkeypatch.setattr(payment_coordinator, "trigger_headbutt_from_zap", trigger)
    monitor = payment_coordinator.PaymentCoordinatorService(user_id="owner")
    assert not await monitor._process_payment_for_zap(
        payment, settings_override=settings
    )
    assert monitor.last_error == "retryable_processing"
    assert not await crud.is_payment_processed("owner", payment.payment_hash)
    trigger.assert_not_awaited()


@pytest.mark.anyio
async def test_evening_chat_only_reuses_a_recent_authenticated_ended_stream(audit_db):
    await save_settings(nostr_pubkey_override=HOST, computed_effective_pubkey=HOST)
    old = stream_event(status="ended", created_at=int(time.time()) - 1000, d="old")
    await streams.remember("owner", old)
    assert await streams.closing_chat("owner") is None
    forged = stream_event(status="ended", key="3" * 64, d="forged")
    await streams.remember("owner", forged)
    assert await streams.closing_chat("owner") is None
    recent = stream_event(status="ended", created_at=int(time.time()) - 30)
    await streams.remember("owner", recent)
    assert (await streams.closing_chat("owner"))["id"] == recent["id"]
    assert await streams.active("owner") is None
