"""Treat distribution contracts: disposable databases and mocked external effects."""

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from lnbits.extensions.cyberherd import crud, tasks
from lnbits.extensions.cyberherd.services import send_splits, treats
from lnbits.extensions.cyberherd.tests.test_database_operations import (
    add_member,
    save_settings,
)
from lnbits.extensions.cyberherd.tests.test_database_operations import (
    audit_db as _audit_db,
)
from lnbits.extensions.cyberherd_messaging import services as messages
from lnbits.extensions.lightning_goats import tasks as goat_tasks
from lnbits.extensions.splitpayments import tasks as split_tasks

# Queries only interpolate fixture-generated schema identifiers.
# ruff: noqa: S608
audit_db = _audit_db
PUBKEY = "a" * 64
ROOT = "b" * 64


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture
def enabled(monkeypatch):
    monkeypatch.setattr(
        "lnbits.extensions.cyberherd.utils.common.is_extension_enabled_for_user",
        AsyncMock(return_value=True),
    )


async def setup_distribution():
    await save_settings(herd_wallet="herd", source_wallet="source")
    await add_member(
        pubkey=PUBKEY,
        display_name="Alice",
        lud16="alice@example.test",
        kinds=[7],
        amount=0,
    )
    payment = SimpleNamespace(
        wallet_id="source",
        payment_hash="d" * 64,
        amount=1_000_000,
        extra={},
        memo="CyberHerd Distribution",
    )
    targets = [
        SimpleNamespace(wallet="alice@example.test", percent=0.16, alias="Alice"),
        SimpleNamespace(wallet="goats@example.test", percent=90, alias="Goats"),
    ]
    return payment, targets


async def rows(kind):
    result = await crud.db.fetchall(
        f"SELECT * FROM {crud.db.references_schema}delivery_records WHERE kind=:kind",
        {"kind": kind},
    )
    return [{**r, "data": treats.decode(r["payload"])} for r in result]


@pytest.mark.anyio
async def test_snapshot_skip_and_duplicate_event(audit_db, enabled, monkeypatch):
    payment, targets = await setup_distribution()
    monkeypatch.setattr(split_tasks, "get_targets", AsyncMock(return_value=targets[:1]))
    monkeypatch.setattr(split_tasks, "fee_reserve", lambda amount: 2000)
    invoice = AsyncMock()
    monkeypatch.setattr(split_tasks, "get_lnurl_invoice", invoice)
    await split_tasks.on_invoice_paid(payment)
    await split_tasks.on_invoice_paid(payment)
    invoice.assert_not_awaited()
    recipients = await rows("treat")
    assert len(recipients) == 1 and recipients[0]["state"] == "too_small"
    monkeypatch.setattr(treats, "_publish", AsyncMock())
    await treats.process_once()
    await treats.process_once()
    notices = [
        r
        for r in await rows("message")
        if r["data"]["event_type"] == "cyber_herd_treats_skipped"
    ]
    assert len(notices) == 1
    assert notices[0]["data"]["pubkey"] == PUBKEY
    assert (await rows("treat"))[0]["state"] == "announced"


@pytest.mark.anyio
async def test_frozen_recipient_and_pending_settlement(audit_db, enabled, monkeypatch):
    payment, targets = await setup_distribution()
    frozen, contexts = await treats.prepare_distribution(payment, targets)
    identifier = contexts[targets[0].wallet]
    await treats.outcome(identifier, "attempted", payment_hash="f" * 64)
    await crud.deactivate_cyberherd_member(PUBKEY, user_id="owner")
    replacement = [
        SimpleNamespace(wallet="wrong@example.test", percent=100, alias="Wrong")
    ]
    frozen, repeated = await treats.prepare_distribution(payment, replacement)
    assert [t.wallet for t in frozen] == [t.wallet for t in targets]
    assert repeated == contexts
    outgoing = SimpleNamespace(
        wallet_id="source", amount=-1000, success=False, failed=False
    )
    lookup = AsyncMock(return_value=outgoing)
    monkeypatch.setattr("lnbits.core.crud.get_standalone_payment", lookup)
    monkeypatch.setattr(treats, "_publish", AsyncMock())
    await treats.process_once()
    assert (await treats.record(identifier))["state"] == "attempted"
    outgoing.wallet_id = "wrong-wallet"
    outgoing.success = True
    await treats.process_once()
    assert (await treats.record(identifier))["state"] == "attempted"
    outgoing.wallet_id = "source"
    await treats.process_once()
    await treats.process_once()
    notices = [
        r
        for r in await rows("message")
        if r["data"]["event_type"] == "cyber_herd_treats"
    ]
    assert len(notices) == 1
    assert notices[0]["data"]["values"]["new_amount"] == 1
    lookup.assert_awaited_with("f" * 64, wallet_id="source")


@pytest.mark.anyio
async def test_failed_payment_never_announced_paid(audit_db, enabled, monkeypatch):
    payment, targets = await setup_distribution()
    _, contexts = await treats.prepare_distribution(payment, targets)
    identifier = contexts[targets[0].wallet]
    await treats.outcome(identifier, "attempted", payment_hash="f" * 64)
    monkeypatch.setattr(
        "lnbits.core.crud.get_standalone_payment",
        AsyncMock(
            return_value=SimpleNamespace(
                wallet_id="source", amount=-1000, success=False, failed=True
            )
        ),
    )
    monkeypatch.setattr(treats, "_publish", AsyncMock())
    await treats.process_once()
    assert (await treats.record(identifier))["state"] == "failed"
    assert not [
        r
        for r in await rows("message")
        if r["data"]["event_type"] == "cyber_herd_treats"
    ]


@pytest.mark.anyio
async def test_shared_address_not_misattributed(audit_db, enabled):
    payment, targets = await setup_distribution()
    await add_member(pubkey="c" * 64, lud16=targets[0].wallet)
    _, contexts = await treats.prepare_distribution(payment, targets)
    assert contexts
    assert not await rows("treat")


@pytest.mark.anyio
async def test_threads_and_signed_retry_survive_cycles(audit_db, enabled, monkeypatch):
    await save_settings()
    await treats.queue_member(
        "new_member",
        "owner",
        {"member_pubkey": PUBKEY, "source_event_id": "join", "name": "Alice"},
        "herd",
        [ROOT],
        "cyberherd",
    )
    first = (await rows("message"))[0]
    signed = {"id": "e" * 64, "content": "<Alice> 🐐", "tags": []}
    calls = []

    async def publish(content, **kwargs):
        calls.append(kwargs)
        await kwargs["persist_signed"](signed)
        # Simulate a lost reply after publication, so no submitted receipt.
        return False

    monkeypatch.setattr(
        messages, "render_member_message", AsyncMock(return_value="Alice <joined>")
    )
    monkeypatch.setattr(messages, "publish_note", publish)
    monkeypatch.setattr(
        messages, "send_to_websocket_clients", AsyncMock(return_value=True)
    )
    await treats._publish(first)
    saved = await treats.record(first["id"])
    assert saved["data"]["signed"] == signed
    saved["data"]["retry_at"] = 0
    await treats.put(first["id"], "owner", "message", "queued", saved["data"])
    retry = AsyncMock(return_value=True)
    monkeypatch.setattr(messages, "publish_signed_note", retry)
    await treats._publish(await treats.record(first["id"]))
    retry.assert_awaited_once_with(signed, wallet_id="herd", user_id="owner")
    thread = (await rows("thread"))[0]
    assert thread["data"]["event_id"] == signed["id"]
    assert calls[0]["e_tags"] == [ROOT]
    await treats.queue_member(
        "member_increase",
        "owner",
        {"member_pubkey": PUBKEY, "source_event_id": "zap2"},
        "herd",
        ["c" * 64],
        "cyberherd",
    )
    assert len(await rows("thread")) == 1
    await treats.cycle("owner", reset=True)
    await treats.queue_member(
        "new_member",
        "owner",
        {"member_pubkey": PUBKEY, "source_event_id": "join3"},
        "herd",
        [ROOT],
        "cyberherd",
    )
    assert len(await rows("thread")) == 2


@pytest.mark.anyio
async def test_disabled_publication_has_no_fake_receipt(audit_db, monkeypatch):
    await treats.queue_member(
        "new_member",
        "owner",
        {"member_pubkey": PUBKEY, "name": "Alice"},
        "herd",
        [ROOT],
        "cyberherd",
    )
    monkeypatch.setattr(
        messages, "render_member_message", AsyncMock(return_value="Alice")
    )
    monkeypatch.setattr(
        messages, "is_nostr_publishing_enabled", AsyncMock(return_value=False)
    )
    monkeypatch.setattr(
        messages, "_is_publishing_setting_enabled", AsyncMock(return_value=False)
    )
    await treats._publish((await rows("message"))[0])
    assert (await rows("thread"))[0]["state"] == "waiting"
    assert (await rows("message"))[0]["state"] == "queued"


@pytest.mark.anyio
async def test_notices_use_real_mention_and_optional_hint(monkeypatch):
    from lnbits.extensions.cyberherd_messaging.defaults import SEED_DEFAULTS

    async def stored_templates(owner, category):
        result = []
        for key, entry in SEED_DEFAULTS[category].items():
            result.append(
                SimpleNamespace(
                    key=key,
                    content=entry.get("content") if isinstance(entry, dict) else entry,
                )
            )
        return result

    monkeypatch.setattr(
        "lnbits.extensions.cyberherd_messaging.crud.ensure_default_templates",
        AsyncMock(),
    )
    monkeypatch.setattr(
        "lnbits.extensions.cyberherd_messaging.crud.get_message_templates",
        stored_templates,
    )
    monkeypatch.setattr("random.choice", lambda items: items[0])
    plain = await messages.render_member_message(
        "cyber_herd_treats_skipped", {"name": "Alice"}, PUBKEY, "owner"
    )
    hinted = await messages.render_member_message(
        "cyber_herd_treats_skipped", {"name": "Alice"}, PUBKEY, "owner", True
    )
    assert (
        "big goats ate all the food" in plain
        and "nostr:npub" in plain
        and "Alice" in plain
    )
    assert "repost" not in plain.lower() and "3 points" in hinted
    paid = await messages.render_member_message(
        "cyber_herd_treats", {"name": "Alice", "new_amount": 42}, PUBKEY, "owner"
    )
    assert "42 sats" in paid


@pytest.mark.anyio
async def test_signed_event_persisted_before_submission(monkeypatch):
    import lnbits.extensions.nostrclient.router as router
    from lnbits.extensions.nsec_oracle import services as signer

    sequence = []
    signed = {"id": "e" * 64}
    monkeypatch.setattr(signer, "sign_event", AsyncMock(return_value=signed))

    def emit(wire):
        assert json.loads(wire) == ["EVENT", signed]
        sequence.append("submitted")

    monkeypatch.setattr(
        router,
        "nostr_client",
        SimpleNamespace(relay_manager=SimpleNamespace(publish_message=emit, relays=[])),
    )

    async def persist(event):
        assert event == signed
        sequence.append("persisted")

    receipt = {}
    assert await messages._try_bunker_sign_and_publish(
        "herd", "hello", 1, [], receipt=receipt, persist_signed=persist
    )
    assert sequence == ["persisted", "submitted"]
    assert receipt == {"event_id": signed["id"], "submitted": True}


@pytest.mark.anyio
@pytest.mark.parametrize("msat", [-1000, 0, 999])
async def test_lnurl_never_requests_zero_or_negative(monkeypatch, msat):
    request = AsyncMock()
    monkeypatch.setattr(split_tasks, "get_pr_from_lnurl", request)
    assert (
        await split_tasks.get_lnurl_invoice(
            "alice@example.test", "source", msat, "memo"
        )
        is None
    )
    request.assert_not_awaited()


@pytest.mark.anyio
@pytest.mark.parametrize(
    "balance,expected,remainder",
    [
        (1234, [1000], 234),
        (2345, [1000, 1000], 345),
        (3456, [1000] * 3, 456),
        (4000, [1000] * 4, 0),
        (10345, [1000] * 10, 345),
    ],
)
async def test_each_threshold_is_a_feeding(
    audit_db, monkeypatch, balance, expected, remainder
):
    wallet = SimpleNamespace(id="herd", user="owner", balance_msat=balance * 1000)
    settings = SimpleNamespace(
        herd_wallet_id="herd", feeder_trigger_sats=1000, openhab_feeder_rule_id="feed"
    )
    payment = SimpleNamespace(payment_hash="d" * 64, amount=2345000)
    openhab = SimpleNamespace(
        is_feeder_override_enabled=AsyncMock(return_value=False),
        trigger_feeder=AsyncMock(return_value=True),
    )
    monkeypatch.setattr(goat_tasks, "get_wallet", AsyncMock(return_value=wallet))
    monkeypatch.setattr(goat_tasks, "FEEDER_COOLDOWN_SECONDS", 0)
    monkeypatch.setattr(goat_tasks, "FEEDER_BATCH_INTERVAL_SECONDS", 0)
    monkeypatch.setattr(goat_tasks, "send_feeder_message", AsyncMock())
    amounts = []

    async def distribute(owner, **kwargs):
        amounts.append(kwargs["amount_sats"])
        wallet.balance_msat -= kwargs["amount_sats"] * 1000

    monkeypatch.setattr(goat_tasks, "_distribute_herd_balance", distribute)
    await goat_tasks._process_feeding_batch(
        wallet, settings, payment, openhab, "deposit"
    )
    assert amounts == expected
    assert wallet.balance_msat == remainder * 1000
    assert openhab.trigger_feeder.await_count == min(3, len(expected))
    assert goat_tasks.send_feeder_message.await_count == 1
    assert goat_tasks.send_feeder_message.await_args.kwargs["payout_cycles"] == len(
        expected
    )
    # Running again below threshold does not feed or distribute again.
    await goat_tasks._process_feeding_batch(
        wallet, settings, payment, openhab, "deposit2"
    )
    assert amounts == expected


@pytest.mark.anyio
async def test_restart_retries_transfer_without_another_feeding(audit_db, monkeypatch):
    wallet = SimpleNamespace(id="herd", user="owner", balance_msat=1234000)
    settings = SimpleNamespace(
        herd_wallet_id="herd", feeder_trigger_sats=1000, openhab_feeder_rule_id="feed"
    )
    payment = SimpleNamespace(payment_hash="d" * 64, amount=2345000)
    openhab = SimpleNamespace(
        is_feeder_override_enabled=AsyncMock(return_value=False),
        trigger_feeder=AsyncMock(return_value=True),
    )
    monkeypatch.setattr(goat_tasks, "get_wallet", AsyncMock(return_value=wallet))
    monkeypatch.setattr(goat_tasks, "FEEDER_COOLDOWN_SECONDS", 0)
    monkeypatch.setattr(goat_tasks, "FEEDER_BATCH_INTERVAL_SECONDS", 0)
    monkeypatch.setattr(goat_tasks, "send_feeder_message", AsyncMock())
    monkeypatch.setattr(
        goat_tasks,
        "_distribute_herd_balance",
        AsyncMock(side_effect=RuntimeError("lost reply")),
    )
    with pytest.raises(RuntimeError):
        await goat_tasks._process_feeding_batch(
            wallet, settings, payment, openhab, "deposit"
        )
    assert openhab.trigger_feeder.await_count == 1

    async def settled(*args, **kwargs):
        wallet.balance_msat -= 1000000

    monkeypatch.setattr(goat_tasks, "_distribute_herd_balance", settled)
    await goat_tasks._process_feeding_batch(
        wallet, settings, payment, openhab, "deposit"
    )
    assert openhab.trigger_feeder.await_count == 1
    assert wallet.balance_msat == 234000


@pytest.mark.anyio
async def test_transfer_exact_threshold_and_lost_reply_recovery(
    audit_db, enabled, monkeypatch
):
    config = await save_settings(
        herd_wallet="herd", source_wallet="source", midnight_reset_enabled=False
    )
    wallets = {
        "herd": SimpleNamespace(id="herd", balance_msat=2345000),
        "source": SimpleNamespace(id="source", balance_msat=0),
    }
    monkeypatch.setattr(
        send_splits, "get_wallet", AsyncMock(side_effect=lambda ref: wallets.get(ref))
    )
    invoice = SimpleNamespace(
        bolt11="invoice", payment_hash="d" * 64, wallet_id="source", amount=1000000
    )
    create = AsyncMock(return_value=invoice)
    monkeypatch.setattr(send_splits, "create_wallet_invoice", create)
    monkeypatch.setattr(
        "lnbits.extensions.splitpayments.crud.get_targets", AsyncMock(return_value=[])
    )
    paid = SimpleNamespace(
        success=True, checking_id="done", wallet_id="herd", amount=-1000000
    )
    lookup = AsyncMock(return_value=None)
    monkeypatch.setattr("lnbits.core.crud.get_standalone_payment", lookup)
    pay = AsyncMock(return_value=paid)
    monkeypatch.setattr(send_splits, "pay_invoice", pay)
    result = await send_splits.transfer_herd_balance_to_source(
        config, amount_sats=1000, feeding_id="cycle1"
    )
    assert result["ok"] and result["sats"] == 1000
    assert create.await_args.args[1].amount == 1000
    # A response lost after settlement must reconcile the same hash, not pay again.
    identifier = treats.key("feeding-transfer", "owner", "cycle1")
    prior = await treats.record(identifier)
    await treats.put(identifier, "owner", "feeding-transfer", "prepared", prior["data"])
    lookup.return_value = paid
    result = await send_splits.transfer_herd_balance_to_source(
        config, amount_sats=1000, feeding_id="cycle1"
    )
    assert result["ok"]
    assert pay.await_count == 1 and create.await_count == 1


@pytest.mark.anyio
async def test_ambiguous_feeder_request_is_never_replayed(audit_db, monkeypatch):
    wallet = SimpleNamespace(id="herd", user="owner", balance_msat=1000000)
    settings = SimpleNamespace(
        herd_wallet_id="herd", feeder_trigger_sats=1000, openhab_feeder_rule_id="feed"
    )
    batch_id = treats.key("feeding-batch", "herd")
    await treats.put(
        batch_id,
        "owner",
        "feeding-batch",
        "pending",
        {
            "index": 0,
            "origin": "deposit",
            "wallet": "herd",
            "threshold": 1000,
            "cycles": 1,
        },
    )
    feeding_id = treats.key("feed", "herd", "deposit", 0)
    await treats.put(feeding_id, "owner", "feeding", "requested", {"threshold": 1000})
    monkeypatch.setattr(goat_tasks, "get_wallet", AsyncMock(return_value=wallet))
    trigger = AsyncMock()
    with pytest.raises(RuntimeError, match="uncertain"):
        await goat_tasks._process_feeding_batch(
            wallet,
            settings,
            SimpleNamespace(),
            SimpleNamespace(trigger_feeder=trigger),
            "deposit",
        )
    trigger.assert_not_awaited()
    assert (await treats.record(batch_id))["state"] == "blocked"


@pytest.mark.anyio
async def test_manual_reset_rotates_cycle_and_preserves_old_thread(
    audit_db, monkeypatch
):
    await save_settings()
    await treats.queue_member(
        "new_member", "owner", {"member_pubkey": PUBKEY}, "herd", [ROOT], "cyberherd"
    )
    before = await treats.cycle("owner")
    await tasks.manual_reset_membership("owner")
    assert await treats.cycle("owner") != before
    assert (await rows("thread"))[0]["data"]["cycle"] == before


@pytest.mark.anyio
async def test_one_durable_feeder_notice_per_payment(audit_db, enabled, monkeypatch):
    from lnbits.extensions.lightning_goats.services import messaging as goat_messages

    monkeypatch.setattr(
        goat_messages, "_resolve_websocket_topic", AsyncMock(return_value="overlay")
    )
    monkeypatch.setattr(
        goat_messages, "_resolve_herd_wallet_id", AsyncMock(return_value="herd")
    )
    for _ in range(3):
        assert await goat_messages.send_feeder_message(
            3000, 3456, "owner", payment_id="deposit", feedings=3, payout_cycles=3
        )
    notices = await rows("message")
    assert len(notices) == 1
    assert notices[0]["data"]["values"]["feedings"] == 3
    assert notices[0]["data"]["values"]["new_amount"] == 3456
    render = AsyncMock(return_value="One message: three feedings")
    monkeypatch.setattr(messages, "render_feeder_message", render)

    async def publish(content, **kwargs):
        kwargs["receipt"].update(event_id="f" * 64, submitted=True)
        return True

    publish_mock = AsyncMock(side_effect=publish)
    monkeypatch.setattr(messages, "publish_note", publish_mock)
    monkeypatch.setattr(messages, "send_to_websocket_clients", AsyncMock())
    await treats.process_once()
    await treats.process_once()
    await goat_messages.send_feeder_message(
        3000, 3456, "owner", payment_id="deposit", feedings=3, payout_cycles=3
    )
    await treats.process_once()
    assert publish_mock.await_count == 1
    assert (await rows("message"))[0]["state"] == "submitted"


@pytest.mark.anyio
async def test_receiver_minimum_is_skip_not_payment_failure(
    audit_db, enabled, monkeypatch
):
    from lnurl import LnurlPayResponse

    payment, targets = await setup_distribution()
    targets[0].percent = 0.5  # 5 sats gross, 3 sats after the reserve
    _, contexts = await treats.prepare_distribution(payment, targets[:1])
    identifier = contexts[targets[0].wallet]
    monkeypatch.setattr(split_tasks, "get_targets", AsyncMock(return_value=targets[:1]))
    monkeypatch.setattr(split_tasks, "fee_reserve", lambda amount: 2000)
    monkeypatch.setattr(
        split_tasks,
        "get_pr_from_lnurl",
        AsyncMock(side_effect=ValueError("provider minimum")),
    )
    monkeypatch.setattr(
        "lnurl.handle",
        AsyncMock(return_value=LnurlPayResponse.construct(minSendable=5000)),
    )
    pay = AsyncMock()
    monkeypatch.setattr(split_tasks, "pay_invoice", pay)
    await split_tasks.on_invoice_paid(payment)
    assert (await treats.record(identifier))["state"] == "too_small"
    pay.assert_not_awaited()


@pytest.mark.anyio
async def test_saved_signed_event_is_verified_and_not_resigned(monkeypatch):
    from pynostr.event import Event
    from pynostr.key import PrivateKey

    import lnbits.extensions.nostrclient.router as router

    signer = PrivateKey()
    event = Event(
        content="<Alice> received a treat",
        pubkey=signer.public_key.hex(),
        created_at=1234567890,
        kind=1,
        tags=[],
    )
    event.sign(signer.hex())
    signed = event.to_dict()
    emitted = []
    monkeypatch.setattr(
        messages, "is_nostr_publishing_enabled", AsyncMock(return_value=True)
    )
    keys = AsyncMock(
        return_value=[SimpleNamespace(id="key", pubkey_hex=signed["pubkey"])]
    )
    permissions = AsyncMock(
        return_value=[
            SimpleNamespace(key_id="key", extension_id="cyberherd_messaging", kind=1)
        ]
    )
    monkeypatch.setattr("lnbits.extensions.nsec_oracle.crud.get_keys", keys)
    monkeypatch.setattr(
        "lnbits.extensions.nsec_oracle.crud.get_permissions", permissions
    )
    monkeypatch.setattr(
        router,
        "nostr_client",
        SimpleNamespace(relay_manager=SimpleNamespace(publish_message=emitted.append)),
    )
    assert await messages.publish_signed_note(signed, wallet_id="herd", user_id="owner")
    assert json.loads(emitted[0]) == ["EVENT", signed]
    assert not await messages.publish_signed_note(
        {**signed, "id": "0" * 64}, wallet_id="herd", user_id="owner"
    )
    assert not await messages.publish_signed_note(
        {**signed, "content": "tampered"}, wallet_id="herd", user_id="owner"
    )
    keys.return_value = [SimpleNamespace(id="other", pubkey_hex="f" * 64)]
    assert not await messages.publish_signed_note(
        signed, wallet_id="herd", user_id="owner"
    )
    keys.return_value = [SimpleNamespace(id="key", pubkey_hex=signed["pubkey"])]
    permissions.return_value = []
    assert not await messages.publish_signed_note(
        signed, wallet_id="herd", user_id="owner"
    )
    assert len(emitted) == 1


@pytest.mark.anyio
async def test_stale_recovery_does_not_reopen_completed_payment(audit_db, monkeypatch):
    wallet = SimpleNamespace(id="herd", user="owner", balance_msat=5000000)
    batch_id = treats.key("feeding-batch", "herd")
    await treats.put(
        batch_id, "owner", "feeding-batch", "complete", {"origin": "old-payment"}
    )
    trigger = AsyncMock()
    await goat_tasks._process_feeding_batch(
        wallet,
        SimpleNamespace(),
        SimpleNamespace(),
        SimpleNamespace(trigger_feeder=trigger),
        "old-payment",
        resume=True,
    )
    trigger.assert_not_awaited()
    assert (await treats.record(batch_id))["state"] == "complete"


@pytest.mark.anyio
async def test_repost_hint_at_most_once_and_never_after_repost(audit_db, enabled):
    payment, targets = await setup_distribution()
    _, contexts = await treats.prepare_distribution(payment, targets[:1])
    original = await treats.record(contexts[targets[0].wallet])
    for identifier in ("a" * 64, "c" * 64):
        row = {**original, "id": identifier, "state": "too_small"}
        async with crud.admission_transaction():
            await treats._queue_treat(row)
    notices = [
        r
        for r in await rows("message")
        if r["data"]["event_type"] == "cyber_herd_treats_skipped"
    ]
    assert sum(r["data"]["nudge"] for r in notices) == 1
    original["data"]["reposted"] = True
    async with crud.admission_transaction():
        await treats._queue_treat({**original, "id": "e" * 64, "state": "too_small"})
    last = await treats.record(treats.key("notice", "e" * 64))
    assert last["data"]["nudge"] is False


@pytest.mark.anyio
async def test_repeated_distribution_does_not_repeat_member_or_goat_payment(
    audit_db, enabled, monkeypatch
):
    import asyncio

    payment, targets = await setup_distribution()
    targets[0].percent = 5
    monkeypatch.setattr(split_tasks, "get_targets", AsyncMock(return_value=targets))
    monkeypatch.setattr(split_tasks, "fee_reserve", lambda amount: 2000)
    invoice = AsyncMock(side_effect=lambda address, *args, **kwargs: address)
    monkeypatch.setattr(split_tasks, "get_lnurl_invoice", invoice)
    monkeypatch.setattr(
        split_tasks.bolt11,
        "decode",
        lambda request: SimpleNamespace(
            payment_hash=treats.key(request), amount_msat=48000
        ),
    )
    sender = AsyncMock(return_value="settled (mock)")
    monkeypatch.setattr(split_tasks, "pay_invoice_in_background", sender)
    await split_tasks.on_invoice_paid(payment)
    await asyncio.sleep(0)
    await split_tasks.on_invoice_paid(payment)
    await asyncio.sleep(0)
    assert sender.await_count == 2 and invoice.await_count == 2
    assert len(await rows("treat")) == 1
    assert len(await rows("split-recipient")) == 1
    assert (await rows("treat"))[0]["state"] == "attempted"


@pytest.mark.anyio
async def test_internal_sub_satoshi_target_does_not_create_zero_invoice(
    audit_db, monkeypatch
):
    payment = SimpleNamespace(
        wallet_id="source",
        payment_hash="d" * 64,
        amount=1000,
        extra={},
        memo="distribution",
    )
    targets = [SimpleNamespace(wallet="internal-wallet", percent=50, alias="Internal")]
    monkeypatch.setattr(split_tasks, "get_targets", AsyncMock(return_value=targets))
    invoice = AsyncMock()
    monkeypatch.setattr(split_tasks, "create_invoice", invoice)
    await split_tasks.on_invoice_paid(payment)
    invoice.assert_not_awaited()
