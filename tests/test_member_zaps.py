"""NIP-57 payouts: disposable DBs and real signatures; no live payments."""

import hashlib
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock

import bolt11
import pytest
from bolt11 import Bolt11, MilliSatoshi, Tag, TagChar, Tags
from lnurl import LnurlPayResponse
from pynostr.event import Event

from lnbits.extensions.cyberherd.services import treats, zaps
from lnbits.extensions.cyberherd.tests.test_database_operations import (
    audit_db as _audit_db,
)
from lnbits.extensions.cyberherd.tests.test_messaging_template_storage import (
    messaging_db as _messaging_db,
)
from lnbits.extensions.cyberherd.tests.test_treats import (
    PUBKEY,
    ROOT,
    rows,
    setup_distribution,
)
from lnbits.extensions.cyberherd_messaging import services as messaging
from lnbits.extensions.splitpayments import tasks as split_tasks

audit_db = _audit_db
messaging_db = _messaging_db
TEST_KEY = "1" * 64


@pytest.fixture
def anyio_backend():
    return "asyncio"


def signed_event(unsigned):
    event = Event(**unsigned)
    event.sign(TEST_KEY)
    return event.to_dict()


RECEIPT_KEY = signed_event({"kind": 1, "content": "", "tags": [], "created_at": 1})[
    "pubkey"
]


def metadata(**overrides):
    return LnurlPayResponse(
        **{
            "callback": "https://example.test/callback",
            "minSendable": 1000,
            "maxSendable": 1000000,
            "metadata": '[["text/plain","Treats"]]',
            "allowsNostr": True,
            "nostrPubkey": RECEIPT_KEY,
            **overrides,
        }
    )


def invoice_for(request_json, amount=10000, **overrides):
    tags = Tags(
        [
            Tag(TagChar.payment_hash, "a" * 64),
            Tag(TagChar.payment_secret, "b" * 64),
            Tag(
                TagChar.description_hash,
                overrides.get(
                    "hash", hashlib.sha256(request_json.encode()).hexdigest()
                ),
            ),
            Tag(TagChar.expire_time, 3600),
        ]
    )
    invoice = Bolt11(
        currency="bc",
        date=overrides.get("date", int(time.time())),
        tags=tags,
        amount_msat=MilliSatoshi(amount),
    )
    return bolt11.encode(invoice, private_key=TEST_KEY)


async def planned_member(monkeypatch):
    monkeypatch.setattr(
        "lnbits.extensions.cyberherd.utils.common.is_extension_enabled_for_user",
        AsyncMock(return_value=True),
    )
    payment, targets = await setup_distribution()
    targets[0].percent = 10
    await treats.prepare_distribution(payment, targets)
    row = (await rows("treat"))[0]
    await treats.claim_recipient(row["id"])
    thread = await treats.record(row["data"]["thread"])
    thread["data"]["event_id"] = ROOT
    await treats.put(thread["id"], "owner", "thread", "submitted", thread["data"])
    return await treats.record(row["id"])


@pytest.fixture
async def zap_dependencies(monkeypatch):
    monkeypatch.setattr(zaps, "handle", AsyncMock(return_value=metadata()))

    async def sign(**kwargs):
        tags = [
            ["p", kwargs["pubkey"]],
            ["amount", str(kwargs["amount_msat"])],
            ["lnurl", kwargs["lnurl"]],
            ["relays", "wss://nos.lol"],
        ]
        if kwargs["event_id"]:
            tags.append(["e", kwargs["event_id"]])
        return signed_event(
            {
                "kind": 9734,
                "content": "Treats",
                "tags": tags,
                "created_at": int(time.time()),
            }
        )

    signer = AsyncMock(side_effect=sign)
    monkeypatch.setattr(messaging, "sign_treat_zap_request", signer)

    async def callback(meta, amount, request_json, lnurl):
        return invoice_for(request_json, amount)

    request = AsyncMock(side_effect=callback)
    monkeypatch.setattr(zaps, "_callback_invoice", request)
    return signer, request


@pytest.mark.anyio
async def test_verified_zap_keeps_budget_and_reuses_saved_request(
    audit_db, monkeypatch, zap_dependencies
):
    row = await planned_member(monkeypatch)
    signer, request = zap_dependencies
    pr = await zaps.member_zap_invoice(
        row["id"], row["data"]["target"], "source", 10000
    )
    assert bolt11.decode(pr).amount_msat == 10000
    saved = (await treats.record(row["id"]))["data"]
    assert saved["payment_method"] == "zap" and saved["zap_invoice_verified"]
    assert saved["zap_receipt_status"] == "requested"
    assert ["e", ROOT] in saved["zap_request"]["tags"]
    await zaps.member_zap_invoice(row["id"], row["data"]["target"], "source", 10000)
    assert signer.await_count == 1
    assert request.await_args_list[0].args[2] == request.await_args_list[1].args[2]
    assert saved["gross_msat"] == 100000


@pytest.mark.anyio
@pytest.mark.parametrize(
    "overrides",
    [{"allowsNostr": False}, {"nostrPubkey": None}, {"nostrPubkey": "f" * 64}],
)
async def test_unsupported_recipient_uses_recorded_lightning_fallback(
    audit_db, monkeypatch, zap_dependencies, overrides
):
    row = await planned_member(monkeypatch)
    monkeypatch.setattr(zaps, "handle", AsyncMock(return_value=metadata(**overrides)))
    assert (
        await zaps.member_zap_invoice(row["id"], row["data"]["target"], "source", 10000)
        is None
    )
    saved = (await treats.record(row["id"]))["data"]
    assert saved["payment_method"] == "lightning"
    assert saved["zap_fallback_reason"] == "recipient_unsupported"
    zap_dependencies[0].assert_not_awaited()
    zap_dependencies[1].assert_not_awaited()


@pytest.mark.anyio
@pytest.mark.parametrize("bad", ["amount", "hash", "expired"])
async def test_mismatched_invoice_never_paid_or_downgraded(
    audit_db, monkeypatch, zap_dependencies, bad
):
    row = await planned_member(monkeypatch)

    async def callback(meta, amount, request_json, lnurl):
        return invoice_for(
            request_json,
            amount + 1000 if bad == "amount" else amount,
            **(
                {"hash": "0" * 64}
                if bad == "hash"
                else {"date": 1} if bad == "expired" else {}
            )
        )

    monkeypatch.setattr(zaps, "_callback_invoice", callback)
    ordinary = AsyncMock()
    monkeypatch.setattr(split_tasks, "get_pr_from_lnurl", ordinary)
    monkeypatch.setattr(
        split_tasks, "_below_receiver_minimum", AsyncMock(return_value=False)
    )
    assert (
        await split_tasks.get_lnurl_invoice(
            row["data"]["target"], "source", 10000, "Treats", context=row["id"]
        )
        is None
    )
    ordinary.assert_not_awaited()
    assert not (await treats.record(row["id"]))["data"].get("zap_invoice_verified")


@pytest.mark.anyio
async def test_operator_is_not_zapped(audit_db, monkeypatch, zap_dependencies):
    await planned_member(monkeypatch)
    recipient = (await rows("split-recipient"))[0]
    assert (
        await zaps.member_zap_invoice(
            recipient["id"], "goats@example.test", "source", 900000
        )
        is None
    )
    zap_dependencies[0].assert_not_awaited()
    zap_dependencies[1].assert_not_awaited()


@pytest.mark.anyio
@pytest.mark.parametrize("amount", [101000, 0, 10001])
async def test_invalid_amount_rejected_before_lnurl_request(
    audit_db, monkeypatch, zap_dependencies, amount
):
    row = await planned_member(monkeypatch)
    with pytest.raises(ValueError):
        await zaps.member_zap_invoice(
            row["id"], row["data"]["target"], "source", amount
        )
    zaps.handle.assert_not_awaited()


@pytest.mark.anyio
async def test_sign_request_is_valid_and_not_published(messaging_db, monkeypatch):
    monkeypatch.setattr(
        messaging, "is_nostr_publishing_enabled", AsyncMock(return_value=True)
    )
    monkeypatch.setattr(
        "lnbits.extensions.nostrclient.crud.get_relays",
        AsyncMock(
            return_value=[
                SimpleNamespace(url="wss://nos.lol", active=True),
                SimpleNamespace(url="wss://disabled.test", active=False),
            ]
        ),
    )

    async def sign(wallet_id, extension_id, unsigned):
        assert wallet_id == "herd" and extension_id == "cyberherd_messaging"
        return signed_event(unsigned)

    monkeypatch.setattr("lnbits.extensions.nsec_oracle.services.sign_event", sign)
    publish = AsyncMock()
    monkeypatch.setattr(messaging, "publish_note", publish)
    result = await messaging.sign_treat_zap_request(
        wallet_id="herd",
        user_id="owner",
        pubkey=PUBKEY,
        name="Alice",
        amount_msat=10000,
        lnurl="lnurl1test",
        event_id=ROOT,
    )
    assert Event.from_dict(result).verify()
    assert result["kind"] == 9734 and ["amount", "10000"] in result["tags"]
    assert ["relays", "wss://nos.lol"] in result["tags"]
    assert ["e", ROOT] in result["tags"] and ["p", PUBKEY] in result["tags"]
    assert "Alice" in result["content"] and "10 sats" in result["content"]
    publish.assert_not_awaited()


@pytest.mark.anyio
async def test_missing_thread_event_uses_profile_zap(
    audit_db, monkeypatch, zap_dependencies
):
    row = await planned_member(monkeypatch)
    thread = await treats.record(row["data"]["thread"])
    thread["data"].pop("event_id")
    await treats.put(thread["id"], "owner", "thread", "waiting", thread["data"])
    await zaps.member_zap_invoice(row["id"], row["data"]["target"], "source", 10000)
    saved = (await treats.record(row["id"]))["data"]
    assert saved["zap_event_id"] is None
    assert not any(t[0] == "e" for t in saved["zap_request"]["tags"])


@pytest.mark.anyio
async def test_signing_failure_never_downgrades(
    audit_db, monkeypatch, zap_dependencies
):
    row = await planned_member(monkeypatch)
    monkeypatch.setattr(
        messaging,
        "sign_treat_zap_request",
        AsyncMock(side_effect=PermissionError("No grant")),
    )
    ordinary = AsyncMock()
    monkeypatch.setattr(split_tasks, "get_pr_from_lnurl", ordinary)
    monkeypatch.setattr(
        split_tasks, "_below_receiver_minimum", AsyncMock(return_value=False)
    )
    assert (
        await split_tasks.get_lnurl_invoice(
            row["data"]["target"], "source", 10000, "Treats", context=row["id"]
        )
        is None
    )
    ordinary.assert_not_awaited()
    zap_dependencies[1].assert_not_awaited()


@pytest.mark.anyio
async def test_unsupported_fallback_integrates_with_split_invoice(
    audit_db, monkeypatch, zap_dependencies
):
    row = await planned_member(monkeypatch)
    monkeypatch.setattr(
        zaps, "handle", AsyncMock(return_value=metadata(allowsNostr=False))
    )
    ordinary = AsyncMock(return_value=invoice_for("ordinary", 10000))
    monkeypatch.setattr(split_tasks, "get_pr_from_lnurl", ordinary)
    monkeypatch.setattr(
        split_tasks, "get_standalone_payment", AsyncMock(return_value=None)
    )
    assert await split_tasks.get_lnurl_invoice(
        row["data"]["target"], "source", 10000, "Treats", context=row["id"]
    )
    ordinary.assert_awaited_once()
    zap_dependencies[0].assert_not_awaited()


@pytest.mark.anyio
async def test_callback_sends_signed_request_only_to_lnurl(monkeypatch):
    client = AsyncMock()
    response = SimpleNamespace(
        raise_for_status=lambda: None, json=lambda: {"pr": "invoice"}
    )
    client.get.return_value = response
    manager = AsyncMock()
    manager.__aenter__.return_value = client

    def constructor(**kwargs):
        assert kwargs["follow_redirects"] is False
        return manager

    monkeypatch.setattr(zaps.httpx, "AsyncClient", constructor)
    assert (
        await zaps._callback_invoice(metadata(), 10000, '{"kind":9734}', "lnurl1test")
        == "invoice"
    )
    args = client.get.await_args
    assert args.kwargs["params"] == {
        "amount": 10000,
        "nostr": '{"kind":9734}',
        "lnurl": "lnurl1test",
    }


@pytest.mark.anyio
async def test_zap_template_migration_preserves_custom_edits_and_deletion(messaging_db):
    from lnbits.extensions.cyberherd_messaging import crud, migrations

    await crud.ensure_default_templates("owner")
    await crud.update_message_template(
        "owner", "cyber_herd_zap_request", "0", "Custom {new_amount} sats"
    )
    await migrations.m008_seed_zap_request_template(messaging_db)
    assert (await crud.get_message_templates("owner", "cyber_herd_zap_request"))[
        0
    ].content == "Custom {new_amount} sats"
    await crud.delete_message_template("owner", "cyber_herd_zap_request", "0")
    await crud.ensure_default_templates("owner")
    remaining = await crud.get_message_templates("owner", "cyber_herd_zap_request")
    assert all(template.key != "0" for template in remaining)
