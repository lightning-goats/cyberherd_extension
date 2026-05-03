from types import SimpleNamespace

import pytest
from fastapi import HTTPException

import lnbits.extensions.cyberherd.views_api as views_api
from lnbits.extensions.cyberherd.services import nostr_websocket_monitor
from lnbits.extensions.cyberherd.services import subscriptions


def _settings(**overrides):
    base = {
        "source_wallet": "source-wallet-id",
        "zap_wallet": "zap-wallet-id",
        "zap_wallet_alias": "Zap",
        "herd_wallet": "herd-wallet-id",
        "max_members": 3,
        "tracked_tags": ["#CyberHerd"],
        "tracked_event_ids": ["a" * 64],
        "nostr_pubkey_override": "b" * 64,
        "zap_tracking_enabled": True,
        "midnight_reset_enabled": True,
        "nip05_verification_enabled": True,
        "zap_monitor_mode": "websocket",
        "repost_tracking_enabled": True,
        "likes_tracking_enabled": True,
        "minimum_sats": 10,
        "feeder_trigger_sats": 850,
        "member_allocation_percent": 10,
        "send_splits_enabled": False,
    }
    base.update(overrides)
    return SimpleNamespace(**base)


def test_anonymous_settings_response_omits_wallet_ids_and_invoice_key():
    response = views_api._build_settings_response(
        _settings(),
        effective_pubkey="c" * 64,
        websocket_url="wss://example.test/api/v1/ws/invoice-key",
        include_private=False,
    )

    assert "source_wallet" not in response
    assert "zap_wallet" not in response
    assert "herd_wallet" not in response
    assert "tracked_event_ids" not in response
    assert "websocket_url" not in response
    assert response["tracked_tags"] == ["#CyberHerd"]
    assert response["effective_nostr_pubkey"] == "c" * 64


@pytest.mark.anyio
async def test_resolve_owned_wallet_id_rejects_wallet_from_other_user(monkeypatch):
    async def fake_get_wallet(value):
        return SimpleNamespace(id=value, user="other-user")

    monkeypatch.setattr(views_api, "get_wallet", fake_get_wallet)
    monkeypatch.setattr(views_api, "get_wallet_for_key", fake_get_wallet)

    with pytest.raises(HTTPException) as exc:
        await views_api._resolve_owned_wallet_id(
            "wallet-id",
            field_name="herd_wallet",
            owner_user_id="owner-user",
        )

    assert exc.value.status_code == 403


@pytest.mark.anyio
async def test_resolve_owned_wallet_id_normalizes_owned_key_to_wallet_id(monkeypatch):
    async def fake_get_wallet(value):
        return None

    async def fake_get_wallet_for_key(value):
        return SimpleNamespace(id="wallet-id", user="owner-user")

    monkeypatch.setattr(views_api, "get_wallet", fake_get_wallet)
    monkeypatch.setattr(views_api, "get_wallet_for_key", fake_get_wallet_for_key)

    wallet_id = await views_api._resolve_owned_wallet_id(
        "admin-or-invoice-key",
        field_name="source_wallet",
        owner_user_id="owner-user",
    )

    assert wallet_id == "wallet-id"


@pytest.mark.anyio
async def test_notes_subscription_uses_author_filter_without_exact_case_t_filter(
    monkeypatch,
):
    settings = _settings(tracked_event_ids=[])
    sent_messages = []

    async def fake_get_settings(user_id):
        return settings

    async def fake_send(message):
        sent_messages.append(message)

    monkeypatch.setattr(nostr_websocket_monitor, "get_settings", fake_get_settings)
    monkeypatch.setattr(subscriptions, "get_effective_pubkey", lambda _settings: "b" * 64)

    monitor = nostr_websocket_monitor.NostrWebSocketMonitor("user-id")
    monkeypatch.setattr(monitor, "_send", fake_send)

    await monitor._subscribe_to_tracked_notes()

    note_reqs = [
        msg for msg in sent_messages
        if msg[0] == "REQ" and msg[2].get("kinds") == [1, 30311]
    ]
    assert len(note_reqs) == 1
    note_filter = note_reqs[0][2]
    assert note_filter["authors"] == ["b" * 64]
    assert "#t" not in note_filter


@pytest.mark.anyio
async def test_recovery_queries_author_fallback_for_content_only_hashtags(monkeypatch):
    settings = _settings(tracked_event_ids=[])
    captured_filters = []

    async def fake_get_settings(user_id):
        return settings

    async def fake_query_events(filters, limit=500, timeout=10.0):
        captured_filters.append(filters)
        return []

    class FakePaymentCoordinator:
        async def _recover_missed_payment_zaps(self, settings):
            return {"scanned": 0, "processed": 0}

    monkeypatch.setattr(subscriptions.crud, "get_settings", fake_get_settings)
    monkeypatch.setattr(subscriptions, "get_effective_pubkey", lambda _settings: "b" * 64)
    monkeypatch.setattr(subscriptions.nostr_helpers, "check_availability", lambda: True)
    monkeypatch.setattr(subscriptions.nostr_helpers, "query_events", fake_query_events)
    monkeypatch.setattr(
        "lnbits.extensions.cyberherd.services.payment_coordinator.get_payment_coordinator",
        lambda app=None, db=None, user_id=None: FakePaymentCoordinator(),
    )

    await subscriptions.force_requery_for_user(app=None, user_id="user-id")

    note_filters = [
        filters for filters in captured_filters
        if filters.get("kinds") == [1, 30311]
    ]
    assert any("#t" not in filters for filters in note_filters)
