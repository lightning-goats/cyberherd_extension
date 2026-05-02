from types import SimpleNamespace

import pytest
from fastapi import HTTPException

import lnbits.extensions.cyberherd.views_api as views_api


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
