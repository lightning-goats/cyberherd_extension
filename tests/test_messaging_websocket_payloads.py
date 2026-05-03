from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

import lnbits.extensions.cyberherd.services.messaging as messaging


@pytest.mark.anyio
async def test_publish_event_message_uses_display_name_in_new_member_websocket_payload(
    monkeypatch,
):
    captured: dict[str, object] = {}

    async def fake_send_to_websocket_clients(topic: str, payload: dict) -> bool:
        captured["topic"] = topic
        captured["payload"] = payload
        return True

    fake_msg = SimpleNamespace(
        is_nostr_publishing_enabled=AsyncMock(return_value=False),
        publish_note=AsyncMock(return_value=False),
        send_to_websocket_clients=AsyncMock(
            side_effect=fake_send_to_websocket_clients
        ),
    )

    async def fake_make_messages(event_type: str, **kwargs):
        return {
            "content": "nostr content",
            "payload": {"message": "websocket content"},
        }

    monkeypatch.setattr(messaging, "_msg", fake_msg)
    monkeypatch.setattr(messaging, "_messaging_available", True)
    monkeypatch.setattr(messaging, "make_messages", fake_make_messages)

    ok = await messaging.publish_event_message(
        "new_member",
        values={
            "member_name": "nostr:npub1joinedgoat",
            "member_display_name": "Joined Goat",
            "name": "nostr:npub1joinedgoat",
            "member_pubkey": "abcdef123456",
            "cyber_herd_item": {
                "display_name": "Joined Goat",
                "pubkey": "abcdef123456",
                "amount": 42,
            },
        },
        websocket_topic="cyberherd",
        wallet_id="wallet123",
    )

    assert ok is True
    assert captured["topic"] == "cyberherd"

    payload = captured["payload"]
    assert isinstance(payload, dict)
    assert payload["message"] == "websocket content"
    assert payload["values"]["member_display_name"] == "Joined Goat"
    assert payload["values"]["member_name"] == "Joined Goat"
    assert payload["values"]["name"] == "Joined Goat"
    assert payload["members"] == [
        {
            "display_name": "Joined Goat",
            "pubkey": "abcdef123456",
            "amount": 42,
        }
    ]
