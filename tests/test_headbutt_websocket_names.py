from types import SimpleNamespace

import pytest

from lnbits.extensions.cyberherd.services import headbutt


class _FakeDb:
    async def get_cyberherd_member_by_pubkey(self, pubkey, user_id=None):
        if pubkey == "victim-pubkey":
            return {
                "display_name": "Victim Goat",
                "nprofile": "nprofile-victim",
                "relays": None,
            }
        return {
            "display_name": "Attacker Goat",
            "nprofile": "nprofile-attacker",
            "relays": None,
        }

    async def get_settings(self, user_id):
        return SimpleNamespace(herd_wallet="herd-wallet")


@pytest.mark.anyio
async def test_headbutt_success_websocket_values_use_display_names(monkeypatch):
    captured: dict[str, object] = {}

    async def fake_publish_event_message(event_type, **kwargs):
        captured["event_type"] = event_type
        captured["values"] = kwargs["values"]
        return True

    monkeypatch.setattr(
        "lnbits.extensions.cyberherd.services.messaging.publish_event_message",
        fake_publish_event_message,
    )

    service = headbutt.EnhancedHeadbuttService(
        db=_FakeDb(),
        messaging_module=SimpleNamespace(),
        app=SimpleNamespace(),
        user_id="user-id",
    )
    async def fake_websocket_topic():
        return "topic"

    monkeypatch.setattr(service, "_get_websocket_topic", fake_websocket_topic)

    async def fake_reply_params(note_id, settings=None):
        return None, None

    monkeypatch.setattr(service, "_resolve_reply_params", fake_reply_params)

    attacker = SimpleNamespace(
        pubkey="attacker-pubkey",
        display_name="Attacker Goat",
        nprofile="nprofile-attacker",
        amount=21,
        note="a" * 64,
        event_id="event-id",
        kinds=[9735],
    )
    victim = {
        "pubkey": "victim-pubkey",
        "display_name": "Victim Goat",
        "nprofile": "nprofile-victim",
        "amount": 10,
    }

    await service._send_headbutt_success_notification(attacker, victim)

    values = captured["values"]
    assert values["attacker_name"] == "Attacker Goat"
    assert values["victim_name"] == "Victim Goat"
    assert values["name"] == "Attacker Goat"
    assert values["attacker_nprofile"] == "nprofile-attacker"
    assert values["victim_nprofile"] == "nprofile-victim"


@pytest.mark.anyio
async def test_headbutt_failure_websocket_values_use_display_names(monkeypatch):
    captured: dict[str, object] = {}

    async def fake_publish_event_message(event_type, **kwargs):
        captured["event_type"] = event_type
        captured["values"] = kwargs["values"]
        return True

    monkeypatch.setattr(
        "lnbits.extensions.cyberherd.services.messaging.publish_event_message",
        fake_publish_event_message,
    )

    service = headbutt.EnhancedHeadbuttService(
        db=_FakeDb(),
        messaging_module=SimpleNamespace(),
        app=SimpleNamespace(),
        user_id="user-id",
    )
    async def fake_websocket_topic():
        return "topic"

    monkeypatch.setattr(service, "_get_websocket_topic", fake_websocket_topic)

    async def fake_reply_params(note_id, settings=None):
        return None, None

    monkeypatch.setattr(service, "_resolve_reply_params", fake_reply_params)

    attacker = SimpleNamespace(
        pubkey="attacker-pubkey",
        display_name="Attacker Goat",
        nprofile="nprofile-attacker",
        amount=9,
        note="a" * 64,
        event_id="event-id",
        kinds=[9735],
    )
    victim = {
        "pubkey": "victim-pubkey",
        "display_name": "Victim Goat",
        "nprofile": "nprofile-victim",
        "amount": 10,
    }

    await service._send_headbutt_failure_notification(attacker, victim, 11)

    values = captured["values"]
    assert values["attacker_name"] == "Attacker Goat"
    assert values["victim_name"] == "Victim Goat"
    assert values["name"] == "Attacker Goat"
    assert values["attacker_nprofile"] == "nprofile-attacker"
    assert values["victim_nprofile"] == "nprofile-victim"
