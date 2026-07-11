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
async def test_headbutt_success_values_carry_mentions_and_display_names(monkeypatch):
    captured: dict[str, object] = {}

    async def fake_publish_event_message(event_type, **kwargs):
        captured["event_type"] = event_type
        captured["values"] = kwargs["values"]
        return True

    monkeypatch.setattr(
        "lnbits.extensions.cyberherd.services.messaging.publish_event_message",
        fake_publish_event_message,
    )

    async def fake_is_processed(user_id, event_hash, note_id=None):
        return False

    async def fake_register(user_id, event_hash, note_id=None, pubkey=None, event_type=None):
        return True

    monkeypatch.setattr(headbutt.crud, "is_event_processed", fake_is_processed)
    monkeypatch.setattr(headbutt.crud, "register_processed_event", fake_register)

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
    # Nostr-facing names are nprofile mentions (the websocket render swaps these
    # back to the *_display_name values for the live-stream overlay).
    assert values["attacker_name"] == "nostr:nprofile-attacker"
    assert values["victim_name"] == "nostr:nprofile-victim"
    assert values["name"] == "nostr:nprofile-attacker"
    assert values["attacker_display_name"] == "Attacker Goat"
    assert values["victim_display_name"] == "Victim Goat"
    assert values["attacker_nprofile"] == "nprofile-attacker"
    assert values["victim_nprofile"] == "nprofile-victim"


@pytest.mark.anyio
async def test_headbutt_failure_values_carry_mentions_and_display_names(monkeypatch):
    captured: dict[str, object] = {}

    async def fake_publish_event_message(event_type, **kwargs):
        captured["event_type"] = event_type
        captured["values"] = kwargs["values"]
        return True

    monkeypatch.setattr(
        "lnbits.extensions.cyberherd.services.messaging.publish_event_message",
        fake_publish_event_message,
    )

    async def fake_is_processed(user_id, event_hash, note_id=None):
        return False

    async def fake_register(user_id, event_hash, note_id=None, pubkey=None, event_type=None):
        return True

    monkeypatch.setattr(headbutt.crud, "is_event_processed", fake_is_processed)
    monkeypatch.setattr(headbutt.crud, "register_processed_event", fake_register)

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
    assert values["attacker_name"] == "nostr:nprofile-attacker"
    assert values["victim_name"] == "nostr:nprofile-victim"
    assert values["name"] == "nostr:nprofile-attacker"
    assert values["attacker_display_name"] == "Attacker Goat"
    assert values["victim_display_name"] == "Victim Goat"
    assert values["attacker_nprofile"] == "nprofile-attacker"
    assert values["victim_nprofile"] == "nprofile-victim"


@pytest.mark.anyio
async def test_headbutt_failure_resolves_attacker_name_when_unset(monkeypatch):
    """Regression: a failed headbutt never runs admission, so a reposter's
    attacker.display_name is unset here. It must be resolved from the member
    row / metadata, not fall back to 'Anon'."""
    captured: dict[str, object] = {}

    async def fake_publish_event_message(event_type, **kwargs):
        captured["values"] = kwargs["values"]
        return True

    monkeypatch.setattr(
        "lnbits.extensions.cyberherd.services.messaging.publish_event_message",
        fake_publish_event_message,
    )

    async def fake_is_processed(user_id, event_hash, note_id=None):
        return False

    async def fake_register(user_id, event_hash, note_id=None, pubkey=None, event_type=None):
        return True

    monkeypatch.setattr(headbutt.crud, "is_event_processed", fake_is_processed)
    monkeypatch.setattr(headbutt.crud, "register_processed_event", fake_register)

    service = headbutt.EnhancedHeadbuttService(
        db=_FakeDb(), messaging_module=SimpleNamespace(), app=SimpleNamespace(), user_id="user-id"
    )

    async def fake_websocket_topic():
        return "topic"

    async def fake_reply_params(note_id, settings=None):
        return None, None

    monkeypatch.setattr(service, "_get_websocket_topic", fake_websocket_topic)
    monkeypatch.setattr(service, "_resolve_reply_params", fake_reply_params)

    # Reposter: no display_name / nprofile set on the attacker object.
    attacker = SimpleNamespace(
        pubkey="attacker-pubkey",
        display_name=None,
        nprofile=None,
        amount=0,
        note="a" * 64,
        event_id="event-id",
        kinds=[6],
    )
    victim = {"pubkey": "victim-pubkey", "display_name": "Victim Goat", "amount": 10}

    await service._send_headbutt_failure_notification(attacker, victim, 11)

    values = captured["values"]
    assert values["attacker_display_name"] == "Attacker Goat"  # resolved, not "Anon"
    assert values["name"] == "nostr:nprofile-attacker"


def _make_failure_service(monkeypatch, processed, publishes, *, recovery_mode=False):
    async def fake_is_processed(user_id, event_hash, note_id=None):
        return (event_hash, note_id) in processed

    async def fake_register(user_id, event_hash, note_id=None, pubkey=None, event_type=None):
        processed.add((event_hash, note_id))
        return True

    async def fake_publish_event_message(event_type, **kwargs):
        publishes.append(event_type)
        return True

    monkeypatch.setattr(headbutt.crud, "is_event_processed", fake_is_processed)
    monkeypatch.setattr(headbutt.crud, "register_processed_event", fake_register)
    monkeypatch.setattr(
        "lnbits.extensions.cyberherd.services.messaging.publish_event_message",
        fake_publish_event_message,
    )

    service = headbutt.EnhancedHeadbuttService(
        db=_FakeDb(),
        messaging_module=SimpleNamespace(),
        app=SimpleNamespace(),
        user_id="user-id",
        recovery_mode=recovery_mode,
    )

    async def fake_websocket_topic():
        return "topic"

    async def fake_reply_params(note_id, settings=None):
        return None, None

    monkeypatch.setattr(service, "_get_websocket_topic", fake_websocket_topic)
    monkeypatch.setattr(service, "_resolve_reply_params", fake_reply_params)
    return service


def _repost_attacker(event_id="repost-evt", note="note-1"):
    return SimpleNamespace(
        pubkey="attacker-pubkey", display_name="Sat", nprofile="nprofile-attacker",
        amount=0, note=note, note_id=note, event_id=event_id, kinds=[6], relays=None,
    )


@pytest.mark.anyio
async def test_headbutt_failure_publishes_once_and_registers(monkeypatch):
    """Regression: a failed headbutt must register its event as processed and not
    publish twice. Previously failures were never registered, so the same repost
    was re-processed every poll / replayed on recovery, spamming duplicates."""
    processed: set = set()
    publishes: list = []
    service = _make_failure_service(monkeypatch, processed, publishes)
    victim = {"pubkey": "victim-pubkey", "display_name": "Victim Goat", "amount": 10}

    # Same event delivered twice (e.g. two relays, or recovery + realtime).
    await service._send_headbutt_failure_notification(_repost_attacker(), victim, 11)
    await service._send_headbutt_failure_notification(_repost_attacker(), victim, 11)

    assert publishes == ["kind_6_headbutt_failure"]      # only the first publishes
    assert ("repost-evt", "note-1") in processed          # event marked processed


@pytest.mark.anyio
async def test_headbutt_failure_recovery_registers_without_publishing(monkeypatch):
    """During startup recovery the failure is reconciled silently: the event is
    registered as processed but no (stale) rejection note is published."""
    processed: set = set()
    publishes: list = []
    service = _make_failure_service(monkeypatch, processed, publishes, recovery_mode=True)
    victim = {"pubkey": "victim-pubkey", "display_name": "Victim Goat", "amount": 10}

    await service._send_headbutt_failure_notification(_repost_attacker(), victim, 11)

    assert publishes == []                                 # nothing published in recovery
    assert ("repost-evt", "note-1") in processed           # but still registered
