import time
from types import SimpleNamespace

import pytest

from lnbits.extensions.cyberherd.services import subscriptions
from lnbits.extensions.cyberherd.services import time_utils


# --- Unit: prune stale tracked note ids ----------------------------------


def test_prune_stale_tracked_ids_drops_previous_day_notes():
    now = int(time.time())
    old = now - 3 * 86400  # clearly before today's local window
    tracked = ["today_note", "old_note", "unknown_note"]
    timestamps = {"today_note": now, "old_note": old}

    kept = time_utils.prune_stale_tracked_ids(tracked, timestamps)

    assert "today_note" in kept          # today's note stays
    assert "old_note" not in kept        # a previous day's note is dropped
    assert "unknown_note" in kept        # no recorded timestamp -> fail open


# --- Unit: the freshness predicate ---------------------------------------


def test_event_before_todays_reset_is_stale(monkeypatch):
    monkeypatch.setattr(subscriptions, "_local_midnight_timestamp", lambda: 1000)
    settings = SimpleNamespace(midnight_reset_enabled=True)

    assert subscriptions._event_created_before_todays_reset(
        {"created_at": 999}, settings
    ) is True
    # At or after the reset boundary is fresh.
    assert subscriptions._event_created_before_todays_reset(
        {"created_at": 1000}, settings
    ) is False
    assert subscriptions._event_created_before_todays_reset(
        {"created_at": 2000}, settings
    ) is False


def test_no_freshness_filter_when_midnight_reset_disabled(monkeypatch):
    monkeypatch.setattr(subscriptions, "_local_midnight_timestamp", lambda: 1000)
    settings = SimpleNamespace(midnight_reset_enabled=False)
    # With daily reset off, engagement may span days — never treated as stale.
    assert subscriptions._event_created_before_todays_reset(
        {"created_at": 1}, settings
    ) is False


def test_missing_created_at_is_not_treated_as_stale(monkeypatch):
    monkeypatch.setattr(subscriptions, "_local_midnight_timestamp", lambda: 1000)
    settings = SimpleNamespace(midnight_reset_enabled=True)
    assert subscriptions._event_created_before_todays_reset({}, settings) is False


# --- Integration: the guard is wired into process_event_for_user ---------


@pytest.mark.anyio
async def test_process_event_skips_stale_repost(monkeypatch):
    """A kind-6 repost created before today's reset is skipped without ever
    reaching the headbutt trigger — old events are not re-processed as new."""
    triggered = []

    monkeypatch.setattr(
        subscriptions.nostr_helpers, "verify_event_signature", lambda event: True
    )
    monkeypatch.setattr(subscriptions, "get_effective_pubkey", lambda settings: "e" * 64)
    monkeypatch.setattr(subscriptions, "_local_midnight_timestamp", lambda: 2000)

    async def fake_trigger(*args, **kwargs):
        triggered.append(args)
        return {"status": "new"}

    monkeypatch.setattr(subscriptions, "_trigger_repost_headbutt", fake_trigger)

    settings = SimpleNamespace(
        repost_tracking_enabled=True,
        midnight_reset_enabled=True,
        tracked_tags=[],
        tracked_event_ids=["a" * 64],
    )
    event = {
        "id": "b" * 64,
        "pubkey": "c" * 64,   # not the effective pubkey -> not a self-repost
        "kind": 6,
        "created_at": 1000,   # < today's reset (2000) -> stale
        "tags": [["e", "a" * 64]],
        "sig": "sig",
    }

    result = await subscriptions.process_event_for_user(
        "user-1", event, settings, app=SimpleNamespace()
    )

    assert result is False
    assert triggered == []  # never reached the trigger
