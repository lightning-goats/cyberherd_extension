import json
import os
import time
from datetime import datetime
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest
from fastapi import HTTPException

import lnbits.extensions.cyberherd.views_api as views_api
from lnbits.extensions.cyberherd.services import headbutt
from lnbits.extensions.cyberherd.services import payment_coordinator
from lnbits.extensions.cyberherd.services import time_utils
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
async def test_notes_subscription_uses_local_day_since_for_realtime_detection(
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
    assert note_filter["since"] == int(
        subscriptions._get_today_boundaries_utc().local_since_ts
    )


@pytest.mark.anyio
async def test_realtime_note_poll_queries_and_processes_today_author_notes(
    monkeypatch,
):
    settings = _settings(tracked_event_ids=[])
    note_id = "d" * 64
    event = {
        "id": note_id,
        "kind": 1,
        "pubkey": "b" * 64,
        "created_at": subscriptions._get_today_boundaries_utc().local_since_ts + 30,
        "tags": [["t", "cyberherd"]],
        "content": "hello #CyberHerd",
    }
    captured = {"processed": []}

    async def fake_get_settings(user_id):
        return settings

    async def fake_query_events(filters, limit=100, timeout=6.0):
        captured["filters"] = filters
        captured["limit"] = limit
        return [event]

    async def fake_process_event(received_event):
        captured["processed"].append(received_event)

    monkeypatch.setattr(nostr_websocket_monitor, "get_settings", fake_get_settings)
    monkeypatch.setattr(subscriptions, "get_effective_pubkey", lambda _settings: "b" * 64)
    monkeypatch.setattr(
        "lnbits.extensions.cyberherd.services.nostr_helpers.query_events",
        fake_query_events,
    )

    monitor = nostr_websocket_monitor.NostrWebSocketMonitor("user-id")
    monkeypatch.setattr(monitor, "_process_event", fake_process_event)

    processed = await monitor._poll_realtime_notes_once()

    assert processed == 1
    assert captured["processed"] == [event]
    assert captured["filters"]["kinds"] == [1, 30311]
    assert captured["filters"]["authors"] == ["b" * 64]
    assert "#t" not in captured["filters"]
    assert captured["filters"]["since"] == int(
        subscriptions._get_today_boundaries_utc().local_since_ts
    )


@pytest.mark.anyio
async def test_engagement_subscription_uses_local_day_since(monkeypatch):
    note_id = "c" * 64
    settings = _settings(tracked_event_ids=[note_id])
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

    engagement_reqs = [
        msg for msg in sent_messages
        if msg[0] == "REQ" and msg[2].get("kinds") == [6, 7]
    ]
    assert len(engagement_reqs) == 1
    engagement_filter = engagement_reqs[0][2]
    assert engagement_filter["#e"] == [note_id]
    assert engagement_filter["since"] == int(
        subscriptions._get_today_boundaries_utc().local_since_ts
    )


@pytest.mark.anyio
async def test_realtime_interaction_poll_queries_and_processes_tracked_events(
    monkeypatch,
):
    note_id = "c" * 64
    settings = _settings(tracked_event_ids=[note_id])
    event = {
        "id": "e" * 64,
        "kind": 7,
        "pubkey": "f" * 64,
        "created_at": subscriptions._get_today_boundaries_utc().local_since_ts + 45,
        "tags": [["e", note_id]],
        "content": "+",
    }
    captured = {"processed": []}

    async def fake_get_settings(user_id):
        return settings

    async def fake_query_events(filters, limit=200, timeout=6.0):
        captured["filters"] = filters
        captured["limit"] = limit
        return [event]

    async def fake_process_event(received_event):
        captured["processed"].append(received_event)

    monkeypatch.setattr(nostr_websocket_monitor, "get_settings", fake_get_settings)
    monkeypatch.setattr(
        "lnbits.extensions.cyberherd.services.nostr_helpers.query_events",
        fake_query_events,
    )

    monitor = nostr_websocket_monitor.NostrWebSocketMonitor("user-id")
    monkeypatch.setattr(monitor, "_process_event", fake_process_event)

    processed = await monitor._poll_realtime_interactions_once()

    assert processed == 1
    assert captured["processed"] == [event]
    assert captured["filters"]["kinds"] == [6, 7]
    assert captured["filters"]["#e"] == [note_id]
    assert captured["filters"]["since"] == int(
        subscriptions._get_today_boundaries_utc().local_since_ts
    )


@pytest.mark.anyio
async def test_zap_receipt_websocket_subscription_remains_disabled(monkeypatch):
    settings = _settings(tracked_event_ids=["c" * 64], zap_tracking_enabled=True)
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

    req_kinds = [
        msg[2].get("kinds")
        for msg in sent_messages
        if msg[0] == "REQ"
    ]
    assert [9735] not in req_kinds


def test_local_day_boundaries_handle_dst_spring_forward(monkeypatch):
    original_tz = os.environ.get("TZ")
    monkeypatch.setenv("TZ", "America/Denver")
    if hasattr(time, "tzset"):
        time.tzset()

    real_datetime = datetime
    denver = ZoneInfo("America/Denver")

    class FakeDateTime(real_datetime):
        @classmethod
        def now(cls, tz=None):
            current = real_datetime(2026, 3, 8, 12, 0, tzinfo=denver)
            if tz is None:
                return current
            return current.astimezone(tz)

    monkeypatch.setattr(time_utils, "datetime", FakeDateTime)

    try:
        boundaries = time_utils.get_day_boundaries_utc()
    finally:
        if original_tz is None:
            monkeypatch.delenv("TZ", raising=False)
        else:
            monkeypatch.setenv("TZ", original_tz)
        if hasattr(time, "tzset"):
            time.tzset()

    expected_since = int(real_datetime(2026, 3, 8, 0, 0, tzinfo=denver).timestamp())
    expected_until = int(real_datetime(2026, 3, 9, 0, 0, tzinfo=denver).timestamp())

    assert boundaries.local_since_ts == expected_since
    assert boundaries.local_until_ts == expected_until
    assert boundaries.local_until_ts - boundaries.local_since_ts == 23 * 60 * 60


@pytest.mark.anyio
async def test_headbutt_uses_configured_minimum_sats(monkeypatch):
    note_id = "c" * 64
    admitted = {"called": False}

    class FakeDb:
        async def get_settings(self, user_id):
            return _settings(
                max_members=3,
                minimum_sats=50,
                tracked_event_ids=[note_id],
            )

        async def get_active_cyberherd_members(self, user_id=None):
            return []

    async def fake_is_pubkey_banned(pubkey, user_id):
        return False

    async def fake_get_today_notes(self):
        return [note_id]

    async def fake_admission(self, attacker):
        admitted["called"] = True
        return "new"

    async def fake_failure(self, attacker, victim, required):
        admitted["required"] = required

    monkeypatch.setattr(headbutt.crud, "is_pubkey_banned", fake_is_pubkey_banned)
    monkeypatch.setattr(
        headbutt.EnhancedHeadbuttService,
        "_get_today_cyberherd_notes",
        fake_get_today_notes,
    )
    monkeypatch.setattr(
        headbutt.EnhancedHeadbuttService,
        "_handle_attacker_admission",
        fake_admission,
    )
    monkeypatch.setattr(
        headbutt.EnhancedHeadbuttService,
        "_send_headbutt_failure_notification",
        fake_failure,
    )

    service = headbutt.EnhancedHeadbuttService(
        db=FakeDb(),
        messaging_module=SimpleNamespace(),
        user_id="user-id",
    )
    attacker = headbutt._Attacker(
        pubkey="d" * 64,
        amount=25,
        kinds=[9735],
        note_id=note_id,
        event_id="e" * 64,
    )

    result = await service.attempt_headbutt(attacker)

    assert result is None
    assert admitted["called"] is False
    assert admitted["required"] == 50


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


@pytest.mark.anyio
async def test_process_note_for_tracked_tags_returns_whether_note_was_tracked(
    monkeypatch,
):
    settings = _settings(tracked_event_ids=[])
    event = {
        "id": "c" * 64,
        "kind": 1,
        "pubkey": "b" * 64,
        "created_at": 1777788010,
        "tags": [["t", "other"]],
        "content": "not a matching note",
    }

    async def fake_get_settings(user_id):
        return settings

    async def fake_append_today(user_id, eff_pub, tags, event, app=None):
        return False

    monkeypatch.setattr(subscriptions.crud, "get_settings", fake_get_settings)
    monkeypatch.setattr(subscriptions, "get_effective_pubkey", lambda _settings: "b" * 64)
    monkeypatch.setattr(subscriptions, "_append_today", fake_append_today)

    tracked = await subscriptions.process_note_for_tracked_tags(
        user_id="user-id",
        event=event,
        app=None,
    )

    assert tracked is False


@pytest.mark.anyio
async def test_payment_recovery_uses_supplied_tracked_settings(monkeypatch):
    settings = _settings(
        herd_wallet="herd-wallet-id",
        tracked_event_ids=["c" * 64],
        tracked_event_timestamps={},
    )
    captured = {}

    async def fake_get_payments(wallet_id, incoming, since, limit):
        return [SimpleNamespace(wallet_id="herd-wallet-id", extra={"nostr": "{}"})]

    async def fake_process_payment_for_zap(self, payment, settings_override=None):
        captured["settings_override"] = settings_override
        return False

    monkeypatch.setattr(
        "lnbits.core.services.payments.get_payments",
        fake_get_payments,
    )
    monkeypatch.setattr(
        payment_coordinator.PaymentCoordinatorService,
        "_process_payment_for_zap",
        fake_process_payment_for_zap,
    )

    monitor = payment_coordinator.PaymentCoordinatorService(user_id="user-id")

    await monitor._recover_missed_payment_zaps(settings)

    assert captured["settings_override"] is settings


@pytest.mark.anyio
async def test_payment_recovery_scans_when_tracked_ids_empty(monkeypatch):
    settings = _settings(
        herd_wallet="herd-wallet-id",
        tracked_event_ids=[],
        tracked_event_timestamps={},
    )
    captured = {"processed": 0}

    async def fake_get_payments(wallet_id, incoming, since, limit):
        return [SimpleNamespace(wallet_id="herd-wallet-id", extra={"nostr": "{}"})]

    async def fake_process_payment_for_zap(self, payment, settings_override=None):
        captured["processed"] += 1
        captured["settings_override"] = settings_override
        return False

    monkeypatch.setattr(
        "lnbits.core.services.payments.get_payments",
        fake_get_payments,
    )
    monkeypatch.setattr(
        payment_coordinator.PaymentCoordinatorService,
        "_process_payment_for_zap",
        fake_process_payment_for_zap,
    )

    monitor = payment_coordinator.PaymentCoordinatorService(user_id="user-id")

    result = await monitor._recover_missed_payment_zaps(settings)

    assert result["scanned"] == 1
    assert captured["processed"] == 1
    assert captured["settings_override"] is settings


@pytest.mark.anyio
async def test_manual_zap_recovery_runs_when_tracked_ids_empty(monkeypatch):
    settings = _settings(tracked_event_ids=[])
    captured = {}

    async def fake_get_settings(user_id):
        return settings

    async def fake_get_cyberherd_notes_for_settings(settings_arg):
        captured["settings_arg"] = settings_arg
        return []

    class FakeZapMonitor:
        last_zap_at = None
        last_error = None

        async def diagnose_missed_payment_zaps(self, settings_arg):
            captured["diag_settings"] = settings_arg
            return []

        async def _recover_missed_payment_zaps(self, settings_arg):
            captured["recovery_settings"] = settings_arg
            return {"scanned": 1, "processed": 1}

    monkeypatch.setattr(views_api.crud, "get_settings", fake_get_settings)
    monkeypatch.setattr(
        views_api.crud,
        "get_cyberherd_notes_for_settings",
        fake_get_cyberherd_notes_for_settings,
    )
    monkeypatch.setattr(
        views_api,
        "get_zap_monitor",
        lambda app=None, db=None, user_id=None: FakeZapMonitor(),
    )

    request = SimpleNamespace(app=SimpleNamespace())
    wallet_info = SimpleNamespace(wallet=SimpleNamespace(user="user-id"))

    result = await views_api.api_trigger_zap_recovery(request, wallet_info)

    assert result["tracked_notes_count"] == 0
    assert result["recovery_method"] == "payment"
    assert result["recovery_completed"] is True
    assert captured["settings_arg"] is settings
    assert captured["diag_settings"] is settings
    assert captured["recovery_settings"] is settings


@pytest.mark.anyio
async def test_diagnostics_endpoints_reject_anonymous_callers():
    request = SimpleNamespace(app=SimpleNamespace())
    auth = {"type": "anonymous", "value": None}

    with pytest.raises(HTTPException) as zap_exc:
        await views_api.api_get_zap_monitor_status(request, auth=auth)
    assert zap_exc.value.status_code == 401

    with pytest.raises(HTTPException) as diag_exc:
        await views_api.api_get_nostr_diagnostics(request, auth=auth)
    assert diag_exc.value.status_code == 401


@pytest.mark.anyio
async def test_existing_websocket_monitor_gets_late_app_context(monkeypatch):
    nostr_websocket_monitor._active_monitors.clear()
    nostr_websocket_monitor._app_instance = None
    app = SimpleNamespace()
    monitor = nostr_websocket_monitor.NostrWebSocketMonitor("user-id", app=None)
    nostr_websocket_monitor._active_monitors["user-id"] = monitor

    async def fake_start(self):
        self.started = True

    monkeypatch.setattr(
        nostr_websocket_monitor.NostrWebSocketMonitor,
        "start",
        fake_start,
    )

    returned = await nostr_websocket_monitor.start_monitor_for_user("user-id", app=app)

    assert returned is monitor
    assert monitor.app is app
    assert nostr_websocket_monitor._app_instance is app
    assert monitor.started is True
    nostr_websocket_monitor._active_monitors.clear()


@pytest.mark.anyio
async def test_websocket_monitor_start_requires_app_context(monkeypatch):
    nostr_websocket_monitor._active_monitors.clear()
    nostr_websocket_monitor._app_instance = None

    with pytest.raises(RuntimeError):
        await nostr_websocket_monitor.start_monitor_for_user("user-id")


def test_process_zap_receipt_route_is_registered():
    paths = {
        getattr(route, "path", None)
        for route in views_api.cyberherd_api_router.routes
    }

    assert "/api/v1/process_zap_receipt" in paths


@pytest.mark.anyio
async def test_process_zap_receipt_does_not_use_zapper_as_target_author(monkeypatch):
    captured = {}

    class FakeRequest:
        app = SimpleNamespace()

        async def json(self):
            return {
                "zap_receipt_id": "a" * 64,
                "zapper_pubkey": "b" * 64,
                "amount_sats": 21,
                "zapped_event_id": "c" * 64,
            }

    class FakeZapMonitor:
        async def _process_payment_for_zap(self, payment, settings_override=None):
            captured["nostr"] = json.loads(payment.extra["nostr"])
            captured["settings_override"] = settings_override
            return True

    monkeypatch.setattr(
        views_api,
        "get_zap_monitor",
        lambda app=None, db=None, user_id=None: FakeZapMonitor(),
    )

    async def fake_get_settings(user_id):
        return _settings(herd_wallet="herd-wallet-id")

    monkeypatch.setattr(
        views_api.crud,
        "get_settings",
        fake_get_settings,
    )

    wallet_info = SimpleNamespace(wallet=SimpleNamespace(user="user-id"))

    response = await views_api.api_process_zap_receipt(FakeRequest(), wallet_info)

    assert response.status_code == 200
    assert ["e", "c" * 64] in captured["nostr"]["tags"]
    assert ["p", "b" * 64] not in captured["nostr"]["tags"]
    assert captured["nostr"]["pubkey"] == "b" * 64
    assert captured["settings_override"].herd_wallet == "herd-wallet-id"


@pytest.mark.anyio
async def test_fetch_tagged_note_returns_event_matching_tracked_tag(monkeypatch):
    settings = _settings(
        tracked_tags=["#CyberHerd"],
        tracked_event_ids=[],
        nostr_pubkey_override="b" * 64,
    )
    note_id = "c" * 64
    event = {
        "id": note_id,
        "kind": 1,
        "pubkey": "b" * 64,
        "created_at": subscriptions._get_today_boundaries_utc().local_since_ts + 10,
        "tags": [["t", "cyberherd"]],
        "content": "hello #CyberHerd",
    }

    async def fake_query_events(filters, limit=1, timeout=5.0, extra_relays=None):
        assert filters == {"ids": [note_id]}
        assert extra_relays is None
        return [event]

    monkeypatch.setattr(
        "lnbits.extensions.cyberherd.services.nostr_helpers.query_events",
        fake_query_events,
    )
    monkeypatch.setattr(
        payment_coordinator,
        "resolve_effective_pubkey",
        lambda _settings: "b" * 64,
    )

    monitor = payment_coordinator.PaymentCoordinatorService(user_id="user-id")

    fetched = await monitor._fetch_tagged_note(
        settings=settings,
        note_id=note_id,
        author_hint="b" * 64,
    )

    assert fetched is event


@pytest.mark.anyio
async def test_payment_zap_uses_relay_hints_for_opportunistic_tracking(monkeypatch):
    settings = _settings(
        herd_wallet="herd-wallet-id",
        tracked_event_ids=[],
        tracked_event_timestamps={},
        nostr_pubkey_override="b" * 64,
        minimum_sats=10,
    )
    captured = {}
    note_id = "c" * 64
    zap_request = {
        "pubkey": "d" * 64,
        "tags": [
            ["e", note_id],
            ["p", "b" * 64],
            ["relays", "wss://relay.example", "wss://relay2.example"],
        ],
    }
    payment = SimpleNamespace(
        wallet_id="herd-wallet-id",
        amount=33_000,
        extra={"nostr": zap_request},
        payment_hash="e" * 64,
        checking_id="e" * 64,
    )

    async def fake_get_settings(user_id):
        return settings

    async def fake_get_member(pubkey, user_id=None):
        return None

    async def fake_ensure_note_tracked(
        self,
        settings,
        note_id,
        created_at=None,
        author_hint=None,
        relay_hints=None,
    ):
        captured["relay_hints"] = relay_hints
        captured["author_hint"] = author_hint
        settings.tracked_event_ids = [note_id]
        return True

    async def fake_register_processed_event(*args, **kwargs):
        return False

    monkeypatch.setattr(payment_coordinator.crud, "get_settings", fake_get_settings)
    monkeypatch.setattr(
        payment_coordinator.crud,
        "get_cyberherd_member_by_pubkey",
        fake_get_member,
    )
    monkeypatch.setattr(
        payment_coordinator.crud,
        "register_processed_event",
        fake_register_processed_event,
    )
    monkeypatch.setattr(
        payment_coordinator,
        "resolve_effective_pubkey",
        lambda _settings: "b" * 64,
    )
    monkeypatch.setattr(
        payment_coordinator.PaymentCoordinatorService,
        "_ensure_note_tracked",
        fake_ensure_note_tracked,
    )

    monitor = payment_coordinator.PaymentCoordinatorService(user_id="user-id")

    ok = await monitor._process_payment_for_zap(payment, settings_override=settings)

    assert ok is True
    assert captured["relay_hints"] == ["wss://relay.example", "wss://relay2.example"]
    assert captured["author_hint"] == "b" * 64


@pytest.mark.anyio
async def test_payment_recovery_rejects_previous_day_tracked_note_for_new_member(
    monkeypatch,
):
    note_id = "c" * 64
    zapper_pubkey = "d" * 64
    target_pubkey = "b" * 64
    boundaries = subscriptions._get_today_boundaries_utc()
    settings = _settings(
        herd_wallet="herd-wallet-id",
        tracked_event_ids=[note_id],
        tracked_event_timestamps={note_id: boundaries.local_since_ts - 60},
        nostr_pubkey_override=target_pubkey,
    )
    payment = SimpleNamespace(
        wallet_id="herd-wallet-id",
        amount=33_000,
        extra={
            "nostr": {
                "pubkey": zapper_pubkey,
                "tags": [["e", note_id], ["p", target_pubkey]],
            }
        },
        payment_hash="e" * 64,
        checking_id="e" * 64,
    )
    captured = {"headbutts": 0, "registered": 0}

    async def fake_get_payments(wallet_id, incoming, since, limit):
        assert since == boundaries.local_since_ts - 60
        return [payment]

    async def fake_get_member(pubkey, user_id=None):
        return None

    async def fake_register_processed_event(*args, **kwargs):
        captured["registered"] += 1
        return True

    async def fake_trigger_headbutt_from_zap(*args, **kwargs):
        captured["headbutts"] += 1
        return {"status": "new"}

    monkeypatch.setattr(
        "lnbits.core.services.payments.get_payments",
        fake_get_payments,
    )
    monkeypatch.setattr(
        payment_coordinator.crud,
        "get_cyberherd_member_by_pubkey",
        fake_get_member,
    )
    monkeypatch.setattr(
        payment_coordinator.crud,
        "register_processed_event",
        fake_register_processed_event,
    )
    monkeypatch.setattr(
        payment_coordinator,
        "trigger_headbutt_from_zap",
        fake_trigger_headbutt_from_zap,
    )
    monkeypatch.setattr(
        payment_coordinator,
        "resolve_effective_pubkey",
        lambda _settings: target_pubkey,
    )

    monitor = payment_coordinator.PaymentCoordinatorService(user_id="user-id")

    result = await monitor._recover_missed_payment_zaps(settings)

    assert result["scanned"] == 1
    assert result["processed"] == 1
    assert result["successful"] == 0
    assert monitor.last_error == "note_not_current"
    assert captured["registered"] == 0
    assert captured["headbutts"] == 0
