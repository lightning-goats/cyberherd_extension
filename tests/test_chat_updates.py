"""Extra stream notices use durable records and mocked device/network effects."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from pynostr.event import Event

from lnbits.extensions.cyberherd.services import chat_updates, streams, treats
from lnbits.extensions.cyberherd.tests.test_database_operations import (
    audit_db as _audit_db,
)
from lnbits.extensions.cyberherd.tests.test_database_operations import save_settings
from lnbits.extensions.cyberherd.tests.test_messaging_template_storage import (
    messaging_db as _messaging_db,
)
from lnbits.extensions.cyberherd.tests.test_stream_integration import stream_event
from lnbits.extensions.cyberherd.tests.test_treats import rows
from lnbits.extensions.cyberherd_messaging import crud as messages_crud
from lnbits.extensions.cyberherd_messaging import migrations
from lnbits.extensions.cyberherd_messaging import services as messages
from lnbits.extensions.lightning_goats import tasks as goat_tasks
from lnbits.extensions.lightning_goats.services.openhab import OpenHABService

# Only disposable database-owned schemas are interpolated.
# ruff: noqa: S608
audit_db = _audit_db
messaging_db = _messaging_db


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.mark.anyio
async def test_pause_transitions_have_independent_note_and_chat_deliveries(audit_db):
    stream = stream_event()
    await chat_updates.observe_pause("owner", "herd", stream, False)
    assert not await rows("message")
    for state in [True, True, None, False, False]:
        await chat_updates.observe_pause("owner", "herd", stream, state)
    notices = await rows("message")
    assert len(notices) == 4
    assert sum(bool(r["data"]["stream_address"]) for r in notices) == 2
    assert {r["data"]["event_type"] for r in notices} == {
        "feeder_requests_paused",
        "feeder_requests_enabled",
    }
    enabled = [
        r for r in notices if r["data"]["event_type"] == "feeder_requests_enabled"
    ]
    assert all(r["data"]["reply_message"] for r in enabled)
    # Stream refreshes and worker restarts must not repeat the public note.
    await chat_updates.observe_pause("owner", "herd", stream_event(d="second"), False)
    assert len(await rows("message")) == 4


@pytest.mark.anyio
async def test_pause_notes_work_without_an_active_stream(audit_db):
    await chat_updates.observe_pause("owner", "herd", None, True)
    await chat_updates.observe_pause("owner", "herd", None, False)
    notices = await rows("message")
    assert len(notices) == 2
    assert all(r["data"]["stream_address"] is None for r in notices)


@pytest.mark.anyio
async def test_dual_destinations_sign_expected_kinds_once(messaging_db, monkeypatch):
    stream = stream_event()
    await chat_updates.observe_pause("owner", "herd", stream, True)
    await chat_updates.observe_pause("owner", "herd", stream, False)
    signed = []

    async def publish(wallet, content, kind, tags, *, receipt, persist_signed):
        event = Event(
            kind=kind, content=content, tags=tags, created_at=1000 + len(signed)
        )
        event.sign("1" * 64)
        result = event.to_dict()
        await persist_signed(result)
        receipt.update(event_id=result["id"], submitted=True)
        signed.append(result)
        return True

    monkeypatch.setattr(
        messages, "is_nostr_publishing_enabled", AsyncMock(return_value=True)
    )
    monkeypatch.setattr(messages, "_try_bunker_sign_and_publish", publish)
    monkeypatch.setattr(messages, "send_to_websocket_clients", AsyncMock())
    for _ in range(3):
        for row in await rows("message"):
            if row["state"] == "queued":
                await treats._publish(row)
    assert sorted(e["kind"] for e in signed) == [1, 1, 1311, 1311]
    assert all(
        any(t[0] == "a" for t in e["tags"]) == (e["kind"] == 1311) for e in signed
    )
    assert len({e["id"] for e in signed}) == 4
    assert all(r["state"] == "submitted" for r in await rows("message"))


@pytest.mark.anyio
async def test_invitation_interval_and_deleted_template_opt_out(messaging_db):
    settings = await save_settings(
        herd_wallet="herd", zap_wallet="goats@example.test", minimum_sats=21
    )
    stream = stream_event()
    assert await chat_updates.queue_invitation(settings, stream, now=1000)
    assert not await chat_updates.queue_invitation(settings, stream, now=8199)
    assert await chat_updates.queue_invitation(settings, stream, now=8200)
    assert len(await rows("message")) == 2
    await messages_crud.delete_templates_by_category("owner", "cyber_herd_invitation")
    assert not await chat_updates.queue_invitation(settings, stream, now=15400)
    assert len(await rows("message")) == 2


@pytest.mark.anyio
async def test_availability_observer_reads_state_without_actuating(
    messaging_db, monkeypatch
):
    await save_settings(herd_wallet="herd")
    monkeypatch.setattr(streams, "active", AsyncMock(return_value=stream_event()))
    monkeypatch.setattr(
        "lnbits.extensions.cyberherd.utils.common.is_extension_enabled_for_user",
        AsyncMock(return_value=True),
    )
    monkeypatch.setattr(
        "lnbits.extensions.lightning_goats.crud.get_settings",
        AsyncMock(
            return_value=SimpleNamespace(
                openhab_url="http://10.8.0.2", openhab_auth="test"
            )
        ),
    )
    state = [None]
    calls = []

    class ReadOnlyClient:
        def __init__(self, *args):
            pass

        async def get_feeder_override_state(self):
            calls.append("read")
            return state[0]

        async def get_item_state(self, item):
            return "DAYLIGHT" if item == "Sun_SunPhaseName" else None

        async def trigger_feeder(self, *args):
            raise AssertionError("The status observer must never feed")

        async def close(self):
            pass

    monkeypatch.setattr(
        "lnbits.extensions.lightning_goats.services.openhab.OpenHABService",
        ReadOnlyClient,
    )
    await chat_updates.process_once()
    assert not await rows("message")
    state[0] = False
    await chat_updates.process_once()
    state[0] = True
    await chat_updates.process_once()
    await chat_updates.process_once()
    state[0] = False
    await chat_updates.process_once()
    notices = await rows("message")
    assert len(calls) == 5
    assert sum(r["data"]["event_type"] == "cyber_herd_invitation" for r in notices) == 1
    assert not any("reset" in r["data"]["event_type"] for r in notices)


@pytest.mark.anyio
async def test_batch_notice_is_atomic_and_does_not_claim_physical_delivery(
    messaging_db, monkeypatch
):
    monkeypatch.setattr("random.choice", lambda templates: templates[0])
    await save_settings(herd_wallet="herd")
    stream = stream_event()
    monkeypatch.setattr(streams, "active", AsyncMock(return_value=stream))
    await treats.queue_feeder(
        "owner",
        "herd",
        "deposit",
        {"new_amount": 3456, "feedings": 3, "payout_cycles": 3},
        "cyberherd",
    )
    data = {"origin": "deposit", "index": 3}
    await goat_tasks._record_batch_outcome("batch", "owner", "complete", data)
    await goat_tasks._record_batch_outcome("batch", "owner", "complete", data)
    notices = [
        r
        for r in await rows("message")
        if r["data"]["event_type"] == "feeder_batch_finished"
    ]
    assert len(notices) == 1
    notice = notices[0]["data"]
    assert notice["values"] == {"accepted_feedings": 3, "funding_cycles": 3}
    content = await messages.render_stream_notice(
        "feeder_batch_finished", notice["values"], "owner"
    )
    assert "Physical food delivery is not confirmed" in content
    assert "funding transfer(s) completed" in content
    assert notice["reply_message"] == treats.key("feeder-notice", "owner", "deposit")
    # Publication waits for the original trigger notice so the outcome can reply to it.
    publish = AsyncMock()
    monkeypatch.setattr(messages, "publish_note", publish)
    await treats._publish(notices[0])
    publish.assert_not_awaited()


@pytest.mark.anyio
async def test_template_upgrade_preserves_edits_and_deletions(messaging_db):
    await messages_crud.ensure_default_templates("owner")
    await messages_crud.update_message_template(
        "owner", "feeder_requests_paused", "0", "Custom pause"
    )
    await messages_crud.delete_templates_by_category("owner", "cyber_herd_invitation")
    await migrations.m009_seed_stream_chat_templates(messaging_db)
    await messages_crud.ensure_default_templates("owner")
    templates = await messages_crud.get_message_templates(
        "owner", "feeder_requests_paused"
    )
    assert next(t.content for t in templates if t.key == "0") == "Custom pause"
    assert not await messages_crud.get_message_templates(
        "owner", "cyber_herd_invitation"
    )


@pytest.mark.anyio
@pytest.mark.parametrize("state", [None, "NULL", "UNDEF", "unexpected"])
async def test_undefined_pause_is_not_reported_as_reenabled(monkeypatch, state):
    client = OpenHABService("http://10.8.0.2:8080", "test")
    monkeypatch.setattr(client, "get_item_state", AsyncMock(return_value=state))
    assert await client.get_feeder_override_state() is None
    assert await client.is_feeder_override_enabled() is True


@pytest.mark.anyio
async def test_solar_changes_never_repeat_pause_or_signoff(audit_db):
    stream = stream_event()
    await chat_updates.observe_pause("owner", "herd", stream, True)
    await chat_updates.observe_pause("owner", "herd", stream, True, evening=True)
    await chat_updates.observe_pause("owner", "herd", stream, True, evening=False)
    assert len(await rows("message")) == 2
    await chat_updates.observe_pause("owner", "herd", stream, False)
    await chat_updates.observe_pause("owner", "herd", stream, False, evening=True)
    assert len(await rows("message")) == 4
    await chat_updates.observe_pause("owner", "herd", stream, True, evening=True)
    await chat_updates.observe_pause("owner", "herd", stream, True, evening=True)
    await chat_updates.observe_pause("owner", "herd", stream_event(d="second"), True)
    notices = await rows("message")
    assert len(notices) == 6
    evening = [
        r for r in notices if r["data"]["event_type"] == "feeder_requests_paused_night"
    ]
    assert len(evening) == 2
    await chat_updates.observe_pause("owner", "herd", stream, False)
    resumed = [
        r
        for r in await rows("message")
        if r["data"]["event_type"] == "feeder_requests_enabled"
    ]
    assert (
        sum(r["data"]["reply_message"] in {p["id"] for p in evening} for r in resumed)
        == 2
    )


@pytest.mark.anyio
async def test_new_message_pools_preserve_custom_edits_and_deletions(messaging_db):
    from lnbits.extensions.cyberherd.tests.test_messaging_template_storage import (
        insert_template,
    )
    from lnbits.extensions.cyberherd_messaging.defaults import MESSAGE_VARIATIONS

    await insert_template(messaging_db, "cyber_herd_invitation", "0", "Custom invite")
    await insert_template(
        messaging_db, "feeder_requests_paused_night", "0", "Custom evening"
    )
    await migrations.expand_message_variations(messaging_db)
    await migrations.seed_evening_pause_templates(messaging_db)
    templates = await messages_crud.get_message_templates(
        "owner", "feeder_requests_paused_night"
    )
    assert len(templates) == 8
    assert next(t.content for t in templates if t.key == "0") == "Custom evening"
    invites = await messages_crud.get_message_templates(
        "owner", "cyber_herd_invitation"
    )
    assert next(t.content for t in invites if t.key == "0") == "Custom invite"
    # Existing deleted categories must not be resurrected by the variation upgrade.
    for category in MESSAGE_VARIATIONS:
        if category != "cyber_herd_invitation":
            assert not await messages_crud.get_message_templates("owner", category)
    await messaging_db.execute(
        f"DELETE FROM {messaging_db.references_schema}message_templates "
        "WHERE category IN ('feeder_requests_paused_night','cyber_herd_invitation')"
    )
    await migrations.expand_message_variations(messaging_db)
    await migrations.seed_evening_pause_templates(messaging_db)
    assert not await messages_crud.get_message_templates(
        "owner", "feeder_requests_paused_night"
    )
    assert not await messages_crud.get_message_templates(
        "owner", "cyber_herd_invitation"
    )


@pytest.mark.anyio
async def test_template_upgrade_entrypoints_accept_runner_connection(
    messaging_db, audit_db, monkeypatch
):
    from contextlib import asynccontextmanager
    from types import SimpleNamespace

    from lnbits.core import helpers

    await messages_crud.ensure_default_templates("owner")
    versions = []

    async def record_version(connection, name, version):
        assert connection is not None
        assert name == "cyberherd_messaging"
        versions.append(version)

    monkeypatch.setattr(helpers, "update_migration_version", record_version)

    @asynccontextmanager
    async def version_connection():
        # Version writes are recorded above, without opening the live core DB.
        yield messaging_db

    monkeypatch.setattr(helpers, "core_db", SimpleNamespace(connect=version_connection))
    async with audit_db.connect() as connection:
        await helpers.run_migration(
            connection,
            migrations,
            "cyberherd_messaging",
            SimpleNamespace(version=9),
        )
    assert versions == [10, 11, 12]
    assert (
        len(
            await messages_crud.get_message_templates(
                "owner", "feeder_requests_paused_night"
            )
        )
        == 8
    )
