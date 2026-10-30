"""Occasional live-chat invitations and observed feeder pause transitions.

This worker only reads OpenHAB state. It never requests feeding or sends payments.
"""

import asyncio
import time

from loguru import logger

from .. import crud
from . import streams, treats
from .note_metadata import extract_30311_address

INVITATION_INTERVAL_SECONDS = 7200
CHECK_INTERVAL_SECONDS = 60
_update_lock = asyncio.Lock()


async def queue_notice(
    owner,
    wallet,
    event_type,
    values,
    source,
    stream,
    *,
    parent=None,
    destination="chat"
):
    """Store one destination and immutable inputs for each stream-chat notice."""
    address = extract_30311_address(stream) if stream else None
    if destination == "chat" and not address:
        return False
    channel = address if destination == "chat" else "kind1"
    await treats.put(
        treats.key("stream-notice", owner, channel, event_type, source),
        owner,
        "message",
        "queued",
        {
            "event_type": event_type,
            "values": values,
            "stream_notice": True,
            "stream_address": address,
            "roots": [stream["id"]] if stream else [],
            "wallet": wallet,
            "topic": "cyberherd",
            "reply_message": parent,
        },
        insert=True,
    )
    return True


async def observe_pause(owner, wallet, stream, paused, *, evening=False):
    """Publish confirmed pause transitions; unknown readings preserve state."""
    if paused is None:
        return
    identifier = treats.key("feeder-availability", owner)
    async with _update_lock, crud.admission_transaction():
        previous = await treats.record(identifier)
        state = "paused" if paused else "enabled"
        reason = "evening" if paused and evening else "ordinary"
        if previous and previous["state"] == state:
            return
        sequence = previous["data"]["sequence"] + 1 if previous else 1
        if previous or paused:
            event_type = (
                "feeder_requests_paused" if paused else "feeder_requests_enabled"
            )
            if paused and evening:
                event_type = "feeder_requests_paused_night"
            previous_type = (
                "feeder_requests_paused"
                if previous and previous["state"] == "paused"
                else "feeder_requests_enabled"
            )
            if previous:
                previous_type = previous["data"].get("event_type", previous_type)
            previous_sequence = previous["data"]["sequence"] if previous else 0
            note_parent = treats.key(
                "stream-notice", owner, "kind1", previous_type, previous_sequence
            )
            if not await treats.record(note_parent):
                note_parent = None
            await queue_notice(
                owner,
                wallet,
                event_type,
                {},
                sequence,
                None,
                destination="note",
                parent=note_parent,
            )
            if stream:
                chat_parent = treats.key(
                    "stream-notice",
                    owner,
                    extract_30311_address(stream),
                    previous_type,
                    previous_sequence,
                )
                if not await treats.record(chat_parent):
                    chat_parent = None
                await queue_notice(
                    owner, wallet, event_type, {}, sequence, stream, parent=chat_parent
                )
        await treats.put(
            identifier,
            owner,
            "stream-availability",
            state,
            {
                "sequence": sequence,
                "reason": reason,
                "event_type": (
                    event_type if previous or paused else "feeder_requests_enabled"
                ),
            },
        )


async def queue_invitation(settings, stream, *, now=None):
    from lnbits.extensions.cyberherd_messaging.crud import (
        ensure_default_templates,
        get_message_templates,
    )

    now = time.time() if now is None else now
    address = extract_30311_address(stream)
    marker = treats.key("stream-invitation", settings.user_id, address)
    await ensure_default_templates(settings.user_id)
    # Removing the invitation category is an explicit opt-out; do not restore it.
    if not await get_message_templates(settings.user_id, "cyber_herd_invitation"):
        return False
    async with _update_lock, crud.admission_transaction():
        previous = await treats.record(marker)
        if (
            previous
            and now - previous["data"]["queued_at"] < INVITATION_INTERVAL_SECONDS
        ):
            return False
        sequence = previous["data"]["sequence"] + 1 if previous else 1
        members = await crud.get_active_cyberherd_members(settings.user_id)
        active = len([m for m in members if not m.get("banned")])
        values = {
            "minimum_sats": max(1, settings.minimum_sats),
            "member_allocation": (
                settings.member_allocation_percent if settings.zap_wallet else 100
            ),
            "active_members": active,
            "max_members": settings.max_members,
            "spots_remaining": max(0, settings.max_members - active),
        }
        await queue_notice(
            settings.user_id,
            settings.herd_wallet,
            "cyber_herd_invitation",
            values,
            sequence,
            stream,
        )
        await treats.put(
            marker,
            settings.user_id,
            "stream-invitation",
            "queued",
            {"sequence": sequence, "queued_at": now},
        )
    return True


async def queue_feeding_outcome(owner, data, state, accepted_feedings):
    """Report recorded command/transfer outcomes, never infer physical food delivery."""
    if state == "complete" and not data.get("index"):
        return
    trigger_id = treats.key("feeder-notice", owner, data["origin"])
    trigger = await treats.record(trigger_id)
    parent = None
    if trigger and trigger["data"].get("stream_address"):
        # Keep the channel selected for this payment even if another stream starts.
        context = trigger["data"]
        address = context["stream_address"]
        stream = {
            "kind": 30311,
            "pubkey": address.split(":", 2)[1],
            "id": (context.get("roots") or [""])[0],
            "tags": [["d", address.split(":", 2)[2]]],
        }
        wallet = context["wallet"]
        parent = trigger_id
    else:
        stream = await streams.active(owner)
        if not stream:
            return
        settings = await crud.get_settings(owner)
        wallet = settings.herd_wallet if settings else None
    if not wallet:
        return
    await queue_notice(
        owner,
        wallet,
        "feeder_batch_finished" if state == "complete" else "feeder_outcome_uncertain",
        {"accepted_feedings": accepted_feedings, "funding_cycles": data["index"]},
        data["origin"],
        stream,
        parent=parent,
    )


async def process_once():
    from lnbits.extensions.lightning_goats.crud import get_settings as goat_settings
    from lnbits.extensions.lightning_goats.services.openhab import OpenHABService

    from ..utils.common import is_extension_enabled_for_user

    for settings in await crud.get_all_settings():
        owner = settings.user_id
        if (
            not owner
            or not settings.herd_wallet
            or not await is_extension_enabled_for_user(owner)
        ):
            continue
        try:
            stream = await streams.active(owner)
            config = await goat_settings(owner)
            paused = None
            if config and config.openhab_url and config.openhab_auth:
                client = OpenHABService(config.openhab_url, config.openhab_auth)
                try:
                    paused = await client.get_feeder_override_state()
                    from .evening import is_evening_pause

                    evening = (
                        await is_evening_pause(client) if paused is True else False
                    )
                finally:
                    await client.close()
                closing_stream = stream
                if evening and not closing_stream:
                    closing_stream = await streams.closing_chat(owner)
                await observe_pause(
                    owner, settings.herd_wallet, closing_stream, paused, evening=evening
                )
            if paused is False and stream:
                await queue_invitation(settings, stream)
        except Exception:
            logger.warning("CyberHerd stream-chat status check deferred")


async def run():
    while True:
        try:
            await process_once()
        except Exception:
            logger.warning("CyberHerd stream-chat updates deferred")
        await asyncio.sleep(CHECK_INTERVAL_SECONDS)
