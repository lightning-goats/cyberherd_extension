"""Project one announcement, one live-stream version and their signed zap goal."""

import time

from .. import crud
from . import streams, treats

# Database-owned schemas only; event and user data are bound parameters.
# ruff: noqa: S608

_known_goal_ids = set()


def goal_id(event):
    return next(
        (
            t[1].lower()
            for t in streams.tags(event, "goal")
            if treats.valid_pubkey(t[1])
        ),
        None,
    )


def valid_goal(event, host):
    from .nostr_helpers import verify_event_signature

    if (
        not isinstance(event, dict)
        or event.get("kind") != 9041
        or event.get("pubkey") != host
        or not verify_event_signature(event)
    ):
        return False
    try:
        amount = streams.tags(event, "amount")
        relays = streams.tags(event, "relays")
        closed = streams.tags(event, "closed_at")
        return bool(
            amount
            and int(amount[0][1]) > 0
            and relays
            and (not closed or int(closed[0][1]) > time.time())
        )
    except (ValueError, TypeError):
        return False


async def saved(owner, identifier):
    row = await treats.record(treats.key("tracking-target", owner, identifier))
    return row["data"] if row else None


async def _save(owner, event):
    if event["kind"] == 9041:
        _known_goal_ids.add((owner, event["id"]))
    await treats.put(
        treats.key("tracking-target", owner, event["id"]),
        owner,
        "tracking-target",
        "verified",
        event,
        insert=True,
    )


async def announcement(owner, settings, stream=None):
    from .subscriptions import _would_event_be_tracked, get_effective_pubkey

    rows = await crud._db().fetchall(
        f"SELECT payload FROM {crud.db.references_schema}delivery_records "
        "WHERE user_id=:uid AND kind='tracking-target'",
        {"uid": owner},
    )
    notes = [treats.decode(r["payload"]) for r in rows]
    notes = [
        e
        for e in notes
        if e.get("kind") == 1
        and _would_event_be_tracked(
            e, get_effective_pubkey(settings), settings.tracked_tags
        )
    ]
    reference = goal_id(stream) if stream else None
    matching = [e for e in notes if reference and goal_id(e) == reference]
    return max(
        matching or notes, key=lambda e: (e["created_at"], e["id"]), default=None
    )


async def reference_goal(owner, settings):
    stream = await streams.active(owner)
    note = await announcement(owner, settings, stream)
    return (goal_id(stream) if stream else None) or (goal_id(note) if note else None)


async def accept_goal(owner, settings, event):
    from .subscriptions import get_effective_pubkey

    if not valid_goal(event, get_effective_pubkey(settings)):
        return False
    if event["id"] != await reference_goal(owner, settings):
        return False
    await _save(owner, event)
    return True


async def observe(owner, settings, event):
    """Notes/streams are authenticated by callers; goals also need a trusted link."""
    from . import nostr_helpers
    from .subscriptions import get_effective_pubkey

    if event["kind"] == 1:
        await _save(owner, event)
    elif event["kind"] == 9041:
        return await accept_goal(owner, settings, event)
    reference = await reference_goal(owner, settings)
    existing = await saved(owner, reference) if reference else None
    if existing and valid_goal(existing, get_effective_pubkey(settings)):
        _known_goal_ids.add((owner, reference))
    if reference and not existing:
        events = await nostr_helpers.query_events(
            {"ids": [reference]}, limit=1, timeout=5.0
        )
        for goal in events or []:
            if goal.get("id") == reference and valid_goal(
                goal, get_effective_pubkey(settings)
            ):
                await _save(owner, goal)
                break
    return True


async def project(owner, settings):
    from .subscriptions import get_effective_pubkey

    stream = await streams.active(owner)
    note = await announcement(owner, settings, stream)
    reference = (goal_id(stream) if stream else None) or (
        goal_id(note) if note else None
    )
    goal = await saved(owner, reference) if reference else None
    if goal and not valid_goal(goal, get_effective_pubkey(settings)):
        goal = None
    events = [e for e in (note, stream, goal) if e]
    return events


async def current_goal(owner, settings, identifier):
    if (
        owner,
        identifier,
    ) not in _known_goal_ids or identifier not in settings.tracked_event_ids:
        return False
    event = await saved(owner, identifier)
    return bool(
        event
        and event.get("kind") == 9041
        and await accept_goal(owner, settings, event)
    )
