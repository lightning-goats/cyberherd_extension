"""Authenticated live-event discovery, stable stream addresses and chat routing."""

import asyncio
import hashlib
import time

import httpx
from pynostr.key import PublicKey

from .. import crud
from .note_metadata import extract_30311_address

# Only the Database-owned schema is interpolated; user values are bound.
# ruff: noqa: S608

# Provider names alone in relay events do not establish authority. Authenticate
# their publishing key over a fixed official HTTPS endpoint, never an event URL.
PROVIDERS = {
    "https://api-core.zap.stream/api/v1": "https://api-core.zap.stream",
    "https://api-uk.zap.stream/api/v1": "https://api-uk.zap.stream",
}
_provider_keys = {}
_stream_lock = asyncio.Lock()


class StreamVerificationUnavailableError(RuntimeError):
    """Provider identity verification can be retried without claiming a payment."""


def tags(event, name):
    return [
        t
        for t in event.get("tags", [])
        if isinstance(t, list) and len(t) > 1 and t[0] == name
    ]


def stream_host(event, host):
    """Accept an owner, host proof or provider after signature validation."""
    if event.get("kind") != 30311 or not host:
        return False
    if event.get("pubkey") == host:
        return True
    address = extract_30311_address(event)
    service = next(
        (t[1].rstrip("/") for t in tags(event, "service") if isinstance(t[1], str)), ""
    )
    for tag in tags(event, "p"):
        if len(tag) < 4 or tag[1] != host or str(tag[3]).lower() != "host":
            continue
        if len(tag) >= 5:
            try:
                if PublicKey.from_hex(host).verify(
                    bytes.fromhex(tag[4]), hashlib.sha256(address.encode()).digest()
                ):
                    return True
            except (TypeError, ValueError):
                pass
        cached = _provider_keys.get(service)
        if (
            service in PROVIDERS
            and cached
            and cached[1] > time.time()
            and cached[0] == event.get("pubkey")
        ):
            return True
    return False


async def authenticate(event, host):
    from .nostr_helpers import verify_event_signature

    if (
        not isinstance(event, dict)
        or event.get("kind") != 30311
        or not isinstance(event.get("created_at"), int)
        or not verify_event_signature(event)
    ):
        return False
    if int(event.get("created_at") or 0) > time.time() + 300:
        return False
    if stream_host(event, host):
        return True
    service = next(
        (t[1].rstrip("/") for t in tags(event, "service") if isinstance(t[1], str)), ""
    )
    if service not in PROVIDERS or not any(
        len(t) >= 4 and t[1] == host and str(t[3]).lower() == "host"
        for t in tags(event, "p")
    ):
        return False
    from lnbits.settings import settings

    try:
        async with httpx.AsyncClient(follow_redirects=False) as client:
            response = await client.get(
                f"{PROVIDERS[service]}/.well-known/lnurlp/{host}",
                headers={"User-Agent": settings.user_agent},
                timeout=10,
            )
            response.raise_for_status()
            key = response.json().get("nostrPubkey")
        if not isinstance(key, str) or len(key) != 64:
            return False
        _provider_keys[service] = (key, time.time() + 1800)
        return key == event.get("pubkey")
    except (httpx.HTTPError, ValueError) as exc:
        raise StreamVerificationUnavailableError(
            "Stream provider verification unavailable"
        ) from exc


async def remember(owner, event):
    """Store validated versions without regressing a stream to older state."""
    from .treats import key, put, record

    address = extract_30311_address(event)
    if not address:
        return
    identifier = key("stream", owner, address)
    async with _stream_lock, crud.admission_transaction():
        previous = await record(identifier)
        if previous:
            old = previous["data"]
            # NIP-01 uses the lowest ID to break equal-timestamp version ties.
            if old["created_at"] > event["created_at"] or (
                old["created_at"] == event["created_at"] and old["id"] <= event["id"]
            ):
                return
        status = next((t[1] for t in tags(event, "status")), "planned")
        await put(identifier, owner, "stream", status, event)


async def lookup(owner, address):
    from .treats import key, record

    row = await record(key("stream", owner, address))
    return row["data"] if row else None


async def active(owner):
    from .treats import decode

    rows = await crud._db().fetchall(
        f"SELECT payload FROM {crud.db.references_schema}delivery_records "
        "WHERE user_id=:uid AND kind='stream' AND state='live'",
        {"uid": owner},
    )
    from .subscriptions import get_effective_pubkey

    settings = await crud.get_settings(owner)
    host = get_effective_pubkey(settings) if settings else None
    events = [decode(row["payload"]) for row in rows]
    events = [e for e in events if stream_host(e, host)]
    events = [e for e in events if int(e["created_at"]) >= time.time() - 3600]
    return max(events, key=lambda e: (e["created_at"], e["id"]), default=None)


def engagement_filters(settings, kinds, since, limit):
    addresses = sorted(
        set((getattr(settings, "tracked_event_addresses", {}) or {}).values())
    )
    kinds = list(kinds)
    # Goals use generic reposts even when no live-stream address is available.
    if 6 in kinds and 16 not in kinds:
        kinds.append(16)
    result = [
        {
            "kinds": kinds,
            "#e": settings.tracked_event_ids,
            "since": since,
            "limit": limit,
        }
    ]
    if addresses:
        result.append({"kinds": kinds, "#a": addresses, "since": since, "limit": limit})
    return result


async def query_engagements(settings, kinds, since, limit, timeout):
    from .nostr_helpers import query_events

    events = {}
    for filters in engagement_filters(settings, kinds, since, limit):
        for event in await query_events(filters, limit=limit, timeout=timeout) or []:
            if isinstance(event, dict) and event.get("id"):
                events[event["id"]] = event
    return list(events.values())


def conversation(settings, root=None):
    return (getattr(settings, "tracked_event_addresses", {}) or {}).get(root)


async def closing_chat(owner):
    """Allow the night sign-off in a stream that ended in the last 15 minutes."""
    from .subscriptions import get_effective_pubkey
    from .treats import decode

    settings = await crud.get_settings(owner)
    host = get_effective_pubkey(settings) if settings else None
    rows = await crud._db().fetchall(
        f"SELECT payload FROM {crud.db.references_schema}delivery_records "
        "WHERE user_id=:uid AND kind='stream' AND state='ended'",
        {"uid": owner},
    )
    events = [decode(row["payload"]) for row in rows]
    events = [
        event
        for event in events
        if stream_host(event, host)
        and time.time() - 900 <= event["created_at"] <= time.time()
    ]
    return max(events, key=lambda e: (e["created_at"], e["id"]), default=None)
