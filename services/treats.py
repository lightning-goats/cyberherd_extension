"""Distribution accounting and member messages; never initiates member payments.

Payloads are base64 encoded to preserve signed Nostr bytes through LNbits' SQL
value sanitizer. Payment hashes and recipient identities are frozen before send.
"""

import asyncio
import base64
import hashlib
import json
import time
from uuid import uuid4

from loguru import logger

from .. import crud
from .shares import compute_member_share_percentages, member_point_units

# Only the Database-owned schema and fixed SQL clauses are interpolated.
# All identifiers from events, wallets and users are bound values.
# ruff: noqa: S608

_lock = asyncio.Lock()
_worker_lock = asyncio.Lock()


def key(*parts):
    return hashlib.sha256(json.dumps(parts, separators=(",", ":")).encode()).hexdigest()


def encode(value):
    return base64.b64encode(json.dumps(value, ensure_ascii=False).encode()).decode()


def decode(value):
    return json.loads(base64.b64decode(value))


async def record(identifier):
    row = await crud._db().fetchone(
        f"SELECT * FROM {crud.db.references_schema}delivery_records WHERE id=:id",
        {"id": identifier},
    )
    return {**row, "data": decode(row["payload"])} if row else None


async def put(identifier, user_id, kind, state, data, *, insert=False):
    conflict = (
        "DO NOTHING"
        if insert
        else (
            "DO UPDATE SET state=excluded.state,payload=excluded.payload,"
            "updated_at=excluded.updated_at"
        )
    )
    await crud._db().execute(
        f"""INSERT INTO {crud.db.references_schema}delivery_records
        (id,user_id,kind,state,payload,updated_at)
        VALUES (:id,:uid,:kind,:state,:payload,:ts)
        ON CONFLICT (id) {conflict}""",
        {
            "id": identifier,
            "uid": user_id,
            "kind": kind,
            "state": state,
            "payload": encode(data),
            "ts": int(time.time()),
        },
    )


async def cycle(user_id, *, reset=False):
    token = uuid4().hex
    # Unknown initial cycle may predate today when daily resets are disabled.
    # Daily rollover writes its midnight boundary; manual reset writes now.
    started_at = int(time.time()) if reset else 0
    conflict = (
        "DO UPDATE SET cycle_id=excluded.cycle_id,started_at=excluded.started_at"
        if reset
        else "DO NOTHING"
    )
    await crud._db().execute(
        f"""INSERT INTO {crud.db.references_schema}member_cycles
        (user_id,cycle_id,started_at)
        VALUES (:uid,:token,:started) ON CONFLICT (user_id) {conflict}""",
        {"uid": user_id, "token": token, "started": started_at},
    )
    row = await crud._db().fetchone(
        f"SELECT cycle_id FROM {crud.db.references_schema}member_cycles "
        "WHERE user_id=:uid",
        {"uid": user_id},
    )
    return row["cycle_id"]


def valid_pubkey(value):
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(c in "0123456789abcdef" for c in value.lower())
    )


FAILED_ADMISSIONS = {
    "headbutt_failure",
    "kind_6_headbutt_failure",
    "kind_7_headbutt_failure",
    "join_below_minimum",
}


def score_snapshot(member, settings, percent):
    return {
        "zap_sats": max(0, int(member.get("amount") or 0)),
        "minimum_sats": max(1, int(settings.minimum_sats)),
        "repost_points": 3 * int(6 in member.get("engagement_kinds", [])),
        "like_points": int(7 in member.get("engagement_kinds", [])),
        "total_point_units": member_point_units(member, settings.minimum_sats),
        "share_percent": percent,
    }


async def current_scores(owner):
    """Read current score inputs without changing membership or payout targets."""
    from ..utils.common import is_valid_lightning_address

    settings = await crud.get_settings(owner)
    members = await crud.get_active_cyberherd_members(owner)
    members = [m for m in members if not m.get("banned")]
    if not settings:
        return None, members, {}
    eligible = [m for m in members if is_valid_lightning_address(m.get("lud16"))]
    pool = settings.member_allocation_percent if settings.zap_wallet else 100
    shares = compute_member_share_percentages(eligible, pool, settings.minimum_sats)
    return settings, members, shares


async def member_summary(owner, pubkey, event_type, values):
    if event_type in FAILED_ADMISSIONS:
        return {"status": "not_admitted"}
    settings, members, shares = await current_scores(owner)
    member = next((m for m in members if m["pubkey"] == pubkey), None)
    if not settings or not member:
        return None
    snapshot = score_snapshot(member, settings, shares.get(pubkey, 0))
    if event_type in {"existing_member_repost", "existing_member_reaction"}:
        kind = 6 if event_type == "existing_member_repost" else 7
        source = values.get("source_event_id") or values.get("event_id")
        observation = await crud._db().fetchone(
            f"SELECT event_id FROM {crud.db.references_schema}engagement_observations "
            "WHERE user_id=:uid AND pubkey=:pubkey AND kind=:kind AND cycle_id=:cycle",
            {"uid": owner, "pubkey": pubkey, "kind": kind, "cycle": await cycle(owner)},
        )
        snapshot["earned_points"] = (
            (3 if kind == 6 else 1)
            if (observation and observation["event_id"] == source)
            else 0
        )
    elif event_type == "member_increase":
        snapshot["earned_points"] = (
            10
            * max(0, int(values.get("increase_amount") or 0))
            / snapshot["minimum_sats"]
        )
    return snapshot


async def queue_member(event_type, owner, values, wallet_id, roots, topic):
    """Queue a lifecycle message, reserving its joining-message branch atomically."""
    pubkey = (
        values.get("member_pubkey")
        or values.get("pubkey")
        or values.get("attacker_pubkey")
    )
    if not owner or not valid_pubkey(pubkey):
        return False
    async with _lock, crud.admission_transaction():
        token = await cycle(owner)
        values = dict(values)
        settings = await crud.get_settings(owner)
        root = next(iter(roots or []), None)
        from .streams import conversation

        stream_address = values.get("stream_address") or conversation(settings, root)
        summary = await member_summary(owner, pubkey, event_type, values)
        if summary:
            values["current_score"] = summary
        if settings and event_type not in FAILED_ADMISSIONS:
            members = await crud.get_active_cyberherd_members(owner)
            active = len([m for m in members if not m.get("banned")])
            values["herd_capacity"] = {
                "active_members": active,
                "max_members": settings.max_members,
                "spots_remaining": max(0, settings.max_members - active),
            }
        thread_id = key("thread", owner, pubkey, token)
        thread = await record(thread_id)
        if thread and thread["data"].get("stream_address") != stream_address:
            thread_id = key("thread", owner, pubkey, token, stream_address or "notes")
            thread = await record(thread_id)
        source = values.get("source_event_id") or values.get("event_id")
        # Admission supplies an event identity. Fallback is a stable snapshot.
        message_id = key("member", owner, token, pubkey, event_type, source or values)
        if not thread and event_type not in FAILED_ADMISSIONS:
            await put(
                thread_id,
                owner,
                "thread",
                "waiting",
                {
                    "join_message": message_id,
                    "root": root,
                    "stream_address": stream_address,
                    "wallet": wallet_id,
                    "pubkey": pubkey,
                    "cycle": token,
                    "name": values.get("member_display_name") or "Member",
                },
                insert=True,
            )
        await put(
            message_id,
            owner,
            "message",
            "queued",
            {
                "event_type": event_type,
                "values": values,
                "thread": thread_id if event_type not in FAILED_ADMISSIONS else None,
                "roots": list(roots or []),
                "stream_address": stream_address,
                "p_tags": [
                    p for p in (pubkey, values.get("victim_pubkey")) if valid_pubkey(p)
                ],
                "wallet": wallet_id,
                "pubkey": pubkey,
                "topic": topic,
            },
            insert=True,
        )
    return True


async def prepare_distribution(payment, targets):
    """Called by SplitPayments before making invoices, freezing all member outcomes."""
    from types import SimpleNamespace

    from ..utils.common import is_extension_enabled_for_user

    manifest_id = key("distribution", payment.wallet_id, payment.payment_hash)
    settings = await crud.get_settings_for_source_wallet(payment.wallet_id)
    if (
        not settings
        or not settings.user_id
        or not await is_extension_enabled_for_user(settings.user_id)
    ):
        return targets, {}
    owner = settings.user_id
    async with _lock, crud.admission_transaction():
        previous = await record(manifest_id)
        if previous:
            data = previous["data"]
            return [SimpleNamespace(**t) for t in data["targets"]], data["contexts"]
        token = await cycle(owner)
        members = await crud.get_active_cyberherd_members(owner)
        contexts = {}
        for target in targets:
            matches = [
                m
                for m in members
                if m.get("lud16")
                and m["lud16"].lower() == target.wallet.lower()
                and target.wallet.lower() != (settings.zap_wallet or "").lower()
            ]
            # Shared addresses cannot uniquely identify a member: never guess.
            if len(matches) != 1:
                identifier = key(
                    "recipient", owner, payment.payment_hash, target.wallet.lower()
                )
                await put(
                    identifier,
                    owner,
                    "split-recipient",
                    "planned",
                    {
                        "wallet": payment.wallet_id,
                        "target": target.wallet,
                        "distribution": payment.payment_hash,
                    },
                    insert=True,
                )
                contexts[target.wallet] = identifier
                continue
            member = matches[0]
            identifier = key(
                "treat", owner, payment.payment_hash, target.wallet.lower()
            )
            thread_id = key("thread", owner, member["pubkey"], token)
            thread = await record(thread_id)
            if not thread:
                # Pre-upgrade joins have no trustworthy event receipt. Label an
                # explicit anchor rather than inventing a historical joining ID.
                anchor_id = key("anchor", thread_id)
                await put(
                    thread_id,
                    owner,
                    "thread",
                    "waiting",
                    {
                        "join_message": anchor_id,
                        "root": member.get("note"),
                        "stream_address": (settings.tracked_event_addresses or {}).get(
                            member.get("note")
                        ),
                        "wallet": settings.herd_wallet,
                        "pubkey": member["pubkey"],
                        "cycle": token,
                        "name": member.get("display_name") or "Member",
                    },
                    insert=True,
                )
                await put(
                    anchor_id,
                    owner,
                    "message",
                    "queued",
                    {
                        "event_type": "member_thread",
                        "values": {
                            "name": member.get("display_name") or "Member",
                            "current_score": await member_summary(
                                owner, member["pubkey"], "member_thread", {}
                            ),
                        },
                        "thread": thread_id,
                        "wallet": settings.herd_wallet,
                        "pubkey": member["pubkey"],
                        "topic": "cyberherd",
                    },
                    insert=True,
                )
            await put(
                identifier,
                owner,
                "treat",
                "planned",
                {
                    "distribution": payment.payment_hash,
                    "wallet": payment.wallet_id,
                    "target": target.wallet,
                    "percent": target.percent,
                    "score_breakdown": score_snapshot(member, settings, target.percent),
                    "gross_msat": int(payment.amount * target.percent / 100),
                    "pubkey": member["pubkey"],
                    "name": member.get("display_name") or "Member",
                    "thread": thread_id,
                    "cycle": token,
                    "reposted": 6 in member.get("engagement_kinds", []),
                },
                insert=True,
            )
            contexts[target.wallet] = identifier
        await put(
            manifest_id,
            owner,
            "distribution",
            "snapshotted",
            {
                "targets": [
                    {"wallet": t.wallet, "percent": t.percent, "alias": t.alias}
                    for t in targets
                ],
                "contexts": contexts,
                "cycle": token,
            },
            insert=True,
        )
    return targets, contexts


async def outcome(identifier, state, *, payment_hash=None, amount_msat=None):
    if not identifier:
        return
    async with crud.admission_transaction():
        row = await record(identifier)
        if not row or row["state"] in ("announced", "paid", "too_small"):
            return
        data = row["data"]
        if payment_hash:
            data["payment_hash"] = payment_hash
        if amount_msat is not None:
            data["amount_msat"] = amount_msat
        await put(identifier, row["user_id"], row["kind"], state, data)


async def _queue_treat(row):
    data = row["data"]
    skipped = row["state"] == "too_small"
    values = {"name": data["name"], "new_amount": data.get("amount_msat", 0) / 1000}
    if "score_breakdown" in data:
        values["score_breakdown"] = data["score_breakdown"]
    nudge = False
    if skipped and not data["reposted"]:
        # At most one suggestion per membership cycle, and only on half of rounds.
        claim = key("nudge", row["user_id"], data["pubkey"], data["cycle"])
        if int(row["id"][0], 16) % 2 == 0 and not await record(claim):
            await put(claim, row["user_id"], "nudge", "used", {}, insert=True)
            nudge = True
    thread = await record(data["thread"])
    await put(
        key("notice", row["id"]),
        row["user_id"],
        "message",
        "queued",
        {
            "event_type": (
                "cyber_herd_treats_skipped" if skipped else "cyber_herd_treats"
            ),
            "values": values,
            "nudge": nudge,
            "thread": data["thread"],
            "wallet": thread["data"]["wallet"],
            "pubkey": data["pubkey"],
            "topic": "cyberherd",
        },
        insert=True,
    )
    await put(row["id"], row["user_id"], "treat", "announced", data)


async def _parent_reference(data):
    if not data.get("reply_message"):
        return True, None
    parent = await record(data["reply_message"])
    if not parent or parent["state"] != "submitted":
        return False, None
    event_id = parent["data"].get("event_id") or parent["data"].get("signed", {}).get(
        "id"
    )
    return bool(event_id), event_id


async def _render_queued_message(row, data):
    from lnbits.extensions.cyberherd_messaging import services as messaging

    if data["event_type"] == "feeder_triggered":
        return await messaging.render_feeder_message(data["values"], row["user_id"])
    if data.get("stream_notice"):
        return await messaging.render_stream_notice(
            data["event_type"], data["values"], row["user_id"]
        )
    return await messaging.render_member_message(
        data["event_type"],
        data["values"],
        data["pubkey"],
        row["user_id"],
        data.get("nudge", False),
    )


async def _publish(row):
    from lnbits.extensions.cyberherd_messaging import services as messaging

    data = row["data"]
    if data.get("retry_at", 0) > time.time():
        return
    thread_row = await record(data["thread"]) if data.get("thread") else None
    thread = thread_row["data"] if thread_row else {}
    is_join = thread.get("join_message") == row["id"]
    if thread and not is_join and not thread.get("event_id"):
        return
    ready, reply_event = await _parent_reference(data)
    if not ready:
        return
    if "content" not in data:
        data["content"] = await _render_queued_message(row, data)
    data["retry_at"] = int(time.time()) + 30
    await put(row["id"], row["user_id"], "message", "queued", data)
    receipt = {}

    async def persist(signed):
        data["signed"] = signed
        await put(row["id"], row["user_id"], "message", "queued", data)

    if data.get("signed"):
        ok = await messaging.publish_signed_note(
            data["signed"], wallet_id=data["wallet"], user_id=row["user_id"]
        )
        submitted_id = data["signed"]["id"] if ok else None
    else:
        roots = [thread["root"]] if thread.get("root") else data.get("roots", [])
        if thread and not is_join:
            roots.append(thread["event_id"])
        source = data["values"].get("source_event_id")
        context_tags = (
            [("e", source, "", "mention")]
            if valid_pubkey(source) and source not in roots
            else []
        )
        stream_address = thread.get("stream_address") or data.get("stream_address")
        await messaging.publish_note(
            data["content"],
            reply_to_30311_a_tag=stream_address,
            reply_to_30311_event=(
                (reply_event or thread.get("event_id"))
                if stream_address and not is_join
                else None
            ),
            e_tags=roots,
            tags=context_tags,
            p_tags=data.get("p_tags")
            or ([data["pubkey"]] if data.get("pubkey") else []),
            wallet_id=data["wallet"],
            user_id=row["user_id"],
            receipt=receipt,
            persist_signed=persist,
        )
        submitted_id = receipt.get("event_id") if receipt.get("submitted") else None
    if not submitted_id:
        return
    async with crud.admission_transaction():
        if is_join:
            thread["event_id"] = submitted_id
            await put(data["thread"], row["user_id"], "thread", "submitted", thread)
        data["event_id"] = submitted_id
        await put(row["id"], row["user_id"], "message", "submitted", data)
    await messaging.send_to_websocket_clients(
        data["topic"],
        {
            "type": data["event_type"],
            "message": data["content"],
            "values": data["values"],
            "event_id": submitted_id,
        },
    )


async def _reconcile_payment(row):
    from lnbits.core.crud import get_standalone_payment

    if row["state"] not in ("attempted", "pending"):
        return row
    payment = await get_standalone_payment(
        row["data"]["payment_hash"], wallet_id=row["data"]["wallet"]
    )
    if not payment or payment.amount >= 0 or payment.wallet_id != row["data"]["wallet"]:
        return None
    if payment.success:
        row["state"] = "paid"
        row["data"]["amount_msat"] = abs(payment.amount)
        return row
    if payment.failed:
        await put(row["id"], row["user_id"], row["kind"], "failed", row["data"])
    return None


async def process_once():
    """Reconcile only scoped, recorded outgoing payments; never retry a payment."""
    from ..utils.common import is_extension_enabled_for_user

    rows = await crud.db.fetchall(
        f"""SELECT * FROM {crud.db.references_schema}delivery_records
        WHERE (kind='treat' AND state IN ('attempted','pending','too_small','paid'))
        OR (kind='split-recipient' AND state IN ('attempted','pending'))
        OR (kind='message' AND state='queued') ORDER BY updated_at,id"""
    )
    async with _worker_lock:
        for raw in rows:
            row = await record(raw["id"])
            if not row or row["state"] not in (
                "queued",
                "attempted",
                "pending",
                "too_small",
                "paid",
            ):
                continue
            if not await is_extension_enabled_for_user(row["user_id"]):
                continue
            try:
                if row["kind"] == "message":
                    await _publish(row)
                    continue
                row = await _reconcile_payment(row)
                if not row:
                    continue
                if row["kind"] == "split-recipient":
                    await put(
                        row["id"], row["user_id"], row["kind"], "paid", row["data"]
                    )
                else:
                    async with crud.admission_transaction():
                        await _queue_treat(row)
            except Exception:
                logger.error(
                    "CyberHerd treat reconciliation failed for record {}",
                    row["id"][:12],
                )


async def run():
    while True:
        await process_once()
        await asyncio.sleep(10)


async def claim_recipient(identifier):
    if not identifier:
        return True
    async with _lock, crud.admission_transaction():
        row = await record(identifier)
        if not row or row["state"] != "planned":
            return False
        await put(identifier, row["user_id"], row["kind"], "preparing", row["data"])
        return True


async def queue_feeder(owner, wallet, payment_id, values, topic):
    """One durable feeder announcement per received payment, regardless of cycles."""
    values = dict(values)
    from .note_metadata import extract_30311_address
    from .streams import active

    stream = await active(owner)
    stream_address = extract_30311_address(stream) if stream else None
    async with crud.admission_transaction():
        settings, members, _ = await current_scores(owner)
        if settings:
            basis = max(1, int(settings.minimum_sats))
            values["herd_score"] = {
                "active_members": len(members),
                "total_points": sum(member_point_units(m, basis) for m in members)
                / basis,
                "member_allocation": (
                    settings.member_allocation_percent if settings.zap_wallet else 100
                ),
            }
    await put(
        key("feeder-notice", owner, payment_id),
        owner,
        "message",
        "queued",
        {
            "event_type": "feeder_triggered",
            "values": values,
            "stream_address": stream_address,
            "roots": [stream["id"]] if stream else [],
            "wallet": wallet,
            "topic": topic,
        },
        insert=True,
    )
    return True
