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


async def queue_member(event_type, owner, values, wallet_id, roots, topic):
    """Queue a lifecycle message, reserving its joining-message branch atomically."""
    pubkey = values.get("member_pubkey") or values.get("pubkey")
    if not owner or not valid_pubkey(pubkey):
        return False
    async with _lock, crud.admission_transaction():
        token = await cycle(owner)
        thread_id = key("thread", owner, pubkey, token)
        thread = await record(thread_id)
        source = values.get("source_event_id") or values.get("event_id")
        # Admission supplies an event identity. Fallback is a stable snapshot.
        message_id = key("member", owner, token, pubkey, event_type, source or values)
        if not thread:
            await put(
                thread_id,
                owner,
                "thread",
                "waiting",
                {
                    "join_message": message_id,
                    "root": next(iter(roots or []), None),
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
                "thread": thread_id,
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
                if m.get("lud16") and m["lud16"].lower() == target.wallet.lower()
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
                        "values": {"name": member.get("display_name") or "Member"},
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
    if "content" not in data:
        if data["event_type"] == "feeder_triggered":
            data["content"] = await messaging.render_feeder_message(
                data["values"], row["user_id"]
            )
        else:
            data["content"] = await messaging.render_member_message(
                data["event_type"],
                data["values"],
                data["pubkey"],
                row["user_id"],
                data.get("nudge", False),
            )
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
        roots = [thread["root"]] if thread.get("root") else []
        if thread and not is_join:
            roots.append(thread["event_id"])
        source = data["values"].get("source_event_id")
        context_tags = (
            [("e", source, "", "mention")]
            if valid_pubkey(source) and source not in roots
            else []
        )
        await messaging.publish_note(
            data["content"],
            e_tags=roots,
            tags=context_tags,
            p_tags=[data["pubkey"]] if data.get("pubkey") else [],
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
    await put(
        key("feeder-notice", owner, payment_id),
        owner,
        "message",
        "queued",
        {
            "event_type": "feeder_triggered",
            "values": values,
            "wallet": wallet,
            "topic": topic,
        },
        insert=True,
    )
    return True
