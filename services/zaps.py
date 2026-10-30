"""NIP-57 invoice requests for frozen CyberHerd recipients; never sends payments."""

import hashlib
import json

import bolt11
import httpx
from coincurve import PublicKeyXOnly
from lnurl import LnAddress, Lnurl, LnurlPayResponse, handle
from lnurl.core import TOR_SOCKS

from lnbits.helpers import check_callback_url
from lnbits.settings import settings

from .. import crud
from . import treats


def valid_receipt_key(value):
    try:
        PublicKeyXOnly(bytes.fromhex(value))
        return True
    except (TypeError, ValueError):
        return False


async def _save(identifier, values):
    async with crud.admission_transaction():
        row = await treats.record(identifier)
        if not row or row["kind"] != "treat" or row["state"] != "preparing":
            raise ValueError("Treat is not preparing an invoice")
        await treats.put(
            identifier, row["user_id"], "treat", row["state"], {**row["data"], **values}
        )


async def _callback_invoice(metadata, amount_msat, request_json, lnurl):
    check_callback_url(str(metadata.callback))
    proxy = TOR_SOCKS if metadata.callback.host.endswith(".onion") else None
    async with httpx.AsyncClient(
        headers={"User-Agent": settings.user_agent}, proxy=proxy, follow_redirects=False
    ) as client:
        response = await client.get(
            str(metadata.callback),
            params={"amount": amount_msat, "nostr": request_json, "lnurl": lnurl},
            timeout=10,
        )
        response.raise_for_status()
        result = response.json()
    if result.get("status") == "ERROR" or not isinstance(result.get("pr"), str):
        raise ValueError("Zap callback did not return an invoice")
    return result["pr"]


async def member_zap_invoice(identifier, address, wallet_id, amount_msat):
    """Return a verified zap invoice, or None for a documented ordinary-payment path.

    Supported recipients never silently downgrade after a signing/callback failure.
    The caller claims the recipient and persists the invoice hash before payment.
    """
    row = await treats.record(identifier)
    if not row or row["kind"] != "treat":
        return None  # Operator and ambiguous/non-member recipients keep normal splits.
    data = row["data"]
    if (
        row["state"] != "preparing"
        or data["wallet"] != wallet_id
        or data["target"].lower() != address.lower()
        or amount_msat < 1000
        or amount_msat % 1000
        or amount_msat > data["gross_msat"]
    ):
        raise ValueError("Zap request does not match the frozen treat")
    metadata = await handle(address, user_agent=settings.user_agent, timeout=10)
    if not isinstance(metadata, LnurlPayResponse):
        raise ValueError("Recipient did not return LNURL-pay metadata")
    if not metadata.allowsNostr or not valid_receipt_key(metadata.nostrPubkey):
        await _save(
            identifier,
            {
                "payment_method": "lightning",
                "zap_fallback_reason": "recipient_unsupported",
            },
        )
        return None
    if not metadata.is_valid_amount(amount_msat):
        raise ValueError("Treat amount is outside recipient limits")
    # LnAddress preserves the exact LUD-16 endpoint; Lnurl encodes other LNURLs.
    from bech32 import bech32_encode, convertbits

    endpoint = (
        str(LnAddress(address).url) if "@" in address else str(Lnurl(address).url)
    )
    lnurl = bech32_encode("lnurl", convertbits(endpoint.encode(), 8, 5))
    thread_row = await treats.record(data["thread"])
    thread = thread_row["data"] if thread_row else {}
    event_id = thread.get("event_id")
    from lnbits.extensions.cyberherd_messaging.services import sign_treat_zap_request

    request = data.get("zap_request")
    if not request:
        request = await sign_treat_zap_request(
            wallet_id=thread.get("wallet"),
            user_id=row["user_id"],
            pubkey=data["pubkey"],
            name=data["name"],
            amount_msat=amount_msat,
            lnurl=lnurl,
            event_id=event_id,
            event_kind=1311 if thread.get("stream_address") else 1,
        )
        request_json = json.dumps(request, ensure_ascii=False, separators=(",", ":"))
        await _save(
            identifier,
            {
                "payment_method": "zap",
                "zap_request": request,
                "zap_request_json": request_json,
                "zap_receipt_pubkey": metadata.nostrPubkey,
                "zap_receipt_status": "requested",
                "zap_event_id": event_id,
            },
        )
    else:
        request_json = data["zap_request_json"]
        if ["amount", str(amount_msat)] not in request["tags"]:
            raise ValueError("Saved zap request amount changed")
    payment_request = await _callback_invoice(
        metadata, amount_msat, request_json, lnurl
    )
    invoice = bolt11.decode(payment_request)
    if (
        invoice.amount_msat != amount_msat
        or invoice.description_hash != hashlib.sha256(request_json.encode()).hexdigest()
        or invoice.has_expired()
    ):
        raise ValueError("Zap invoice does not match the signed request")
    await _save(identifier, {"zap_invoice_verified": True})
    return payment_request
