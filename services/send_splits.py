from __future__ import annotations

import asyncio
import time
from http import HTTPStatus
from typing import Any, Dict, Optional, Tuple

from loguru import logger

from ..models import CyberherdSettings
from .. import crud

from lnbits.core.crud.wallets import get_wallet, get_wallet_for_key
from lnbits.core.models.payments import CreateInvoice
from lnbits.core.services.payments import create_wallet_invoice, pay_invoice

_LAST_ATTEMPT: dict[str, float] = {}
_MIN_INTERVAL_SECONDS = 1.0

# Per-wallet lock prevents concurrent transfer_herd_balance_to_source calls
# (e.g. Lightning Goats feeder trigger racing CyberHerd auto-splits).
_transfer_locks: dict[str, asyncio.Lock] = {}


async def _resolve_wallet(wallet_ref: Optional[str]) -> Tuple[Optional[str], Any]:
    """Resolve a wallet reference (id or key) to a wallet id and object."""
    if not wallet_ref:
        return None, None

    wallet = await get_wallet(wallet_ref)
    if wallet:
        return wallet.id, wallet

    wallet = await get_wallet_for_key(wallet_ref)
    if wallet:
        return wallet.id, wallet

    return None, None


async def transfer_herd_balance_to_source(settings: CyberherdSettings) -> Dict[str, Any]:
    """Move the full herd wallet balance to the source (CyberHerd) wallet.

    Uses a per-wallet lock to prevent concurrent transfers from racing
    (e.g. Lightning Goats feeder trigger vs CyberHerd auto-splits).
    """
    herd_wallet_ref = getattr(settings, "herd_wallet", None)
    target_wallet_ref = getattr(settings, "source_wallet", None)

    if not herd_wallet_ref:
        return {
            "ok": False,
            "error": "No herd_wallet configured in cyberherd settings",
            "status_code": HTTPStatus.BAD_REQUEST,
        }

    if not target_wallet_ref:
        return {
            "ok": False,
            "error": "No source_wallet configured in cyberherd settings",
            "status_code": HTTPStatus.BAD_REQUEST,
        }

    # Serialize transfers for the same herd wallet to prevent double-spend races
    lock_key = str(herd_wallet_ref)
    if lock_key not in _transfer_locks:
        _transfer_locks[lock_key] = asyncio.Lock()

    async with _transfer_locks[lock_key]:
        herd_wallet_id, herd_wallet = await _resolve_wallet(herd_wallet_ref)
        target_wallet_id, target_wallet = await _resolve_wallet(target_wallet_ref)

        if not herd_wallet_id or not herd_wallet:
            return {
                "ok": False,
                "error": "Could not resolve herd_wallet",
                "status_code": HTTPStatus.BAD_REQUEST,
            }

        if not target_wallet_id or not target_wallet:
            return {
                "ok": False,
                "error": "Could not resolve source_wallet",
                "status_code": HTTPStatus.BAD_REQUEST,
            }

        balance_msat = getattr(herd_wallet, "balance_msat", None)
        if balance_msat is None:
            balance_msat = getattr(herd_wallet, "balance", 0)

        try:
            balance_sats = int(balance_msat) // 1000
        except Exception:
            balance_sats = 0

        if balance_sats <= 0:
            return {
                "ok": False,
                "error": "Herd wallet balance is zero",
                "status_code": HTTPStatus.BAD_REQUEST,
            }

        try:
            invoice_data = CreateInvoice(
                amount=balance_sats,
                unit="sat",
                memo="CyberHerd Distribution",
                internal=True,
            )
            internal_payment = await create_wallet_invoice(target_wallet_id, invoice_data)
        except Exception as exc:
            return {
                "ok": False,
                "error": f"Failed to create internal invoice on target wallet: {exc}",
                "status_code": HTTPStatus.INTERNAL_SERVER_ERROR,
            }

        try:
            paid = await pay_invoice(
                wallet_id=herd_wallet_id, payment_request=internal_payment.bolt11
            )
        except Exception as exc:
            return {
                "ok": False,
                "error": f"Failed to pay internal invoice from herd wallet: {exc}",
                "status_code": HTTPStatus.INTERNAL_SERVER_ERROR,
            }

        return {
            "ok": True,
            "from_wallet": herd_wallet_id,
            "to_wallet": target_wallet_id,
            "sats": balance_sats,
            "payment_checking_id": getattr(paid, "checking_id", None),
        }


async def maybe_send_splits_for_user(
    user_id: str, settings: Optional[CyberherdSettings] = None
) -> Optional[Dict[str, Any]]:
    """Send splits automatically when enabled and the trigger threshold is met."""
    settings_obj = settings or await crud.get_settings(user_id)
    if not getattr(settings_obj, "send_splits_enabled", False):
        return None

    trigger_value = getattr(settings_obj, "feeder_trigger_sats", None)
    try:
        trigger_int = int(trigger_value) if trigger_value not in (None, "") else None
    except Exception:
        trigger_int = None

    if trigger_int is None or trigger_int <= 0:
        logger.debug(
            "CyberHerd send-splits skipped: trigger not configured for user %s", user_id
        )
        return None

    herd_wallet_ref = getattr(settings_obj, "herd_wallet", None)
    if not herd_wallet_ref:
        logger.debug(
            "CyberHerd send-splits skipped: herd_wallet missing for user %s", user_id
        )
        return None

    herd_wallet_id, herd_wallet = await _resolve_wallet(herd_wallet_ref)
    if not herd_wallet_id or not herd_wallet:
        logger.warning(
            "CyberHerd send-splits: unable to resolve herd_wallet for user %s",
            user_id,
        )
        return None

    balance_msat = getattr(herd_wallet, "balance_msat", None)
    if balance_msat is None:
        balance_msat = getattr(herd_wallet, "balance", 0)

    try:
        balance_sats = int(balance_msat) // 1000
    except Exception:
        balance_sats = 0

    if balance_sats < trigger_int:
        logger.debug(
            "CyberHerd send-splits: balance %s sats below trigger %s sats for user %s",
            balance_sats,
            trigger_int,
            user_id,
        )
        return None

    last_ts = _LAST_ATTEMPT.get(user_id, 0.0)
    now = time.time()
    if now - last_ts < _MIN_INTERVAL_SECONDS:
        logger.debug(
            "CyberHerd send-splits: throttled for user %s (last attempt %.2fs ago)",
            user_id,
            now - last_ts,
        )
        return None

    result = await transfer_herd_balance_to_source(settings_obj)
    _LAST_ATTEMPT[user_id] = now

    if result.get("ok"):
        logger.info(
            "CyberHerd send-splits: transferred %s sats from herd to source for user %s",
            result.get("sats"),
            user_id,
        )
    else:
        logger.warning(
            "CyberHerd send-splits failed for user %s: %s",
            user_id,
            result.get("error"),
        )

    return result
