"""SplitPayments integration helpers for CyberHerd.

Computes proportional split targets from active members' `amount` values
and updates the SplitPayments extension targets accordingly.
"""

from __future__ import annotations

from loguru import logger

from lnbits.extensions.splitpayments.crud import (
    set_targets as split_set_targets,  # type: ignore
)
from lnbits.extensions.splitpayments.models import Target  # type: ignore
from lnbits.helpers import urlsafe_short_hash

from .. import crud
from .shares import (
    _proportional_percentages,
    compute_member_share_percentages,
)
import asyncio
import hashlib
from typing import Any, Dict, Tuple

# Simple in-process cache & scheduling (per source_wallet) to avoid excessive writes.
_split_signature_cache: Dict[str, str] = {}
_pending_recompute: Dict[str, asyncio.Task] = {}
_RECOMPUTE_DELAY_SEC = 0.75  # debounce window


def _compute_signature(members: list[dict], zap_wallet: str | None) -> str:
    """Build a compact signature representing the current split-relevant state.

    We include pubkey, amount, lud16, *and* kinds/is_active so that changes in
    engagement types (e.g. new kind 6/7 events) or activation status cause the
    signature to change and trigger a real recompute. Previously we only used
    pubkey/amount/lud16 which could cause recomputes to be skipped when only
    kinds changed.
    """
    parts: list[str] = []
    for m in sorted(members, key=lambda x: (x.get("pubkey") or "")):
        pubkey = (m.get("pubkey") or "").strip().lower()
        amount = int(m.get("amount") or 0)
        lud16 = (m.get("lud16") or "").strip().lower()
        kinds = (m.get("kinds") or "").strip()
        is_active = int(m.get("is_active") or 0)
        parts.append(f"{pubkey}:{amount}:{lud16}:{kinds}:{is_active}")
    parts.append(f"zap={ (zap_wallet or '').strip().lower() }")
    raw = "|".join(parts)
    return hashlib.sha256(raw.encode()).hexdigest()[:32]


async def schedule_split_recompute(source_wallet: str, user_id: str | None = None):
    """Debounce split recomputation for a source_wallet."""
    if not source_wallet:
        return
    # If user_id not provided, attempt to resolve it but require a user_id to proceed
    if not user_id:
        try:
            settings = await crud.get_settings_for_source_wallet(source_wallet)
            user_id = getattr(settings, "user_id", None)
        except Exception:
            user_id = None
    if not user_id:
        logger.warning(f"schedule_split_recompute: no user_id for source_wallet {source_wallet}; skipping recompute")
        return
    # If a task already pending, let it run (reset not implemented to keep simple)
    if source_wallet in _pending_recompute and not _pending_recompute[source_wallet].done():
        return
    async def _delayed():
        try:
            await asyncio.sleep(_RECOMPUTE_DELAY_SEC)
            await update_split_targets_proportional(source_wallet, user_id=user_id)
        except Exception as e:
            logger.debug(f"schedule_split_recompute failed: {e}")
        finally:
            _pending_recompute.pop(source_wallet, None)

    _pending_recompute[source_wallet] = asyncio.create_task(_delayed())



async def update_split_targets_proportional(
    source_wallet: str,
    user_id: str | None = None,
) -> dict[str, list[dict[str, str | float | None]]]:
    """Compute proportional split targets from active CyberHerd members and update
    SplitPayments.

    Members without a valid lud16 are skipped.
    If a zap wallet is configured in settings for this source wallet, allocate 90%
    to that wallet and split the remaining 10% among eligible members proportionally.
    If no members are eligible, allocate 100% to the zap wallet. If no zap wallet
    is configured and no eligible members exist, clear split targets.
    """
    try:
        resolved_user_id = user_id
        if resolved_user_id:
            settings = await crud.get_settings(resolved_user_id)
        else:
            settings = await crud.get_settings_for_source_wallet(source_wallet)
            resolved_user_id = getattr(settings, "user_id", None)
            if not resolved_user_id:
                logger.warning(
                    f"update_split_targets_proportional: no user_id found for source_wallet {source_wallet}, skipping recompute"
                )
                return {"targets": []}
            settings = await crud.get_settings(resolved_user_id)
    except Exception:
        logger.warning(
            f"update_split_targets_proportional: failed to resolve settings for source_wallet {source_wallet}"
        )
        return {"targets": []}

    members = await crud.get_active_cyberherd_members(resolved_user_id)
    zap_wallet = getattr(settings, "zap_wallet", None)
    zap_wallet_alias = getattr(settings, "zap_wallet_alias", None) or "Zap Wallet"
    # Use configured member allocation percentage, default to 10 if zap wallet present, 100 otherwise
    member_allocation_percent = getattr(settings, "member_allocation_percent", 10 if zap_wallet else 100)
    member_total = member_allocation_percent if zap_wallet else 100

    signature = _compute_signature(members, zap_wallet)
    prev_sig = _split_signature_cache.get(source_wallet)
    if prev_sig == signature:
        logger.debug("cyberherd: split recompute skipped (unchanged signature)")
        return {"targets": []}

    eligible_members: list[dict[str, Any]] = []
    for raw in members:
        if not isinstance(raw, dict):
            continue
        row = dict(raw)
        pubkey = (row.get("pubkey") or "").strip().lower()
        if not pubkey:
            continue
        row["pubkey"] = pubkey
        lud16 = (row.get("lud16") or "").strip()
        if lud16:
            eligible_members.append(row)

    if not eligible_members:
        if zap_wallet:
            target = Target(
                id=urlsafe_short_hash(),
                wallet=zap_wallet,
                source=source_wallet,
                percent=100,
                alias=zap_wallet_alias,
            )
            await split_set_targets(source_wallet, [target])
            _split_signature_cache[source_wallet] = signature
            return {
                "targets": [
                    {"wallet": target.wallet, "percent": target.percent, "alias": target.alias}
                ]
            }
        logger.info(
            "cyberherd: no eligible members with lud16 for splitpayments update"
        )
        try:
            await split_set_targets(source_wallet, [])
        except Exception as e:
            logger.warning(f"failed to clear split targets: {e}")
        return {"targets": []}

    shares_map = compute_member_share_percentages(eligible_members, member_total)

    targets: list[Target] = []
    member_percent_used = 0

    for row in eligible_members:
        share_percent = int(shares_map.get(row["pubkey"], 0))
        if share_percent <= 0:
            continue
        lud16 = (row.get("lud16") or "").strip()
        if not lud16:
            continue
        member_percent_used += share_percent
        alias = row.get("display_name") or "Anon"
        targets.append(
            Target(
                id=urlsafe_short_hash(),
                wallet=lud16,
                source=source_wallet,
                percent=share_percent,
                alias=alias,
            )
        )

    if zap_wallet:
        # Give the zap wallet its base share plus any unallocated member
        # percentage (from rounding or skipped members) so that the total
        # across all targets always sums to 100%.
        zap_wallet_percent = 100 - member_percent_used
        targets.insert(
            0,
            Target(
                id=urlsafe_short_hash(),
                wallet=zap_wallet,
                source=source_wallet,
                percent=zap_wallet_percent,
                alias=zap_wallet_alias,
            ),
        )

    await split_set_targets(source_wallet, targets)
    _split_signature_cache[source_wallet] = signature
    return {
        "targets": [
            {"wallet": t.wallet, "percent": t.percent, "alias": t.alias}
            for t in targets
        ]
    }


async def set_single_target_100(
    source_wallet: str, zap_wallet: str, alias: str | None = None
) -> dict[str, list[dict[str, str | float | None]]]:
    """Set SplitPayments targets to a single predefined wallet at 100%.

    Returns a summary dict of targets applied.
    """
    target = Target(
        id=urlsafe_short_hash(),
        wallet=zap_wallet,
        source=source_wallet,
        percent=100,
        alias=alias or "Zap Wallet",
    )
    await split_set_targets(source_wallet, [target])
    return {"targets": [{"wallet": target.wallet, "percent": target.percent, "alias": target.alias}]}


async def reset_splits_to_predefined_wallet(source_wallet: str, user_id: str | None = None) -> dict[str, list[dict[str, str | float | None]]]:
    """Reset SplitPayments so that only the predefined zap wallet receives 100%.

    If no zap wallet configured in settings, clears split targets.
    """
    # Resolve settings and require a user_id to avoid touching global row
    try:
        if not user_id:
            settings = await crud.get_settings_for_source_wallet(source_wallet)
            user_id = getattr(settings, "user_id", None)
        else:
            settings = await crud.get_settings(user_id)
    except Exception:
        settings = None
        user_id = None

    if not settings or not user_id:
        logger.warning(f"reset_splits_to_predefined_wallet: cannot resolve user for source_wallet {source_wallet}; skipping reset")
        return {"targets": []}

    zap_wallet = getattr(settings, "zap_wallet", None)
    alias = getattr(settings, "zap_wallet_alias", None) or "Zap Wallet"
    if zap_wallet:
        # Clear signature cache so next proportional recompute (after members activate)
        # isn't skipped due to stale signature representing old member state.
        try:
            _split_signature_cache.pop(source_wallet, None)
        except Exception:
            pass
        result = await set_single_target_100(source_wallet, zap_wallet, alias)
        # Store a synthetic signature representing the 100% zap wallet state to avoid
        # immediate redundant writes if another reset fires.
        try:
            _split_signature_cache[source_wallet] = f"zap_only:{zap_wallet}"
        except Exception:
            pass
        return result
    # No zap wallet configured: clear all targets
    try:
        _split_signature_cache.pop(source_wallet, None)
    except Exception:
        pass
    try:
        await split_set_targets(source_wallet, [])
    except Exception as e:
        logger.warning(f"failed to clear split targets during reset: {e}")
    return {"targets": []}
