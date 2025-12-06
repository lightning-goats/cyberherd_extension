"""
Utilities for computing CyberHerd member payout shares.

These helpers centralize the logic that determines how much of the member
allocation each active participant should receive based on their zap amounts
and interaction kinds. The results are reused by both the SplitPayments
integration and the database layer so that displayed shares and applied split
targets stay in sync.
"""

from __future__ import annotations

import math
from typing import Any, Dict, Iterable, List, Set


def _normalize_pubkey(value: Any) -> str:
    """Return a trimmed, lower-cased pubkey or empty string when unavailable."""
    if value is None:
        return ""
    try:
        text = str(value).strip().lower()
    except Exception:
        return ""
    return text


def _parse_kinds(value: Any) -> Set[int]:
    """Parse the stored kinds value into a set of ints."""
    kinds: Set[int] = set()
    if value is None:
        return kinds

    if isinstance(value, str):
        tokens = [token.strip() for token in value.split(",") if token and token.strip()]
        for token in tokens:
            try:
                kinds.add(int(token))
            except Exception:
                digits = "".join(ch for ch in token if ch.isdigit())
                if digits:
                    try:
                        kinds.add(int(digits))
                    except Exception:
                        continue
        return kinds

    if isinstance(value, (list, tuple, set)):
        for item in value:
            try:
                kinds.add(int(item))
            except Exception:
                continue
        return kinds

    try:
        kinds.add(int(value))
    except Exception:
        pass
    return kinds


def _proportional_percentages(weights: List[int], total: int = 100) -> List[int]:
    """Return integer percentages summing to `total` based on non-negative weights.

    When the combined weight is zero we split the total evenly across entries.
    Remainders are distributed to entries with the largest fractional share.
    """
    n = len(weights)
    if n == 0:
        return []

    safe_total = max(0, int(total))
    sum_weights = sum(max(0, int(w or 0)) for w in weights)

    if sum_weights <= 0:
        base = safe_total // n
        rem = safe_total - base * n
        result = [base] * n
        for idx in range(rem):
            result[idx % n] += 1
        return result

    floats = [(w / sum_weights) * float(safe_total) for w in weights]
    floors = [math.floor(val) for val in floats]
    remainder = safe_total - sum(floors)

    if remainder:
        fractions = sorted(
            ((idx, floats[idx] - floors[idx]) for idx in range(n)),
            key=lambda item: item[1],
            reverse=True,
        )
        for idx in range(remainder):
            target = fractions[idx % n][0]
            floors[target] += 1

    return floors


def compute_member_share_percentages(
    members: Iterable[Dict[str, Any]], member_total: int
) -> Dict[str, int]:
    """Compute per-member share percentages for the member allocation block.

    Share allocation is simple:
    - Members with kind 6 (repost) or kind 7 (reaction) receive exactly 1%
      (having both kind 6 AND kind 7 still gives only 1%, not 2%)
    - Members with zaps (kind 9735) receive a proportional share of the
      remaining pool based on their zap amount
    - Members with BOTH engagement AND zaps receive 1% for engagement PLUS
      their proportional zap share

    For example, with member_total=10 and 3 members where 2 have engagement:
    - Engagement allocation = 2% (1% each for the 2 engaged members)
    - Zap pool = 8% (distributed proportionally by zap amount)

    The splits extension requires whole percentages, so every member must
    receive at least 1%. Members with only engagement (no zaps) get exactly 1%.

    Args:
        members: Iterable of member row dictionaries. Only active members
            contribute to the distribution.
        member_total: Total percentage available for members (e.g. 10 or 100).

    Returns:
        Mapping of normalized pubkey -> integer percentage of the member pool.
        Members that are inactive, missing a pubkey, or otherwise ineligible
        will appear in the mapping with a value of 0.
    """
    safe_total = max(0, int(member_total))
    shares: Dict[str, int] = {}
    active_records: List[Dict[str, Any]] = []

    for raw in members:
        if not isinstance(raw, dict):
            continue

        pubkey = _normalize_pubkey(raw.get("pubkey"))
        if not pubkey:
            continue

        # Default every seen pubkey to zero so callers can update inactive rows.
        shares.setdefault(pubkey, 0)

        is_active_flag = raw.get("is_active", 0)
        try:
            is_active = bool(is_active_flag) if isinstance(is_active_flag, bool) else int(is_active_flag) == 1
        except Exception:
            is_active = False

        if not is_active:
            continue

        try:
            amount = int(raw.get("amount") or 0)
        except Exception:
            amount = 0
        amount = max(0, amount)

        kinds_set = _parse_kinds(raw.get("kinds"))
        has_kind_6 = 6 in kinds_set
        has_kind_7 = 7 in kinds_set

        active_records.append(
            {
                "pubkey": pubkey,
                "amount": amount,
                "has_engagement": has_kind_6 or has_kind_7,
            }
        )

    if safe_total <= 0 or not active_records:
        return shares

    # --- Step 1: Grant 1% to each member with engagement (kind 6 or 7) ---
    # Engagement gives exactly 1%, regardless of having kind 6, kind 7, or both.
    engaged_members = [r for r in active_records if r["has_engagement"]]
    engagement_allocation = len(engaged_members)  # 1% per engaged member

    # Cap engagement allocation to safe_total
    if engagement_allocation > safe_total:
        # More engaged members than available percentage - distribute evenly
        even_percents = _proportional_percentages([1] * len(engaged_members), total=safe_total)
        for record, pct in zip(engaged_members, even_percents):
            shares[record["pubkey"]] = pct
        return shares

    # Grant 1% to each engaged member
    for record in engaged_members:
        shares[record["pubkey"]] = 1

    # --- Step 2: Distribute remaining pool proportionally by zap amount ---
    zap_pool = safe_total - engagement_allocation
    zappers = [r for r in active_records if r["amount"] > 0]
    total_zap_amount = sum(r["amount"] for r in zappers)

    if zap_pool > 0 and zappers and total_zap_amount > 0:
        weights = [record["amount"] for record in zappers]
        zap_percents = _proportional_percentages(weights, total=zap_pool)
        for record, pct in zip(zappers, zap_percents):
            shares[record["pubkey"]] = shares.get(record["pubkey"], 0) + pct

    return shares


__all__ = [
    "_proportional_percentages",
    "compute_member_share_percentages",
]
