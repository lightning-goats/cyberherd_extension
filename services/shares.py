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

    The member allocation is split into two pools:
    - 90% of member_total (zap pool): Distributed proportionally by zap amount
    - 10% of member_total (engagement bonus pool): Split evenly among members
      with kind 6 (repost) or kind 7 (reaction) events

    For example, with member_total=10:
    - Zap pool = 9% (distributed by zap amount)
    - Bonus pool = 1% (split evenly among members with kind 6/7 engagement)

    Members with BOTH zaps AND engagement receive their proportional zap share
    PLUS their share of the bonus pool. Having both kind 6 AND kind 7 does NOT
    give double bonus - maximum 1 share of the bonus pool per member.

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
                "has_kind_6": has_kind_6,
                "has_kind_7": has_kind_7,
                "has_engagement": has_kind_6 or has_kind_7,
            }
        )

    if safe_total <= 0 or not active_records:
        return shares

    # Identify members with engagement (kind 6 or kind 7)
    engaged_members = [r for r in active_records if r["has_engagement"]]
    has_any_engagement = len(engaged_members) > 0

    # Identify zappers (members with amount > 0)
    zappers = [r for r in active_records if r["amount"] > 0]
    total_zap_amount = sum(r["amount"] for r in zappers)

    if not has_any_engagement:
        # No kind 6/7 activity at all: entire member_total is distributed
        # proportionally by zap amount (no bonus pool needed).
        if zappers and total_zap_amount > 0:
            weights = [record["amount"] for record in zappers]
            percents = _proportional_percentages(weights, total=safe_total)
            for record, pct in zip(zappers, percents):
                shares[record["pubkey"]] = pct
        return shares

    # Split member_total into zap pool (90%) and engagement bonus pool (10%)
    # For member_total=10: zap_pool=9, bonus_pool=1
    # For member_total=100: zap_pool=90, bonus_pool=10
    bonus_pool = max(1, safe_total // 10)  # At least 1% for the bonus pool
    zap_pool = safe_total - bonus_pool

    # --- Step 1: Distribute the zap pool proportionally by zap amount ---
    if zappers and total_zap_amount > 0 and zap_pool > 0:
        weights = [record["amount"] for record in zappers]
        zap_percents = _proportional_percentages(weights, total=zap_pool)
        for record, pct in zip(zappers, zap_percents):
            shares[record["pubkey"]] = shares.get(record["pubkey"], 0) + pct

    # --- Step 2: Distribute the bonus pool evenly among engaged members ---
    # Each engaged member gets ONE share of the bonus pool, regardless of
    # whether they have kind 6, kind 7, or both.
    num_engaged = len(engaged_members)
    if num_engaged > 0 and bonus_pool > 0:
        # Split bonus_pool evenly among engaged members
        bonus_percents = _proportional_percentages([1] * num_engaged, total=bonus_pool)
        for record, bonus_pct in zip(engaged_members, bonus_percents):
            shares[record["pubkey"]] = shares.get(record["pubkey"], 0) + bonus_pct

    # --- Step 3: Ensure minimum 1% for eligible members when possible ---
    # This handles edge cases where rounding might leave someone at 0%.
    # Eligible members: have zap amount > 0 OR have engagement.
    try:
        total_assigned = sum(max(0, int(v or 0)) for v in shares.values())
        if total_assigned > 0 and safe_total > 0:
            eligible_pubkeys: List[str] = []
            for record in active_records:
                if record["amount"] > 0 or record["has_engagement"]:
                    eligible_pubkeys.append(record["pubkey"])
            # Deduplicate while preserving order
            seen: Set[str] = set()
            eligible_pubkeys = [
                pk for pk in eligible_pubkeys
                if not (pk in seen or seen.add(pk))
            ]

            if eligible_pubkeys and len(eligible_pubkeys) <= safe_total:
                to_boost = [pk for pk in eligible_pubkeys if shares.get(pk, 0) == 0]
                if to_boost:
                    extra_needed = len(to_boost)
                    # Grant +1 to each zero-share eligible member
                    for pk in to_boost:
                        shares[pk] = shares.get(pk, 0) + 1
                    # Trim from largest shareholders to stay at safe_total
                    trim_needed = (total_assigned + extra_needed) - safe_total
                    if trim_needed > 0:
                        donors: List[str] = [
                            pk for pk, pct in shares.items()
                            if pk not in to_boost and pct > 1
                        ]
                        capacity = sum(shares[pk] - 1 for pk in donors)
                        if capacity >= trim_needed and donors:
                            donors_sorted = sorted(
                                donors, key=lambda pk: shares[pk], reverse=True
                            )
                            idx = 0
                            while trim_needed > 0 and donors_sorted:
                                pk = donors_sorted[idx]
                                if shares[pk] > 1:
                                    shares[pk] -= 1
                                    trim_needed -= 1
                                idx = (idx + 1) % len(donors_sorted)
    except Exception:
        # Never let safety adjustments break share computation.
        pass

    return shares


__all__ = [
    "_proportional_percentages",
    "compute_member_share_percentages",
]
