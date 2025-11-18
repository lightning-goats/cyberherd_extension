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
        has_kind_67 = 6 in kinds_set or 7 in kinds_set

        active_records.append(
            {
                "pubkey": pubkey,
                "amount": amount,
                "has_kind_67": has_kind_67,
            }
        )

    if safe_total <= 0 or not active_records:
        return shares

    kind_67_only = [
        record for record in active_records if record["amount"] == 0 and record["has_kind_67"]
    ]
    zappers_with_kind = [
        record for record in active_records if record["amount"] > 0 and record["has_kind_67"]
    ]
    zappers_only = [
        record for record in active_records if record["amount"] > 0 and not record["has_kind_67"]
    ]

    total_with_kind = len(kind_67_only) + len(zappers_with_kind)

    if total_with_kind == 0:
        # No kind 6/7 activity at all: entire member_total is distributed
        # proportionally by zap amount.
        all_zappers = [record for record in active_records if record["amount"] > 0]
        if all_zappers:
            weights = [record["amount"] for record in all_zappers]
            percents = _proportional_percentages(weights, total=safe_total)
            for record, pct in zip(all_zappers, percents):
                shares[record["pubkey"]] = pct
        return shares

    # Allocate at most 1 percentage point of the overall split pool (i.e. 1% of
    # the SplitPayments distribution) to the kind 6/7 bonus, regardless of the
    # size of `member_total`. The remaining portion is distributed purely based
    # on zap amounts.
    #
    # For the standard configuration (zap wallet present) `member_total` is 10,
    # so kind 6/7 events collectively receive up to 1% of the *overall* payout
    # (1 out of 100 percentage points), with the other 9% of the member pool
    # allocated proportionally by zap amounts.
    bonus_from_pool = max(1, safe_total // 100)
    kind67_bonus_total = min(safe_total, bonus_from_pool)
    zap_distribution_total = max(0, safe_total - kind67_bonus_total)

    kind67_members = kind_67_only + zappers_with_kind
    bonus_distribution = []
    if kind67_members and kind67_bonus_total > 0:
        bonus_distribution = _proportional_percentages(
            [1] * len(kind67_members), total=kind67_bonus_total
        )

    for record, bonus_pct in zip(kind67_members, bonus_distribution):
        if bonus_pct > 0:
            shares[record["pubkey"]] = shares.get(record["pubkey"], 0) + bonus_pct

    all_zappers = [record for record in active_records if record["amount"] > 0]
    if all_zappers and zap_distribution_total > 0:
        weights = [record["amount"] for record in all_zappers]
        zap_percents = _proportional_percentages(weights, total=zap_distribution_total)
        for record, pct in zip(all_zappers, zap_percents):
            shares[record["pubkey"]] = shares.get(record["pubkey"], 0) + pct

    # Enforce a minimum of 1 percentage point for any active member who
    # meaningfully participates in the pool (has zap amount > 0 or a kind 6/7
    # engagement), since SplitPayments only accepts whole percentages. We keep
    # the overall sum at `safe_total` by shaving points from members with the
    # largest shares.
    try:
        total_assigned = sum(max(0, int(v or 0)) for v in shares.values())
        if total_assigned > 0 and safe_total > 0:
            # Eligible members: active with either zap amount or kind 6/7
            eligible_pubkeys: List[str] = []
            for record in active_records:
                if record["amount"] > 0 or record["has_kind_67"]:
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
                    # First, grant +1 to each zero-share eligible member
                    for pk in to_boost:
                        shares[pk] = shares.get(pk, 0) + 1
                    # Recompute total and required trimming
                    trim_needed = (total_assigned + extra_needed) - safe_total
                    if trim_needed > 0:
                        # Donors: members not just boosted and currently >1
                        donors: List[str] = [
                            pk for pk, pct in shares.items()
                            if pk not in to_boost and pct > 1
                        ]
                        # Check that we have enough capacity to trim without
                        # driving any donor below 1.
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
                        # If capacity is insufficient, we leave the original
                        # distribution unchanged to avoid negative or zero sums.
                        # (Given typical herd sizes, this should be rare.)
    except Exception:
        # Never let safety adjustments break share computation.
        pass

    return shares


__all__ = [
    "_proportional_percentages",
    "compute_member_share_percentages",
]
