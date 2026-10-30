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
        tokens = [
            token.strip() for token in value.split(",") if token and token.strip()
        ]
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


def member_point_units(member: Dict[str, Any], minimum_sats: int = 10) -> int:
    """Exact score multiplied by the joining minimum; no floating-point weights."""
    basis = max(1, int(minimum_sats))
    try:
        amount = max(0, int(member.get("amount") or 0))
    except (TypeError, ValueError):
        amount = 0
    kinds = _parse_kinds(member.get("engagement_kinds", member.get("kinds")))
    return 10 * amount + basis * (3 * int(6 in kinds) + int(7 in kinds))


def compute_member_share_percentages(
    members: Iterable[Dict[str, Any]], member_total: int, minimum_sats: int = 10
) -> Dict[str, float]:
    """Divide the member pool by cumulative zap, first-repost and first-like points.

    A minimum qualifying zap earns 10 points, repost earns 3, like earns 1.
    Each additional sat adds 10/minimum points. Durable engagement_kinds take
    precedence over the current membership's kinds (which eviction can clear).
    Round the pool to micro-percentage units with stable largest remainders.
    """
    total = max(0, int(member_total))
    shares: Dict[str, float] = {}
    eligible = []
    for member in members:
        if not isinstance(member, dict):
            continue
        pubkey = _normalize_pubkey(member.get("pubkey"))
        if not pubkey:
            continue
        shares[pubkey] = 0.0
        if int(member.get("is_active") or 0) != 1 or member.get("banned"):
            continue
        weight = member_point_units(member, minimum_sats)
        if weight > 0:
            eligible.append((pubkey, weight))
    eligible.sort()  # deterministic ties, independent of database row order
    weight_sum = sum(weight for _, weight in eligible)
    if not weight_sum or not total:
        return shares
    units = total * 1_000_000
    allocated = [units * weight // weight_sum for _, weight in eligible]
    remainder = units - sum(allocated)
    order = sorted(
        range(len(eligible)),
        key=lambda i: (-(units * eligible[i][1] % weight_sum), eligible[i][0]),
    )
    for index in order[:remainder]:
        allocated[index] += 1
    for (pubkey, _), amount in zip(eligible, allocated):
        shares[pubkey] = amount / 1_000_000
    return shares


__all__ = [
    "_proportional_percentages",
    "compute_member_share_percentages",
    "member_point_units",
]
