"""Utilities for extracting zap request payloads from LNbits payments."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from loguru import logger


@dataclass(slots=True)
class ZapRequestInfo:
    """Result of normalizing a zap request from a payment object."""

    payload: dict[str, Any]
    source: str | None = None


def _coerce_extra_dict(payment: Any) -> dict[str, Any]:
    """Ensure ``payment.extra`` is a mutable dictionary."""

    extra = getattr(payment, "extra", None)
    if extra is None:
        extra = {}
    elif isinstance(extra, str):
        try:
            extra = json.loads(extra)
        except Exception:
            extra = {}
    elif not isinstance(extra, dict):
        try:
            extra = dict(extra)
        except Exception:
            extra = {}

    setattr(payment, "extra", extra)
    return extra


def _load_json_candidate(candidate: Any) -> dict[str, Any] | None:
    """Best-effort JSON loader for zap request candidates."""

    if isinstance(candidate, dict):
        return candidate
    if isinstance(candidate, str):
        text = candidate.strip()
        if not text:
            return None
        try:
            loaded = json.loads(text)
        except Exception:
            return None
        return loaded if isinstance(loaded, dict) else None
    return None


def extract_zap_request(payment: Any) -> ZapRequestInfo | None:
    """Return a normalized zap request payload when present on *payment*.

    The helper inspects common locations used by LNURLp providers and legacy
    CyberHerd releases and stores the resulting dict back under
    ``payment.extra['nostr']`` for consistent downstream access.
    """

    extra = _coerce_extra_dict(payment)

    sources = (
        "nostr",
        "zap_request",
        "zapRequest",
        "comment",
    )

    for key in sources:
        if key not in extra:
            continue
        if key == "comment" and not isinstance(extra[key], str):
            continue
        payload = _load_json_candidate(extra[key])
        if payload:
            extra["nostr"] = payload
            logger.debug(
                "CyberHerd: normalized zap request from %s field for payment %s",
                key,
                getattr(payment, "checking_id", None)
                or getattr(payment, "payment_hash", None),
            )
            return ZapRequestInfo(payload=payload, source=key)

    memo = getattr(payment, "memo", None)
    if isinstance(memo, str):
        payload = _load_json_candidate(memo)
        if payload:
            extra["nostr"] = payload
            logger.debug(
                "CyberHerd: normalized zap request from memo field for payment %s",
                getattr(payment, "checking_id", None)
                or getattr(payment, "payment_hash", None),
            )
            return ZapRequestInfo(payload=payload, source="memo")

    logger.debug(
        "CyberHerd: payment %s missing zap request payload",
        getattr(payment, "checking_id", None)
        or getattr(payment, "payment_hash", None),
    )
    return None


__all__ = ["ZapRequestInfo", "extract_zap_request"]
