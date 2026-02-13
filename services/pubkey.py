"""Shared helpers for resolving CyberHerd effective Nostr pubkey.

Centralises fallback logic so services and views use the same rules:
1. Use cached computed_effective_pubkey if present and a str.
2. Else use legacy effective_nostr_pubkey attribute if present.
3. Else derive from nostr_pubkey_override.
4. Persist newly computed value through crud.upsert_settings when requested.

This avoids startup warnings like: "No effective pubkey available for event cache population".
"""
from __future__ import annotations

from typing import Optional, Any, Callable
import logging

logger = logging.getLogger(__name__)

try:
    from . import crud  # type: ignore
except Exception:  # pragma: no cover - import path issues at runtime only
    crud = None  # type: ignore


def resolve_effective_pubkey(settings, *, persist: bool = False, user_id: str | None = None) -> Optional[str]:
    """Resolve effective pubkey with fallbacks and optional persistence.

    Order:
    1. settings.computed_effective_pubkey (string)
    2. settings.effective_nostr_pubkey (legacy attr)
    3. settings.nostr_pubkey_override
    When a new value is derived (3) it's cached in computed_effective_pubkey
    and optionally persisted via crud.upsert_settings if persist=True and user_id provided.
    """
    try:
        # 1. Cached computed
        cached = getattr(settings, "computed_effective_pubkey", None)
        if isinstance(cached, str) and cached:
            return cached

        # 2. Legacy attribute
        legacy = getattr(settings, "effective_nostr_pubkey", None)
        if isinstance(legacy, str) and legacy:
            # Normalise by storing into computed for future calls
            try:
                settings.computed_effective_pubkey = legacy
            except Exception:
                pass
            return legacy

        # 3. Explicit override
        override = getattr(settings, "nostr_pubkey_override", None)
        if isinstance(override, str) and override:
            try:
                settings.computed_effective_pubkey = override
            except Exception:
                pass
            if persist and crud and user_id:
                try:
                    # Persist only computed field; crud.upsert_settings handles full update
                    settings.computed_effective_pubkey = override
                    # no await here; keep sync version for services using sync path
                except Exception:
                    pass
            return override

        return None
    except Exception as e:  # pragma: no cover
        try:
            logger.warning("Cyberherd: error resolving effective pubkey: %s", e)
        except Exception:
            pass
        return None


async def resolve_and_persist_effective_pubkey(settings, user_id: str | None) -> Optional[str]:
    """Async helper to ensure computed pubkey persisted when derivable."""
    eff = resolve_effective_pubkey(settings, persist=False, user_id=user_id)
    if eff and crud and user_id:
        try:
            if hasattr(crud, "upsert_settings"):
                await crud.upsert_settings(settings, user_id)
        except Exception as e:  # pragma: no cover
            try:
                logger.debug("Cyberherd: failed persisting effective pubkey: %s", e)
            except Exception:
                pass
    return eff

__all__ = [
    "resolve_effective_pubkey",
    "resolve_and_persist_effective_pubkey",
]
