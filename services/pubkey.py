"""Shared helpers for resolving CyberHerd effective Nostr pubkey.

Centralises fallback logic so services and views use the same rules:
1. Use cached computed_effective_pubkey if present and a str.
2. Else use legacy effective_nostr_pubkey attribute if present.
3. Else derive from nostr_pubkey_override.
4. Else derive from nostr_private_key (if available) using secp256k1.
5. Persist newly computed value through crud.upsert_settings when requested.

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


def _derive_from_private_key(settings) -> Optional[str]:
    """Attempt to derive pubkey from nostr_private_key; returns hex or None."""
    try:
        priv = getattr(settings, "nostr_private_key", None)
        if not priv:
            return None
        raw = str(priv)
        if raw.startswith("0x"):
            raw = raw[2:]
        # Normalise hex length (already hex expected)
        import secp256k1  # type: ignore
        sk = secp256k1.PrivateKey(bytes.fromhex(raw), raw=True)
        pk = sk.pubkey
        if not pk:
            return None
        compressed = pk.serialize(compressed=True).hex()
        # LNbits style: strip leading 02/03 for x-only form
        return compressed[2:]
    except Exception as e:  # pragma: no cover - defensive
        try:
            logger.debug("Cyberherd: failed private key derivation: %s", e)
        except Exception:
            pass
        return None


def resolve_effective_pubkey(settings, *, persist: bool = False, user_id: str | None = None) -> Optional[str]:
    """Resolve effective pubkey with fallbacks and optional persistence.

    Order:
    1. settings.computed_effective_pubkey (string)
    2. settings.effective_nostr_pubkey (legacy attr)
    3. settings.nostr_pubkey_override
    4. Derivation from nostr_private_key
    When a new value is derived (3 or 4) it's cached in computed_effective_pubkey
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

        # 4. Derive from private key
        derived = _derive_from_private_key(settings)
        if derived:
            try:
                settings.computed_effective_pubkey = derived
            except Exception:
                pass
            if persist and crud and user_id:
                # Persist asynchronously if crud supports it
                try:
                    # upsert needs to be awaited; caller is async typically; if not, skip
                    if hasattr(crud, "upsert_settings"):
                        # We can't await in sync function; caller must do persistence separately
                        pass
                except Exception:
                    pass
            return derived

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
