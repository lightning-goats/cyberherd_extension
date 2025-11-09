"""Nostr lookup helpers for Cyberherd.

DEPRECATED: This module is now a thin compatibility layer around nostr_helpers.
New code should import from nostr_helpers directly.

Provides backward compatibility for:
- _query_events_via_manager() -> use nostr_helpers.query_events()
- lookup_metadata() -> still useful, but uses nostr_helpers internally
- lookup_relays() -> use nostr_helpers.query_user_relays()
- verify_nip05() -> still useful for NIP-05 validation

Migration path:
    Old: from .nostr_lookup import _query_events_via_manager
    New: from . import nostr_helpers
         events = await nostr_helpers.query_events(filters, ...)
"""

from __future__ import annotations

import json
import warnings
from typing import Any

import httpx

from loguru import logger

from . import nostr_helpers
from lnbits.utils.nostr import normalize_public_key


async def _query_events_via_manager(
    filters: dict[str, Any] | list[dict[str, Any]],
    limit: int = 50,
    timeout: float = 6.0,
    extra_relays: list[str] | None = None,
) -> list[dict]:
    """Query events using nostr_helpers.
    
    DEPRECATED: Use nostr_helpers.query_events() directly instead.
    
    This function is kept for backward compatibility but will be removed
    in a future version. Update your code to use:
        
        from . import nostr_helpers
        events = await nostr_helpers.query_events(filters, limit, timeout, extra_relays)
    
    Args:
        filters: Nostr filter dict or list of filter dicts
        limit: Maximum number of events to return
        timeout: Timeout in seconds
        extra_relays: Optional additional relay URLs
        
    Returns:
        List of event dicts
    """
    # Emit deprecation warning
    warnings.warn(
        "_query_events_via_manager is deprecated. "
        "Use nostr_helpers.query_events() directly instead.",
        DeprecationWarning,
        stacklevel=2
    )
    
    # Delegate entirely to nostr_helpers
    return await nostr_helpers.query_events(
        filters=filters,
        limit=limit,
        timeout=timeout,
        extra_relays=extra_relays
    )


async def lookup_metadata(pubkey: str, api_key: str | None = None) -> dict[str, str | None] | None:
    """Return {display_name, lud16, picture, nip05} from kind 0 metadata.
    
    Uses nostr_helpers.query_events() and nostr_helpers.query_user_relays()
    to fetch the most recent kind 0 metadata event for the given pubkey.
    
    Args:
        pubkey: Nostr pubkey (hex)
        api_key: Optional API key (unused, kept for compatibility)
        
    Returns:
        Dict with display_name, lud16, picture, nip05 or None if not found
    """

    def _extract_best(ev_list: list[dict]) -> dict[str, Any] | None:
        best = None
        best_created = 0
        for ev in ev_list or []:
            try:
                created = int(ev.get("created_at") or 0)
                if created >= best_created:
                    content = ev.get("content")
                    if isinstance(content, str):
                        content = json.loads(content)
                    if isinstance(content, dict) and (
                        content.get("lud16")
                        or content.get("name")
                        or content.get("display_name")
                        or content.get("nip05")
                    ):
                        best = content
                        best_created = created
            except Exception as e:
                logger.warning(e)
                continue
        return best

    # Get user's relays via nostr_helpers for better hit rate
    try:
        rels = await nostr_helpers.query_user_relays(pubkey)
    except Exception as e:
        logger.debug(f"lookup_metadata: relays fetch failed: {e}")
        rels = []
    
    # Query metadata via nostr_helpers
    evs = await nostr_helpers.query_events(
        {"kinds": [0], "authors": [pubkey]}, 
        limit=3, 
        timeout=8.0, 
        extra_relays=rels or None
    )
    
    best = _extract_best(evs)
    if best:
        dn = best.get("display_name") or best.get("name")
        if not dn:
            nip = best.get("nip05")
            if isinstance(nip, str) and "@" in nip:
                dn = nip.split("@", 1)[0]
        logger.info(
            f"cyberherd: metadata for {pubkey} display_name={dn or ''} lud16={best.get('lud16') or ''} nip05={best.get('nip05') or ''} picture={'yes' if best.get('picture') else 'no'}"
        )
        return {
            "display_name": dn or "Anon",
            "lud16": best.get("lud16"),
            "picture": best.get("picture"),
            "nip05": best.get("nip05"),
        }

    # No metadata available
    logger.info(f"cyberherd: no metadata found for {pubkey[:8]}...")
    return None


async def lookup_relays(pubkey: str, api_key: str | None = None) -> list[str]:
    """Return relays from kind 10002 relay list.
    
    DEPRECATED: Use nostr_helpers.query_user_relays() directly instead.
    
    This function is kept for backward compatibility but delegates to
    nostr_helpers internally.
    
    Args:
        pubkey: Nostr pubkey (hex)
        api_key: Optional API key (unused, kept for compatibility)
        
    Returns:
        List of relay URLs
    """
    try:
        return await nostr_helpers.query_user_relays(pubkey)
    except Exception as e:
        logger.warning(f"lookup_relays: error via nostr_helpers: {e}")
        return []


async def verify_nip05(pubkey_hex: str, nip05: str) -> bool:
    """Validate that nip05 identifier resolves to the given pubkey.

    Implements a pragmatic check consistent with common clients: fetch
    https://<domain>/.well-known/nostr.json?name=<user> and compare
    names[<user>] with the expected hex pubkey (case-insensitive).

    Falls back to the core fetch_nip5_details if direct fetch fails.
    """
    pubkey = (pubkey_hex or "").strip().lower()
    ident = (nip05 or "").strip()
    if not pubkey or len(pubkey) != 64:
        logger.debug("verify_nip05: invalid pubkey '%s'", pubkey_hex)
        return False
    if "@" not in ident:
        logger.debug("verify_nip05: invalid nip05 '%s'", nip05)
        return False

    try:
        username, domain = ident.split("@", 1)
        username = username.strip().lower()
        domain = domain.strip()
        if not username or not domain:
            return False

        url = f"https://{domain}/.well-known/nostr.json?name={username}"
        async with httpx.AsyncClient() as client:
            resp = await client.get(url, timeout=10)
            resp.raise_for_status()
            data = resp.json()

        mapped = None
        try:
            mapped = data.get("names", {}).get(username)
        except Exception:
            mapped = None

        if isinstance(mapped, str):
            mapped_clean = mapped.strip()
            if mapped_clean:
                try:
                    mapped_hex = normalize_public_key(mapped_clean)
                except Exception:
                    mapped_hex = mapped_clean.lower()
                else:
                    mapped_hex = mapped_hex.strip().lower()

                if mapped_hex == pubkey:
                    return True
        logger.info(
            "verify_nip05: mapping mismatch for %s -> %s (expected %s)",
            ident,
            mapped,
            pubkey,
        )
    except Exception as e:
        logger.debug("verify_nip05 direct fetch failed: %s", e)

    # Fallback to core helper if available
    try:
        from lnbits.core.services.nostr import fetch_nip5_details  # type: ignore
        try:
            resolved_pubkey, _ = await fetch_nip5_details(ident)
            if resolved_pubkey:
                try:
                    resolved_hex = normalize_public_key(resolved_pubkey)
                except Exception:
                    resolved_hex = resolved_pubkey.strip().lower()
                else:
                    resolved_hex = resolved_hex.strip().lower()
                return resolved_hex == pubkey
            return False
        except Exception as e:  # pragma: no cover - network/env dependent
            logger.debug("verify_nip05 core fallback failed: %s", e)
            return False
    except Exception:
        return False
