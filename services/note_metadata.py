"""Helpers for CyberHerd tracked note metadata.

Provides utilities to derive and refresh additional metadata for tracked notes,
notably the Nostr `a` tag required to reply to kind 30311 events.
"""

from __future__ import annotations

from typing import Dict, List

from loguru import logger


def extract_30311_address(event: dict) -> str | None:
    """Return the Nostr `a` tag for a kind 30311 event, if available."""
    if not isinstance(event, dict):
        return None

    kind = event.get("kind")
    try:
        if int(kind) != 30311:
            return None
    except Exception:
        if kind != 30311:  # Handles non-int kinds without raising
            return None

    pubkey = event.get("pubkey")
    if not isinstance(pubkey, str) or not pubkey:
        return None

    # Per NIP-33: d-tag is REQUIRED for parameterized replaceable events
    # If missing, treat as empty string (default identifier)
    d_tag = ""
    for tag in event.get("tags") or []:
        if isinstance(tag, list) and len(tag) > 1 and tag[0] == "d":
            candidate = tag[1]
            if isinstance(candidate, str):
                d_tag = candidate
                break

    # Always return address, using empty string if no d-tag found
    # This matches NIP-33 spec: missing d-tag = empty string identifier
    return f"30311:{pubkey}:{d_tag}"


def apply_event_address(address_map: Dict[str, str], event: dict) -> None:
    """Mutate *address_map* with the 30311 address from *event* if present."""
    if not isinstance(address_map, dict):
        return

    note_id = event.get("id") if isinstance(event, dict) else None
    if not isinstance(note_id, str) or not note_id:
        return

    address = extract_30311_address(event)
    if address:
        address_map[note_id] = address
    else:
        # Ensure stale entries are removed if event is not 30311 anymore
        address_map.pop(note_id, None)


async def refresh_tracked_event_addresses(
    note_ids: List[str],
    existing: Dict[str, str] | None = None,
) -> Dict[str, str]:
    """Ensure address mappings for the supplied tracked note IDs are populated.

    Args:
        note_ids: Tracked note IDs to refresh
        existing: Existing mapping of note_id -> address
    Returns:
        Updated mapping limited to the provided note IDs.
    """
    valid_ids = [nid for nid in note_ids if isinstance(nid, str) and nid]
    addresses: Dict[str, str] = {
        nid: addr
        for nid, addr in (existing or {}).items()
        if nid in valid_ids and isinstance(addr, str) and addr
    }

    missing = [nid for nid in valid_ids if nid not in addresses]
    if not missing:
        return addresses

    try:
        from . import nostr_helpers

        if not nostr_helpers.check_availability():
            logger.info(
                "cyberherd: nostr helpers unavailable, cannot refresh 30311 addresses for %d note(s)",
                len(missing),
            )
            return addresses

        events = await nostr_helpers.query_events(
            {"ids": missing},
            limit=len(missing),
            timeout=6.0,
        )
    except Exception as exc:
        logger.warning(
            "cyberherd: failed to refresh 30311 note addresses for %d note(s): %s",
            len(missing),
            exc,
        )
        return addresses

    for event in events or []:
        note_id = event.get("id")
        if not isinstance(note_id, str) or not note_id:
            continue
        address = extract_30311_address(event)
        if address:
            addresses[note_id] = address

    return addresses


__all__ = [
    "extract_30311_address",
    "apply_event_address",
    "refresh_tracked_event_addresses",
]
