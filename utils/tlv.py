"""TLV encoding utilities for Cyberherd extension.
"""

from bech32 import bech32_encode, convertbits


def _encode_tlv(entries: dict[int, list[bytes]]) -> bytes:
    """Encode a simple TLV structure used by NIP-19 (single-byte type and length).

    entries: mapping of type -> list of value bytes
    returns raw TLV bytes
    """
    chunks: list[bytes] = []
    # Align with nostr.js behavior: iterate in reverse order not necessary here
    for t, values in entries.items():
        for v in values or []:
            if not isinstance(v, (bytes, bytearray)):
                v = str(v).encode()
            if len(v) > 255:
                raise ValueError("TLV value too long for single-byte length")
            chunks.append(bytes([int(t) & 0xFF, len(v) & 0xFF]) + bytes(v))
    return b"".join(chunks)


def hex_to_nprofile(pubkey_hex: str, relays: list[str] | None = None) -> str:
    """Encode an nprofile (NIP-19) from hex pubkey and optional relay URLs.

    TLV:
    - type 0: 32-byte pubkey
    - type 1: one entry per relay (UTF-8)
    """
    if len(pubkey_hex) != 64:
        raise ValueError("pubkey must be 32-byte hex")
    try:
        pk_bytes = bytes.fromhex(pubkey_hex)
    except Exception as exc:
        raise ValueError("pubkey must be hex") from exc

    relay_vals = []
    for url in relays or []:
        if not isinstance(url, str):
            continue
        relay_vals.append(url.encode("utf-8"))

    tlv = _encode_tlv({0: [pk_bytes], 1: relay_vals})
    words = convertbits(tlv, 8, 5, True)
    if not words:
        raise ValueError("Failed to convert TLV to bech32 words")
    return bech32_encode("nprofile", words)
