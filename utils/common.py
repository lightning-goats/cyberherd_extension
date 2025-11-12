"""Common utility functions to eliminate redundancy across the Cyberherd extension.

This module provides single-source-of-truth implementations for commonly repeated patterns:
- Dynamic module imports
- Environment variable parsing
- Timestamp utilities
- Nostr event tag extraction
- Boolean parsing
"""

from __future__ import annotations

import importlib
import os
from datetime import datetime, timezone
from typing import Any, Callable, Optional


# ==================== Dynamic Module Imports ====================

def import_lnbits_core_crud():
    """Import lnbits.core.crud module dynamically.
    
    Returns:
        Module object or None if import fails
    """
    try:
        return importlib.import_module('lnbits.core.crud')
    except Exception:
        return None


def import_lnbits_nostr_utils():
    """Import lnbits.utils.nostr module dynamically.
    
    Returns:
        Module object or None if import fails
    """
    try:
        return importlib.import_module('lnbits.utils.nostr')
    except Exception:
        return None


def get_lnbits_users_function() -> Optional[Callable]:
    """Get the get_users function from lnbits.core.crud.
    
    Returns:
        get_users function or None if not available
    """
    try:
        core_mod = import_lnbits_core_crud()
        if core_mod:
            return getattr(core_mod, 'get_users', None)
    except Exception:
        pass
    return None


async def is_extension_enabled_for_user(user_id: str, extension_id: str = 'cyberherd') -> bool:
    """Check if an extension is enabled for a specific user.
    
    This is the standard LNbits way to check if a user has enabled an extension.
    All user iteration loops should use this check to respect user preferences.
    
    Args:
        user_id: The user ID to check
        extension_id: The extension ID (defaults to 'cyberherd')
        
    Returns:
        True if the extension is enabled for the user, False otherwise
    """
    try:
        from lnbits.core.crud import get_user_active_extensions_ids
        user_extensions = await get_user_active_extensions_ids(user_id)
        return extension_id in user_extensions
    except Exception:
        # If we can't check, assume it's enabled (backward compatibility)
        return True


def get_nip19_decoder():
    """Get the nip19 decoder from lnbits.utils.nostr.
    
    Returns:
        nip19 object with decode method, or None if not available
    """
    try:
        nostr_mod = import_lnbits_nostr_utils()
        if nostr_mod:
            return getattr(nostr_mod, 'nip19', None)
    except Exception:
        pass
    return None


# ==================== Environment Variable Utilities ====================

def parse_bool_env(env_var: str, default: bool = False) -> bool:
    """Parse a boolean environment variable.
    
    Args:
        env_var: Environment variable name
        default: Default value if not set
        
    Returns:
        True if env value is "1", "true", "yes", or "y" (case-insensitive)
    """
    value = os.getenv(env_var, str(default)).lower()
    return value in ("1", "true", "yes", "y")


def coerce_bool(value: Any, default: bool = False) -> bool:
    """Convert loose user/db input into a strict boolean.
    
    Accepts actual booleans, ints, and common string representations while
    falling back to *default* when the value is None or empty.
    """
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    try:
        text = str(value).strip().lower()
    except Exception:
        return default
    if text in {"", "unset"}:
        return default
    if text in {"1", "true", "yes", "on", "y"}:
        return True
    if text in {"0", "false", "no", "off", "n"}:
        return False
    return default


def parse_int_env(env_var: str, default: int) -> int:
    """Parse an integer environment variable with fallback.
    
    Args:
        env_var: Environment variable name
        default: Default value if not set or invalid
        
    Returns:
        Integer value from environment or default
    """
    try:
        value = os.getenv(env_var, str(default))
        return int(value or default)
    except (ValueError, TypeError):
        return default


def parse_float_env(env_var: str, default: float) -> float:
    """Parse a float environment variable with fallback.
    
    Args:
        env_var: Environment variable name
        default: Default value if not set or invalid
        
    Returns:
        Float value from environment or default
    """
    try:
        value = os.getenv(env_var, str(default))
        return float(value or default)
    except (ValueError, TypeError):
        return default


def parse_string_env(env_var: str, default: str) -> str:
    """Parse a string environment variable with fallback.
    
    Args:
        env_var: Environment variable name
        default: Default value if not set
        
    Returns:
        String value from environment or default
    """
    return os.getenv(env_var, default) or default


# ==================== Timestamp Utilities ====================

def utc_now() -> datetime:
    """Get current UTC datetime.
    
    Returns:
        Current datetime with UTC timezone
    """
    return datetime.now(timezone.utc)


def utc_now_timestamp() -> int:
    """Get current UTC timestamp as integer seconds.
    
    Returns:
        Current UTC timestamp (seconds since epoch)
    """
    return int(utc_now().timestamp())


def utc_now_iso() -> str:
    """Get current UTC datetime as ISO format string.
    
    Returns:
        Current UTC datetime in ISO 8601 format
    """
    return utc_now().isoformat()


# ==================== Nostr Event Utilities ====================

def extract_t_tags_from_event(event: dict, normalize: bool = True) -> set[str]:
    """Extract 't' tags from a Nostr event.
    
    Args:
        event: Nostr event dictionary
        normalize: If True, strip # and lowercase (default: True)
        
    Returns:
        Set of tag values (normalized if requested)
    """
    if not isinstance(event, dict):
        return set()
    
    tags = event.get('tags', []) or []
    result = set()
    
    for tag in tags:
        if not (isinstance(tag, list) and len(tag) > 1):
            continue
        if tag[0] != 't':
            continue
        try:
            value = str(tag[1])
            if normalize:
                value = value.lstrip('#').lower()
            result.add(value)
        except Exception:
            continue
    
    return result


def extract_e_tags_from_event(event: dict) -> list[str]:
    """Extract 'e' tags (event references) from a Nostr event.
    
    Args:
        event: Nostr event dictionary
        
    Returns:
        List of referenced event IDs (order preserved, duplicates removed)
    """
    if not isinstance(event, dict):
        return []
    
    tags = event.get('tags', []) or []
    result = []
    seen = set()
    
    for tag in tags:
        if not (isinstance(tag, list) and len(tag) >= 2):
            continue
        if tag[0] != 'e':
            continue
        try:
            event_id = str(tag[1])
            if event_id and event_id not in seen:
                seen.add(event_id)
                result.append(event_id)
        except Exception:
            continue
    
    return result


def extract_p_tags_from_event(event: dict) -> list[str]:
    """Extract 'p' tags (pubkey references) from a Nostr event.
    
    Args:
        event: Nostr event dictionary
        
    Returns:
        List of referenced pubkeys (order preserved, duplicates removed)
    """
    if not isinstance(event, dict):
        return []
    
    tags = event.get('tags', []) or []
    result = []
    seen = set()
    
    for tag in tags:
        if not (isinstance(tag, list) and len(tag) >= 2):
            continue
        if tag[0] != 'p':
            continue
        try:
            pubkey = str(tag[1])
            if pubkey and pubkey not in seen:
                seen.add(pubkey)
                result.append(pubkey)
        except Exception:
            continue
    
    return result


def safe_get_event_field(event: dict, field: str, default: Any = None) -> Any:
    """Safely get a field from a Nostr event dictionary.
    
    Args:
        event: Nostr event dictionary
        field: Field name to retrieve
        default: Default value if field missing or event invalid
        
    Returns:
        Field value or default
    """
    if not isinstance(event, dict):
        return default
    return event.get(field, default)


# ==================== String Utilities ====================

def truncate_id(id_str: str | None, length: int = 16) -> str:
    """Truncate an ID string for logging.
    
    Args:
        id_str: ID string to truncate
        length: Number of characters to keep (default: 16)
        
    Returns:
        Truncated string with "..." suffix or "unknown" if None
    """
    if not id_str:
        return "unknown"
    if len(id_str) <= length:
        return id_str
    return f"{id_str[:length]}..."


# ==================== Validation Utilities ====================

def is_valid_hex_string(s: str, required_length: int | None = None) -> bool:
    """Check if a string is valid hexadecimal.
    
    Args:
        s: String to validate
        required_length: If specified, check for exact length
        
    Returns:
        True if valid hex (and correct length if specified)
    """
    if not isinstance(s, str):
        return False
    if required_length is not None and len(s) != required_length:
        return False
    try:
        int(s, 16)
        return True
    except ValueError:
        return False


def is_valid_event_id(event_id: str | None) -> bool:
    """Check if a string is a valid Nostr event ID (64-char hex).
    
    Args:
        event_id: Event ID to validate
        
    Returns:
        True if valid 64-character hex string
    """
    return is_valid_hex_string(event_id, 64) if event_id else False


def is_valid_pubkey(pubkey: str | None) -> bool:
    """Check if a string is a valid Nostr pubkey (64-char hex).
    
    Args:
        pubkey: Pubkey to validate
        
    Returns:
        True if valid 64-character hex string
    """
    return is_valid_hex_string(pubkey, 64) if pubkey else False
