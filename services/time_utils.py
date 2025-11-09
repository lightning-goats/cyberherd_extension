"""Time utility functions for CyberHerd - UTC-first approach.

This module provides shared time calculation functions to ensure consistency
across all CyberHerd modules (crud.py, subscriptions.py, etc.).

DESIGN PHILOSOPHY - UTC-First:
==============================
1. All internal storage and calculations use UTC timestamps
2. Local timezone conversions ONLY for:
   - User-facing display
   - "Today's notes" filtering (user's local day)
3. Cache keys, database timestamps, and Nostr filters always use UTC values
4. Local times are derived from UTC, never the source of truth
5. System's local timezone is used (consistent with nightly reset behavior)

RATIONALE:
==========
- Nostr events use UTC timestamps (created_at field)
- Database stores UTC timestamps
- System's configured timezone determines "local day" concept
- DST transitions are handled correctly when converting FROM UTC
- Eliminates ambiguity in "what is today?"
- Consistent with nightly reset scheduling (uses system timezone)

MIGRATION PATH:
===============
Old code pattern (local-first):
    now_local = datetime.now()
    local_midnight = datetime.combine(now_local.date(), datetime.min.time())
    utc_ts = int(local_midnight.astimezone(timezone.utc).timestamp())

New code pattern (UTC-first):
    from .services.time_utils import get_day_boundaries_utc
    boundaries = get_day_boundaries_utc()
    utc_ts = boundaries.since_timestamp  # UTC-based, unambiguous

For "local day" filtering:
    boundaries = get_day_boundaries_utc()
    # Use boundaries.local_since_ts and boundaries.local_until_ts
    # These represent the user's local day converted to UTC timestamps
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import os
try:
    # Python 3.9+
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover - very old Python
    ZoneInfo = None
from typing import Optional


@dataclass(frozen=True)
class DayBoundaries:
    """Immutable container for day boundary calculations.
    
    All timestamps are in UTC epoch seconds. Local times are for display/reference only.
    
    Attributes:
        utc_midnight: Start of day in UTC (datetime object, timezone-aware)
        utc_since_ts: Start of day in UTC (epoch seconds) - PRIMARY for storage
        utc_until_ts: End of day in UTC (epoch seconds) - PRIMARY for storage
        local_midnight: Start of day in user's local timezone (datetime object, timezone-aware)
        local_since_ts: Start of user's local day, converted to UTC epoch seconds
        local_until_ts: End of user's local day, converted to UTC epoch seconds
        utc_day_str: ISO date string for UTC day (e.g., "2025-10-04")
        local_day_str: ISO date string for local day (e.g., "2025-10-04")
    """
    # UTC-based (source of truth)
    utc_midnight: datetime
    utc_since_ts: int
    utc_until_ts: int
    
    # Local timezone (for display/filtering only)
    local_midnight: datetime
    local_since_ts: int
    local_until_ts: int
    
    # String representations
    utc_day_str: str
    local_day_str: str
    
    def is_timestamp_in_utc_day(self, timestamp: int) -> bool:
        """Check if a timestamp falls within the UTC day boundaries."""
        return self.utc_since_ts <= timestamp < self.utc_until_ts
    
    def is_timestamp_in_local_day(self, timestamp: int) -> bool:
        """Check if a timestamp falls within the local day boundaries.
        
        This is useful for "today's notes" filtering where users expect
        to see notes from their local calendar day, not UTC day.
        """
        return self.local_since_ts <= timestamp < self.local_until_ts


def get_day_boundaries_utc(days_ago: int = 0) -> DayBoundaries:
    """Get day boundaries with UTC-first approach (RECOMMENDED).
    
    This is the authoritative function for all "today" calculations in CyberHerd.
    All modules should use this function to ensure consistency.
    
    Args:
        days_ago: How many days back to calculate (0 = today, 1 = yesterday, etc.)
    
    Returns:
        DayBoundaries object with both UTC and local time information
        
    Example:
        >>> boundaries = get_day_boundaries_utc()
        >>> # For Nostr filters (always UTC):
        >>> filter = {"since": boundaries.utc_since_ts, "until": boundaries.utc_until_ts}
        >>> # For "today's notes" filtering (user's local day):
        >>> if boundaries.is_timestamp_in_local_day(event['created_at']):
        >>>     # This note is from user's "today"
        >>>     pass
    """
    # Start with UTC - this is our source of truth
    now_utc = datetime.now(timezone.utc)
    target_date = now_utc.date() - timedelta(days=days_ago)
    
    # UTC midnight for the target date
    utc_midnight = datetime.combine(target_date, datetime.min.time()).replace(tzinfo=timezone.utc)
    utc_since_ts = int(utc_midnight.timestamp())
    utc_until_ts = utc_since_ts + 86400
    utc_day_str = target_date.isoformat()
    
    # Local midnight for user-facing "today" concept
    # Use system's local timezone for consistency with nightly reset
    try:
        # Get system local timezone
        local_tz = datetime.now().astimezone().tzinfo
    except Exception:
        # Fallback to UTC if local timezone detection fails
        local_tz = timezone.utc

    now_local = datetime.now(local_tz)
    local_target_date = now_local.date() - timedelta(days=days_ago)
    local_midnight = datetime.combine(local_target_date, datetime.min.time()).replace(tzinfo=local_tz)
    local_since_ts = int(local_midnight.astimezone(timezone.utc).timestamp())
    local_until_ts = local_since_ts + 86400
    local_day_str = local_target_date.isoformat()
    
    return DayBoundaries(
        utc_midnight=utc_midnight,
        utc_since_ts=utc_since_ts,
        utc_until_ts=utc_until_ts,
        local_midnight=local_midnight,
        local_since_ts=local_since_ts,
        local_until_ts=local_until_ts,
        utc_day_str=utc_day_str,
        local_day_str=local_day_str,
    )


def format_timestamp_utc(timestamp: int, fmt: str = "%Y-%m-%d %H:%M:%S UTC") -> str:
    """Format a UTC timestamp for display.
    
    Args:
        timestamp: Unix timestamp (assumed to be UTC)
        fmt: strftime format string
        
    Returns:
        Formatted string
    """
    dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
    return dt.strftime(fmt)


def format_timestamp_local(timestamp: int, fmt: str = "%Y-%m-%d %H:%M:%S %Z") -> str:
    """Format a UTC timestamp as local time for display.
    
    Args:
        timestamp: Unix timestamp (UTC)
        fmt: strftime format string
        
    Returns:
        Formatted string in system's local timezone
    """
    dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
    # Use system's local timezone
    try:
        local_tz = datetime.now().astimezone().tzinfo
    except Exception:
        local_tz = timezone.utc

    local_dt = dt.astimezone(local_tz)
    return local_dt.strftime(fmt)


def get_cache_key_prefix_utc(user_id: str, boundaries: DayBoundaries) -> str:
    """Generate cache key prefix using UTC day boundaries.
    
    Cache keys MUST use UTC-based values to ensure consistency across
    different user timezones and avoid cache invalidation issues during
    DST transitions.
    
    Args:
        user_id: User identifier
        boundaries: DayBoundaries object
        
    Returns:
        Cache key prefix like "user123_utc:2025-10-04" (always UTC date)
    """
    return f"{user_id}_utc:{boundaries.utc_day_str}"


def get_cache_key_with_local_context(user_id: str, boundaries: DayBoundaries) -> str:
    """Generate cache key that includes both UTC and local day context.
    
    This is useful for diagnostics and debugging where you want to see
    both the UTC storage key and the user's local day perception.
    
    Args:
        user_id: User identifier
        boundaries: DayBoundaries object
        
    Returns:
        Cache key like "user123_utc:2025-10-04_local:2025-10-04" 
        (shows both UTC and local dates for clarity)
    """
    return f"{user_id}_utc:{boundaries.utc_day_str}_local:{boundaries.local_day_str}"


# Convenience functions for common use cases

def get_current_utc_timestamp() -> int:
    """Get current UTC timestamp (epoch seconds)."""
    return int(datetime.now(timezone.utc).timestamp())


def get_today_boundaries() -> DayBoundaries:
    """Get boundaries for today (convenience wrapper)."""
    return get_day_boundaries_utc(days_ago=0)


def get_yesterday_boundaries() -> DayBoundaries:
    """Get boundaries for yesterday (convenience wrapper)."""
    return get_day_boundaries_utc(days_ago=1)


__all__ = [
    "DayBoundaries",
    "get_day_boundaries_utc",
    "format_timestamp_utc",
    "format_timestamp_local",
    "get_cache_key_prefix_utc",
    "get_cache_key_with_local_context",
    "get_current_utc_timestamp",
    "get_today_boundaries",
    "get_yesterday_boundaries",
]
