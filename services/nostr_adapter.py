"""Compatibility shim redirecting to callback-based monitoring system.

This module re-exports functions from nostr_event_monitor.py for backward compatibility.
All new code should import directly from nostr_event_monitor.py instead.
"""

from .nostr_event_monitor import (
    start_monitoring_system as start_adapter,
    force_requery_for_user,
)

__all__ = [
    "start_adapter",
    "force_requery_for_user",
]

