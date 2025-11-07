"""Compatibility shim redirecting to current monitoring system.

This module re-exports functions for backward compatibility.
All new code should import directly from the appropriate modules.
"""

from .subscriptions import force_requery_for_user

__all__ = [
    "force_requery_for_user",
]

