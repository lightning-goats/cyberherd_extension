"""Deprecated: moved to lnbits.extensions.cyberherd_messaging.services

This module remains to avoid import errors in older installs. Use the shared
messaging extension instead.
"""

try:
    from lnbits.extensions.cyberherd_messaging.services import publish_note  # noqa: F401
except ImportError:
    # Fallback if cyberherd_messaging is not available
    def publish_note(*args, **kwargs):
        return False
