"""Setup helpers for Cyberherd extension to wire services into the app state.

This registers the PaymentService (existing) and the EnhancedHeadbuttService
so the extension runtime can access them via `app.state`.
"""

import logging

logger = logging.getLogger(__name__)


def register_services(app):
    """Register cyberherd services with the app.

    Uses the singleton factory to avoid duplicate ZapMonitorService instances.
    Safe to call multiple times; subsequent calls just update attributes.
    """
    try:
        from .services.zap_monitor import get_zap_monitor

        zap_monitor = get_zap_monitor(app=app)
        # Ensure stored on app.state for other modules
        app.state.cyberherd_zap_monitor = zap_monitor
        logger.info("Cyberherd services registered (singleton zap monitor ready)")
    except Exception as e:
        logger.error(f"Failed to register cyberherd services: {e}")
