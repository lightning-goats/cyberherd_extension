from lnbits.db import Database
import logging

logger = logging.getLogger(__name__)


def init_db(app):
    """Initialize the cyberherd database."""
    try:
        Database("ext_cyberherd")
        # The database will be initialized when migrations are run
    except Exception as e:
        logger.error(f"Failed to initialize cyberherd database: {e}")
