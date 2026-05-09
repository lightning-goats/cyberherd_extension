from lnbits.db import Database
import logging

logger = logging.getLogger(__name__)


def init_db(app):
    """Initialize the cyberherd database."""
    try:
        db = Database("ext_cyberherd")
        # The database will be initialized when migrations are run
        pass
    except Exception as e:
        logger.error(f"Failed to initialize cyberherd database: {e}")
