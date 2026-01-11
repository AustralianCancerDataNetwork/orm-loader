from .metadata import Base
import logging

logger = logging.getLogger(__name__)

def create_db(engine):
    logger.debug("Creating database schema")
    Base.metadata.create_all(engine)

def bootstrap(engine, *, create: bool = True):
    logger.info("Bootstrapping schema (create=%s)", create)
    if create:
        create_db(engine)
