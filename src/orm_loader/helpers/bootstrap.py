from .metadata import Base
import logging
import sqlalchemy as sa
logger = logging.getLogger(__name__)

def create_db(engine: sa.engine.Engine) -> None:
    logger.debug("Creating database schema")
    Base.metadata.create_all(engine)

def bootstrap(engine: sa.engine.Engine, *, create: bool = True) -> None:
    logger.info("Bootstrapping schema (create=%s)", create)
    if create:
        create_db(engine)
