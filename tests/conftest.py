import pytest
import sqlalchemy as sa
import sqlalchemy.orm as so
from sqlalchemy.orm import declarative_base

Base = declarative_base()

@pytest.fixture
def engine():
    return sa.create_engine("sqlite:///:memory:")

@pytest.fixture
def session(engine):
    Base.metadata.create_all(engine)
    with so.Session(engine) as s:
        yield s
