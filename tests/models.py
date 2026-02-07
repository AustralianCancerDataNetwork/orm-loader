
import sqlalchemy as sa
from sqlalchemy.orm import declarative_base
from orm_loader.tables import CSVLoadableTableInterface

Base = declarative_base()

class PandasLoaderTable(CSVLoadableTableInterface, Base):
    __tablename__ = "test_pandas_loader"
    id = sa.Column(sa.Integer, primary_key=True)
    value = sa.Column(sa.String, nullable=False)