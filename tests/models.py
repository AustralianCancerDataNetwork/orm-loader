
import sqlalchemy as sa
from sqlalchemy.orm import declarative_base
import sqlalchemy.orm as so
from orm_loader.tables import CSVLoadableTableInterface

Base = declarative_base()

class PandasLoaderTable(CSVLoadableTableInterface, Base):
    __tablename__ = "test_pandas_loader"
    id = sa.Column(sa.Integer, primary_key=True)
    value = sa.Column(sa.String, nullable=False)


class SimpleTable(Base, CSVLoadableTableInterface):
    __tablename__ = "test_table"

    id: so.Mapped[int] = so.mapped_column(sa.Integer, primary_key=True)
    name: so.Mapped[str] = so.mapped_column(sa.String, nullable=False)


class RequiredTable(Base, CSVLoadableTableInterface):
    __tablename__ = "required_table"

    id: so.Mapped[int] = so.mapped_column(sa.Integer, primary_key=True)
    name: so.Mapped[str] = so.mapped_column(sa.String, nullable=False)


class CompositeTable(Base, CSVLoadableTableInterface):
    __tablename__ = "composite_table"

    a: so.Mapped[int] = so.mapped_column(sa.Integer, primary_key=True)
    b: so.Mapped[int] = so.mapped_column(sa.Integer, primary_key=True)
    value: so.Mapped[str] = so.mapped_column(sa.String)

