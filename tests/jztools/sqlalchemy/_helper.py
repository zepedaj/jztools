from contextlib import contextmanager
from tempfile import NamedTemporaryFile
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, VARCHAR


def create_tables(metadata):

    # Distinguishes between writing form different DataStore instances.
    table = Table(
        "writers",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("string", VARCHAR),
        extend_existing=True,
    )

    return [table]


@contextmanager
def get_database():
    with NamedTemporaryFile() as tmpfo:
        #
        engine = create_engine(f"sqlite:///{tmpfo.name}")
        metadata = MetaData(bind=engine)
        #
        tables = create_tables(metadata)
        metadata.create_all()

        yield engine, metadata, tables
