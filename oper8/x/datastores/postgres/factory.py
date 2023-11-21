"""
Postgres instance factory
"""

# Local
from ..factory_base import DatastoreSingletonFactoryBase
from .connection import PostgresConnection


class PostgresFactory(DatastoreSingletonFactoryBase):
    """The common factory that will manage instances of Postgres for each deploy"""

    DATASTORE_TYPE = "postgres"
    CONNECTION_TYPE = PostgresConnection
