"""
Redis instance factory
"""

# Local
from ..factory_base import DatastoreSingletonFactoryBase
from .connection import RedisConnection


class RedisFactory(DatastoreSingletonFactoryBase):
    """The common factory that will manage instances of Redis"""

    DATASTORE_TYPE = "redis"
    CONNECTION_TYPE = RedisConnection
