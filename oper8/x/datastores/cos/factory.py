"""
COS instance factory
"""

# Local
from ..factory_base import DatastoreSingletonFactoryBase
from .connection import CosConnection


class CosFactory(DatastoreSingletonFactoryBase):
    """The common factory that will manage instances of COS for each deploy"""

    DATASTORE_TYPE = "cos"
    CONNECTION_TYPE = CosConnection
