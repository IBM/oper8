"""
Base class for all Datastore component implementations
"""

# Standard
from typing import Optional
import abc

# First Party
import aconfig

# Local
from ... import Session
from ..oper8x_component import Oper8xComponent
from .connection_base import DatastoreConnectionBase


class Datastore(Oper8xComponent):
    """
    The Datastore baseclass defines the interface that any datastore must
    conform to. It is a oper8 Component and should be constructed via a per-type
    factory.
    """

    _TYPE_LABEL_ATTRIBUTE = "TYPE_LABEL"

    def __init__(
        self,
        session: Session,
        config: aconfig.Config,
        instance_name: Optional[str] = None,
        disabled: bool = False,
    ):
        """This passthrough constructor enforces that all datastores have a
        class attribute TYPE_LABEL (str)
        """
        type_label = getattr(self, self._TYPE_LABEL_ATTRIBUTE, None)
        assert isinstance(
            type_label, str
        ), f"All datastores types must define {self._TYPE_LABEL_ATTRIBUTE} as a str"
        super().__init__(session=session, disabled=disabled)
        self._config = config
        self.instance_name = instance_name

    @property
    def config(self) -> aconfig.Config:
        """The config for this instance of the datastore"""
        return self._config

    @abc.abstractmethod
    def get_connection(self) -> DatastoreConnectionBase:
        """Get the connection object for this datastore instance. Each datastore
        type must manage a common abstraction for a connection which clients
        will use to connect to the datastore.
        """
