"""
Base class definition for all datastore connections
"""

# Standard
from typing import Optional
import abc

# First Party
import alog

# Local
from ... import Session, assert_cluster
from ..utils import common
from ..utils.abc_static import ABCStatic

log = alog.use_channel("DCONN")


class DatastoreConnectionBase(ABCStatic):
    """
    A DatastoreConnection is an object that holds all of the critical data to
    connect to a specific datastore type. A DatastoreConnection for a given
    datastore type MUST not care what implementation backs the connection.
    """

    ## Construction ############################################################

    def __init__(self, session: Session):
        """Construct with the session so that it can be saved as a member"""
        self._session = session

    @property
    def session(self) -> Session:
        return self._session

    ## Abstract Interface ######################################################

    @abc.abstractmethod
    def to_dict(self) -> dict:
        """Serialize the internal connection details to a dict object which can
        be added directly to a subsystem's CR.

        Returns:
            config_dict:  dict
                This dict will hold the keys and values that can be used to add
                to a subsystem's datastores.connections section.
        """

    @classmethod
    @abc.abstractmethod
    def from_dict(
        cls, session: Session, config_dict: dict
    ) -> "DatastoreConnectionBase":
        """Parse a config_dict from a subsystem CR to create an instance of the
        DatastoreConnection class.

        Args:
            session:  Session
                The current deploy session
            config_dict:  dict
                This dict will hold the keys and values created by to_dict and
                pulled from the subsystem CR.

        Returns:
            datastore_connection:  DatastoreConnectionBase
                The constructed instance of the connection
        """

    ## Shared Utilities ########################################################

    def _fetch_secret_data(self, secret_name: str) -> Optional[dict]:
        """Most connection implementations will need the ability to fetch secret
        data from the cluster when loading from the CR dict, so this provides a
        common implementation.

        Args:
            secret_name:  str
                The name of the secret to fetch

        Returns:
            secret_data:  Optional[dict]
                The content of the 'data' field in the secret with values base64
                decoded if the secret is found, otherwise None
        """
        success, content = self.session.get_object_current_state("Secret", secret_name)
        assert_cluster(success, f"Fetching connection secret [{secret_name}] failed")
        if content is None:
            return None
        assert "data" in content, "Got a secret without 'data'?"
        return {
            key: common.b64_secret_decode(val) for key, val in content["data"].items()
        }
