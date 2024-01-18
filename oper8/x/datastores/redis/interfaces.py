"""
Base class interface for a Redis component
"""

# Standard
from abc import abstractmethod
from typing import Optional

# Local
from .... import component
from ..interfaces import Datastore
from .connection import RedisConnection

COMPONENT_NAME = "redis"


@component(COMPONENT_NAME)
class IRedisComponent(Datastore):
    """A redis chart provides access to a redis instance"""

    ## Parent Interface ########################################################

    def get_connection(self) -> RedisConnection:
        """Get the connection object for this instance"""
        return RedisConnection(
            session=self.session,
            hostname=self._get_hostname(),
            port=self._get_port(),
            auth_secret_name=self._get_auth_secret_name(),
            auth_secret_username_field=self._get_auth_secret_username_field(),
            auth_secret_password_field=self._get_auth_secret_password_field(),
            tls_secret_name=self._get_tls_secret_name(),
            tls_secret_cert_field=self._get_tls_secret_cert_field(),
            auth_username=self._get_auth_username(),
            auth_password=self._get_auth_password(),
            tls_cert=self._get_tls_cert(),
        )

    ## Abstract Interface ######################################################
    #
    # This is the interface that needs to be implemented by a child in order to
    # provide the common information that a client will use.
    ##

    ##################
    ## General Info ##
    ##################

    @abstractmethod
    def _get_hostname(self) -> str:
        """Gets the hotsname for the connection. Can be IP address as well.

        Returns:
            hostname:  str
                The hostname (without schema) for the service
        """

    @abstractmethod
    def _get_port(self) -> int:
        """Gets the port where the service is listening

        Returns:
            port:  int
                The port where the service is listening
        """

    ###############
    ## Auth Info ##
    ###############

    @abstractmethod
    def _get_auth_secret_name(self) -> str:
        """Get the Auth secret name with any scoping applied

        Returns:
            auth_secret_name:  str
                The name of the secret containing the auth secret
        """

    @abstractmethod
    def _get_auth_secret_username_field(self) -> Optional[str]:
        """Get the field form within the auth secret that contains the
        username

        Returns:
            username_key_field:  str
                The field within the auth secret that contains the username
        """

    @abstractmethod
    def _get_auth_secret_password_field(self) -> str:
        """Get the field form within the auth secret that contains the
        password for the user

        Returns:
            password_key_field:  str
                The field within the auth secret that contains the password_key
        """

    @abstractmethod
    def _get_auth_username(self) -> Optional[str]:
        """Get the un-encoded content of the username if available in-memory.
        Components which proxy an external secret don't need to fetch this
        content from the cluster.

        Returns:
            username:  Optional[str]
                The content of the username if known
        """

    @abstractmethod
    def _get_auth_password(self) -> Optional[str]:
        """Get the un-encoded content of the password if available in-memory.
        Components which proxy an external secret don't need to fetch this
        content from the cluster.

        Returns:
            password:  Optional[str]
                The content of the password if known
        """

    ##############
    ## TLS Info ##
    ##############

    @abstractmethod
    def _get_tls_secret_name(self) -> Optional[str]:
        """Get the TLS secret name with any scoping applied if tls is enabled

        Returns:
            tls_secret_name:  Optional[str]
                If tls is enabled, returns the name of the secret, otherwise
                None
        """

    @abstractmethod
    def _get_tls_secret_cert_field(self) -> Optional[str]:
        """Get the field from within the tls secret that contains the CA
        certificate a client would need to use to connect

        Returns:
            cert_field:  Optional[str]
                The field within the tls secret where the CA certificate lives
        """

    @abstractmethod
    def _get_tls_cert(self) -> Optional[str]:
        """Get the un-encoded content of the TLS cert if TLS is enabled and
        available in-memory. Components which proxy an external secret don't
        need to fetch this content from the cluster.

        Returns:
            cert_content:  Optional[str]
                The content of the cert if tls is enabled
        """
