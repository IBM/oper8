"""
The common connection type for a Redis instance
"""

# Standard
from typing import Optional, Tuple

# First Party
import alog

# Local
from .... import Session, assert_cluster, assert_precondition
from ....exceptions import ConfigError
from ...utils import common
from ..connection_base import DatastoreConnectionBase

## RedisConnection ###########################################################

log = alog.use_channel("RDS")


class RedisConnection(DatastoreConnectionBase):
    """
    A RedisConnection holds the core connection information for a named Redis
    instance, regardless of what IRedisComponent implements it. The key pieces
    of information are:

    * General config
        * hostname: The hostname where the instance can be reached
        * port: The port where the instance is listening

    * Auth
        * auth_secret_name: The in-cluster name for the secret holding the
            username and password
        * auth_secret_username_field: The field within the auth secret that
            holds the username.
        * auth_secret_password_field: The field within the auth secret that
            holds the password

    * TLS:
        * tls_cert: The content of the TLS cert if tls is enabled
        * tls_secret_name: The in-cluster name for the secret holding the TLS
            creds if tls is enabled
        * tls_secret_cert_field: The field within the tls secret that holds the
            cert
    """

    def __init__(
        self,
        session: Session,
        hostname: str,
        port: int,
        auth_secret_name: str,
        auth_secret_password_field: str,
        auth_secret_username_field: str,
        tls_secret_name: Optional[str] = None,
        tls_secret_cert_field: Optional[str] = None,
        auth_username: Optional[str] = None,
        auth_password: Optional[str] = None,
        tls_cert: Optional[str] = None,
    ):
        super().__init__(session)

        # These fields must be passed in directly
        self._hostname = hostname
        self._port = port
        self._auth_secret_name = auth_secret_name
        self._auth_secret_username_field = auth_secret_username_field
        self._auth_secret_password_field = auth_secret_password_field
        self._tls_secret_name = tls_secret_name
        self._tls_secret_cert_field = tls_secret_cert_field

        # The secret content may be populated or not, depending on whether this
        # Connection is being created by the component or a CR config. If not
        # populated now, they will be lazily populated on client request.
        self._auth_username = auth_username
        self._auth_password = auth_password
        self._tls_cert = tls_cert

        # Ensure that the TLS arguments are provided in a reasonable way. The
        # cert may be omitted
        tls_args = {tls_secret_name, tls_secret_cert_field}
        assert (
            tls_args == {None} or None not in tls_args
        ), "All TLS arguments must be provided if tls is enabled"
        self._tls_enabled = tls_args != {None}
        assert (
            self._tls_enabled or self._tls_cert is None
        ), "Cannot give a tls cert value when tls is disabled"

        # Schema is deduced based on the presence of the tls arguments
        self._schema = "redis" if tls_secret_name is None else "rediss"

    ## Properties ##############################################################

    @property
    def schema(self) -> str:
        """The schema (redis or rediss)"""
        return self._schema

    @property
    def hostname(self) -> str:
        """The hostname (without schema)"""
        return self._hostname

    @property
    def port(self) -> int:
        """The numeric port"""
        return self._port

    @property
    def auth_secret_name(self) -> str:
        """Secret name containing the username_key and password_key"""
        return self._auth_secret_name

    @property
    def auth_secret_username_field(self) -> str:
        """Field in the auth secret containing the username"""
        return self._auth_secret_username_field

    @property
    def auth_secret_password_field(self) -> str:
        """Field in the auth secret containing the password"""
        return self._auth_secret_password_field

    @property
    def tls_secret_name(self) -> str:
        """The name of the secret holding the tls certificate (for mounting)"""
        return self._tls_secret_name

    @property
    def tls_secret_cert_field(self) -> str:
        """The field within the tls secret that holds the CA cert"""
        return self._tls_secret_cert_field

    @property
    def tls_enabled(self) -> bool:
        return self._tls_enabled

    ## Interface ###############################################################

    _DICT_FIELDS = [
        "hostname",
        "port",
        "auth_secret_name",
        "auth_secret_password_field",
        "auth_secret_username_field",
        "tls_secret_name",
        "tls_secret_cert_field",
    ]

    def to_dict(self) -> dict:
        """Return the dict representation of the object for the CR"""
        return {field: getattr(self, f"_{field}") for field in self._DICT_FIELDS}

    @classmethod
    def from_dict(cls, session: Session, config_dict: dict) -> "RedisConnection":
        kwargs = {"session": session}
        config_dict = common.camelcase_to_snake_case(config_dict)
        uri_secret = config_dict.get("uri_secret")
        uri_hostname_field = config_dict.get("uri_secret_hostname_field")
        uri_port_field = config_dict.get("uri_secret_port_field")

        # First pull provided hostname/port secret if available and fill in
        # hostname/port fields into config_dict
        if uri_secret and uri_hostname_field and uri_port_field:
            # If we have provided host/port credentials, we need to extract them
            # and place these values in our config dict
            success, secret_content = session.get_object_current_state(
                "Secret", uri_secret
            )
            assert_cluster(success, f"Fetching connection secret [{uri_secret}] failed")
            assert "data" in secret_content, "Got a secret without 'data'?"
            secret_content = secret_content.get("data")
            assert_precondition(
                secret_content,
                f"Missing expected Secret/{uri_secret} holding [hostname] and [port]",
            )
            hostname = common.b64_secret_decode(secret_content.get(uri_hostname_field))
            port = common.b64_secret_decode(secret_content.get(uri_port_field))
            if None in [hostname, port]:
                log.debug2(
                    "Failed to find hostname/port in uri secret [%s]",
                    uri_secret,
                )

            try:
                port = int(port)
            except ValueError as err:
                raise ConfigError(f"Invalid non-int port: {port}") from err
            config_dict["hostname"], config_dict["port"] = hostname, port

        for field in cls._DICT_FIELDS:
            if field not in config_dict:
                raise ValueError(f"Missing required connection element [{field}]")

            # Set the kwargs (using None in place of empty strings)
            kwargs[field] = config_dict[field] or None

        return cls(**kwargs)

    ## Client Utilities ########################################################

    def get_auth_username_password(self) -> Tuple[Optional[str], Optional[str]]:
        """Get the current username_key/password_key pair from the auth secret if
        available

        Returns:
            username:  str or None
                The plain-text username (not encoded) if available
            password:  str or None
                The plain-text password (not encoded) if available
        """
        if None in [self._auth_username, self._auth_password]:
            secret_content = self._fetch_secret_data(self._auth_secret_name) or {}
            log.debug4("Auth secret content: %s", secret_content)
            log.debug3(
                "Looking for [%s/%s]",
                self._auth_secret_username_field,
                self._auth_secret_password_field,
            )
            username = secret_content.get(self._auth_secret_username_field)
            password = secret_content.get(self._auth_secret_password_field)
            # username not required as expect username only used when using ACL
            # redis-cli also does support username in URI to be used for that
            # CITE: https://redis.io/commands/auth
            if None in [username, password]:
                log.debug2(
                    "Failed to find username/password in auth secret [%s]",
                    self._auth_secret_name,
                )
                return None, None
            self._auth_username = username
            self._auth_password = password
        return self._auth_username, self._auth_password

    def get_tls_cert(self) -> Optional[str]:
        """Get the current TLS certificate for a client connection if TLS is
        enabled

        If TLS is enabled, but the cert is not found, this function will raise
        an AssertionError

        Returns:
            tls_cert: str or None
                PEM encoded cert string (not base64-encoded) if found, otherwise
                None
        """
        if self._tls_enabled:
            if self._tls_cert is None:
                secret_data = self._fetch_secret_data(self._tls_secret_name)
                if secret_data is not None:
                    self._tls_cert = secret_data.get(self._tls_secret_cert_field)
            return self._tls_cert

        return None

    def get_connection_string(self) -> str:
        """Get the formatted Redis connection string to connect to the instance

        Returns:
            connection_string:  str
                The formatted connection string
        """
        username_key, password_key = self.get_auth_username_password()
        assert_precondition(
            None not in [username_key, password_key],
            "No auth keys available for Redis connection string",
        )
        # NOTE: username/password required and needs to change if ever need to
        # support the rediss://<host>:<port> format without username/password
        # CITE: https://redis.io/topics/rediscli
        return "{}://{}:{}@{}:{}".format(
            self._schema, username_key, password_key, self._hostname, self._port
        )
