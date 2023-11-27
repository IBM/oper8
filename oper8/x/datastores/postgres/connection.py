"""
The common Connection type for a postgres instance
"""

# Standard
from typing import List, Optional, Tuple

# First Party
import alog

# Local
from .... import Session, assert_cluster, assert_config, assert_precondition
from ...utils import common
from .. import DatastoreConnectionBase

DEFAULT_TLS_VOLUME_NAME = "postgres-tls"
DEFAULT_TLS_VOLUME_MOUNT_PATH = "/tls"


log = alog.use_channel("PGCON")


class PostgresConnection(DatastoreConnectionBase):
    """A connection for postgres defines the client operations and utilities
    needed to configure a microservice to interact with a single postgres
    instance. The key pieces of information are:

    * General config:
        * hostname: The hostname where the database can be reached
        * port: The port the database service is listening on

    * Auth:
        * auth_secret_name: The in-cluster name for the secret holding the
            username and password
        * auth_secret_username_field: The field within the auth secret that
            holds the username
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
        auth_secret_username_field: str,
        auth_secret_password_field: str,
        tls_secret_name: Optional[str] = None,
        tls_secret_cert_field: Optional[str] = None,
        auth_username: Optional[str] = None,
        auth_password: Optional[str] = None,
        tls_cert: Optional[str] = None,
    ):
        """Construct with all of the crucial information pieces"""
        super().__init__(session)

        # Save internal values
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

    ## Properties ##############################################################

    @property
    def hostname(self) -> str:
        return self._hostname

    @property
    def port(self) -> int:
        return self._port

    @property
    def auth_secret_name(self) -> str:
        return self._auth_secret_name

    @property
    def auth_secret_username_field(self) -> str:
        return self._auth_secret_username_field

    @property
    def auth_secret_password_field(self) -> str:
        return self._auth_secret_password_field

    @property
    def tls_enabled(self) -> bool:
        return self._tls_enabled

    @property
    def tls_secret_name(self) -> str:
        return self._tls_secret_name

    @property
    def tls_secret_cert_field(self) -> str:
        return self._tls_secret_cert_field

    ## Interface ###############################################################

    _DICT_FIELDS = [
        "hostname",
        "port",
        "auth_secret_name",
        "auth_secret_username_field",
        "auth_secret_password_field",
        "tls_secret_name",
        "tls_secret_cert_field",
    ]

    _PROVIDED_DICT_FIELDS = [
        "uri_secret",
        "uri_secret_hostname_field",
        "uri_secret_port_field",
    ]

    def to_dict(self) -> dict:
        """Return the dict representation of the object for the CR"""
        return {field: getattr(self, f"_{field}") for field in self._DICT_FIELDS}

    @classmethod
    def from_dict(cls, session: Session, config_dict: dict) -> "PostgresConnection":
        kwargs = {"session": session}
        config_dict = common.camelcase_to_snake_case(config_dict)
        uri_secret = config_dict.get("uri_secret", {})
        uri_hostname_field = config_dict.get("uri_secret_hostname_field", {})
        uri_port_field = config_dict.get("uri_secret_port_field", {})

        # First pull provided hostname/port secret if available and fill in
        # hostname/port fields into config_dict
        if uri_secret and uri_hostname_field and uri_port_field:
            # If we have provided host/port credentials, we need to extract them
            # and place these values in our config dict
            success, secret_content = session.get_object_current_state(
                "Secret", uri_secret
            )
            assert_cluster(success, f"Fetching connection secret [{uri_secret}] failed")
            assert_precondition(
                secret_content,
                f"Missing expected Secret/{uri_secret} holding [hostname] and [port]",
            )
            assert "data" in secret_content, "Got a secret without 'data'?"
            secret_content = secret_content.get("data", {})
            hostname_raw = secret_content.get(uri_hostname_field)
            port_raw = secret_content.get(uri_port_field)
            assert_config(
                None not in [hostname_raw, port_raw],
                f"Failed to find hostname/port in uri secret [{uri_secret}]",
            )
            hostname = common.b64_secret_decode(hostname_raw)
            port = common.b64_secret_decode(port_raw)

            config_dict["hostname"], config_dict["port"] = hostname, int(port)

        for field in cls._DICT_FIELDS:
            if field not in config_dict:
                raise ValueError(f"Missing required connection element [{field}]")

            # Set the kwargs (using None in place of empty strings)
            kwargs[field] = config_dict[field] or None
        return cls(**kwargs)

    ## Client Utilities ########################################################

    def get_ssl_mode(self) -> str:
        """Get Postgres SSL mode to operate in

        Returns:
            ssl_mode: str
                "require" (tls enabled) or "disable" (tls disabled)
        """
        return "require" if self.tls_enabled else "disable"

    def get_auth_username_password(self) -> Tuple[str, str]:
        """Get the current username/password pair from the auth secret if
        available

        Returns:
            username: str or None
                The plain-text username for the instance or None if not
                available
            password: str or None
                The plain-text password for the instance or None if not
                available
        """
        # If not already known, fetch from the cluster
        if None in [self._auth_username, self._auth_password]:
            secret_content = self._fetch_secret_data(self._auth_secret_name) or {}
            username = secret_content.get(self._auth_secret_username_field)
            password = secret_content.get(self._auth_secret_password_field)
            if None in [username, password]:
                log.debug2(
                    "Failed to find username/password in auth secret [%s]",
                    self._auth_secret_name,
                )
                return None, None
            self._auth_username = username
            self._auth_password = password
        return self._auth_username, self._auth_password

    def get_tls_secret_volume_mounts(
        self,
        mount_path: str = DEFAULT_TLS_VOLUME_MOUNT_PATH,
        volume_name: str = DEFAULT_TLS_VOLUME_NAME,
    ) -> List[dict]:
        """Get the list of volumeMounts entries needed to support TLS for a
        client. If TLS is not enabled, this will be an empty list.

        Args:
            mount_path: str
                A path where the tls entries should be mounted
            volume_name: str
                The name of the volume within the pod spec

        Returns:
            volume_mounts: List[dict]
                A list of dict entries for the volume mounts which can be used
                to extend other volume lists
        """
        if self._tls_enabled:
            return [dict(name=volume_name, mountPath=mount_path)]
        return []

    def get_tls_secret_volumes(
        self,
        cert_mount_path: Optional[str] = None,
        volume_name: str = DEFAULT_TLS_VOLUME_NAME,
    ) -> List[dict]:
        """Get the list of dict entries needed to support TLS for a
        client. If TLS is not enabled, this will be an empty list.

        Args:
            cert_mount_path: Optional[str]
                The name of the file that the ca cert should be mounted to
            volume_name: str
                The name of the volume within the pod spec

        Returns:
            volumes: List[dict]
                A list of dict Volume entries which can be used to extend other
                volume lists
        """
        if self._tls_enabled:
            cert_mount_path = cert_mount_path or self._tls_secret_cert_field
            return [
                dict(
                    name=volume_name,
                    secret=dict(
                        defaultMode=common.mount_mode(440),
                        secretName=self._tls_secret_name,
                        items=[
                            dict(key=self._tls_secret_cert_field, path=cert_mount_path)
                        ],
                    ),
                )
            ]
        return []

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
                secret_data = self._fetch_secret_data(self._tls_secret_name) or {}
                self._tls_cert = secret_data.get(self._tls_secret_cert_field)
                assert_precondition(
                    self._tls_cert is not None, "Failed to find TLS cert"
                )
            return self._tls_cert

        return None
