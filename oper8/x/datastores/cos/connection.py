"""
The common Connection type for a COS instance
"""

# Standard
from typing import Optional, Tuple

# First Party
import alog

# Local
from .... import Session, assert_cluster, assert_precondition
from ...utils import common
from .. import DatastoreConnectionBase

## CosConnection ###############################################################

log = alog.use_channel("COSCON")


class CosConnection(DatastoreConnectionBase):
    """
    A CosConnection holds the core connection information for a named COS
    instance, regardless of what ICosComponent implements it. The key pieces of
    information are:

    * General config
        * hostname: The hostname where the instance can be reached
        * port: The port where the instance is listening
        * bucket_name: The name of the bucket within the instance

    * Auth
        * auth_secret_name: The in-cluster name for the secret holding the
            access_key and secret_key
        * auth_secret_access_key_field: The field within the auth secret that
            holds the access_key
        * auth_secret_secret_key_field: The field within the auth secret that
            holds the secret_key

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
        bucket_name: str,
        auth_secret_name: str,
        auth_secret_access_key_field: str,
        auth_secret_secret_key_field: str,
        tls_secret_name: Optional[str] = None,
        tls_secret_cert_field: Optional[str] = None,
        access_key: Optional[str] = None,
        secret_key: Optional[str] = None,
        tls_cert: Optional[str] = None,
    ):
        super().__init__(session)

        # These fields must be passed in directly
        self._hostname = hostname
        self._port = port
        self._bucket_name = bucket_name
        self._auth_secret_name = auth_secret_name
        self._auth_secret_access_key_field = auth_secret_access_key_field
        self._auth_secret_secret_key_field = auth_secret_secret_key_field
        self._tls_secret_name = tls_secret_name
        self._tls_secret_cert_field = tls_secret_cert_field

        # The secret content may be populated or not, depending on whether this
        # Connection is being created by the component or a CR config. If not
        # populated now, they will be lazily populated on client request.
        self._access_key = access_key
        self._secret_key = secret_key
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
        self._schema = "http" if tls_secret_name is None else "https"

    ## Properties ##############################################################

    @property
    def schema(self) -> str:
        """The schema (http or https)"""
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
    def endpoint(self) -> int:
        """The fully constructed endpoint for the COS instance"""
        return f"{self._schema}://{self._hostname}:{self._port}"

    @property
    def bucket_name(self) -> int:
        """The numeric bucket_name"""
        return self._bucket_name

    @property
    def auth_secret_name(self) -> str:
        """Secret name containing the access_key and secret_key"""
        return self._auth_secret_name

    @property
    def auth_secret_access_key_field(self) -> str:
        """Field in the auth secret containing the access_key"""
        return self._auth_secret_access_key_field

    @property
    def auth_secret_secret_key_field(self) -> str:
        """Field in the auth secret containing the secret_key"""
        return self._auth_secret_secret_key_field

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
        "bucket_name",
        "auth_secret_name",
        "auth_secret_access_key_field",
        "auth_secret_secret_key_field",
        "tls_secret_name",
        "tls_secret_cert_field",
    ]

    def to_dict(self) -> dict:
        """Return the dict representation of the object for the CR"""
        return {field: getattr(self, f"_{field}") for field in self._DICT_FIELDS}

    @classmethod
    def from_dict(cls, session: Session, config_dict: dict) -> "CosConnection":
        kwargs = {"session": session}
        config_dict = common.camelcase_to_snake_case(config_dict)

        uri_secret = config_dict.get("uri_secret")
        uri_hostname_field = config_dict.get("uri_secret_hostname_field")
        uri_port_field = config_dict.get("uri_secret_port_field")
        uri_bucketname_field = config_dict.get("uri_secret_bucketname_field")

        # First pull provided hostname/port secret if available and fill in
        # hostname/port fields into config_dict
        if (
            uri_secret
            and uri_hostname_field
            and uri_port_field
            and uri_bucketname_field
        ):
            # If we have provided host/port credentials, we need to extract them
            # and place these values in our config dict
            success, secret_content = session.get_object_current_state(
                "Secret", uri_secret
            )
            assert_cluster(success, f"Fetching connection secret [{uri_secret}] failed")
            assert "data" in secret_content, "Got a secret without 'data'?"
            secret_content = secret_content.get("data", {})
            assert_precondition(
                secret_content,
                f"Missing expected Secret/{uri_secret} holding [hostname] and [port]",
            )
            hostname = common.b64_secret_decode(secret_content.get(uri_hostname_field))
            port = common.b64_secret_decode(secret_content.get(uri_port_field))
            bucketname = common.b64_secret_decode(
                secret_content.get(uri_bucketname_field)
            )
            if None in [hostname, port, bucketname]:
                log.debug2(
                    "Failed to find hostname/port/bucketname in uri secret [%s]",
                    uri_secret,
                )

            config_dict["hostname"], config_dict["port"], config_dict["bucket_name"] = (
                hostname,
                int(port),
                bucketname,
            )

        for field in cls._DICT_FIELDS:
            if field not in config_dict:
                raise ValueError(f"Missing required connection element [{field}]")

            # Set the kwargs (using None in place of empty strings)
            kwargs[field] = config_dict[field] or None
        return cls(**kwargs)

    ## Client Utilities ########################################################

    def get_auth_keys(self) -> Tuple[Optional[str], Optional[str]]:
        """Get the current access_key/secret_key pair from the auth secret if
        available

        Returns:
            access_key:  str or None
                The plain-text access_key (not encoded) if available
            secret_key:  str or None
                The plain-text secret_key (not encoded) if available
        """
        if None in [self._access_key, self._secret_key]:
            secret_content = self._fetch_secret_data(self._auth_secret_name) or {}
            log.debug4("Auth secret content: %s", secret_content)
            log.debug3(
                "Looking for [%s/%s]",
                self._auth_secret_access_key_field,
                self._auth_secret_secret_key_field,
            )
            access_key = secret_content.get(self._auth_secret_access_key_field)
            secret_key = secret_content.get(self._auth_secret_secret_key_field)
            if None in [access_key, secret_key]:
                log.debug2(
                    "Failed to find access_key/secret_key in auth secret [%s]",
                    self._auth_secret_name,
                )
                return None, None
            self._access_key = access_key
            self._secret_key = secret_key
        return self._access_key, self._secret_key

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
        """Get the formatted s3 connection string to connect to the given bucket
        in the instance

        Returns:
            connection_string:  str
                The formatted connection string
        """
        access_key, secret_key = self.get_auth_keys()
        assert_precondition(
            None not in [access_key, secret_key],
            "No auth keys available for COS connection string",
        )
        return (
            "s3,endpoint={}://{}:{},accesskey={},secretkey={},bucketsuffix={}".format(
                self._schema,
                self._hostname,
                self._port,
                access_key,
                secret_key,
                self._bucket_name,
            )
        )
