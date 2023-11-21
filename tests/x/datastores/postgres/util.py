"""
Sharted utils for testing postgres
"""

# First Party
import aconfig

# Local
from oper8.test_helpers.helpers import TEST_INSTANCE_NAME
from oper8.test_helpers.oper8x_helpers import set_secret_data
from oper8.x.datastores.postgres.factory import PostgresFactory
from oper8.x.utils import common, constants

## Globals #####################################################################

SECRET_NAME_UNSCOPED_AUTH = "postgres-admin-auth"
SECRET_NAME_UNSCOPED_TLS = "postgres-ca"
# Used for testing/dry run purposes
SECRET_NAME_UNSCOPED_URI = "postgres-uri"


class AuthSecretKeys:
    USING_SECRET = "USING_SECRET"
    USERNAME = "username"
    PASSWORD = "password"
    PGPASS = "pgpass"
    PG_REPLICATION_USER = "PG_REPLICATION_USER"
    PG_REPLICATION_PASSWORD = "PG_REPLICATION_PASSWORD"
    ALL_KEYS = [
        USERNAME,
        PASSWORD,
        PGPASS,
    ]


class TlsSecretKeys:
    CERT = "ca.crt"
    KEY = "ca.key"
    CA_CERT = "ca.crt"
    ALL_KEYS = [
        CERT,
        KEY,
        CA_CERT,
    ]


class UriSecretKeys:
    HOSTNAME = "hostname"
    PORT = "port"


TEST_POSTGRES_AUTH = {
    AuthSecretKeys.USING_SECRET: common.b64_secret("fake-secret"),
    AuthSecretKeys.USERNAME: common.b64_secret("fake-user"),
    AuthSecretKeys.PASSWORD: common.b64_secret("fake-password"),
    AuthSecretKeys.PGPASS: common.b64_secret("fake-pgpass"),
    AuthSecretKeys.PG_REPLICATION_USER: common.b64_secret("fake-replication-user"),
    AuthSecretKeys.PG_REPLICATION_PASSWORD: common.b64_secret(
        "fake-replication-password"
    ),
}

TEST_POSTGRES_TLS = {
    TlsSecretKeys.CERT: common.b64_secret("fake-tls-cert"),
    TlsSecretKeys.CA_CERT: common.b64_secret("fake-tls-ca-cert"),
    TlsSecretKeys.KEY: common.b64_secret("fake-tls-key"),
}


TEST_POSTGRES_URI = {
    UriSecretKeys.HOSTNAME: common.b64_secret("fake-uri-hostname"),
    UriSecretKeys.PORT: common.b64_secret("51423"),
}

POSTGRES_PROVIDED_CONNECTION_DEPLOY_CONFIG_OVERRIDES = {
    constants.SPEC_DATASTORES: {
        PostgresFactory.DATASTORE_TYPE: {
            constants.SPEC_DATASTORE_CONNECTION: {
                "uri_secret": f"{TEST_INSTANCE_NAME}-{SECRET_NAME_UNSCOPED_URI}",
                "uri_secret_hostname_field": UriSecretKeys.HOSTNAME,
                "uri_secret_port_field": UriSecretKeys.PORT,
                "auth_secret_name": f"{TEST_INSTANCE_NAME}-{SECRET_NAME_UNSCOPED_AUTH}",
                "auth_secret_username_field": AuthSecretKeys.USERNAME,
                "auth_secret_password_field": AuthSecretKeys.PASSWORD,
                "tls_secret_name": f"{TEST_INSTANCE_NAME}-{SECRET_NAME_UNSCOPED_TLS}",
                "tls_secret_cert_field": TlsSecretKeys.CERT,
            }
        }
    }
}


def set_postgres_secrets(session):
    set_postgres_auth_secret(session)
    set_postgres_tls_secret(session)
    set_postgres_uri_secret(session)


def set_postgres_auth_secret(session, override=None):
    secret_data = override or TEST_POSTGRES_AUTH
    set_secret_data(
        session,
        SECRET_NAME_UNSCOPED_AUTH,
        secret_data,
    )


def set_postgres_tls_secret(session, override=None):
    secret_data = override or TEST_POSTGRES_TLS
    set_secret_data(
        session,
        SECRET_NAME_UNSCOPED_TLS,
        data=secret_data,
    )


def set_postgres_uri_secret(session, override=None):
    secret_data = override or TEST_POSTGRES_URI
    set_secret_data(session, SECRET_NAME_UNSCOPED_URI, secret_data)


def get_spec_overrides():
    return {
        constants.SPEC_DATASTORES: {"postgres": {"storageClassName": "test-storage"}}
    }
