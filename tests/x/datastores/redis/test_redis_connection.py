"""
Tests for the RedisConnection
"""
# Standard
import copy

# Third Party
import pytest

# Local
from oper8.exceptions import ConfigError, PreconditionError
from oper8.test_helpers.helpers import setup_session
from oper8.test_helpers.oper8x_helpers import set_secret_data
from oper8.x.datastores.redis.connection import RedisConnection
from oper8.x.datastores.redis.factory import RedisFactory
from oper8.x.utils import common, constants

## Helpers #####################################################################

sample_base_kwargs = {
    "hostname": "foo.bar.com",
    "port": 1234,
}

sample_auth_kwargs = {
    "auth_secret_name": "test-auth-secret",
    "auth_secret_username_field": "username",
    "auth_secret_password_field": "password",
}

sample_tls_kwargs = {
    "tls_secret_name": "test-tls-secret",
    "tls_secret_cert_field": "ca.pem",
}

sample_uri_kwargs = {
    "uri_secret_name": "test-uri-secret",
    "uri_secret_hostname_field": "hostname",
    "uri_secret_port_field": "port",
}


REDIS_PROVIDED_CONNECTION_DEPLOY_CONFIG_OVERRIDES = {
    constants.SPEC_DATASTORES: {
        RedisFactory.DATASTORE_TYPE: {
            constants.SPEC_DATASTORE_CONNECTION: {
                "uri_secret": sample_uri_kwargs["uri_secret_name"],
                "uri_secret_hostname_field": sample_uri_kwargs[
                    "uri_secret_hostname_field"
                ],
                "uri_secret_port_field": sample_uri_kwargs["uri_secret_port_field"],
                "auth_secret_name": sample_auth_kwargs["auth_secret_name"],
                "auth_secret_username_field": sample_auth_kwargs[
                    "auth_secret_username_field"
                ],
                "auth_secret_password_field": sample_auth_kwargs[
                    "auth_secret_password_field"
                ],
                "tls_secret_name": sample_tls_kwargs["tls_secret_name"],
                "tls_secret_cert_field": sample_tls_kwargs["tls_secret_cert_field"],
            },
            "secondInstance": {
                constants.SPEC_DATASTORE_CONNECTION: {
                    "uri_secret": "second-instance-secret",
                    "uri_secret_hostname_field": "hn",
                    "uri_secret_port_field": "port",
                    "auth_secret_name": "second-instance-secret",
                    "auth_secret_username_field": "un",
                    "auth_secret_password_field": "pw",
                    "tls_secret_name": "second-instance-secret",
                    "tls_secret_cert_field": "crt",
                },
            },
        }
    }
}

sample_auth_secret_kwargs = {
    "auth_username": "itsmeamario",
    "auth_password": "IAmVerySecret",
}

sample_tls_secret_kwargs = {
    "tls_cert": "No cert to be found",
}

sample_uri_secret_kwargs = {
    "hostname": "fake-hostname",
    "port": str(141414),
}

auth_secret_data = {
    sample_auth_kwargs["auth_secret_username_field"]: common.b64_secret(
        sample_auth_secret_kwargs["auth_username"]
    ),
    sample_auth_kwargs["auth_secret_password_field"]: common.b64_secret(
        sample_auth_secret_kwargs["auth_password"]
    ),
}

tls_secret_data = {
    sample_tls_kwargs["tls_secret_cert_field"]: common.b64_secret(
        sample_tls_secret_kwargs["tls_cert"]
    ),
}

uri_secret_data = {
    sample_uri_kwargs["uri_secret_hostname_field"]: common.b64_secret(
        sample_uri_secret_kwargs["hostname"]
    ),
    sample_uri_kwargs["uri_secret_port_field"]: common.b64_secret(
        sample_uri_secret_kwargs["port"]
    ),
}

second_instance_secret_data = {
    "hn": "foo.bar",
    "port": "1234",
    "un": "me",
    "pw": "shhhh",
    "crt": "---BEGIN CERTIFICATE OR SOMETHING---",
}


def set_auth_secret_data(session):
    set_secret_data(
        session=session,
        scoped_name=False,
        name=sample_auth_kwargs["auth_secret_name"],
        data=auth_secret_data,
    )


def set_tls_secret_data(session):
    set_secret_data(
        session=session,
        scoped_name=False,
        name=sample_tls_kwargs["tls_secret_name"],
        data=tls_secret_data,
    )


def set_uri_secret_data(session):
    set_secret_data(
        session=session,
        scoped_name=False,
        name=sample_uri_kwargs["uri_secret_name"],
        data=uri_secret_data,
    )


def set_redis_secrets(session):
    set_auth_secret_data(session)
    set_tls_secret_data(session)
    set_uri_secret_data(session)

    # Quick-n-dirty for the second secret
    set_secret_data(
        session=session,
        scoped_name=False,
        name="second-instance-secret",
        data={k: common.b64_secret(v) for k, v in second_instance_secret_data.items()},
    )


@pytest.fixture
def redis_session():
    override_deploy_configs = REDIS_PROVIDED_CONNECTION_DEPLOY_CONFIG_OVERRIDES
    session = setup_session(deploy_config=override_deploy_configs)
    set_redis_secrets(session)
    yield session


## Tests #######################################################################


def test_get_component_with_provided_connection(redis_session):
    """Test that a provided connection passed in through the CR returns None on
    a get_component call
    """
    component = RedisFactory.get_component(redis_session)
    assert component is None


def test_get_connection_with_provided_connection(redis_session):
    """Test that a provided connection passed in through the CR returns valid
    connection details on get_connection call
    """
    connection = RedisFactory.get_connection(redis_session)
    assert isinstance(connection, RedisConnection)
    assert connection.hostname == sample_uri_secret_kwargs["hostname"]
    assert connection.port == int(sample_uri_secret_kwargs["port"])


def test_get_connection_with_provided_named_second_connection(redis_session):
    """Test that a provided second connection passed in through the CR returns
    valid connection details on get_connection call
    """
    connection = RedisFactory.get_connection(redis_session, "secondInstance")
    assert isinstance(connection, RedisConnection)
    assert connection.hostname == second_instance_secret_data["hn"]
    assert connection.port == int(second_instance_secret_data["port"])


def test_get_connection_string_missing_secrets():
    """Make sure that trying to get the connection string when the secrets are
    not available raises a PreconditionError as Redis needs username / password
    """
    session = setup_session(
        deploy_config=REDIS_PROVIDED_CONNECTION_DEPLOY_CONFIG_OVERRIDES
    )
    conn = RedisConnection(
        session,
        **sample_base_kwargs,
        **sample_auth_kwargs,
        **sample_tls_kwargs,
    )
    with pytest.raises(PreconditionError):
        conn.get_connection_string()


def test_properties(redis_session):
    """Test that all the properties return correctly"""
    connection = RedisFactory.get_connection(redis_session)
    assert isinstance(connection, RedisConnection)
    assert connection.schema == "rediss"
    # URI
    assert connection.hostname == sample_uri_secret_kwargs["hostname"]
    assert connection.port == int(sample_uri_secret_kwargs["port"])
    # Auth
    assert connection.auth_secret_name == sample_auth_kwargs["auth_secret_name"]
    assert (
        connection.auth_secret_username_field
        == sample_auth_kwargs["auth_secret_username_field"]
    )
    assert (
        connection.auth_secret_password_field
        == sample_auth_kwargs["auth_secret_password_field"]
    )
    # TLS
    assert connection.tls_secret_name == sample_tls_kwargs["tls_secret_name"]
    assert (
        connection.tls_secret_cert_field == sample_tls_kwargs["tls_secret_cert_field"]
    )
    assert connection.tls_enabled


def test_to_dict(redis_session):
    """Test that serializing to a dict works as expected"""
    connection = RedisFactory.get_connection(redis_session)
    conn_dict = connection.to_dict()
    assert conn_dict == {
        "hostname": connection.hostname,
        "port": connection.port,
        "auth_secret_name": connection.auth_secret_name,
        "auth_secret_password_field": connection.auth_secret_password_field,
        "auth_secret_username_field": connection.auth_secret_username_field,
        "tls_secret_name": connection.tls_secret_name,
        "tls_secret_cert_field": connection.tls_secret_cert_field,
    }


def test_get_auth_username_password(redis_session):
    """Make sure get_auth_username_password correctly looks up the contents in
    the cluster
    """
    connection = RedisFactory.get_connection(redis_session)
    un, pw = connection.get_auth_username_password()
    assert un == common.b64_secret_decode(
        auth_secret_data[connection.auth_secret_username_field]
    )
    assert pw == common.b64_secret_decode(
        auth_secret_data[connection.auth_secret_password_field]
    )


def test_get_tls_cert(redis_session):
    """Make sure get_tls_cert correctly looks up the contents in the cluster"""
    connection = RedisFactory.get_connection(redis_session)
    crt = connection.get_tls_cert()
    assert crt == common.b64_secret_decode(
        tls_secret_data[connection.tls_secret_cert_field]
    )


def test_get_connection_string(redis_session):
    """Make sure get_connection_string correctly formats everything into a
    single connection string
    """
    connection = RedisFactory.get_connection(redis_session)
    conn_str = connection.get_connection_string()
    un, pw = connection.get_auth_username_password()
    assert conn_str == "{}://{}:{}@{}:{}".format(
        connection.schema,
        un,
        pw,
        connection.hostname,
        connection.port,
    )


def test_get_tls_disabled():
    """Make sure that the connection handles being set up without TLS"""
    session = setup_session(
        deploy_config=REDIS_PROVIDED_CONNECTION_DEPLOY_CONFIG_OVERRIDES
    )
    set_redis_secrets(session)
    conn = RedisConnection(
        session,
        **sample_base_kwargs,
        **sample_auth_kwargs,
    )
    assert not conn.tls_enabled
    assert not conn.get_tls_cert()


def test_non_int_port(redis_session):
    """Make sure a non-int port raises a ConfigError"""
    bad_port_secret = copy.deepcopy(uri_secret_data)
    bad_port_secret[sample_uri_kwargs["uri_secret_port_field"]] = common.b64_secret(
        "not an int"
    )
    set_secret_data(
        session=redis_session,
        scoped_name=False,
        name=sample_uri_kwargs["uri_secret_name"],
        data=bad_port_secret,
    )
    with pytest.raises(ConfigError):
        RedisFactory.get_connection(redis_session)
