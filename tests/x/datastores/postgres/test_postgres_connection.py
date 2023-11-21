"""
Tests for the PostgresConnection
"""

# Standard
import copy

# Third Party
import pytest

# Local
from oper8.exceptions import ConfigError, PreconditionError
from oper8.test_helpers.helpers import MockDeployManager, setup_session
from oper8.test_helpers.oper8x_helpers import set_secret_data
from oper8.x.datastores.postgres import PostgresConnection, PostgresFactory
from oper8.x.utils import common, constants

## Helpers #####################################################################

sample_base_kwargs = {
    "hostname": "foo.bar.com",
    "port": 1234,
}

sample_auth_kwargs = {
    "auth_secret_name": "test-auth-secret",
    "auth_secret_username_field": "accessKey",
    "auth_secret_password_field": "secretKey",
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


def set_pg_secrets(session):
    set_auth_secret_data(session)
    set_tls_secret_data(session)
    set_uri_secret_data(session)


PG_PROVIDED_CONNECTION_DEPLOY_CONFIG_OVERRIDES = {
    constants.SPEC_DATASTORES: {
        PostgresFactory.DATASTORE_TYPE: {
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
            }
        }
    }
}


## Tests #######################################################################

#################
## Constructor ##
#################


def test_construct_no_secrets():
    """Test that constructing a connection without secrets works as expected and
    does not have the secret values pre-populated
    """
    session = setup_session()
    conn = PostgresConnection(
        session,
        **sample_base_kwargs,
        **sample_auth_kwargs,
        **sample_tls_kwargs,
    )
    for key, val in sample_base_kwargs.items():
        assert getattr(conn, key) == val
    for key, val in sample_auth_kwargs.items():
        assert getattr(conn, key) == val
    for key, val in sample_tls_kwargs.items():
        assert getattr(conn, key) == val
    assert conn._auth_username is None
    assert conn._auth_password is None
    assert conn._tls_cert is None


def test_construct_with_secrets():
    """Test that constructing a connection with secrets works as expected and
    has the secret values pre-populated
    """
    session = setup_session()
    conn = PostgresConnection(
        session,
        **sample_base_kwargs,
        **sample_auth_kwargs,
        **sample_tls_kwargs,
        **sample_auth_secret_kwargs,
        **sample_tls_secret_kwargs,
    )
    for key, val in sample_base_kwargs.items():
        assert getattr(conn, key) == val
    for key, val in sample_auth_kwargs.items():
        assert getattr(conn, key) == val
    for key, val in sample_tls_kwargs.items():
        assert getattr(conn, key) == val
    assert conn._auth_username == sample_auth_secret_kwargs["auth_username"]
    assert conn._auth_password == sample_auth_secret_kwargs["auth_password"]
    assert conn._tls_cert == sample_tls_secret_kwargs["tls_cert"]


def test_construct_no_tls():
    """Test that constructing a connection without tls values works as expected
    and correctly determines that tls is disabled
    """
    session = setup_session()
    conn = PostgresConnection(
        session,
        **sample_base_kwargs,
        **sample_auth_kwargs,
    )
    assert not conn.tls_enabled


def test_construct_with_tls():
    """Test that constructing a connection with tls values works as expected
    and correctly determines that tls is enabled
    """
    session = setup_session()
    conn = PostgresConnection(
        session,
        **sample_base_kwargs,
        **sample_auth_kwargs,
        **sample_tls_kwargs,
    )
    assert conn.tls_enabled


def test_construct_tls_args_inconsistent():
    """Test that constructing with inconsistent tls args raises an exception"""
    session = setup_session()
    with pytest.raises(AssertionError):
        PostgresConnection(
            session,
            tls_secret_name="not-going-to-give-cert-field",
            **sample_base_kwargs,
            **sample_auth_kwargs,
        )
    with pytest.raises(AssertionError):
        PostgresConnection(
            session,
            tls_secret_cert_field="not-going-to-give-secret-name",
            **sample_base_kwargs,
            **sample_auth_kwargs,
        )
    with pytest.raises(AssertionError):
        PostgresConnection(
            session,
            tls_cert="not-going-to-give-secret-info",
            **sample_base_kwargs,
            **sample_auth_kwargs,
        )


################
## Properties ##
################


def test_all_properties():
    """Test that all properties reflect the constructed values and that they are
    read-only
    """
    session = setup_session()
    conn = PostgresConnection(
        session,
        **sample_base_kwargs,
        **sample_auth_kwargs,
        **sample_tls_kwargs,
    )
    for property in [
        "hostname",
        "port",
        "auth_secret_name",
        "auth_secret_username_field",
        "auth_secret_password_field",
        "tls_secret_name",
        "tls_secret_cert_field",
        "tls_enabled",
    ]:
        assert hasattr(conn, property)
        with pytest.raises(AttributeError):
            setattr(conn, property, "foobar")


###################
## Serialization ##
###################


def test_to_dict_all_required_fields():
    """Make sure that all expected fields are present. This is a bit silly since
    it's just a copy-paste of the list of keys, but it's less likely that we
    screw that up in two places than in one!
    """
    session = setup_session()
    conn_dict = PostgresConnection(
        session,
        **sample_base_kwargs,
        **sample_auth_kwargs,
        **sample_tls_kwargs,
    ).to_dict()
    for key in [
        "hostname",
        "port",
        "auth_secret_name",
        "auth_secret_username_field",
        "auth_secret_password_field",
        "tls_secret_name",
        "tls_secret_cert_field",
    ]:
        assert key in conn_dict


def test_to_dict_snake_case_keys():
    """Make sure that the keys returned by to_dict maintain their snake casing."""
    session = setup_session()
    conn_dict = PostgresConnection(
        session,
        **sample_base_kwargs,
        **sample_auth_kwargs,
        **sample_tls_kwargs,
    ).to_dict()
    assert common.camelcase_to_snake_case(conn_dict) == conn_dict


def test_from_dict_camel_case_values_preserved():
    """Make sure that values in the config dict which are camelCase are
    preserved during from_dict
    """
    session = setup_session()
    all_kwargs = dict(
        **sample_base_kwargs,
        **sample_auth_kwargs,
        **sample_tls_kwargs,
    )
    camel_dict = common.snake_case_to_camelcase(all_kwargs)
    conn = PostgresConnection.from_dict(session, camel_dict)
    for key, val in all_kwargs.items():
        assert getattr(conn, key) == val


def test_from_dict_missing_keys():
    """Make sure that calling from_dict with missing keys raises an error"""
    session = setup_session()
    all_kwargs = dict(
        **sample_base_kwargs,
        **sample_tls_kwargs,
    )
    camel_dict = common.snake_case_to_camelcase(all_kwargs)
    with pytest.raises(ValueError):
        PostgresConnection.from_dict(session, camel_dict)


def test_from_dict_extra_keys():
    """Make sure that calling from_dict with extra keys ignores them"""
    session = setup_session()
    all_kwargs = dict(
        foo="bar",
        **sample_base_kwargs,
        **sample_auth_kwargs,
        **sample_tls_kwargs,
    )
    camel_dict = common.snake_case_to_camelcase(all_kwargs)
    PostgresConnection.from_dict(session, camel_dict)


################################
## get_auth_username_password ##
################################


def test_get_auth_username_password_secret_found():
    """Test that get_auth_username_password returns the secret values when the
    secret is found and the expected keys are present
    """
    session = setup_session(
        deploy_manager=MockDeployManager(),
    )
    set_auth_secret_data(session)
    conn = PostgresConnection(
        session,
        **sample_base_kwargs,
        **sample_auth_kwargs,
        **sample_tls_kwargs,
    )
    username, password = conn.get_auth_username_password()
    assert username == sample_auth_secret_kwargs["auth_username"]
    assert password == sample_auth_secret_kwargs["auth_password"]


def test_get_auth_username_password_missing_keys():
    """Test that get_auth_username_password returns no values if the secret is found, but the
    expected keys are not found
    """
    session = setup_session(
        deploy_manager=MockDeployManager(),
    )
    set_secret_data(
        session=session,
        scoped_name=False,
        name=sample_auth_kwargs["auth_secret_name"],
        data={
            sample_auth_kwargs["auth_secret_username_field"]: common.b64_secret(
                sample_auth_secret_kwargs["auth_username"]
            ),
        },
    )
    conn = PostgresConnection(
        session,
        **sample_base_kwargs,
        **sample_auth_kwargs,
        **sample_tls_kwargs,
    )
    username, password = conn.get_auth_username_password()
    assert username is None
    assert password is None


def test_get_auth_username_password_secret_not_found():
    """Test that get_auth_username_password returns no values if the secret is not found"""
    session = setup_session()
    conn = PostgresConnection(
        session,
        **sample_base_kwargs,
        **sample_auth_kwargs,
        **sample_tls_kwargs,
    )
    username, password = conn.get_auth_username_password()
    assert username is None
    assert password is None


def test_get_auth_username_password_prepopulated():
    """Test that get_auth_username_password doesn't look in the cluster if the values are
    given at construct time
    """
    session = setup_session()
    conn = PostgresConnection(
        session,
        **sample_base_kwargs,
        **sample_auth_kwargs,
        **sample_tls_kwargs,
        **sample_auth_secret_kwargs,
    )
    username, password = conn.get_auth_username_password()
    assert username == sample_auth_secret_kwargs["auth_username"]
    assert password == sample_auth_secret_kwargs["auth_password"]


##################
## get_tls_cert ##
##################


def test_get_tls_cert_secret_found():
    """Test that get_tls_cert returns the secret value when the secret is
    found and the expected key is present
    """
    session = setup_session(
        deploy_manager=MockDeployManager(),
    )
    set_tls_secret_data(session)
    conn = PostgresConnection(
        session,
        **sample_base_kwargs,
        **sample_auth_kwargs,
        **sample_tls_kwargs,
    )
    tls_cert = conn.get_tls_cert()
    assert tls_cert == sample_tls_secret_kwargs["tls_cert"]


def test_get_tls_cert_missing_key():
    """Test that get_tls_cert raises a precondition error if the secret is
    found, but the expected key is not found
    """
    session = setup_session(
        deploy_manager=MockDeployManager(),
    )
    set_secret_data(
        session=session,
        scoped_name=False,
        name=sample_tls_kwargs["tls_secret_name"],
        data={},
    )
    conn = PostgresConnection(
        session,
        **sample_base_kwargs,
        **sample_auth_kwargs,
        **sample_tls_kwargs,
    )
    with pytest.raises(PreconditionError):
        conn.get_tls_cert()


def test_get_tls_cert_secret_not_found():
    """Test that get_tls_cert raises a precondition error if the secret is not
    found
    """
    session = setup_session()
    conn = PostgresConnection(
        session,
        **sample_base_kwargs,
        **sample_auth_kwargs,
        **sample_tls_kwargs,
    )
    with pytest.raises(PreconditionError):
        conn.get_tls_cert()


def test_get_tls_cert_prepopulated():
    """Test that get_tls_cert doesn't look in the cluster if the value is given
    at construct time
    """
    session = setup_session()
    conn = PostgresConnection(
        session,
        **sample_base_kwargs,
        **sample_auth_kwargs,
        **sample_tls_kwargs,
        **sample_tls_secret_kwargs,
    )
    tls_cert = conn.get_tls_cert()
    assert tls_cert == sample_tls_secret_kwargs["tls_cert"]


def test_get_tls_cert_disabled():
    """Test that get_tls_cert reurns None if tls is disabled"""
    session = setup_session()
    conn = PostgresConnection(
        session,
        **sample_base_kwargs,
        **sample_auth_kwargs,
    )
    tls_cert = conn.get_tls_cert()
    assert tls_cert is None


##############
## provided ##
##############


def test_get_component_with_provided_connection():
    """Test that a provided connection passed in through the CR returns None on a get_component call"""
    override_deploy_configs = PG_PROVIDED_CONNECTION_DEPLOY_CONFIG_OVERRIDES

    session = setup_session(
        deploy_config=override_deploy_configs,
        deploy_manager=MockDeployManager(),
    )
    set_pg_secrets(session)
    component = PostgresFactory.get_component(session)
    # Since the component is provided, we should be getting None back
    assert component is None


def test_get_connection_with_provided_connection():
    """Test that a provided connection passed in through the CR returns valid connection details on get_connection call"""
    override_deploy_configs = PG_PROVIDED_CONNECTION_DEPLOY_CONFIG_OVERRIDES
    session = setup_session(
        deploy_config=override_deploy_configs,
        deploy_manager=MockDeployManager(),
    )
    set_pg_secrets(session)
    connection = PostgresFactory.get_connection(session)
    assert connection.hostname == sample_uri_secret_kwargs["hostname"]
    # Need to convert port value to int since Factory converts string input of port to type int
    assert connection.port == int(sample_uri_secret_kwargs["port"])


def test_get_connection_with_provided_connection_missing_uri_content():
    """Test that a provided connection passed in through the CR that points to a
    URI secret which is missing content raises a config error
    """
    override_deploy_configs = PG_PROVIDED_CONNECTION_DEPLOY_CONFIG_OVERRIDES
    session = setup_session(
        deploy_config=override_deploy_configs,
        deploy_manager=MockDeployManager(),
    )
    set_auth_secret_data(session)
    set_tls_secret_data(session)
    bad_uri_secret_content = copy.deepcopy(uri_secret_data)
    del bad_uri_secret_content[sample_uri_kwargs["uri_secret_port_field"]]
    set_secret_data(
        session=session,
        scoped_name=False,
        name=sample_uri_kwargs["uri_secret_name"],
        data=bad_uri_secret_content,
    )
    with pytest.raises(ConfigError):
        PostgresFactory.get_connection(session)


#######################################################
# get_tls_secret_volume_mounts/get_tls_secret_volumes #
#######################################################


def test_get_tls_secret_volume_mounts_tls_enabled():
    """Make sure that volume mounts are returned when tls is enabled"""
    session = setup_session()
    conn = PostgresConnection(
        session,
        **sample_base_kwargs,
        **sample_auth_kwargs,
        **sample_tls_kwargs,
    )
    assert conn.get_tls_secret_volume_mounts()
    assert conn.get_tls_secret_volumes()


def test_get_tls_secret_volume_mounts_tls_disabled():
    """Make sure that volume mounts are not returned when tls is disabled"""
    session = setup_session()
    conn = PostgresConnection(
        session,
        **sample_base_kwargs,
        **sample_auth_kwargs,
    )
    assert not conn.get_tls_secret_volume_mounts()
    assert not conn.get_tls_secret_volumes()
