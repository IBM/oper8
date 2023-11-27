"""
Tests for the CosConnection
"""

# Third Party
import pytest

# Local
from oper8.exceptions import PreconditionError
from oper8.test_helpers.helpers import MockDeployManager, setup_session
from oper8.test_helpers.oper8x_helpers import set_secret_data
from oper8.x.datastores.cos import CosConnection, CosFactory
from oper8.x.utils import common, constants

## Helpers #####################################################################

sample_base_kwargs = {
    "hostname": "foo.bar.com",
    "port": 1234,
    "bucket_name": "test-bucket",
}

sample_auth_kwargs = {
    "auth_secret_name": "test-auth-secret",
    "auth_secret_access_key_field": "accessKey",
    "auth_secret_secret_key_field": "secretKey",
}

sample_tls_kwargs = {
    "tls_secret_name": "test-tls-secret",
    "tls_secret_cert_field": "ca.pem",
}

sample_uri_kwargs = {
    "uri_secret_name": "test-uri-secret",
    "uri_secret_hostname_field": "hostname",
    "uri_secret_port_field": "port",
    "uri_secret_bucketname_field": "bucketname",
}

sample_auth_secret_kwargs = {
    "access_key": "accessMe",
    "secret_key": "IAmVerySecret",
}

sample_tls_secret_kwargs = {
    "tls_cert": "No cert to be found",
}

sample_uri_secret_kwargs = {
    "hostname": "fake-hostname",
    "port": "12345",
    "bucketname": "fake-bucketname",
}

auth_secret_data = {
    sample_auth_kwargs["auth_secret_access_key_field"]: common.b64_secret(
        sample_auth_secret_kwargs["access_key"]
    ),
    sample_auth_kwargs["auth_secret_secret_key_field"]: common.b64_secret(
        sample_auth_secret_kwargs["secret_key"]
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
    sample_uri_kwargs["uri_secret_bucketname_field"]: common.b64_secret(
        sample_uri_secret_kwargs["bucketname"]
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


def set_cos_secrets(session):
    set_auth_secret_data(session)
    set_tls_secret_data(session)
    set_uri_secret_data(session)


COS_PROVIDED_CONNECTION_DEPLOY_CONFIG_OVERRIDES = {
    constants.SPEC_DATASTORES: {
        CosFactory.DATASTORE_TYPE: {
            constants.SPEC_DATASTORE_CONNECTION: {
                "uri_secret": sample_uri_kwargs["uri_secret_name"],
                "uri_secret_hostname_field": sample_uri_kwargs[
                    "uri_secret_hostname_field"
                ],
                "uri_secret_port_field": sample_uri_kwargs["uri_secret_port_field"],
                "uri_secret_bucketname_field": sample_uri_kwargs[
                    "uri_secret_bucketname_field"
                ],
                "auth_secret_name": sample_auth_kwargs["auth_secret_name"],
                "auth_secret_access_key_field": sample_auth_kwargs[
                    "auth_secret_access_key_field"
                ],
                "auth_secret_secret_key_field": sample_auth_kwargs[
                    "auth_secret_secret_key_field"
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
    conn = CosConnection(
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
    assert conn._access_key is None
    assert conn._secret_key is None
    assert conn._tls_cert is None


def test_construct_with_secrets():
    """Test that constructing a connection with secrets works as expected and
    has the secret values pre-populated
    """
    session = setup_session()
    conn = CosConnection(
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
    assert conn._access_key == sample_auth_secret_kwargs["access_key"]
    assert conn._secret_key == sample_auth_secret_kwargs["secret_key"]
    assert conn._tls_cert == sample_tls_secret_kwargs["tls_cert"]


def test_construct_no_tls():
    """Test that constructing a connection without tls values works as expected
    and correctly determines that tls is disabled
    """
    session = setup_session()
    conn = CosConnection(
        session,
        **sample_base_kwargs,
        **sample_auth_kwargs,
    )
    assert not conn.tls_enabled
    assert conn.schema == "http"


def test_construct_with_tls():
    """Test that constructing a connection with tls values works as expected
    and correctly determines that tls is enabled
    """
    session = setup_session()
    conn = CosConnection(
        session,
        **sample_base_kwargs,
        **sample_auth_kwargs,
        **sample_tls_kwargs,
    )
    assert conn.tls_enabled
    assert conn.schema == "https"


def test_construct_tls_args_inconsistent():
    """Test that constructing with inconsistent tls args raises an exception"""
    session = setup_session()
    with pytest.raises(AssertionError):
        CosConnection(
            session,
            tls_secret_name="not-going-to-give-cert-field",
            **sample_base_kwargs,
            **sample_auth_kwargs,
        )
    with pytest.raises(AssertionError):
        CosConnection(
            session,
            tls_secret_cert_field="not-going-to-give-secret-name",
            **sample_base_kwargs,
            **sample_auth_kwargs,
        )
    with pytest.raises(AssertionError):
        CosConnection(
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
    conn = CosConnection(
        session,
        **sample_base_kwargs,
        **sample_auth_kwargs,
        **sample_tls_kwargs,
    )
    for property in [
        "schema",
        "hostname",
        "port",
        "endpoint",
        "bucket_name",
        "auth_secret_name",
        "auth_secret_access_key_field",
        "auth_secret_secret_key_field",
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
    conn_dict = CosConnection(
        session,
        **sample_base_kwargs,
        **sample_auth_kwargs,
        **sample_tls_kwargs,
    ).to_dict()
    for key in [
        "hostname",
        "port",
        "bucket_name",
        "auth_secret_name",
        "auth_secret_access_key_field",
        "auth_secret_secret_key_field",
        "tls_secret_name",
        "tls_secret_cert_field",
    ]:
        assert key in conn_dict


def test_to_dict_snake_case_keys():
    """Make sure that the keys returned by to_dict maintain their snake casing."""
    session = setup_session()
    conn_dict = CosConnection(
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
    conn = CosConnection.from_dict(session, camel_dict)
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
        CosConnection.from_dict(session, camel_dict)


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
    CosConnection.from_dict(session, camel_dict)


###################
## get_auth_keys ##
###################


def test_get_auth_keys_secret_found():
    """Test that get_auth_keys returns the secret values when the secret is
    found and the expected keys are present
    """
    session = setup_session(
        deploy_manager=MockDeployManager(),
    )
    set_auth_secret_data(session)
    conn = CosConnection(
        session,
        **sample_base_kwargs,
        **sample_auth_kwargs,
        **sample_tls_kwargs,
    )
    access_key, secret_key = conn.get_auth_keys()
    assert access_key == sample_auth_secret_kwargs["access_key"]
    assert secret_key == sample_auth_secret_kwargs["secret_key"]


def test_get_auth_keys_missing_keys():
    """Test that get_auth_keys returns no values if the secret is found, but the
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
            sample_auth_kwargs["auth_secret_access_key_field"]: common.b64_secret(
                sample_auth_secret_kwargs["access_key"]
            ),
        },
    )
    conn = CosConnection(
        session,
        **sample_base_kwargs,
        **sample_auth_kwargs,
        **sample_tls_kwargs,
    )
    access_key, secret_key = conn.get_auth_keys()
    assert access_key is None
    assert secret_key is None


def test_get_auth_keys_secret_not_found():
    """Test that get_auth_keys returns no values if the secret is not found"""
    session = setup_session()
    conn = CosConnection(
        session,
        **sample_base_kwargs,
        **sample_auth_kwargs,
        **sample_tls_kwargs,
    )
    access_key, secret_key = conn.get_auth_keys()
    assert access_key is None
    assert secret_key is None


def test_get_auth_keys_prepopulated():
    """Test that get_auth_keys doesn't look in the cluster if the values are
    given at construct time
    """
    session = setup_session()
    conn = CosConnection(
        session,
        **sample_base_kwargs,
        **sample_auth_kwargs,
        **sample_tls_kwargs,
        **sample_auth_secret_kwargs,
    )
    access_key, secret_key = conn.get_auth_keys()
    assert access_key == sample_auth_secret_kwargs["access_key"]
    assert secret_key == sample_auth_secret_kwargs["secret_key"]


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
    conn = CosConnection(
        session,
        **sample_base_kwargs,
        **sample_auth_kwargs,
        **sample_tls_kwargs,
    )
    tls_cert = conn.get_tls_cert()
    assert tls_cert == sample_tls_secret_kwargs["tls_cert"]


def test_get_tls_cert_missing_key():
    """Test that get_tls_cert returns no value if the secret is found, but the
    expected key is not found
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
    conn = CosConnection(
        session,
        **sample_base_kwargs,
        **sample_auth_kwargs,
        **sample_tls_kwargs,
    )
    tls_cert = conn.get_tls_cert()
    assert tls_cert is None


def test_get_tls_cert_secret_not_found():
    """Test that get_tls_cert returns no value if the secret is not found"""
    session = setup_session()
    conn = CosConnection(
        session,
        **sample_base_kwargs,
        **sample_auth_kwargs,
        **sample_tls_kwargs,
    )
    tls_cert = conn.get_tls_cert()
    assert tls_cert is None


def test_get_tls_cert_prepopulated():
    """Test that get_tls_cert doesn't look in the cluster if the value is given
    at construct time
    """
    session = setup_session()
    conn = CosConnection(
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
    conn = CosConnection(
        session,
        **sample_base_kwargs,
        **sample_auth_kwargs,
    )
    tls_cert = conn.get_tls_cert()
    assert tls_cert is None


###########################
## get_connection_string ##
###########################


def test_get_connection_string_secrets_known():
    """Make sure the formatting is right in get_connection_string. This is also
    a bit of a sily test because it's just a copy-and-paste of the
    implementation, but two we'll call it an A/B test. :shrug:
    """
    session = setup_session()
    conn = CosConnection(
        session,
        **sample_base_kwargs,
        **sample_auth_kwargs,
        **sample_tls_kwargs,
        **sample_auth_secret_kwargs,
    )
    connection_string = conn.get_connection_string()
    assert (
        connection_string
        == "s3,endpoint={}://{}:{},accesskey={},secretkey={},bucketsuffix={}".format(
            conn.schema,
            conn.hostname,
            conn.port,
            sample_auth_secret_kwargs["access_key"],
            sample_auth_secret_kwargs["secret_key"],
            conn.bucket_name,
        )
    )


def test_get_connection_string_missing_secrets():
    """Make sure that trying to get the connection string when the secrets are
    not available raises a PreconditionError
    """
    session = setup_session()
    conn = CosConnection(
        session,
        **sample_base_kwargs,
        **sample_auth_kwargs,
        **sample_tls_kwargs,
    )
    with pytest.raises(PreconditionError):
        conn.get_connection_string()


##############
## provided ##
##############


def test_get_component_with_provided_connection():
    """Test that a provided connection passed in through the CR returns None on a get_component call"""
    override_deploy_configs = COS_PROVIDED_CONNECTION_DEPLOY_CONFIG_OVERRIDES

    session = setup_session(
        deploy_config=override_deploy_configs,
        deploy_manager=MockDeployManager(),
    )
    set_cos_secrets(session)
    component = CosFactory.get_component(session)
    # Since the component is provided, we should be getting None back
    assert component is None


def test_get_connection_with_provided_connection():
    """Test that a provided connection passed in through the CR returns valid connection details on get_connection call"""
    override_deploy_configs = COS_PROVIDED_CONNECTION_DEPLOY_CONFIG_OVERRIDES
    session = setup_session(
        deploy_config=override_deploy_configs,
        deploy_manager=MockDeployManager(),
    )
    set_cos_secrets(session)
    connection = CosFactory.get_connection(session)
    assert connection.hostname == sample_uri_secret_kwargs["hostname"]
    # Need to convert port value to int since Factory converts string input of port to type int
    assert connection.port == int(sample_uri_secret_kwargs["port"])
    assert connection.bucket_name == sample_uri_secret_kwargs["bucketname"]
