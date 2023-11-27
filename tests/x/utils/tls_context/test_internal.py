"""
Test the TLS context utility functionality
"""

# Standard
import re

# First Party
import aconfig
import alog

# Local
from oper8.test_helpers.helpers import configure_logging, setup_session
from oper8.x.utils.tls_context import factory, internal
from tests.x.utils.tls_context.util import make_server_component_class

configure_logging()
log = alog.use_channel("TEST")

NAME_CLEANER = re.compile("[^-a-zA-Z0-9]")

## Happy Path ##


def test_register_same_cert_twice():
    """Test that it is possible to call register Cert twice (the second call prints WARNING)"""
    tls_cfg = aconfig.Config({"type": "internal"})
    session = setup_session(app_config={"tls": tls_cfg})
    context = internal.InternalTlsContext(session, config=tls_cfg)
    server_comp = make_server_component_class(request_cert=False)(session)
    context.request_server_key_cert_pair(server_comp, ["localhost"])
    context.request_server_key_cert_pair(server_comp, ["localhost"])


def test_passthough_provided_certs():
    """Test that existing certs parameters are passed through"""
    tls_cfg = aconfig.Config({"type": "internal"})
    session = setup_session(deploy_config={"tls": tls_cfg})
    existing_key_pem = "-----BEGIN FAKED PRIVATE KEY-----\nfake-key"
    existing_cert_pem = "-----BEGIN CERTIFICATE-----\nfake-cert"

    context = internal.InternalTlsContext(session, config=tls_cfg)
    server_comp = make_server_component_class(request_cert=False)(session)
    context.request_server_key_cert_pair(server_comp, ["localhost"])

    (key_pem, cert_pem) = context.get_server_key_cert_pair(
        server_component=server_comp,
        encode=False,
        existing_key_pem=existing_key_pem,
        existing_cert_pem=existing_cert_pem,
    )

    assert key_pem == existing_key_pem
    assert cert_pem == existing_cert_pem


def test_multi_session():
    """Test that reusing an existing session where the component has already
    been registered does not error out. This can happen when running in
    standalone mode (i.e. make run)
    """
    session1 = setup_session(app_config={"tls": {"type": "internal"}})

    # Create the instance
    factory.get_tls_context(session1)

    # Create a nested session to simulate standalone recursion
    session2 = setup_session(app_config={"tls": {"type": "internal"}})
    factory.get_tls_context(session2)

    # Use the factory again with the original session
    factory.get_tls_context(session1)


def test_label_overrides():
    """Test that the labels can be overridden in the config"""
    tls_cfg = aconfig.Config(
        {
            "type": "internal",
            "labels": {"foo": "bar"},
        }
    )
    session = setup_session(app_config={"tls": tls_cfg})
    context = internal.InternalTlsContext(session, config=tls_cfg)
    comp = context._component.to_dict(session)[0]
    assert comp["metadata"]["labels"] == tls_cfg.labels
