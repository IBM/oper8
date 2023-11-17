"""
Helper methods for tls factory tests / TLC context tests.
"""

# Standard
import os
import re

# First Party
import alog

# Local
from oper8 import Session, assert_precondition
from oper8.test_helpers.oper8x_helpers import TEST_DATA_DIR, set_secret_data
from oper8.x.oper8x_component import Oper8xComponent
from oper8.x.utils import common
from oper8.x.utils.tls_context.factory import (
    _TlsContextSingletonFactory,
    get_tls_context,
)

log = alog.use_channel("TEST")

CERT_MGR_TEST_TLS_CERT = "-----BEGIN CERTIFICATE-----\nfake-cert"
CERT_MGR_TEST_TLS_KEY = "-----BEGIN FAKED PRIVATE KEY-----\nfake-key"
CERT_MGR_TEST_CA_CERT = "-----BEGIN CERTIFICATE-----\nfake-cacert"

NAME_CLEANER = re.compile("[^-a-zA-Z0-9]")


def reset_tls_factory():
    _TlsContextSingletonFactory._instance = None


def make_server_component_class(server_name="test-server", request_cert=True):
    class ServerComponent(Oper8xComponent):
        name = server_name

        KEY_FIELD = "copied_server_key"
        CRT_FIELD = "copied_server_crt"

        def __init__(self, session, *args, **kwargs):
            super().__init__(session, *args, **kwargs)

            # Get a server key/cert pair to use
            if request_cert:
                log.debug("[%s] Fetching TLS content", self)
                self.server_key, self.server_crt = get_tls_context(
                    self.session
                ).get_server_key_cert_pair(self, [], encode=True)
                log.debug3("Server Key: %s", self.server_key)
                log.debug3("Server Crt: %s", self.server_crt)

        def build_chart(self, *_, **__):
            log.debug("[%s] build_chart", self)
            assert_precondition(None not in [self.server_key, self.server_crt])

            # Add a secret to indicate that the precondition passed
            secret_name = self.get_secret_name()
            self.add_resource(
                name=secret_name,
                obj=dict(
                    kind="Secret",
                    apiVersion="v1",
                    metadata=dict(name=secret_name),
                    data={
                        self.KEY_FIELD: self.server_key,
                        self.CRT_FIELD: self.server_crt,
                    },
                ),
            )

        @classmethod
        def get_secret_name(cls):
            return f"{cls.name}-secret"

    return ServerComponent


def set_cert_manager_secret_for_component(
    session: Session,
    component_name: str,
    data_override={},
    name_override: str = None,
):
    component_name = NAME_CLEANER.sub("", component_name).lower()
    if not data_override:
        data_override = {
            "tls.key": common.b64_secret(CERT_MGR_TEST_TLS_KEY),
            "tls.crt": common.b64_secret(CERT_MGR_TEST_TLS_CERT),
            "ca.crt": common.b64_secret(CERT_MGR_TEST_CA_CERT),
        }
    set_secret_data(
        session,
        name=("tls-" + component_name) if name_override is None else name_override,
        data=data_override,
        secret_type="kubernetes.io/tls",
    )
    set_cert_manager_secret_ca(session)


def set_cert_manager_secret_ca(session: Session):
    with open(os.path.join(TEST_DATA_DIR, "test_ca.key"), "r") as f:
        key_pem = f.read()
    with open(os.path.join(TEST_DATA_DIR, "test_ca.crt"), "r") as f:
        crt_pem = f.read()
    ca_data = {
        "tls.key": common.b64_secret(key_pem),
        "tls.crt": common.b64_secret(crt_pem),
        "ca.crt": common.b64_secret(crt_pem),
    }
    set_secret_data(session, name="ca", data=ca_data, secret_type="kubernetes.io/tls")
