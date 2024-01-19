"""
Test the TLS context utility functionality
"""

# Standard
from contextlib import closing, contextmanager
import multiprocessing
import os
import random
import socket
import tempfile
import time

# Third Party
from flask import Flask
import requests

# First Party
import alog

# Local
from oper8 import Component, component
from oper8.test_helpers.helpers import (
    MockDeployManager,
    configure_logging,
    setup_session,
)
from oper8.test_helpers.oper8x_helpers import set_secret_data, set_tls_ca_secret
from oper8.x.utils import common, tls_context
from oper8.x.utils.tls_context.factory import (
    _TlsContextSingletonFactory,
    get_tls_context,
    register_tls_context_type,
)
from oper8.x.utils.tls_context.internal import InternalCaComponent, InternalTlsContext

## Helpers #####################################################################

configure_logging()
log = alog.use_channel("TEST")


@component(name="Stub")
class StubChart(Component):
    def build_chart(self, session):
        log.debug("Constructing %s", self)
        self.doit(session=session, scope=self)


def reset():
    _TlsContextSingletonFactory._instance = None
    _TlsContextSingletonFactory._instance_deploy_id = None


def make_stub_chart(session, doit, name=None):
    chart_class = StubChart
    if name is not None:
        component_name = name

        class DerivedChart(StubChart):
            name = component_name

        chart_class = DerivedChart

    chart = chart_class(session=session)
    setattr(chart, "doit", doit)
    return chart


def port_open(port):
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
        return sock.connect_ex(("127.0.0.1", port)) != 0


def random_port():
    return int(random.uniform(12345, 55555))


def get_available_port():
    port = random_port()
    while not port_open(port):
        port = random_port
    return port


def get_internal_ctxt_ca_pair(session):
    ctxt = get_tls_context(session)
    return ctxt._component._ca_key_pem, ctxt._component._ca_crt_pem


INTERNAL_TLS_OVERRIDES = {"tls": {"type": "internal"}}


## State Management ############################################################
#
# These tests exercise the global state management to ensure that the core CA
# key and cert are generated and reused correctly
##


def test_generate_when_not_found_get_client():
    """Test that the CA key/cert are generated when not found in the cluster and
    a client cert is requested
    """

    # Create the chart
    def doit(session, scope):
        crt = tls_context.get_client_cert(session, scope, encode=False)
        assert crt is not None
        assert crt == get_internal_ctxt_ca_pair(session)[1]

    # Trigger the render
    reset()
    session = setup_session(
        app_config=INTERNAL_TLS_OVERRIDES,
    )
    chart = make_stub_chart(session, doit)
    chart.to_config(session)


def test_generate_when_not_found_get_server():
    """Test that the CA key/cert are generated when not found in the cluster and
    a server key/cert pair is requested
    """

    # Create the chart
    def doit(session, scope):
        tls_context.request_server_key_cert_pair(session, scope, [])
        key, crt = tls_context.get_server_key_cert_pair(session, scope, encode=False)

        # Make sure that the cached values are populated and the generated key
        # and cert do not match the cached CA
        assert key is not None
        assert crt is not None
        ca_key, ca_crt = get_internal_ctxt_ca_pair(session)
        assert ca_key is not None
        assert ca_crt is not None
        assert key != ca_key
        assert crt != ca_crt

    # Trigger the render
    reset()
    session = setup_session(
        app_config=INTERNAL_TLS_OVERRIDES,
    )
    chart = make_stub_chart(session, doit)
    chart.to_config(session)


def test_generate_when_found_but_missing_keys():
    """Test that the CA key/cert are regenerated when not found in the cluster
    but the secret is missing values
    """
    reset()
    session = setup_session(
        app_config=INTERNAL_TLS_OVERRIDES,
        deploy_manager=MockDeployManager(),
    )
    # Pre-populate the secret but don't add the keys
    set_secret_data(session, InternalCaComponent.CA_SECRET_NAME, data={})
    chart = InternalCaComponent(session)

    # Trigger the render and make sure the generated secret matches the
    # cached CA content
    configs = chart.to_config(session)
    assert len(configs) == 1
    secret_crt = common.b64_secret_decode(
        configs[0]["data"][InternalCaComponent.CA_CRT_FILENAME]
    )
    secret_key = common.b64_secret_decode(
        configs[0]["data"][InternalCaComponent.CA_KEY_FILENAME]
    )
    assert (secret_key, secret_crt) == (chart._ca_key_pem, chart._ca_crt_pem)


def test_reuse_when_found_get_client():
    """Test that the CA key/cert are reused when not found in the cluster and a
    client cert is requested
    """
    reset()
    session = setup_session(
        app_config=INTERNAL_TLS_OVERRIDES,
        deploy_manager=MockDeployManager(),
    )
    # Pre-populate the secret
    test_key, test_crt = set_tls_ca_secret(session)

    # Create the chart and render it
    def doit(session, scope):
        crt = tls_context.get_client_cert(session, scope, encode=False)
        assert crt is not None
        ca_key, ca_crt = get_internal_ctxt_ca_pair(session)
        assert crt == ca_crt
        assert crt == test_crt

    chart = make_stub_chart(session, doit)
    chart.to_config(session)


def test_reuse_when_found_get_server():
    """Test that the CA key/cert are reused when not found in the cluster and a
    server key/cert pair is requested
    """
    reset()
    session = setup_session(
        app_config=INTERNAL_TLS_OVERRIDES,
        deploy_manager=MockDeployManager(),
    )
    # Pre-populate the secret
    test_key, test_crt = set_tls_ca_secret(session)

    # Create the chart and render it
    def doit(session, scope):
        tls_context.request_server_key_cert_pair(session, scope, [])
        key, crt = tls_context.get_server_key_cert_pair(session, scope, encode=False)
        assert key is not None
        assert crt is not None
        ca_key, ca_crt = get_internal_ctxt_ca_pair(session)
        assert crt != ca_crt
        assert crt != test_crt
        assert key != ca_key
        assert key != test_key

    chart = make_stub_chart(session, doit)
    chart.to_config(session)


def test_state_shared_between_components():
    """Test two independent components can share the same state"""

    # Create the chart
    crts = set()

    def doit(session, scope):
        crt = tls_context.get_client_cert(session, scope, encode=False)
        assert crt is not None
        crts.add(crt)

    # Trigger the render
    reset()
    session = setup_session(
        app_config=INTERNAL_TLS_OVERRIDES,
    )
    chart1 = make_stub_chart(session, doit, name="chart1")
    chart2 = make_stub_chart(session, doit, name="chart2")
    chart1.to_config(session)
    chart2.to_config(session)
    assert len(crts) == 1


## Get Functions ###############################################################
#
# These tests exercise the functionality to get client and server TLS data
##


def test_encode_flag_server():
    """Test that the encode flag properly performs base64 encoding only if set
    to True for get_server_key_cert_pair
    """

    def doit(session, scope):
        tls_context.request_server_key_cert_pair(session, scope, [])

        key_enc, crt_enc = tls_context.get_server_key_cert_pair(
            session, scope, encode=True
        )
        key, crt = tls_context.get_server_key_cert_pair(session, scope, encode=False)
        common.b64_secret(key) == key_enc
        common.b64_secret(crt) == crt_enc

    # Trigger the render
    reset()
    session = setup_session(
        app_config=INTERNAL_TLS_OVERRIDES,
    )
    chart = make_stub_chart(session, doit)
    chart.to_config(session)


def test_encode_flag_client():
    """Test that the encode flag properly performs base64 encoding only if set
    to True for get_client_cert
    """

    def doit(session, scope):
        crt_enc = tls_context.get_client_cert(session, scope, encode=True)
        crt = tls_context.get_client_cert(session, scope, encode=False)
        common.b64_secret(crt) == crt_enc

    # Trigger the render
    reset()
    session = setup_session(
        app_config=INTERNAL_TLS_OVERRIDES,
    )
    chart = make_stub_chart(session, doit)
    chart.to_config(session)


## Server Communication ########################################################
#
# These tests validate that the communication paradigms work as expected
##


class ServerProcess(multiprocessing.Process):
    """This is a thread implementation which will run a simple Flask server and
    handle get reqeusts in an independent thread.
    """

    HELLO_WORLD = "Hello World!"

    def __init__(self, port, key_file, crt_file, ca_crt_file):
        self.port = port
        self.key_file = key_file
        self.crt_file = crt_file
        self.ca_crt_file = ca_crt_file
        self.started = multiprocessing.Event()
        super().__init__(target=self._start_server)

    def _start_server(self):
        app = Flask("stub")

        @app.route("/")
        def hello():
            return self.HELLO_WORLD

        self.started.set()
        app.run(port=self.port, ssl_context=(self.crt_file, self.key_file))

    def wait_for_boot(self):
        self.started.wait()
        time.sleep(0.1)

    def shutdown(self):
        self.terminate()


@contextmanager
def maybe_temp_dir():
    working_dir = os.environ.get("WORKING_DIR")
    if working_dir is not None:
        os.makedirs(working_dir, exist_ok=True)
        yield working_dir
    else:
        with tempfile.TemporaryDirectory() as working_dir:
            yield working_dir


# Use a stub chart to generate the pem content
@contextmanager
def gen_server_setup(session, server_number):
    generated = {}

    def doit(session, scope):
        tls_context.request_server_key_cert_pair(
            session, scope, ["localhost"], ["127.0.0.1"]
        )
        generated["ca_crt_pem"] = tls_context.get_client_cert(
            session, scope, encode=False
        )
        (
            generated["server_key_pem"],
            generated["server_crt_pem"],
        ) = tls_context.get_server_key_cert_pair(session, scope, encode=False)

    chart = make_stub_chart(session, doit, name=f"server-{server_number}")
    chart.to_config(session)

    # Save the files to the working dir
    with maybe_temp_dir() as working_dir:
        ca_crt_file = os.path.join(working_dir, f"ca_crt{server_number}.pem")
        with open(ca_crt_file, "w") as f:
            f.write(generated["ca_crt_pem"])
        server_key_file = os.path.join(working_dir, f"server_key{server_number}.pem")
        with open(server_key_file, "w") as f:
            f.write(generated["server_key_pem"])
        server_crt_file = os.path.join(working_dir, f"server_crt{server_number}.pem")
        with open(server_crt_file, "w") as f:
            f.write(generated["server_crt_pem"])

        # Also save out the ca key for debugging
        ca_key, _ = get_tls_context(session)._component.get_ca_key_cert()
        ca_key_file = os.path.join(working_dir, f"ca_key{server_number}.pem")
        with open(ca_key_file, "w") as f:
            f.write(ca_key)

        yield ca_crt_file, server_key_file, server_crt_file


def test_client_server_verify():
    """Test that a server with a generated key/cert pair and a client using the
    shared client cert can successfully communicate over TLS with verification
    enabled
    """

    # Generate the setup for the server
    session = setup_session(app_config=INTERNAL_TLS_OVERRIDES)
    with gen_server_setup(session, 1) as (
        ca_crt_file,
        server_key_file,
        server_crt_file,
    ):

        # Launch a server that will use the server pair
        port = get_available_port()
        server = ServerProcess(
            port=port,
            key_file=server_key_file,
            crt_file=server_crt_file,
            ca_crt_file=ca_crt_file,
        )
        server.start()
        server.wait_for_boot()

        # Make a request using the CA cert to verify
        res = requests.get(f"https://localhost:{port}", verify=ca_crt_file)
        res.raise_for_status()
        assert res.text == ServerProcess.HELLO_WORLD

        # Shut the server down
        server.shutdown()
        server.join()


def test_client_multi_server_verify():
    """Test that a client using the common cert can use it to communicate with
    multiple servers serving different key/cert pairs
    """

    session = setup_session(
        app_config=INTERNAL_TLS_OVERRIDES,
    )
    # Generate the setup for two servers
    with gen_server_setup(session, 1) as (
        ca_crt_file1,
        server_key_file1,
        server_crt_file1,
    ):
        with gen_server_setup(session, 2) as (
            ca_crt_file2,
            server_key_file2,
            server_crt_file2,
        ):

            # Make sure the two CA certs are the same
            with open(ca_crt_file1, "r") as f:
                ca1 = f.read()
            with open(ca_crt_file2, "r") as f:
                ca2 = f.read()
            assert ca1 == ca2
            ca_crt_file = ca_crt_file1

            # Launch two servers with the different key/cert pairs
            port1 = get_available_port()
            port2 = get_available_port()
            server1 = ServerProcess(
                port=port1,
                key_file=server_key_file1,
                crt_file=server_crt_file1,
                ca_crt_file=ca_crt_file1,
            )
            server2 = ServerProcess(
                port=port2,
                key_file=server_key_file2,
                crt_file=server_crt_file2,
                ca_crt_file=ca_crt_file2,
            )
            server1.start()
            server2.start()
            server1.wait_for_boot()
            server2.wait_for_boot()

            # Make a request to both servers using the same CA cert
            res = requests.get(f"https://localhost:{port1}", verify=ca_crt_file)
            res.raise_for_status()
            assert res.text == ServerProcess.HELLO_WORLD
            res = requests.get(f"https://localhost:{port2}", verify=ca_crt_file)
            res.raise_for_status()
            assert res.text == ServerProcess.HELLO_WORLD

            # Shut the server down
            server1.shutdown()
            server2.shutdown()
            server1.join()
            server2.join()


def test_get_tls_context_config_overrides():
    """Test that giving config overrides to get_tls_context takes precedende
    over backend config
    """
    session = setup_session(app_config={"tls": {"type": "invalid"}})
    get_tls_context(session, config_overrides=INTERNAL_TLS_OVERRIDES["tls"])


def test_reregister_ok():
    """Make sure that a type can be re-registered without raising. This is
    needed when registration is done in a derived library that uses the PWM and
    therefore re-imports the derived implementation.
    """
    register_tls_context_type(InternalTlsContext)
