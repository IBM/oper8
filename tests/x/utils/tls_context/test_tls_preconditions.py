"""
This test simulates the sequence of events that will be needed when an
implementation of ITlsContext needs to wait on a downstream operator (such as
certificate manager) to create the TLS content.
"""

# Standard
import abc

# First Party
import alog

# Local
from oper8.test_helpers.helpers import MockDeployManager, MockTopApp, setup_session
from oper8.x.oper8x_component import Oper8xComponent
from oper8.x.utils import common, tls
from oper8.x.utils.tls_context.factory import register_tls_context_type
from oper8.x.utils.tls_context.interface import ITlsContext
from tests.x.utils.tls_context.util import make_server_component_class

## Helpers #####################################################################

log = alog.use_channel("TEST")


class FakeCMTlsContext(ITlsContext):

    TYPE_LABEL = "fakecm"

    def __init__(self, session, config):
        super().__init__(session, config)
        self.iteration = 0

        # Add the CA component
        log.debug("Setting up CA component")
        self._ca_component = DelayedCaTlsComponent(
            session, config.get("ca_wait_iterations", 1)
        )

    def get_client_cert(
        self,
        client_component,
        encode=True,
    ):
        # Declare a dependency for the client component on the CA component
        self.session.add_component_dependency(client_component, self._ca_component)

        # Look for the CA component's content to see if it exists
        _, content = self.session.get_object_current_state(
            kind="Secret",
            name=self._ca_component.SECRET_NAME,
        )

        # If it exists, fetch the CA crt and optionally decode
        if content is not None:
            log.debug("Found valid CA content")
            ca_crt = content["data"][self._ca_component.CA_CRT_FIELD]
            if not encode:
                ca_crt = common.b64_secret_decode(ca_crt)
            return ca_crt

        # If it doesn't exist, return None
        log.debug("Still waiting on valid CA content")
        return None

    def get_server_key_cert_pair(
        self,
        server_component,
        san_hostnames_list=None,
        san_ip_list=None,
        encode=True,
        intermediate_ca=False,
    ):
        # Add the component that will provision the server key/cert pair (if already not created)
        # in tests we share deploy manager, i.e. we also share deployment id, contexts and components
        #  between steps. Thus registering the component just once per server component
        if "delayed-tls-{server_component.name}" not in self.session.get_components():
            log.debug("Setting up server tls component for %s", server_component.name)
            tls_component = make_delayed_server_component(
                server_component.name,
                self.session,
                self.config.get("server_wait_iterations", 1),
            )
            self.session.add_component_dependency(server_component, tls_component)
            self.session.add_component_dependency(tls_component, self._ca_component)

        # Check the cluster to see if it's been provisioned yet
        _, content = self.session.get_object_current_state(
            kind="Secret",
            name=tls_component.SECRET_NAME,
        )

        # If it exists, fetch the key/cert pair and optionally decode
        if content is not None:
            log.debug("Found server tls content for %s", server_component.name)
            key = content["data"][tls_component.KEY_FIELD]
            crt = content["data"][tls_component.CRT_FIELD]
            if not encode:
                key = common.b64_secret_decode(key)
                crt = common.b64_secret_decode(crt)
            return key, crt

        # If it doesn't exist, return None
        log.debug("Still waiting on server tls content for %s", server_component.name)
        return None, None


# Register the dummy implementation
register_tls_context_type(FakeCMTlsContext)


class DelayedTlsComponent(Oper8xComponent):
    ITERATION_SECRET_BASE_NAME = "iterations-secret"

    def __init__(self, session, wait_iterations):
        super().__init__(session=session)
        self.wait_iterations = wait_iterations

    def build_chart(self, *_, **__):

        # Get the current iteration from the existing secret
        iteration_secret_name = f"{self.ITERATION_SECRET_BASE_NAME}-{self.name}"
        _, content = self.session.get_object_current_state(
            kind="Secret", name=iteration_secret_name
        )
        current_iteration = 0
        if content is not None:
            current_iteration = int(
                common.b64_secret_decode(content["data"]["iteration"])
            )

        # If we hit the iteration threshold, add the generated TLS content
        log.debug2(
            "[%s] current iteration: %d, wait iterations: %d",
            self,
            current_iteration,
            self.wait_iterations,
        )
        if current_iteration == self.wait_iterations:
            self.add_tls_secret()
        else:
            log.debug2("[%s] Waiting to add TLS secret", self)

        # Increment the iteration
        current_iteration += 1
        self.add_resource(
            name=iteration_secret_name,
            obj=dict(
                kind="Secret",
                apiVersion="v1",
                metadata=dict(name=iteration_secret_name),
                data={"iteration": common.b64_secret(str(current_iteration))},
            ),
        )

    @abc.abstractmethod
    def add_tls_secret(self):
        """Will be implemented below, once for CA and once for derived server"""


class DelayedCaTlsComponent(DelayedTlsComponent):
    name = "delayed-tls-ca"

    SECRET_NAME = "ca-secret"
    CA_KEY_FIELD = "key.pem"
    CA_CRT_FIELD = "crt.pem"

    def add_tls_secret(self):
        log.debug("Creating TLS secret for %s", self)

        # Generate key and CA cert
        key, ca_key = tls.generate_key(encode=True)
        ca_crt = tls.generate_ca_cert(key, encode=True)

        # Create the secret
        self.add_resource(
            name=self.SECRET_NAME,
            obj=dict(
                kind="Secret",
                apiVersion="v1",
                metadata=dict(name=self.SECRET_NAME),
                data={
                    self.CA_KEY_FIELD: ca_key,
                    self.CA_CRT_FIELD: ca_crt,
                },
            ),
        )


def make_delayed_server_tls_secret_name(server_name):
    return f"{server_name}-tls"


def make_delayed_server_component(server_name, session, wait_iterations):
    class DelayedServerTlsComponent(DelayedTlsComponent):
        name = f"delayed-tls-{server_name}"

        SECRET_NAME = make_delayed_server_tls_secret_name(server_name)
        KEY_FIELD = "key.pem"
        CRT_FIELD = "crt.pem"

        def add_tls_secret(self):
            log.debug("Creating TLS secret for %s", self)

            # Get the CA from the cluster
            _, ca_content = self.session.get_object_current_state(
                kind="Secret",
                name=DelayedCaTlsComponent.SECRET_NAME,
            )
            assert ca_content is not None
            key = tls.parse_private_key_pem(
                common.b64_secret_decode(
                    ca_content["data"][DelayedCaTlsComponent.CA_KEY_FIELD]
                )
            )

            # Derive the server key/cert pair
            key, crt = tls.generate_derived_key_cert_pair(key, [], encode=True)

            # Create the secret
            self.add_resource(
                name=self.SECRET_NAME,
                obj=dict(
                    kind="Secret",
                    apiVersion="v1",
                    metadata=dict(name=self.SECRET_NAME),
                    data={
                        self.KEY_FIELD: key,
                        self.CRT_FIELD: crt,
                    },
                ),
            )

    return DelayedServerTlsComponent(session, wait_iterations)


def has_secret(session, name):
    return session.deploy_manager.has_obj(
        kind="Secret",
        name=name,
        namespace=session.namespace,
    )


## Tests #######################################################################


def test_server_precondition_assert():
    """Test that a server component which needs to wait for TLS content
    correctly waits, but proceeds once the content is available
    """
    dm = MockDeployManager()
    app_config_overrides = {
        "tls": {
            "type": FakeCMTlsContext.TYPE_LABEL,
            "ca_wait_iterations": 1,
            "server_wait_iterations": 1,
        }
    }

    # Define the server component class
    ServerComponent = make_server_component_class()

    # Do the first pass. We want to ensure that it does not get past the wait on
    # the TLS data
    session = setup_session(
        deploy_manager=dm,
        app_config=app_config_overrides,
    )
    with alog.ContextLog(log.debug, "---- [FIRST PASS] ----"):
        # Set up a top-level application that will manage this component
        app = MockTopApp(session.config, [ServerComponent])

        # Roll it out
        app.do_rollout(session)

        # Make sure that the component's secret was not created and that none of
        # the TLS data secrets were created
        assert not has_secret(session, DelayedCaTlsComponent.SECRET_NAME)
        assert not has_secret(
            session, make_delayed_server_tls_secret_name(ServerComponent.name)
        )
        assert not has_secret(session, ServerComponent.get_secret_name())

    # Do the second pass. The TLS "backend" should provision the data this time,
    # but since that happens at deploy time, the server will still not see it,
    # so it will again wait on the precondition.
    session = setup_session(
        deploy_manager=dm,
        app_config=app_config_overrides,
    )
    with alog.ContextLog(log.debug, "---- [SECOND PASS] ----"):
        # Set up a top-level application that will manage this component
        app = MockTopApp(session.config, [ServerComponent])

        # Roll it out
        app.do_rollout(session)

        # Make sure that the upstream secrets have been provisioned, but not
        # the server's secret yet
        assert has_secret(session, DelayedCaTlsComponent.SECRET_NAME)
        assert has_secret(
            session, make_delayed_server_tls_secret_name(ServerComponent.name)
        )
        assert not has_secret(session, ServerComponent.get_secret_name())

    # Do the third pass. Now, the TLS data exists in the cluster before the
    # rollout starts, so the server can proceed.
    session = setup_session(
        deploy_manager=dm,
        app_config=app_config_overrides,
    )
    with alog.ContextLog(log.debug, "---- [THIRD PASS] ----"):
        # Set up a top-level application that will manage this component
        app = MockTopApp(session.config, [ServerComponent])

        # Roll it out
        app.do_rollout(session)

        # Make sure that the server's secret is there
        assert has_secret(session, DelayedCaTlsComponent.SECRET_NAME)
        assert has_secret(
            session, make_delayed_server_tls_secret_name(ServerComponent.name)
        )
        assert has_secret(session, ServerComponent.get_secret_name())
