"""
This implementation of the ITlsContext uses internal code to manage the tls
context
"""

# Standard
from typing import List, Optional, Tuple

# First Party
import alog

# Local
from ...oper8x_component import Oper8xComponent
from .. import common, tls
from .interface import ITlsContext
from oper8 import Component, Session, assert_cluster

log = alog.use_channel("TLSINT")


## InternalTlsContext ##########################################################


class InternalTlsContext(ITlsContext):
    __doc__ = __doc__

    TYPE_LABEL = "internal"

    def __init__(self, session: Session, *args, **kwargs):
        """At construct time, this instance will add a Component to the session
        which will manage the CA secret
        """
        super().__init__(session, *args, **kwargs)

        # Add the CA Component if it is not already present in the session.
        # There is a pretty nasty condition here when running in standalone mode
        # where the factory may attempt to recreate the singleton instance after
        # a subsystem has overwritten it, so we need to check and see if there
        # is a matching component in the session already
        pre_existing_component = [
            comp
            for comp in session.get_components()
            if comp.name == InternalCaComponent.name
        ]
        if pre_existing_component:
            self._component = pre_existing_component[0]
        else:
            self._component = InternalCaComponent(
                session=session, labels=self.config.labels
            )

        # Keep track of pairs for each server so that they are only generated
        # once
        self._server_pairs = {}

    ## Interface ###############################################################
    def request_server_key_cert_pair(
        self,
        server_component: Component,
        san_hostnames_list: List[str],
        san_ip_list: List[str] = None,
        key_name: str = None,
        intermediate_ca: bool = False,
    ) -> None:
        """Request creation of the PEM encoded value of the key/cert pair for a
        given server. This function has to be called from before render_chart is
        called. I.e., parse_config / Component constructor phase.
        Implementations of this function will generate the pair (in background)
        if it has not been already requested.

        Args:
            server_component:  Component
                The Component that manages the server. This can be used to add
                a new Component if needed that will manage the resource for the
                derived content and configure dependencies.
            san_list:  List[str]
                The list of Subject Alternate Names
            key_name:  str
                In case component requires multiple certificates. The key_name
                is used to distinguish between component cert requests.
            intermediate_ca:  bool
                Whether or not to configure the certificate for use as an
                intermediate CA. This implies setting the key_cert_sign usage
                bit in the generated cert.
                Reference: https://ldapwiki.com/wiki/KeyUsage
        """
        cache_key = server_component.name + (
            "-" + key_name if key_name is not None else ""
        )

        if cache_key in self._server_pairs:
            log.warning(
                "Certificate server key/cert pair for %s has been already "
                "requested. Ignoring this request.",
                cache_key,
            )
            return

        log.debug("Generating server key/cert pair for %s", cache_key)

        # Mark the server component as dependent on the internal component. This
        # is not strictly necessary since values are consumed by value, but it
        # makes sequential sense.
        self.session.add_component_dependency(server_component, self._component)

        # Get the CA's private key
        ca_key = tls.parse_private_key_pem(self._component.get_ca_key_cert()[0])

        # Generate the derived pair
        san_list = (san_hostnames_list or []) + (san_ip_list or [])
        self._server_pairs[cache_key] = tls.generate_derived_key_cert_pair(
            ca_key=ca_key,
            san_list=san_list,
            encode=False,
            key_cert_sign=intermediate_ca,
        )

    def get_server_key_cert_pair(
        self,
        server_component: Component,
        key_name: str = None,
        encode: bool = True,
        existing_key_pem: str = None,
        existing_cert_pem: str = None,
    ) -> Tuple[str, str]:
        """This function derives a server key/cert pair from the CA key/cert
        managed by the internal component.

        Args:
            server_component:  Component
                The Component that manages the server. This can be used to add
                a new Component if needed that will manage the resource for the
                derived content and configure dependencies.
            key_name:  str
                In case component requires multiple certificates. The key_name
                is used to distinguish between component cert requests.
            encode:  bool
                Whether or not to base64 encode the output pem strings
            existing_key_pem: str
                If both existing key/cert are specified, then they are returned
                immediately without any checks
            existing_cert_pem: str
                If both existing key/cert are specified, then they are returned
                immediately without any checks
         Returns:
            key_pem:  str
                This is the pem-encoded key content (base64
                encoded if encode is set)
            cert_pem:  str
                This is the pem-encoded cert content (base64
                encoded if encode is set)
        """
        log.debug2("Getting server key/cert pair for %s", server_component)
        cache_key = server_component.name + (
            "-" + key_name if key_name is not None else ""
        )

        assert (
            cache_key in self._server_pairs
        ), f"Trying to obtain certificate {key_name} which was not previouly requested"
        if existing_key_pem is not None and existing_cert_pem is not None:
            key_pem = existing_key_pem
            cert_pem = existing_cert_pem
        else:
            # Return the stored pair for this server
            (key_pem, cert_pem) = self._server_pairs[cache_key]
        if encode:
            return (common.b64_secret(key_pem), common.b64_secret(cert_pem))
        return (key_pem, cert_pem)

    def get_client_cert(
        self,
        client_component: Component,
        encode: bool = True,
    ) -> str:
        """Get the CA's public cert

        Args:
            client_component:  Component
                The Component that manages the client. This implementation does
                not need the component.
            encode:  bool
                Whether or not to base64 encode the output pem strings

        Returns:
            crt_pem:  Optional[str]
               The pem-encoded cert (base64 encoded if encode set)
        """
        log.debug2("Getting client cert for %s", client_component)
        _, ca_crt = self._component.get_ca_key_cert()
        if encode:
            return common.b64_secret(ca_crt)
        return ca_crt


## Component ###################################################################


class InternalCaComponent(Oper8xComponent):
    """This Component will manage a single secret containing a CA key/cert pair"""

    CA_SECRET_NAME = "infra-tls-ca"
    CA_KEY_FILENAME = "key.ca.pem"
    CA_CRT_FILENAME = "crt.ca.pem"

    name = "internal-tls"

    ## Component Interface #####################################################

    def __init__(
        self,
        session: Session,
        *args,
        labels: Optional[dict] = None,
        **kwargs,
    ):
        """Construct the parent Component and set up internal data holders"""
        super().__init__(*args, session=session, **kwargs)
        self._ca_key_pem = None
        self._ca_crt_pem = None

        # Pull labels from config or use defaults
        self._labels = labels

    def build_chart(self, *args, **kwargs):
        """Implement delayed chart construction in build_chart"""

        # Make sure the data values are populated
        self._initialize_data()

        # Get the labels to use for the secret
        secret_cluster_name = self._get_secret_name()
        labels = self._labels
        if labels is None:
            labels = common.get_labels(
                cluster_name=secret_cluster_name,
                session=self.session,
                component_name=self.CA_SECRET_NAME,
            )
        log.debug("Creating internal CA secret: %s", secret_cluster_name)
        self.add_resource(
            name=self.CA_SECRET_NAME,
            obj=dict(
                kind="Secret",
                apiVersion="v1",
                metadata=common.metadata_defaults(
                    session=self.session,
                    cluster_name=secret_cluster_name,
                    labels=labels,
                ),
                data={
                    self.CA_KEY_FILENAME: common.b64_secret(self._ca_key_pem),
                    self.CA_CRT_FILENAME: common.b64_secret(self._ca_crt_pem),
                },
            ),
        )

    ## Public Utilities ########################################################

    def get_ca_key_cert(self) -> Tuple[str, str]:
        """Get the pem-encoded CA key cert pair

        Returns:
            ca_key_pem:  str
                The pem-encoded (not base64 encoded) secret key
            ca_crt_pem:  str
                The pem-encoded (not base64 encoded) secret cert
        """
        self._initialize_data()
        return self._ca_key_pem, self._ca_crt_pem

    ## Implementation Details ##################################################

    def _get_secret_name(self) -> str:
        """Get the CA secret name with any scoping applied"""
        return self.get_cluster_name(self.CA_SECRET_NAME)

    def _initialize_data(self):
        """Initialize the data if needed"""

        # If this is the first time, actually do the init
        if None in [self._ca_crt_pem, self._ca_key_pem]:
            secret_cluster_name = self._get_secret_name()
            log.debug2("Cluster TLS Secret Name: %s", secret_cluster_name)
            success, content = self.session.get_object_current_state(
                kind="Secret",
                name=secret_cluster_name,
            )
            assert_cluster(
                success, f"Failed to check cluster for [{secret_cluster_name}]"
            )
            if content is not None:
                # Extract the pem strings
                key_pem = content.get("data", {}).get(self.CA_KEY_FILENAME)
                crt_pem = content.get("data", {}).get(self.CA_CRT_FILENAME)
                if None in [key_pem, crt_pem]:
                    log.warning(
                        "Found CA secret [%s] but content is invalid!",
                        secret_cluster_name,
                    )
                    self._generate()
                else:
                    log.debug("Found valid CA secret content.")
                    self._ca_key_pem = common.b64_secret_decode(key_pem)
                    self._ca_crt_pem = common.b64_secret_decode(crt_pem)
            else:
                log.debug2("No existing CA secret found. Generating.")
                self._generate()

    def _generate(self):
        """Generate a new CA"""
        key, self._ca_key_pem = tls.generate_key(encode=False)
        self._ca_crt_pem = tls.generate_ca_cert(key, encode=False)
