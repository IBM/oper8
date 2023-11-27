"""
This module defines the interface needed to provide TLS key/cert pairs to a
given microservice and fetch a client-side certificate for making calls to a
microservice that serves a key/cert pair derived from this context.
"""

# Standard
from typing import List, Tuple
import abc

# First Party
import aconfig

# Local
from oper8 import Component, Session


class ITlsContext(abc.ABC):
    """This interface encapsulates the management of TLS for a running instance
    of the operand. It encapsulates the following functions:

    * Manage a CA key/cert pair for signing derived microservice certificates
    * Create derived key/cert pairs for individual microservices
    """

    # The string constant that will be used by derived classes to define the
    # type label string
    _TYPE_LABEL_ATTRIBUTE = "TYPE_LABEL"

    ## Construction ############################################################

    def __init__(self, session: Session, config: aconfig.Config):
        """Construct with the current session so that member functions do not
        take it as an argument

        Args:
            session:  Session
                The current deploy session
            config:  aconfig.Config
                The config for this instance
        """
        self._session = session
        self._config = config

    @property
    def session(self) -> Session:
        return self._session

    @property
    def config(self) -> aconfig.Config:
        return self._config

    ## Interface ###############################################################

    def request_server_key_cert_pair(  # noqa: B027
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
            san_hostnames_list:  List[str]
                The list of Subject Alternate Names (hostnames only)
            san_ip_list:  List[str]
                The list of Subject Alternate Names (ip addresses only, IPv4,
                IPv6)
            key_name:  str
                In case component requires multiple certificates. The key_name
                is used to distinguishes between component cert requests.
            intermediate_ca:  bool
                Whether or not to configure the certificate for use as an
                intermediate CA. This implies setting the key_cert_sign usage
                bit in the generated cert.
                Reference: https://ldapwiki.com/wiki/KeyUsage
        """

    @abc.abstractmethod
    def get_server_key_cert_pair(
        self,
        server_component: Component,
        key_name: str = None,
        encode: bool = True,
        existing_key_pem: str = None,
        existing_cert_pem: str = None,
    ) -> Tuple[str, str]:
        """Get the PEM encoded value of the key/cert pair for a given server.
        You have to forst request_server_key_cert_pair in render_config phase,
         and later in render_chart retrieve generated cert.

        Args:
            server_component:  Component
                The Component that manages the server. This can be used to add
                a new Component if needed that will manage the resource for the
                derived content and configure dependencies.
            key_name:  str
                In case component requires multiple certificates. The key_name
                is used to distinguies between component cert requests.
            encode:  bool
                Whether or not to base64 encode the output pem strings
            existing_key_pem: str
                Optionaly, you may provide the (decoded) value of PK/CERK pair.
                TLS context is free to check the Cert/PK and return this pair or
                generate new one.
            existing_cert_pem: str,
                Optionaly, you may provide the (decoded) value of PK/CERK pair.
                TLS context is free to check the Cert/PK and return this pair or
                generate new one.
        Returns:
            key_pem:  Optional[str]
                This is the pem-encoded key content (base64
                encoded if encode is set)
            cert_pem:  Optional[str]
                This is the pem-encoded cert content (base64
                encoded if encode is set)
        """

    @abc.abstractmethod
    def get_client_cert(
        self,
        client_component: Component,
        encode: bool = True,
    ) -> str:
        """Get a cert which can be used by a client to connect to a server which
        is serving using a key/cert pair signed by the shared CA.

        Args:
            client_component:  Component
                The Component that manages the client. This can be used to add
                a new Component if needed that will manage the resource for the
                derived content and configure dependencies.
            encode:  bool
                Whether or not to base64 encode the output pem strings

        Returns:
            crt_pem:  Optional[str]
                The pem-encoded cert (base64 encoded if encode set).
        """
