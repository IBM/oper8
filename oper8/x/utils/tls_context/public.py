"""
This file holds functions that should be used outside of this module by
components, subsystems, and applications that need access to the TLS context
functionality.
"""

# Standard
from typing import Tuple

# Local
from .factory import get_tls_context, register_tls_context_type
from .internal import InternalTlsContext
from oper8 import Session

## Type Registrations ##########################################################

register_tls_context_type(InternalTlsContext)

## Public Functions ############################################################


def request_server_key_cert_pair(
    session: Session,
    *args,
    **kwargs,
) -> None:
    """Request creation of the PEM encoded value of the key/cert pair for a
    given server. This function has to be called from before render_chart is
    called. I.e., parse_config / Component constructor phase. Implementations of
    this function will generate the pair (in background) if it has not been
    already requested.

    Args:
        session:  Session
            The current deploy session

    Passthrough Args:
        See ITlsContext.request_server_key_cert_pair
    """
    return get_tls_context(session).request_server_key_cert_pair(*args, **kwargs)


def get_server_key_cert_pair(
    session: Session,
    *args,
    **kwargs,
) -> Tuple[str, str]:
    """Get the previously requested PEM encoded value of the key/cert pair for a
    given server. Implementations will retrieveh the pair if it does not exist
    and will fetch its content if it does. If the content is not available, the
    assertion is triggered.

    Args:
        session:  Session
            The current deploy session

    Passthrough Args:
        See ITlsContext.get_server_key_cert_pair

    Returns:
        key_pem:  str
            This is the pem-encoded key content (base64 encoded if
            encode is set)
        cert_pem:  str
            This is the pem-encoded cert content (base64 encoded
            if encode is set)
    """
    return get_tls_context(session).get_server_key_cert_pair(*args, **kwargs)


def get_client_cert(
    session: Session,
    *args,
    **kwargs,
) -> str:
    """Get the CA's public cert

    Args:
        session:  Session
            The current deploy session

    Passthrough Args:
        See ITlsContext.get_client_cert

    Returns:
        crt_pem:  Optional[str]
                           The pem-encoded cert (base64 encoded if encode set),
    """
    return get_tls_context(session).get_client_cert(*args, **kwargs)
