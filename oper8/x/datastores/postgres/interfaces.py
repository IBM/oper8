"""
Base class interface for a Postgres component
"""

# Local
from .... import component
from ..interfaces import Datastore

COMPONENT_NAME = "postgres"


@component(COMPONENT_NAME)
class IPostgresComponent(Datastore):
    """A postgres chart provides access to a single running Postgres cluster"""

    ## Shared Utilities ########################################################

    def tls_enabled(self) -> bool:
        """Return whether TLS is enabled or not
        Returns:
            bool: True (TLS enabled), False (TLS disabled)
        """
        return self.config.get("tls", {}).get("enabled", True)
