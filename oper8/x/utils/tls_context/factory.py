"""
This module implements a factory for TlsContext implementations
"""

# Standard
from typing import Optional, Type

# First Party
import alog

# Local
from .interface import ITlsContext
from oper8 import Session, assert_config
from oper8.utils import merge_configs

log = alog.use_channel("TLSFY")


## Interface ###################################################################


def get_tls_context(
    session: Session,
    config_overrides: Optional[dict] = None,
) -> ITlsContext:
    """Get an instance of the configured implementation of the tls context

    Args:
        session:  Session
            The current deploy session
        config_overrides:  Optional[dict]
            Optional runtime config values. These will overwrite any values
            pulled from the session.config

    Returns:
        tls_context:  ITlsContext
            The constructed instance of the context
    """
    return _TlsContextSingletonFactory.get_tls_context(
        session,
        config_overrides=config_overrides,
    )


def register_tls_context_type(context_class: Type[ITlsContext]):
    """Register a constructor for a given context implementation type

    Args:
        context_class:  Type[ITlsContext]
            The ITlsContext child class to register
    """
    _TlsContextSingletonFactory.register(context_class)


## Implementation Details ######################################################


class _TlsContextSingletonFactory:
    """The _TlsContextSingletonFactory will manage a singleton instance of an
    ITlsContext based on the session's config.
    """

    # The section of the app_config that will hold the config
    _APP_CONFIG_SECTION = "tls"
    _CONFIG_TYPE_FIELD = "type"

    # Internal class dict holding the registered types
    _registered_types = {}

    # Singleton instance details
    _instance = None
    _instance_deploy_id = None

    ## Interface ###############################################################

    @classmethod
    def get_tls_context(
        cls,
        session: Session,
        config_overrides: Optional[dict] = None,
    ) -> ITlsContext:
        """Get an instance of the configured implementation of the tls context

        Args:
            session:  Session
                The current deploy session
            config_overrides:  Optional[dict]
                Optional runtime config values. These will overwrite any values
                pulled from the session.config

        Returns:
            tls_context:  ITlsContext
                The constructed instance of the context
        """
        # Check to see if this instance already exists
        if cls._instance is None or cls._instance_deploy_id != session.id:
            log.debug("Constructing TlsContext for [%s]", session.id)

            # Get the config
            tls_config = merge_configs(
                session.config.get(cls._APP_CONFIG_SECTION),
                config_overrides or {},
            )
            assert_config(
                tls_config is not None,
                f"Missing required config section: {cls._APP_CONFIG_SECTION}",
            )
            type_label = tls_config.get(cls._CONFIG_TYPE_FIELD)
            type_class = cls._registered_types.get(type_label)
            assert_config(
                type_class is not None,
                f"Cannot construct unknown TlsContext type [{type_label}]",
            )

            # Construct the instance
            cls._instance = type_class(session=session, config=tls_config)
            cls._instance_deploy_id = session.id

        log.debug2("Returning TlsContext for [%s]", session.id)
        return cls._instance

    @classmethod
    def register(cls, context_class: Type[ITlsContext]):
        """Register a constructor for a given context implementation type

        Args:
            context_class:  Type[ITlsContext]
                The ITlsContext child class to register
        """
        assert hasattr(context_class, ITlsContext._TYPE_LABEL_ATTRIBUTE), (
            "All derived ITlsContext classes must have an attribute "
            f"{ITlsContext._TYPE_LABEL_ATTRIBUTE}"
        )
        type_label = getattr(context_class, ITlsContext._TYPE_LABEL_ATTRIBUTE)
        if type_label in cls._registered_types:
            log.warning(
                "Received non-unique %s for %s: %s",
                ITlsContext._TYPE_LABEL_ATTRIBUTE,
                context_class,
                type_label,
            )
        log.debug2("Registering tls context type [%s]", type_label)
        cls._registered_types[type_label] = context_class
