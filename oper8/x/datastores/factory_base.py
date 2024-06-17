"""
The DatastoreSingletonFactoryBase class defines the common functionality that
all datastore type factories will use. It implements common logic for
constructing named singleton instances of a given datastore type.
"""

# Standard
from typing import Optional

# First Party
import alog

# Local
from ... import Component, Session
from ...utils import merge_configs
from ..utils import constants
from .connection_base import DatastoreConnectionBase
from .interfaces import Datastore

## Factory Base ################################################################

log = alog.use_channel("FCTRY")


class classproperty:
    """@classmethod+@property
    CITE: https://stackoverflow.com/a/22729414
    """

    def __init__(self, func):
        self.func = classmethod(func)

    def __get__(self, *args):
        return self.func.__get__(*args)()


class DatastoreSingletonFactoryBase:
    """The DatastoreSingletonFactoryBase manages instances of all datastore
    types as singletons on a per-deployment basis. It provides functionality for
    derived classes to define a specific DATASTORE_TYPE (e.g. redis) and
    register implementations of that type.

    The instances of each type are held as singletons scoped to the individual
    deployment (session.deploy_id). This is done to support multiple calls to
    fetch a named instance within a given deployment without reconstructing, but
    to allow configuration to change between deploys.
    """

    ## Private Members #########################################################

    # Singleton dict of constructors for each implementation type
    _type_constructors = {}

    # Singleton dict of named components
    _components = {}

    # Singleton dict of named connections
    _connections = {}

    # Class attribute that all individual factory types must have.
    # NOTE: This will be used as the key in the CR's datastores section
    _DATASTORE_TYPE_ATTRIBUTE_NAME = "DATASTORE_TYPE"

    # Class attribute that must be defined on an implementation to define the
    # common connection type
    _CONNECTION_TYPE_ATTRIBUTE = "CONNECTION_TYPE"

    ## Public interface ########################################################

    @classproperty
    def datastore_type(cls):
        return getattr(cls, cls._DATASTORE_TYPE_ATTRIBUTE_NAME)

    @classproperty
    def connection_type(cls):
        return getattr(cls, cls._CONNECTION_TYPE_ATTRIBUTE)

    @classmethod
    def get_component(
        cls,
        session: Session,
        name: Optional[str] = None,
        disabled: bool = False,
        config_overrides: Optional[dict] = None,
    ) -> Optional[Component]:
        """Construct an instance of the datastore type's component

        Args:
            session:  Session
                The session for the current deployment
            name:  Optional[str]
                The name of the singleton instance to get. If not provided, a
                top-level instance is used (e.g. datastores.postgres.type)
            disabled:  bool
                Whether or not the component is disabled in this deployment
            config_overrides:  Optional[dict]
                Optional runtime config values. These will overwrite any values
                pulled from the session.config

        Returns:
            instance:  Optional[Component]
                The constructed component if one is needed
        """
        return cls._get_component(
            session=session,
            name=name,
            disabled=disabled,
            config_overrides=config_overrides,
        )

    @classmethod
    def get_connection(
        cls,
        session: Session,
        name: Optional[str] = None,
        allow_from_component: bool = True,
    ) -> DatastoreConnectionBase:
        """Get the connection details for a named instance of the datastore type

        If not pre-constructed by the creation of the Component, connection
        details are pulled from the CR directly
        (spec.datastores.<datastore_type>.[<name>].connection)

        Args:
            session:  Session
                The session for the current deployment
            name:  Optional[str]
                The name of the singleton instance to get. If not provided, a
                top-level instance is used
            allow_from_component:  bool
                If True, use connection info from the component

        Returns:
            connection:  DatastoreConnectionBase
                The connection for this instance
        """
        return cls._get_connection(session, name, allow_from_component)

    @classmethod
    def register_type(cls, type_class: Datastore):
        """Register a new type constructor

        Args:
            type_class:  Datastore
                The class that will be constructed with the config for
        """
        cls._validate_class_attributes()

        assert issubclass(
            type_class, Datastore
        ), "Datastore types use component_class=Datastore"
        datastore_type_classes = cls._type_constructors.setdefault(
            cls.datastore_type, {}
        )
        if type_class.TYPE_LABEL in datastore_type_classes:
            log.warning("Got duplicate registration for %s", type_class.TYPE_LABEL)
        datastore_type_classes[type_class.TYPE_LABEL] = type_class

    ## Implementation Details ##################################################

    @classmethod
    def _validate_class_attributes(cls):
        """Since this class is always used statically, this helper makes sure
        the expected class attributes
        """
        assert isinstance(
            getattr(
                cls, DatastoreSingletonFactoryBase._DATASTORE_TYPE_ATTRIBUTE_NAME, None
            ),
            str,
        ), "Incorrectly configured datastore [{}]. Must define str [{}]".format(
            cls, DatastoreSingletonFactoryBase._DATASTORE_TYPE_ATTRIBUTE_NAME
        )
        connection_type = getattr(
            cls, DatastoreSingletonFactoryBase._CONNECTION_TYPE_ATTRIBUTE, None
        )
        assert isinstance(connection_type, type) and issubclass(
            connection_type, DatastoreConnectionBase
        ), (
            f"Incorrectly configured datastore [{cls}]. Must define "
            f"[{DatastoreSingletonFactoryBase._CONNECTION_TYPE_ATTRIBUTE}] as "
            "a DatastoreConnectionBase type"
        )

    @classmethod
    def _get_component(
        cls,
        session: Session,
        name: Optional[str] = None,
        disabled: bool = False,
        config_overrides: Optional[dict] = None,
        allow_instantiation: bool = True,
    ) -> Optional[Component]:
        """Implementation detail for get_component which can be called by
        get_connection and disallow lazy creation of the singleton.
        """
        cls._validate_class_attributes()

        # First, check to see if there's a connection already available based on
        # conneciton details in the CR. If so, we won't create the component
        conn = cls._get_connection(session, name, allow_from_component=False)
        if conn is not None:
            log.debug(
                "Found connection for [%s] in the CR. Not constructing the component.",
                cls.datastore_type,
            )
            return None

        # Get the pre-existing instances for this datastore type (keyed by the
        # datastore subclass)
        datastore_components = cls._components.setdefault(cls.datastore_type, {})

        # Get the app config section for this instance by name
        instance_config = merge_configs(
            session.config.get(cls.datastore_type, {}),
            config_overrides or {},
        )
        log.debug4("Full config: %s", instance_config)
        if name is not None:
            instance_config = instance_config.get(name)
        assert (
            instance_config is not None
        ), f"Cannot construct unknown [{cls.datastore_type}] instance: {name}"
        assert (
            "type" in instance_config
        ), f"Missing required [type] key for [{cls.datastore_type}/{name}]"

        # Fetch the current instance/deploy_id
        instance, deploy_id = datastore_components.get(name, (None, None))

        # If the deploy_id has changed, remove any current instance
        if deploy_id != session.id:
            instance = None
            datastore_components.pop(name, None)

        # If there is not a valid instance and it's allowed, construct it
        if not instance and allow_instantiation:
            log.debug2(
                "Constructing [%s]/%s for the first time for deploy [%s]",
                cls.datastore_type,
                name,
                session.id,
            )
            type_key = instance_config.type

            # Fetch the class for this type of the datastore
            datastore_type_classes = cls._type_constructors.get(cls.datastore_type, {})
            type_class = datastore_type_classes.get(type_key)
            assert (
                type_class is not None
            ), f"Cannot construct unsupported type [{type_key}]"

            # If there is a name provided, create a wrapper component with the
            # given name
            if name is not None:
                instance_class_name = f"{type_class.name}-{name}"
                log.debug2("Wrapping %s with instance name override", type_class)

                class InstanceClass(type_class):
                    """Wrapper for {}/{} with instance naming""".format(
                        cls.datastore_type, type_key
                    )

                    name = instance_class_name

            else:
                log.debug2("No instance name wrapping needed for %s", type_class)
                InstanceClass = type_class
            log.debug("Constructing %s", type_key)
            instance = InstanceClass(
                session=session,
                config=instance_config,
                instance_name=name,
                disabled=disabled,
            )
            datastore_components[name] = (instance, session.id)

        # Return the singleton
        return instance

    @classmethod
    def _get_connection(
        cls,
        session: Session,
        name: Optional[str] = None,
        allow_from_component: bool = True,
    ) -> Optional[DatastoreConnectionBase]:
        """Implementation for get_connection that can be used by _get_component
        to fetch connections from the CR
        """
        cls._validate_class_attributes()

        # Get the pre-existing instances for this datastore type (keyed by the
        # datastore subclass)
        connection, deploy_id = cls._connections.get(cls.datastore_type, {}).get(
            name, (None, None)
        )

        # If there is no connection for this deploy already, deserialize it from
        # the CR
        if connection is None or deploy_id != session.id:
            log.debug("Constructing %s connection from config", cls.datastore_type)

            # Get the CR config for this datastore type
            ds_config = session.spec.get(constants.SPEC_DATASTORES, {}).get(
                cls.datastore_type, {}
            )
            if name is not None:
                ds_config = ds_config.get(name, {})
            ds_config = ds_config.get(constants.SPEC_DATASTORE_CONNECTION)
            log.debug3(
                "%s/%s connection config: %s", cls.datastore_type, name, ds_config
            )

            if ds_config is not None:
                # Deserialize connection from sub-cr connection specification
                connection = cls.connection_type.from_dict(session, ds_config)
                cls._connections.setdefault(cls.datastore_type, {})[name] = (
                    connection,
                    session.id,
                )
            elif allow_from_component:
                # Add the connection information for this instance
                instance = cls._get_component(session, name, allow_instantiation=False)
                assert (
                    instance is not None
                ), f"No instance or config available for {cls.datastore_type}"
                connection = instance.get_connection()
                assert isinstance(
                    connection, cls.connection_type
                ), f"Got incorrect [{cls.datastore_type}] connection type: {type(connection)}"
                cls._connections.setdefault(cls.datastore_type, {})[name] = (
                    connection,
                    session.id,
                )
            else:
                log.debug2(
                    "No connection details for %s found in CR", cls.datastore_type
                )
                return None

        # Return the connection singleton
        return cls._connections[cls.datastore_type][name][0]
