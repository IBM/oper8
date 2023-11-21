"""
Decorator for making the authoring of "pure" components easier
"""

# Standard
from typing import Callable, Dict, Optional, Type

# Local
from .component import Component
from .controller import Controller


def component(name: str) -> Callable[[Type], Type]:
    """The @component decorator is the primary entrypoint for creating an
    oper8.Component. It ensures the wrapped type's interface matches the expected
    Component interface, including the "name" class attribute.

    Args:
        name:  str
            The name string will be set as the class property for the wrapped
            class

    Returns:
        decorator:  Callable[[Type[Component]], Type[Component]]
            The decorator function that will be invoked on construction of
            decorated classes
    """

    def decorator(cls: Type[Component]) -> Type[Component]:
        cls.name = name
        return cls

    return decorator


def controller(  # pylint: disable=too-many-arguments
    group: str,
    version: str,
    kind: str,
    finalizer: str = None,
    extra_properties: Optional[Dict[str, any]] = None,
) -> Callable[[Type[Controller]], Type[Controller]]:
    """The @controller decorator is the primary entrypoint for creating an
    oper8.Controller. It ensures the wrapped type's interface matches the
    required Controller interface, including class properties.

    NOTE: The `extra_properties` argument is an entrypoint for loosely coupled
        Controller-specific configuration that is tied to the specific
        WatchManager implementation being used. The current list of useful
        properties is:

        * disable_vcs: This can be used to tell the AnsibleWatchManager
            that the Controller will not use ansible-vcs, even if other
            Controllers managed by the same operator do.
        * pwm_filters: This can be used to tell the PythonWatchManager of any
            additional watch filters. If value is a list then the filters are added
            to all watches including dependent watches. If value is a dict than
            it expects the keys to be the resource global id with the values being a list
            of filters for that resource
        * pwm_subsystems: This can be used to tell the PythonWatchManager of any
            subsystem relations. This allows a "subsystem" controller to be ran during
            the reconciliation of another similar to the DryRunWatchManager

    Args:
        group:  str
            The apiVersion group for the resource this controller manages
        version:  str
            The apiVersion version for the resource this controller manages
        kind:  str
            The kind for the resource this controller manages
        extra_properties:  Optional[Dict[str, any]]
            Extra properties that should be defined as class-properties for this
            controller

    Returns:
        decorator:  Callable[[Type[Controller]], Type[Controller]]
            The decorator function that will be invoked on construction of
            decorated classes
    """

    def decorator(cls: Type[Controller]) -> Type[Controller]:
        cls.group = group
        cls.version = version
        cls.kind = kind
        for key, val in (extra_properties or {}).items():
            setattr(cls, key, val)
        if finalizer is not None:
            cls.finalizer = finalizer
        return cls

    return decorator
