"""
Module to validate values in a loaded config
"""

# Standard
from typing import Any, Dict, List, Optional, Union
import abc
import builtins

# First Party
import aconfig
import alog

# Local
from .. import constants
from ..utils import nested_get  # pylint: disable=cyclic-import

log = alog.use_channel("CONFG")


################################################################################
## Public ######################################################################
################################################################################


def get_invalid_params(
    config: aconfig.Config,
    validation_config: aconfig.Config,
) -> List[str]:
    """Get a list of any params that are invalid

    Args:
        config:  aconfig.Config
            The parsed config with any override values
        validation_config:  aconfig.Config
            The parallel config holding validation setup

    Returns:
        invalid_params:  List[str]
            A list of all string keys for parameters that fail validation
    """

    # For each validation element, perform the validation
    invalid_params = []
    for val_key, validator in _parse_validation_config(validation_config).items():
        if not validator.validate(nested_get(config, val_key)):
            log.warning("Found invalid config key [%s]", val_key)
            invalid_params.append(val_key)

    # Return the list of invalid params
    return invalid_params


################################################################################
## Implementation ##############################################################
################################################################################


## Base Class ##################################################################

# NOTE: Pylint dislikes classes with a single public member function, but
#   they're useful here to define the inheritance structure, so we disable this
#   warning and re-enable it later.
#
# pylint: disable=too-few-public-methods


class _ValidatedParameter(abc.ABC):
    """This class represents a parameter with type and value validation"""

    def __init__(
        self,
        valid_types: List[type],
        optional: bool = False,
    ):
        """Construct with the set of valid types

        Args:
            valid_types:  List[type]
                The list of valid types for this parameter
            optional:  bool
                Whether or not the parameter is optional
        """
        assert len(valid_types) > 0, "Must specify at least one valid type"
        assert all(
            isinstance(typ, type) for typ in valid_types
        ), "Got a non-type in valid_types"
        self.valid_types = valid_types
        self.optional = optional

    def validate(self, value: Any) -> bool:
        """Run the validation for a read value

        Args:
            value:  Any
                The value to validate against this parameter

        Returns:
            valid:  bool
                True if the value is valid, False otherwise
        """
        # If the parameter is optional and the value is None, it's valid
        if self.optional and value is None:
            return True

        # Make sure the type is valid
        if not any(isinstance(value, valid_type) for valid_type in self.valid_types):
            log.warning("Invalid type <%s>", type(value))
            return False

        # Make sure the value is valid
        valid_value = self._validate_value(value)
        if not valid_value:
            log.warning("Invalid value [%s]", value)
        return valid_value

    @abc.abstractmethod
    def _validate_value(self, value: Any) -> bool:
        """All child classes must provide value validation that is specific to
        the given type
        """


## Validators ##################################################################


############
## number ##
############


class _NumberParameter(_ValidatedParameter):
    """A parameter that must be a number type and has optional bounds"""

    TYPES = [int, float]
    TYPE_KEY = "number"

    def __init__(
        self,
        *,
        min: Optional[Union[int, float]] = None,  # pylint: disable=redefined-builtin
        max: Optional[Union[int, float]] = None,  # pylint: disable=redefined-builtin
        **kwargs,
    ):
        """Construct with optional bounds

        NOTE: The builtin min/max names are used here so that the arguments have
            the appropriate intuitive names in the configuration yaml file

        Kwargs:
            min:  Optional[Union[int, float]]
                If not None, minimum value (inclusive) allowed
            max:  Optional[Union[int, float]]
                If not None, maximum value (inclusive) allowed
        """
        super().__init__(valid_types=self.TYPES, **kwargs)
        self._min = min
        self._max = max

    def _validate_value(self, value: Union[int, float]) -> bool:
        """Validate the value against the configured bounds"""
        return (self._min is None or value >= self._min) and (
            self._max is None or value <= self._max
        )


#########
## int ##
#########


class _IntParameter(_NumberParameter):
    """A number parameter that must be an int"""

    TYPES = [int]
    TYPE_KEY = "int"

    def __init__(self, *_, **kwargs):
        super().__init__(**kwargs)


###########
## float ##
###########


class _FloatParameter(_NumberParameter):
    """A number parameter that must be an float"""

    TYPES = [float]
    TYPE_KEY = "float"

    def __init__(self, *_, **kwargs):
        super().__init__(**kwargs)


#########
## str ##
#########


class _StrParameter(_ValidatedParameter):
    """A parameter that must be of type str and has optional length bounds"""

    TYPES = [str]
    TYPE_KEY = "str"

    def __init__(
        self,
        *,
        min_len: Optional[int] = None,
        max_len: Optional[int] = None,
        **kwargs,
    ):
        """Construct with optional length bounds

        Kwargs:
            min_len:  Optional[int]
                If not None, minimum length (inclusive) allowed
            max_len:  Optional[int]
                If not None, maximum length (inclusive) allowed
        """
        super().__init__(valid_types=self.TYPES, **kwargs)
        self._min_len = min_len
        self._max_len = max_len

    def _validate_value(self, value: str) -> bool:
        """Validate the value against the configured length bounds"""
        return (self._min_len is None or len(value) >= self._min_len) and (
            self._max_len is None or len(value) <= self._max_len
        )


##########
## bool ##
##########


class _BoolParameter(_ValidatedParameter):
    """A parameter that must be a bool"""

    TYPES = [bool]
    TYPE_KEY = "bool"

    def __init__(self, *_, **kwargs):
        """Pass through constructor"""
        super().__init__(valid_types=self.TYPES, **kwargs)

    def _validate_value(self, value: bool) -> bool:
        """Value validation is a no-op for bool"""
        return True


##########
## enum ##
##########


class _EnumParameter(_ValidatedParameter):
    """A parameter with a fixed set of valid str or int values"""

    TYPES = [str, int, type(None)]
    TYPE_KEY = "enum"

    def __init__(self, *, values: List[Union[str, int, type(None)]], **kwargs):
        """Construct with the list of enum values

        Args:
            values:  List[Union[str, int, type(None)]]
                The list of valid values for this enum. Note that it can be a
                mix of any of the valid value types
        """
        super().__init__(valid_types=self.TYPES, **kwargs)
        assert (
            isinstance(values, list) and values
        ), "Must specify at least one enum value!"
        self.values = values

    def _validate_value(self, value: Union[str, int, type(None)]) -> bool:
        """Make sure the given value is in the configured value set"""
        return value in self.values


##########
## list ##
##########


class _ListParameter(_ValidatedParameter):
    """A parameter that must be of type list and has optional constraints around
    the type and count of elements
    """

    TYPES = [list]
    TYPE_KEY = "list"

    def __init__(
        self,
        *,
        min_len: Optional[int] = None,
        max_len: Optional[int] = None,
        item_type: Optional[str] = None,
        **kwargs,
    ):
        """Construct with optional length bounds

        Kwargs:
            min_len:  Optional[int]
                Minimum number of items that must be provided
            max_len:  Optional[int]
                Maximum number of items that must be provided
            item_type:  Optional[str]
                String name of the type that the items must be (e.g. "str")
        """
        super().__init__(valid_types=self.TYPES, **kwargs)
        self._min_len = min_len
        self._max_len = max_len
        self._item_type = None
        if item_type is not None:
            assert hasattr(builtins, item_type), f"Unsupported item_type: {item_type}"
            self._item_type = getattr(builtins, item_type)
            assert (
                self._item_type is not None
            ), f"Failed to fetch item_type: {item_type}"

    def _validate_value(self, value: str) -> bool:
        """Validate the value against the configured length bounds and item type"""
        return (
            (self._min_len is None or len(value) >= self._min_len)
            and (self._max_len is None or len(value) <= self._max_len)
            and (
                self._item_type is None
                or all(isinstance(item, self._item_type) for item in value)
            )
        )


# Re-enable the pylint warning
# pylint: enable=too-few-public-methods

## Factory #####################################################################


def _create_factory_map(param_class, factory_map=None):
    """Helper to recursively create the singleton factory map"""
    factory_map = factory_map or {}

    # Add this class if it's not abstract
    if not param_class.__abstractmethods__:
        factory_map[param_class.TYPE_KEY] = param_class

    # Recurse
    for subclass in param_class.__subclasses__():
        factory_map = _create_factory_map(subclass, factory_map)

    return factory_map


# Global map from type keys to parameter type classes
_factory_map = _create_factory_map(_ValidatedParameter)


def _construct_parameter(param_args: Dict[str, Any]) -> _ValidatedParameter:
    """Construct a _ValidatedParameter from the given args parsed out of a
    validation file.

    Args:
        param_args:  Dict[str, Any]
            The key/value pairs for this parameter

    Returns:
        parameter:  Optional[_ValidatedParameter]
            The constructed _ValidatedParameter based on the given args if the
            type is known. If unkonwn, None is returned.
    """
    assert "type" in param_args, "All parameters must have a 'type'"
    param_type = param_args["type"]
    if not (isinstance(param_type, str) and param_type in _factory_map):
        return None
    param_args.pop("type")
    return _factory_map[param_type](**param_args)


## Parsing #####################################################################


def _parse_validation_config(
    validation_config: aconfig.Config,
    prefix_parts: Optional[List[str]] = None,
) -> Dict[str, _ValidatedParameter]:
    """Recursively parse the given validation file into a dict of nested keys
    pointing to _ValidatedParameter instances.
    """
    output_dict = {}
    prefix_parts = prefix_parts or []
    for key, val in validation_config.items():
        assert isinstance(key, str), "Only string keys allowed!"
        if isinstance(val, dict):
            # Make the nested key
            key_parts = prefix_parts + [key]
            nested_key = constants.NESTED_DICT_DELIM.join(key_parts)

            # If the dict has a "type" field, try parsing it as a validated
            # parameter
            param = None
            if "type" in val:
                log.debug3(
                    "Attempting to construct parameter at [%s]: %s", nested_key, val
                )
                param = _construct_parameter(val)

            # If a valid param was parsed, update the config
            if param:
                log.debug3("Found parameter at %s", nested_key)
                output_dict[nested_key] = param

            # Otherwise, recurse
            else:
                log.debug3("Recursing into %s", nested_key)
                output_dict.update(
                    _parse_validation_config(
                        val,
                        prefix_parts=key_parts,
                    )
                )

    return output_dict
