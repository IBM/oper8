"""
Tests for the library config module

NOTE: Python makes it hard to change env vars in a way that will effect import
    time, so we're relying on the fact that aconfig is well tested and not
    actually validating the env-var override behavior!
"""

# Third Party
import pytest

# First Party
import aconfig

# Local
from oper8 import config


def test_config_keys():
    """Make sure that at least one expected key is present"""
    assert hasattr(config, "deploy_retries")
    assert isinstance(config.deploy_retries, int)


########################
## get_invalid_params ##
########################


def test_get_invalid_params_all_valid_params():
    """Test that get_invalid_params returns no invalid params when all are set
    to valid values
    """
    assert not config.validation.get_invalid_params(
        config=aconfig.Config({"key": 1}),
        validation_config=aconfig.Config({"key": {"type": "int", "min": 0, "max": 1}}),
    )


def test_get_invalid_params_all_invalid_params():
    """Test that get_invalid_params returns an invalid param when all are set
    to invalid values
    """
    assert config.validation.get_invalid_params(
        config=aconfig.Config({"key": 3}),
        validation_config=aconfig.Config(
            {"key": {"type": "int", "min": 0, "max": 1}},
        ),
    ) == ["key"]


def test_get_invalid_params_some_invalid_params():
    """Test that get_invalid_params returns only the invalid parameters when
    some are invalid and some are valid
    """
    assert config.validation.get_invalid_params(
        config=aconfig.Config({"key": 3, "str": "foo"}),
        validation_config=aconfig.Config(
            {
                "key": {"type": "int", "min": 0, "max": 1},
                "str": {"type": "str", "min_len": 1},
            },
        ),
    ) == ["key"]


#####################
## parameter types ##
#####################


def test_number_parameter():
    """Test all validation cases for _NumberParameter"""
    ParamType = config.validation._NumberParameter

    # Valid Cases
    assert ParamType().validate(1)
    assert ParamType().validate(1.2)
    assert ParamType(min=0).validate(1)
    assert ParamType(max=1).validate(0.5)
    assert ParamType(optional=True).validate(None)

    # Invalid Cases
    assert not ParamType().validate("not a number")
    assert not ParamType(min=0).validate(-1)
    assert not ParamType(max=1).validate(1.5)
    assert not ParamType(optional=False).validate(None)


def test_int_parameter():
    """Test all validation cases for _IntParameter"""
    ParamType = config.validation._IntParameter

    # Valid Cases
    assert ParamType().validate(1)
    assert ParamType(min=0).validate(1)
    assert ParamType(max=1).validate(1)
    assert ParamType(optional=True).validate(None)

    # Invalid Cases
    assert not ParamType().validate("not an int")
    assert not ParamType().validate(1.2)
    assert not ParamType(min=0).validate(-1)
    assert not ParamType(max=1).validate(2)
    assert not ParamType(optional=False).validate(None)


def test_float_parameter():
    """Test all validation cases for _FloatParameter"""
    ParamType = config.validation._FloatParameter

    # Valid Cases
    assert ParamType().validate(1.0)
    assert ParamType(min=0.0).validate(1.0)
    assert ParamType(max=1.0).validate(1.0)
    assert ParamType(optional=True).validate(None)

    # Invalid Cases
    assert not ParamType().validate("not an int")
    assert not ParamType().validate(1)
    assert not ParamType(min=0).validate(-1.0)
    assert not ParamType(max=1).validate(2.0)
    assert not ParamType(optional=False).validate(None)


def test_str_parameter():
    """Test all validation cases for _StrParameter"""
    ParamType = config.validation._StrParameter

    # Valid Cases
    assert ParamType().validate("test")
    assert ParamType(min_len=1).validate("test")
    assert ParamType(max_len=4).validate("test")
    assert ParamType(optional=True).validate(None)

    # Invalid Cases
    assert not ParamType().validate(1)
    assert not ParamType().validate(b"test")
    assert not ParamType(min_len=1).validate("")
    assert not ParamType(max_len=3).validate("test")
    assert not ParamType(optional=False).validate(None)


def test_bool_parameter():
    """Test all validation cases for _BoolParameter"""
    ParamType = config.validation._BoolParameter

    # Valid Cases
    assert ParamType().validate(True)
    assert ParamType().validate(False)
    assert ParamType(optional=True).validate(None)

    # Invalid Cases
    assert not ParamType().validate(1)
    assert not ParamType(optional=False).validate(None)


def test_enum_parameter():
    """Test all validation cases for _EnumParameter"""
    ParamType = config.validation._EnumParameter

    # Invalid Construction
    with pytest.raises(AssertionError):
        ParamType(values=[])
    with pytest.raises(AssertionError):
        ParamType(values="test")

    # Valid Cases
    assert ParamType(values=[1, "two", None]).validate(1)
    assert ParamType(values=[1, "two", None]).validate("two")
    assert ParamType(values=[1, "two", None]).validate(None)
    assert ParamType(values=[1, "two"], optional=True).validate(None)

    # Invalid Cases
    assert not ParamType(values=[1, "two", None]).validate(2)
    assert not ParamType(values=[1, "two"], optional=False).validate(None)


def test_list_parameter():
    """Test all validation cases for _ListParameter"""
    ParamType = config.validation._ListParameter

    # Valid Cases
    assert ParamType().validate([])
    assert ParamType().validate([1.2])
    assert ParamType(min_len=0).validate([1, "two"])
    assert ParamType(max_len=1).validate([0.5])
    assert ParamType(item_type="int").validate([1, 2, 3])
    assert ParamType(optional=True).validate(None)

    # Invalid Cases
    assert not ParamType().validate("not a list")
    assert not ParamType(min_len=1).validate([])
    assert not ParamType(max_len=1).validate([1, 2])
    assert not ParamType(item_type="str").validate([1, 2])
    assert not ParamType(item_type="str").validate(["one", 2])
    assert not ParamType(optional=False).validate(None)


#############
## factory ##
#############


def test_construct_parameter_all_known_types():
    """Make sure that all known types (as of the time of this writing) can be
    constructed via the factory
    """
    assert isinstance(
        config.validation._construct_parameter({"type": "number"}),
        config.validation._NumberParameter,
    )
    assert isinstance(
        config.validation._construct_parameter({"type": "int", "min": 1}),
        config.validation._IntParameter,
    )
    assert isinstance(
        config.validation._construct_parameter({"type": "float", "max": 2}),
        config.validation._FloatParameter,
    )
    assert isinstance(
        config.validation._construct_parameter({"type": "str", "min_len": 1}),
        config.validation._StrParameter,
    )
    assert isinstance(
        config.validation._construct_parameter({"type": "bool"}),
        config.validation._BoolParameter,
    )
    assert isinstance(
        config.validation._construct_parameter({"type": "enum", "values": [1]}),
        config.validation._EnumParameter,
    )


def test_construct_parameter_extra_params_error():
    """Make sure that a param construction call with bad arguments raises an
    error
    """
    with pytest.raises(TypeError):
        config.validation._construct_parameter({"type": "number", "foo": "bar"})


def test_construct_parameter_missing_params_error():
    """Make sure that a param construction call that is missing required params
    raises an error
    """
    with pytest.raises(TypeError):
        config.validation._construct_parameter({"type": "enum"})


def test_construct_parameter_unknown_type():
    """Test that constructing a parameter returns None when the value of 'type'
    doesn't match a configured type
    """
    assert config.validation._construct_parameter({"type": "foobar"}) is None


#############
## parsing ##
#############


def test_parse_validation_config_nested_key():
    """Make sure that parsing a validation config with nested keys works"""
    assert list(
        config.validation._parse_validation_config(
            aconfig.Config({"foo": {"bar": {"baz": {"type": "int"}}}})
        ).keys()
    ) == ["foo.bar.baz"]


def test_parse_validation_config_nested_type_key():
    """Make sure that parsing a validation config is robust to having the key
    'type' not represent an actual parameter
    """
    assert list(
        config.validation._parse_validation_config(
            aconfig.Config({"foo": {"type": {"baz": {"type": "int"}}}})
        ).keys()
    ) == ["foo.type.baz"]
