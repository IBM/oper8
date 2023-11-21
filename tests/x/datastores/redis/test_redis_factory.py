"""
Tests of the redis factory
"""
# Third Party
import pytest

# Local
from oper8.test_helpers.helpers import setup_session
from oper8.x.datastores.redis.factory import RedisFactory

## Helpers #####################################################################


def get_config_overrides(config):
    return {RedisFactory.DATASTORE_TYPE: config}


## Error cases #################################################################


def test_construct_unknown():
    """ " Test error when unsupport type to RedisFactory"""
    session = setup_session(app_config=get_config_overrides({"type": "UnknownType"}))
    with pytest.raises(AssertionError):
        RedisFactory.get_component(session)


def test_construct_notype():
    """ " Test error when no type specified to RedisFactory"""
    session = setup_session(app_config=get_config_overrides({"type": "ToRemove"}))
    if session.config.redis["type"] is not None:
        del session.config.redis["type"]
    with pytest.raises(AssertionError):
        RedisFactory.get_component(session)
