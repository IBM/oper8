"""
Make sure that redis interfaces import cleanly
"""

# Third Party
import pytest

# Local
from oper8.x.datastores.redis.interfaces import IRedisComponent


def test_is_abstract():
    with pytest.raises(TypeError):
        IRedisComponent()
