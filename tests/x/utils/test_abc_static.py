"""
Tests for the ABCStatic class base
"""

# Standard
import abc

# Third Party
import pytest

# Local
from oper8.x.utils.abc_static import ABCStatic


class Base(ABCStatic):
    @classmethod
    def foo(cls):
        """A classmethod that is implemented in the base"""
        return True

    def bar(self):
        """A standard instance method on the base class"""
        return 42

    @classmethod
    @abc.abstractmethod
    def baz(cls, arg):
        """An abstractmethod that is also a classmethod"""

    @abc.abstractmethod
    def bat(self, arg):
        """An abstractmethod that is NOT a classmethod"""


## Tests #######################################################################


def test_correct_implementation():
    """Make sure that defining a class which follows the correct @classmethod
    definitions defines cleanly and can be used as expected
    """

    class Foo(Base):
        @classmethod
        def baz(cls, arg):
            return arg + 1

        def bat(self, arg):
            return arg + 2

    assert Foo.foo()
    assert Foo().bar() == 42
    assert Foo.baz(1) == 2
    assert Foo().bat(1) == 3


def test_staticmethod_implementation():
    """Test that an abstract classmethod can be implemented with a @staticmethod
    in the child
    """

    class Foo(Base):
        @staticmethod
        def baz(arg):
            return arg + 1

        def bat(self, arg):
            return arg + 2

    assert Foo.foo()
    assert Foo().bar() == 42
    assert Foo.baz(1) == 2
    assert Foo().bat(1) == 3


def test_classmethod_implemented_with_star_args():
    """Test that an abstract classmethod which is implemented with *args,
    **kwargs is ok
    """

    class Foo(Base):

        BASE = 10

        @classmethod
        def baz(cls, *args, **kwargs):
            return cls.BASE + 1

        def bat(self, arg):
            return arg + 2

    assert Foo.foo()
    assert Foo.baz(1) == 11


def test_incorrect_instancemethod_implementation():
    """Test that implementing an abstractmethod/classmethod as an instance
    method results in a declaration-time exception.
    """
    with pytest.raises(NotImplementedError):

        class Foo(Base):
            def __init__(self, x):
                self.x = x

            def baz(self, arg):
                return self.x + 1

            def bat(self, arg):
                return arg + 2


def test_abstractclassmethod_cannot_be_called():
    """Test that an abstractmethod/classmethod which is not implemented by a
    child class raises when called
    """

    class Foo(Base):
        pass

    with pytest.raises(NotImplementedError):
        Foo.baz(1)
