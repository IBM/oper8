"""
This module adds metaclass support for declaring an interface with
@abstractmethod methods that MUST be implemented as @classmethod or
@staticmethod
"""

# Standard
import abc
import inspect


class ABCStaticMeta(abc.ABCMeta):
    """The StaticABCMeta class is a metaclass that enforces implementations of
    base class functions marked as both @abstractmethod and @classmethod.
    Methods with this signature MUST be implemented with the @classmethod or
    @staticmethod decorator in derived classes.
    """

    def __init__(cls, name, bases, dct):
        # Find abstract class methods that have not been implemented at all
        attrs = {name: getattr(cls, name) for name in dir(cls)}
        cls.__abstract_class_methods__ = [
            name
            for name, attr in attrs.items()
            if inspect.ismethod(attr) and getattr(attr, "__isabstractmethod__", False)
        ]

        # For any abstract class methods that have not been implemented,
        # overwrite them to raise NotImplementedError if called
        for method_name in cls.__abstract_class_methods__:

            def not_implemented(*_, x=method_name, **__):
                raise NotImplementedError(f"Cannot invoke abstract class method {x}")

            not_implemented.__original_signature__ = inspect.signature(
                getattr(cls, method_name)
            )
            setattr(cls, method_name, not_implemented)

        # Look for abstract class methods of parents
        base_abstract_class_methods = {
            method_name: getattr(base, method_name)
            for base in bases
            for method_name in getattr(base, "__abstract_class_methods__", [])
            if method_name not in cls.__abstract_class_methods__
        }

        # If any parent abstract class methods have been implemented as instance
        # methods, raise an import-time exception
        for method_name, base_method in base_abstract_class_methods.items():
            # A local implementation is valid if it is a bound method (
            # implemented as a @classmethod) or it is a function with a
            # signature that exactly matches the signature of the base class
            # (implemented as @staticmethod).
            this_method = getattr(cls, method_name)
            is_classmethod = inspect.ismethod(this_method)
            original_signature = getattr(base_method, "__original_signature__", None)
            is_staticmethod = inspect.isfunction(this_method) and inspect.signature(
                this_method
            ) in [original_signature, inspect.signature(base_method)]
            if not (is_classmethod or is_staticmethod):
                raise NotImplementedError(
                    f"The method [{method_name}] is an @classmethod @abstractmethod. "
                    f"{cls} implements it as an instance method"
                )


class ABCStatic(metaclass=ABCStaticMeta):
    """An ABCStatic class is a child of abc.ABC which has support for enforcing
    methods which combine @classmethod and @abstractmethod
    """
