"""
Shared test config
"""
# Standard
from unittest import mock
import sys

# Third Party
import pytest

# Local
from oper8.reconcile import ReconcileManager
from oper8.test_helpers.helpers import configure_logging, version_safe_md5

configure_logging()


@pytest.fixture(autouse=True)
def no_local_kubeconfig():
    """This fixture makes sure the tests run as if KUBECONFIG is not exported in
    the environment, even if it is
    """
    with mock.patch(
        "kubernetes.config.new_client_from_config", side_effect=RuntimeError
    ):
        yield


@pytest.fixture(autouse=True)
def no_unimport_oper8_mods():
    """Since our helper classes themselves live within the top of oper8, the
    real logic for unimporting the controller modules will recursively reimport
    _all_ oper8.* modules. This causes a lot of problems with tests like
    misbehaving mocks, Controller is not Controller, etc...
    """
    real_unimport = ReconcileManager._unimport_controller_module

    @staticmethod
    def _patched_unimport(module_name):
        oper8_mods = {
            mod_name: mod
            for mod_name, mod in sys.modules.items()
            if mod_name.startswith("oper8.")
            and not mod_name.startswith("oper8.test_helpers")
        }
        reimport_modules = real_unimport(module_name)
        for mod_name, mod in oper8_mods.items():
            sys.modules.setdefault(mod_name, mod)
        return reimport_modules

    with mock.patch(
        "oper8.reconcile.ReconcileManager._unimport_controller_module",
        new=_patched_unimport,
    ):
        yield
