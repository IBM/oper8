"""
Shared test config
"""
# Standard
from unittest import mock

# Third Party
import pytest

# Local
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
