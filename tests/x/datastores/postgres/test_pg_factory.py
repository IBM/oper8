"""
Tests of the postgres factory
"""


# Local
from oper8.test_helpers.helpers import setup_session_ctx
from oper8.x.datastores.postgres.factory import PostgresFactory
from tests.x.datastores.postgres.util import (
    POSTGRES_PROVIDED_CONNECTION_DEPLOY_CONFIG_OVERRIDES,
    set_postgres_secrets,
)

## Helpers #####################################################################


def get_config_overrides(config):
    return {"postgres": config}


## Tests #######################################################################


def test_get_component_with_provided_connection():
    """Test that a provided connection passed in through the CR returns None on a get_component call"""
    override_deploy_configs = POSTGRES_PROVIDED_CONNECTION_DEPLOY_CONFIG_OVERRIDES

    with setup_session_ctx(deploy_config=override_deploy_configs) as session:
        set_postgres_secrets(session)
        component = PostgresFactory.get_component(session)
        # Since the component is provided, we should be getting None back
        assert component is None
