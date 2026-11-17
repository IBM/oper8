"""Tests for Oper8xComponent"""

# First Party
import aconfig

# Local
from oper8.test_helpers.helpers import setup_session
from oper8.x.oper8x_component import Oper8xComponent


class DummyOper8xComponent(Oper8xComponent):
    """Simple Oper8xComponent that renders a single Secret"""

    name = "dummy-oper8x"

    def build_chart(self, session):
        self.add_resource(
            "secret",
            {
                "apiVersion": "v1",
                "kind": "Secret",
                "metadata": {"name": "dummy-secret"},
                "type": "Opaque",
            },
        )


def test_config_namespace_override_applied():
    """Make sure that a per-component namespace override in the session config
    is applied to rendered objects. This is the supported way to deploy a
    component's resources into a namespace other than the CR's namespace. It
    is only safe because ReconcileManager._get_reconcile_config rejects
    namespace keys from CR-controlled config sources, so the override can only
    originate from trusted controller config (covered in test_reconcile.py).
    """
    session = setup_session(
        app_config=aconfig.Config(
            {DummyOper8xComponent.name: {"namespace": "component-ns"}},
            override_env_vars=False,
        )
    )
    comp = DummyOper8xComponent(session=session)
    comp.render_chart(session)
    objs = comp.to_config(session)
    assert len(objs) == 1
    assert objs[0].metadata.namespace == "component-ns"
    assert objs[0].metadata.namespace != session.namespace
