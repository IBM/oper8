"""
This module holds helpers that rely on oper8.x
"""
# Standard
import os

# Local
from .helpers import TEST_DATA_DIR
from oper8.session import Session
from oper8.x.utils import common
from oper8.x.utils.tls_context.internal import InternalCaComponent


def set_object_test_state(
    session: Session,
    kind: str,
    name: str,
    value,
    scoped_name: bool = True,
    parent_component_name: str = "test",
):
    obj_name = (
        common.get_resource_cluster_name(
            resource_name=name,
            component=parent_component_name,
            session=session,
        )
        if scoped_name
        else name
    )
    value.setdefault("metadata", {}).setdefault("name", obj_name)
    value.setdefault("metadata", {}).setdefault(
        "namespace", session.namespace or "default"
    )
    value.setdefault("kind", kind)
    success, changed = session.deploy_manager.deploy([value])
    assert success, "Failed to set test state!"
    return success, changed


def set_secret_data(
    session: Session,
    name,
    data=None,
    string_data=None,
    secret_type="generic",
    scoped_name: bool = True,
):
    set_object_test_state(
        session,
        "Secret",
        name,
        {"type": secret_type, "data": data, "stringData": string_data},
        scoped_name=scoped_name,
    )


def set_tls_ca_secret(session):
    """Set the key/cert content for the shared CA secret. This function returns
    the pem-encoded values for convenience in other tests
    """
    with open(os.path.join(TEST_DATA_DIR, "test_ca.key")) as f:
        key_pem = f.read()
    with open(os.path.join(TEST_DATA_DIR, "test_ca.crt")) as f:
        crt_pem = f.read()
    set_secret_data(
        session,
        InternalCaComponent.CA_SECRET_NAME,
        data={
            InternalCaComponent.CA_KEY_FILENAME: common.b64_secret(key_pem),
            InternalCaComponent.CA_CRT_FILENAME: common.b64_secret(crt_pem),
        },
    )

    return key_pem, crt_pem
