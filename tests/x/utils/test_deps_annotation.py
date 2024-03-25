"""
Test the functions for adding the dependency hash annotation
"""

# Standard
import hashlib
import shlex
import subprocess
import sys

# Third Party
import pytest

# First Party
import alog

# Local
from oper8.test_helpers.helpers import (
    MockComponent,
    MockDeployManager,
    configure_logging,
    setup_session,
)
from oper8.x.utils import deps_annotation

## Helpers #####################################################################

log = alog.use_channel("TEST")
configure_logging()


def make_data_resource(
    resource_type,
    api_version="v1",
    data=None,
    metadata=None,
    name="test",
    namespace=None,
):
    """Small helper utility for creading dummy cdk8s secrets"""
    metadata = metadata or {}
    if namespace:
        metadata["namespace"] = namespace
    metadata["name"] = name
    kwargs = {"data": data or {}, "metadata": metadata}
    return dict(
        kind=resource_type,
        apiVersion=api_version,
        **kwargs,
    )


def make_secret(*args, **kwargs):
    """Small helper utility for creading dummy cdk8s Secrets"""
    return make_data_resource("Secret", *args, **kwargs)


def make_cm(*args, **kwargs):
    """Small helper utility for creading dummy cdk8s ConfigMaps"""
    return make_data_resource("ConfigMap", *args, **kwargs)


def make_pod_spec(secret_envs=None, cm_envs=None, secret_vols=None, cm_vols=None):
    secret_envs = secret_envs or {}
    cm_envs = cm_envs or {}
    secret_vols = secret_vols or {}
    cm_vols = cm_vols or {}
    return {
        "spec": {
            "containers": [
                {
                    "name": "foo",
                    "image": "foo:latest",
                    "env": [
                        {
                            "name": env_name,
                            "valueFrom": {
                                "secretKeyRef": {"name": secret_name, "key": key}
                            },
                        }
                        for env_name, (secret_name, key) in secret_envs.items()
                    ]
                    + [
                        {
                            "name": env_name,
                            "valueFrom": {
                                "configMapKeyRef": {"name": cm_name, "key": key}
                            },
                        }
                        for env_name, (cm_name, key) in cm_envs.items()
                    ],
                }
            ],
            "volumes": [
                {"name": vol_name, "secret": {"secretName": secret_name}}
                for vol_name, secret_name in secret_vols.items()
            ]
            + [
                {"name": vol_name, "configMap": {"name": cm_name}}
                for vol_name, cm_name in cm_vols.items()
            ],
        }
    }


def make_pod_empty_value_from_spec():
    return {
        "spec": {
            "containers": [
                {
                    "name": "foo",
                    "image": "foo:latest",
                    "env": [
                        {
                            "name": "FOO",
                            "valueFrom": None,
                        }
                    ],
                }
            ]
        }
    }


def make_pod(name="test-pod", namespace=None, *args, **kwargs):
    metadata = dict(name=name)
    if namespace:
        metadata["namespace"] = namespace
    return dict(
        kind="Pod",
        apiVersion="v1",
        metadata=metadata,
        **make_pod_spec(*args, **kwargs),
    )


def make_deployment(pod, name="test-deployment"):
    return dict(
        kind="Deployment",
        apiVersion="apps/v1",
        metadata=dict(name=name),
        spec=dict(selector={}, template=pod),
    )


def make_sts(pod, name="test-sts"):
    return dict(
        kind="StatefulSet",
        apiVersion="v1",
        metadata=dict(name=name),
        spec=dict(selector={}, serviceName="foobar", template=pod),
    )


def make_rs(pod, name="test-rs", chart=None, app=None):
    return dict(
        kind="ReplicaSet",
        apiVersion="apps/v1",
        metadata=dict(name=name),
        spec=dict(selector={}, template=pod),
    )


## Tests #######################################################################


#########################
## add_deps_annotation ##
#########################


def test_add_deps_annotation_pod_secret_env():
    """Test that a component with a single pod that relies on a secret in an env
    var has the annotation added to it
    """
    secret = make_secret(name="secret", data={"key": "val"})
    pod = make_pod(secret_envs={"FOO": ("secret", "key")})
    session = setup_session()

    comp = MockComponent(api_objects=[secret, pod], session=session)
    comp.render_chart(session)
    deps_annotation.add_deps_annotation(comp, session, pod)
    assert (
        pod["metadata"]["annotations"].get(deps_annotation.DEPS_ANNOTATION) is not None
    )


def test_add_deps_annotation_pod_secret_vol():
    """Test that a component with a single pod that relies on a secret in a
    volume has the annotation added to it
    """
    secret = make_secret(name="secret", data={"key": "val"})
    pod = make_pod(secret_vols={"foobar": "secret"})
    session = setup_session()
    comp = MockComponent(api_objects=[secret, pod], session=session)
    comp.render_chart(session)
    deps_annotation.add_deps_annotation(comp, session, pod)
    assert (
        pod["metadata"]["annotations"].get(deps_annotation.DEPS_ANNOTATION) is not None
    )


def test_add_deps_annotation_pod_cm_env():
    """Test that a component with a single pod that relies on a cm in an env var
    has the annotation added to it
    """
    cm = make_cm(name="cm", data={"key": "val"})
    pod = make_pod(cm_envs={"FOO": ("cm", "key")})
    session = setup_session()
    comp = MockComponent(api_objects=[cm, pod], session=session)
    comp.render_chart(session)
    deps_annotation.add_deps_annotation(comp, session, pod)
    assert (
        pod["metadata"]["annotations"].get(deps_annotation.DEPS_ANNOTATION) is not None
    )


def test_add_deps_annotation_pod_cm_vol():
    """Test that a component with a single pod that relies on a cm in a volume
    has the annotation added to it
    """
    cm = make_cm(name="cm", data={"key": "val"})
    pod = make_pod(cm_vols={"foobar": "cm"})
    session = setup_session()
    comp = MockComponent(api_objects=[cm, pod], session=session)
    comp.render_chart(session)
    deps_annotation.add_deps_annotation(comp, session, pod)
    assert (
        pod["metadata"]["annotations"].get(deps_annotation.DEPS_ANNOTATION) is not None
    )


def test_add_deps_annotation_deployment_secret():
    """Test that a component with a single deployment that relies on a secret
    has the annotation added to it
    """
    secret = make_secret(name="secret", data={"key": "val"})
    deployment = make_deployment(
        pod=make_pod_spec(secret_envs={"FOO": ("secret", "key")})
    )
    session = setup_session()
    comp = MockComponent(api_objects=[secret, deployment], session=session)
    comp.render_chart(session)
    deps_annotation.add_deps_annotation(comp, session, deployment)
    assert (
        deployment["spec"]["template"]["metadata"]["annotations"].get(
            deps_annotation.DEPS_ANNOTATION
        )
        is not None
    )


def test_add_deps_annotation_statefulset_secret():
    """Test that a component with a single statefulset that relies on a secret
    has the annotation added to it
    """
    secret = make_secret(name="secret", data={"key": "val"})
    sts = make_sts(pod=make_pod_spec(secret_envs={"FOO": ("secret", "key")}))
    session = setup_session()
    comp = MockComponent(api_objects=[secret, sts], session=session)
    comp.render_chart(session)
    deps_annotation.add_deps_annotation(comp, session, sts)
    assert (
        sts["spec"]["template"]["metadata"]["annotations"].get(
            deps_annotation.DEPS_ANNOTATION
        )
        is not None
    )


def test_add_deps_annotation_replicaset_secret():
    """Test that a component with a single replicaset that relies on a secret
    has the annotation added to it
    """
    secret = make_secret(name="secret", data={"key": "val"})
    rs = make_rs(pod=make_pod_spec(secret_envs={"FOO": ("secret", "key")}))
    session = setup_session()
    comp = MockComponent(api_objects=[secret, rs], session=session)
    comp.render_chart(session)
    deps_annotation.add_deps_annotation(comp, session, rs)
    assert (
        rs["spec"]["template"]["metadata"]["annotations"].get(
            deps_annotation.DEPS_ANNOTATION
        )
        is not None
    )


def test_add_deps_annotation_no_pods():
    """Test that a component with no pods does not have the annotation added to
    it
    """
    secret = make_secret(name="secret", data={"key": "val"})
    session = setup_session()
    comp = MockComponent(api_objects=[secret], session=session)
    comp.render_chart(session)
    deps_annotation.add_deps_annotation(comp, session, secret)
    assert (
        secret["metadata"].get("annotations", {}).get(deps_annotation.DEPS_ANNOTATION)
        is None
    )


def test_add_deps_annotation_multi_deps():
    """Test that a component with a pod that relies on multiple data resources
    (secret and configmap) has the annotation and is different than a pod which
    relies on only one of them
    """
    secret = make_secret(name="secret", data={"key": "val"})
    cm = make_secret(name="cm", data={"key": "val"})
    pod = make_pod(secret_vols={"foo": "secret"}, cm_vols={"bar": "cm"})
    session = setup_session()
    comp = MockComponent(api_objects=[secret, cm, pod], session=session)
    comp.render_chart(session)
    deps_annotation.add_deps_annotation(comp, session, pod)
    assert (
        pod["metadata"]["annotations"].get(deps_annotation.DEPS_ANNOTATION) is not None
    )


def test_add_deps_annotation_external_dep():
    """Test that a component which depends on an external dependency has the
    annotation added
    """
    pod = make_pod(secret_envs={"FOO": ("secret", "key")})
    session = setup_session()
    comp = MockComponent(api_objects=[pod], session=session)
    comp.render_chart(session)
    deps_annotation.add_deps_annotation(comp, session, pod)
    assert (
        pod["metadata"]["annotations"].get(deps_annotation.DEPS_ANNOTATION) is not None
    )


def test_add_deps_annotation_empty_value_from():
    """Test that an empty valueFrom will result in no dependencies"""
    deployment = make_deployment(pod=make_pod_empty_value_from_spec())
    session = setup_session()
    comp = MockComponent(api_objects=[deployment], session=session)
    comp.render_chart(session)
    deps_annotation.add_deps_annotation(comp, session, deployment)
    assert deployment["spec"]["template"].get("metadata") is None


#########################
## get_deps_annotation ##
#########################


def test_get_deps_annotation_multi_process():
    """Test that hash values are consistent across different running instances
    of the python interpreter
    """
    py_script = "; ".join(
        [
            line.strip()
            for line in """
    from watson_assistant_application.utils import deps_annotation
    import alog
    alog.configure("debug3")
    print(deps_annotation.get_deps_annotation(None, [{"key": "val"}]))
    """.split(
                "\n"
            )
            if line
        ]
    )
    cmd = f"{sys.executable} -c '{py_script}'"
    log.debug("Full command: %s", cmd)
    res1, logs1 = subprocess.Popen(
        shlex.split(cmd), stdout=subprocess.PIPE, stderr=subprocess.PIPE
    ).communicate()
    res2, logs2 = subprocess.Popen(
        shlex.split(cmd), stdout=subprocess.PIPE, stderr=subprocess.PIPE
    ).communicate()
    log.debug("LOGS1: %s", logs1)
    log.debug("LOGS2: %s", logs2)
    assert res1 == res2


def test_get_deps_annotation_dict():
    """Test that the deps annotation works as expected based on the content of a
    dict
    """

    secret1 = {"key1": "val1", "key2": "val2"}
    secret2 = {"key2": "val2", "key1": "val1"}
    secret3 = {"key1": "val1", "key2": "val2"}
    secret4 = {"key1": "different", "key2": "val2"}

    session = setup_session()
    hash1 = deps_annotation.get_deps_annotation(session, [secret1])
    hash2 = deps_annotation.get_deps_annotation(session, [secret2])
    hash3 = deps_annotation.get_deps_annotation(session, [secret3])
    hash4 = deps_annotation.get_deps_annotation(session, [secret4])
    hash5 = deps_annotation.get_deps_annotation(session, [secret1, secret4])
    hash6 = deps_annotation.get_deps_annotation(session, [secret2, secret4])

    # Different content dict order
    assert hash1 == hash2
    # Different labels
    assert hash1 == hash3
    # Different data content
    assert hash1 != hash4
    # Multiple entries
    assert hash5 == hash6


@pytest.mark.parametrize(
    ["operand_ns", "data_ns"],
    [
        ("test", "test"),
        ("operand", "data"),
    ],
)
def test_get_deps_annotation_resource_lookup(operand_ns, data_ns):
    """Test that the deps annotation works as expected based on the content of a
    looked up dependency, including across namespaces
    """
    secret1 = make_secret(name="secret1", namespace=data_ns)
    secret2 = make_secret(name="secret2", namespace=data_ns)
    dm = MockDeployManager(resources=[secret1, secret2])

    session = setup_session(deploy_manager=dm, namespace=operand_ns)
    hash1a = deps_annotation.get_deps_annotation(
        session, [("Secret", "secret1")], namespace=data_ns
    )
    hash1b = deps_annotation.get_deps_annotation(
        session, [("Secret", "secret1")], namespace=data_ns
    )
    hash2 = deps_annotation.get_deps_annotation(
        session, [("Secret", "secret2")], namespace=data_ns
    )
    hash3 = deps_annotation.get_deps_annotation(
        session, [("Secret", "secret1"), ("Secret", "secret2")], namespace=data_ns
    )
    hash4 = deps_annotation.get_deps_annotation(
        session, [("Secret", "secret1"), ("Secret", "secret2")], namespace=data_ns
    )

    # Matching resource
    assert hash1a == hash1b
    # Different names
    assert hash1a != hash2
    # Multiple entries
    assert hash3 == hash4


def test_get_deps_annotation_resource_lookup_missing():
    """Test that when a dependency cannot be found, the hash is still computed.
    See the comment in the implementation for full details on why this is done,
    but the short version is that if deps are circular, rejecting this could
    result in an unrecoverable state.
    """

    secret1 = make_secret(name="secret1", metadata={"namespace": "test"})
    secret2 = make_secret(name="secret2", metadata={"namespace": "test"})
    dm = MockDeployManager(resources=[secret1])

    session = setup_session(deploy_manager=dm)

    # Hash before secret2 exists
    hash1 = deps_annotation.get_deps_annotation(
        session, [("Secret", "secret1"), ("Secret", "secret2")]
    )

    # Deploy secret2 and make sure the hash changes
    dm.deploy([secret2])
    hash2 = deps_annotation.get_deps_annotation(
        session, [("Secret", "secret1"), ("Secret", "secret2")]
    )
    assert hash1 != hash2


def test_get_deps_annotation_resource_lookup_fail():
    """Test that if a dependency lookup fails, an error is thrown"""
    dm = MockDeployManager(get_state_fail=True, auto_enable=False)
    session = setup_session(deploy_manager=dm)
    dm.enable_mocks()
    with pytest.raises(AssertionError):
        deps_annotation.get_deps_annotation(session, [("Secret", "secret1")])


def test_get_deps_annotation_object_types():
    """Test that the deps annotation works as expected for various data types"""

    # Secrets
    secret1 = make_secret(
        name="secret",
        metadata={"labels": {"foo": "bar"}},
        data={"key1": "val1", "key2": "val2"},
    )

    # Configmaps
    cm1 = make_cm(
        name="cm",
        metadata={"labels": {"foo": "bar"}},
        data={"key1": "val1", "key2": "val2"},
    )
    cm2 = make_cm(
        name="cm2",
        metadata={"labels": {"foo": "bar"}},
        data={"key1": "val1", "key2": "val2"},
    )
    cm3 = make_cm(
        name="cm",
        metadata={"labels": {"foo": "bar"}},
        data={"key1": "val1", "key2": "changed"},
    )
    cm4 = make_cm(
        name="cm",
        metadata={"labels": {"foo": "bar4"}},
        data={"key1": "val1", "key2": "val2"},
    )

    # Other
    foo1 = {
        "kind": "Foo",
        "metadata": {"name": "foo", "labels": {"foo": "bar"}},
        "key": "val",
    }
    foo2 = {
        "kind": "Foo",
        "metadata": {"name": "bar", "labels": {"foo": "bar"}},
        "key": "val",
    }
    foo3 = {
        "kind": "Foo",
        "metadata": {"name": "foo", "labels": {"foo": "bar"}},
        "key": "different",
    }
    foo4 = {
        "kind": "Foo",
        "metadata": {"name": "bar", "labels": {"foo": "bar4"}},
        "key": "val",
    }

    session = setup_session()

    with alog.ContextLog(log.debug, "Kind different"):
        assert deps_annotation.get_deps_annotation(
            session, [secret1]
        ) != deps_annotation.get_deps_annotation(session, [cm1])

    with alog.ContextLog(log.debug, "ConfigMap"):
        with alog.ContextLog(log.debug, "Match"):
            assert deps_annotation.get_deps_annotation(
                session, [cm1]
            ) == deps_annotation.get_deps_annotation(session, [cm1])
        with alog.ContextLog(log.debug, "Name different"):
            assert deps_annotation.get_deps_annotation(
                session, [cm1]
            ) != deps_annotation.get_deps_annotation(session, [cm2])
        with alog.ContextLog(log.debug, "Val different"):
            assert deps_annotation.get_deps_annotation(
                session, [cm1]
            ) != deps_annotation.get_deps_annotation(session, [cm3])
        with alog.ContextLog(log.debug, "Data Match w/ label different"):
            assert deps_annotation.get_deps_annotation(
                session, [cm1]
            ) == deps_annotation.get_deps_annotation(session, [cm4])

    with alog.ContextLog(log.debug, "Foo"):
        with alog.ContextLog(log.debug, "Match"):
            assert deps_annotation.get_deps_annotation(
                session, [foo1]
            ) == deps_annotation.get_deps_annotation(session, [foo1])
        with alog.ContextLog(log.debug, "Name different"):
            assert deps_annotation.get_deps_annotation(
                session, [foo1]
            ) != deps_annotation.get_deps_annotation(session, [foo2])
        with alog.ContextLog(log.debug, "Val different"):
            assert deps_annotation.get_deps_annotation(
                session, [foo1]
            ) != deps_annotation.get_deps_annotation(session, [foo3])
        with alog.ContextLog(log.debug, "Label different"):
            assert deps_annotation.get_deps_annotation(
                session, [foo1]
            ) != deps_annotation.get_deps_annotation(session, [foo4])
