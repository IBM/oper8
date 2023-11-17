"""
Tests for the TemporaryPatchComponent
"""

# Standard
from datetime import datetime
import json

# Third Party
import pytest

# First Party
import aconfig

# Local
from oper8.constants import TEMPORARY_PATCHES_ANNOTATION_NAME
from oper8.exceptions import ClusterError, ConfigError, PreconditionError
from oper8.patch import STRATEGIC_MERGE_PATCH
from oper8.temporary_patch.temporary_patch_component import TemporaryPatchComponent
from oper8.test_helpers.helpers import (
    FailOnce,
    MockDeployManager,
    make_patch,
    setup_cr,
    setup_session,
)

## Happy Path (enabled) ########################################################


def test_enabled_new_patch():
    """Test that adding a new patch works"""
    target = aconfig.Config(
        {
            "kind": "Foo",
            "apiVersion": "v1",
            "metadata": {"name": "foo", "namespace": "ns"},
        }
    )
    patch_name = "some-patch"
    patch = make_patch(
        STRATEGIC_MERGE_PATCH,
        {"foo": {"key": "value"}},
        namespace=target.metadata.namespace,
        target=target,
        name=patch_name,
    )
    dm = MockDeployManager(resources=[target])
    session = setup_session(
        deploy_manager=dm,
        namespace=target.metadata.namespace,
        full_cr=patch,
    )
    comp = TemporaryPatchComponent(
        session=session,
        disabled=False,
        patch_name=patch_name,
        target_api_version=target.apiVersion,
        target_kind=target.kind,
        target_name=target.metadata.name,
    )
    comp.deploy(session)
    updated_target = dm.get_obj(
        kind=target.kind,
        api_version=target.apiVersion,
        namespace=target.metadata.namespace,
        name=target.metadata.name,
    )
    assert updated_target is not None
    patch_anno = updated_target.metadata.annotations[TEMPORARY_PATCHES_ANNOTATION_NAME]
    assert patch_anno is not None
    patch_anno_content = json.loads(patch_anno)
    assert patch_name in patch_anno_content
    assert "timestamp" in patch_anno_content[patch_name]
    assert patch_anno_content[patch_name]["api_version"] == patch.apiVersion
    assert patch_anno_content[patch_name]["kind"] == patch.kind


def test_enabled_already_present():
    """Test that when a patch is already present, the patch is not changed"""
    patch_name = "some-patch"
    patch_api_version = "foo.bar.com/v2"
    patch_kind = "TestTemporaryPatch"
    existing_anno = {
        patch_name: {
            "api_version": patch_api_version,
            "kind": patch_kind,
            "timestamp": datetime.now().isoformat(),
        }
    }
    target = aconfig.Config(
        {
            "kind": "Foo",
            "apiVersion": "v1",
            "metadata": {
                "name": "foo",
                "namespace": "ns",
                "annotations": {
                    TEMPORARY_PATCHES_ANNOTATION_NAME: json.dumps(existing_anno),
                },
            },
        }
    )
    patch = make_patch(
        STRATEGIC_MERGE_PATCH,
        {"foo": {"key": "value"}},
        namespace=target.metadata.namespace,
        target=target,
        name=patch_name,
        api_version=patch_api_version,
        kind=patch_kind,
    )
    dm = MockDeployManager(resources=[target])
    session = setup_session(
        deploy_manager=dm,
        namespace=target.metadata.namespace,
        full_cr=patch,
    )
    comp = TemporaryPatchComponent(
        session=session,
        disabled=False,
        patch_name=patch_name,
        target_api_version=target.apiVersion,
        target_kind=target.kind,
        target_name=target.metadata.name,
    )
    comp.deploy(session)
    updated_target = dm.get_obj(
        kind=target.kind,
        api_version=target.apiVersion,
        namespace=target.metadata.namespace,
        name=target.metadata.name,
    )
    assert updated_target is not None
    patch_anno = updated_target.metadata.annotations[TEMPORARY_PATCHES_ANNOTATION_NAME]
    assert patch_anno is not None
    patch_anno_content = json.loads(patch_anno)
    assert patch_anno_content == existing_anno


def test_enabled_multi_patch():
    """Test that when an existing patch is present and a new patch is added,
    they both persist
    """
    old_patch_name = "test1"
    new_patch_name = "test2"
    patch_api_version = "foo.bar.com/v2"
    patch_kind = "TestTemporaryPatch"
    existing_anno = {
        old_patch_name: {
            "api_version": patch_api_version,
            "kind": patch_kind,
            "timestamp": datetime.now().isoformat(),
        }
    }
    target = aconfig.Config(
        {
            "kind": "Foo",
            "apiVersion": "v1",
            "metadata": {
                "name": "foo",
                "namespace": "ns",
                "annotations": {
                    TEMPORARY_PATCHES_ANNOTATION_NAME: json.dumps(existing_anno),
                },
            },
        }
    )
    patch = make_patch(
        STRATEGIC_MERGE_PATCH,
        {"foo": {"key": "value"}},
        namespace=target.metadata.namespace,
        target=target,
        name=new_patch_name,
        api_version=patch_api_version,
        kind=patch_kind,
    )
    dm = MockDeployManager(resources=[target])
    session = setup_session(
        deploy_manager=dm,
        namespace=target.metadata.namespace,
        full_cr=patch,
    )
    comp = TemporaryPatchComponent(
        session=session,
        disabled=False,
        patch_name=new_patch_name,
        target_api_version=target.apiVersion,
        target_kind=target.kind,
        target_name=target.metadata.name,
    )
    comp.deploy(session)
    updated_target = dm.get_obj(
        kind=target.kind,
        api_version=target.apiVersion,
        namespace=target.metadata.namespace,
        name=target.metadata.name,
    )
    assert updated_target is not None
    patch_anno = updated_target.metadata.annotations[TEMPORARY_PATCHES_ANNOTATION_NAME]
    assert patch_anno is not None
    patch_anno_content = json.loads(patch_anno)
    assert len(patch_anno_content) == 2
    assert old_patch_name in patch_anno_content
    assert new_patch_name in patch_anno_content


## Happy Path (disabled) #######################################################


def test_disabled_patch_exists():
    """Test that removing a patch which exists works"""
    patch_name = "some-patch"
    patch_api_version = "foo.bar.com/v2"
    patch_kind = "TestTemporaryPatch"
    existing_anno = {
        patch_name: {
            "api_version": patch_api_version,
            "kind": patch_kind,
            "timestamp": datetime.now().isoformat(),
        }
    }
    target = aconfig.Config(
        {
            "kind": "Foo",
            "apiVersion": "v1",
            "metadata": {
                "name": "foo",
                "namespace": "ns",
                "annotations": {
                    TEMPORARY_PATCHES_ANNOTATION_NAME: json.dumps(existing_anno),
                },
            },
        }
    )
    patch = make_patch(
        STRATEGIC_MERGE_PATCH,
        {"foo": {"key": "value"}},
        namespace=target.metadata.namespace,
        target=target,
        name=patch_name,
        api_version=patch_api_version,
        kind=patch_kind,
    )
    dm = MockDeployManager(resources=[target])
    session = setup_session(
        deploy_manager=dm,
        namespace=target.metadata.namespace,
        full_cr=patch,
    )
    comp = TemporaryPatchComponent(
        session=session,
        disabled=True,
        patch_name=patch_name,
        target_api_version=target.apiVersion,
        target_kind=target.kind,
        target_name=target.metadata.name,
    )
    comp.disable(session)
    updated_target = dm.get_obj(
        kind=target.kind,
        api_version=target.apiVersion,
        namespace=target.metadata.namespace,
        name=target.metadata.name,
    )
    assert updated_target is not None
    patch_anno = updated_target.metadata.annotations[TEMPORARY_PATCHES_ANNOTATION_NAME]
    assert patch_anno is not None
    patch_anno_content = json.loads(patch_anno)
    assert patch_anno_content == {}


def test_disabled_multi_patch():
    """Test that removing a patch from a resource with multiple patches only
    removes the desired patch
    """
    old_patch_name = "test1"
    new_patch_name = "test2"
    patch_api_version = "foo.bar.com/v2"
    patch_kind = "TestTemporaryPatch"
    existing_anno = {
        old_patch_name: {
            "api_version": patch_api_version,
            "kind": patch_kind,
            "timestamp": datetime.now().isoformat(),
        },
        new_patch_name: {
            "api_version": patch_api_version,
            "kind": patch_kind,
            "timestamp": datetime.now().isoformat(),
        },
    }
    target = aconfig.Config(
        {
            "kind": "Foo",
            "apiVersion": "v1",
            "metadata": {
                "name": "foo",
                "namespace": "ns",
                "annotations": {
                    TEMPORARY_PATCHES_ANNOTATION_NAME: json.dumps(existing_anno),
                },
            },
        }
    )
    patch = make_patch(
        STRATEGIC_MERGE_PATCH,
        {"foo": {"key": "value"}},
        namespace=target.metadata.namespace,
        target=target,
        name=new_patch_name,
        api_version=patch_api_version,
        kind=patch_kind,
    )
    dm = MockDeployManager(resources=[target])
    session = setup_session(
        deploy_manager=dm,
        namespace=target.metadata.namespace,
        full_cr=patch,
    )
    comp = TemporaryPatchComponent(
        session=session,
        disabled=True,
        patch_name=new_patch_name,
        target_api_version=target.apiVersion,
        target_kind=target.kind,
        target_name=target.metadata.name,
    )
    comp.disable(session)
    updated_target = dm.get_obj(
        kind=target.kind,
        api_version=target.apiVersion,
        namespace=target.metadata.namespace,
        name=target.metadata.name,
    )
    assert updated_target is not None
    patch_anno = updated_target.metadata.annotations[TEMPORARY_PATCHES_ANNOTATION_NAME]
    assert patch_anno is not None
    patch_anno_content = json.loads(patch_anno)
    existing_anno.pop(new_patch_name)
    assert patch_anno_content == existing_anno


def test_disabled_resource_not_found():
    """Test that if the resource is removed before the patch, it's a no-op"""
    patch_name = "some-patch"
    patch_api_version = "foo.bar.com/v2"
    patch_kind = "TestTemporaryPatch"
    namespace = "ns"
    dm = MockDeployManager()
    session = setup_session(
        deploy_manager=dm,
        full_cr=setup_cr(
            namespace=namespace,
            api_version=patch_api_version,
            kind=patch_kind,
            name=patch_name,
        ),
    )
    comp = TemporaryPatchComponent(
        session=session,
        disabled=True,
        patch_name=patch_name,
        target_api_version="foo.bar/v1",
        target_kind="Foo",
        target_name=namespace,
    )
    comp.disable(session)


def test_disabled_resource_annotation_not_there():
    """Test that if the annotation has already been removed, it's a no-op"""
    patch_name = "some-patch"
    patch_api_version = "foo.bar.com/v2"
    patch_kind = "TestTemporaryPatch"
    target = aconfig.Config(
        {
            "kind": "Foo",
            "apiVersion": "v1",
            "metadata": {
                "name": "foo",
                "namespace": "ns",
            },
        }
    )
    patch = make_patch(
        STRATEGIC_MERGE_PATCH,
        {"foo": {"key": "value"}},
        namespace=target.metadata.namespace,
        target=target,
        name=patch_name,
        api_version=patch_api_version,
        kind=patch_kind,
    )
    dm = MockDeployManager(resources=[target])
    session = setup_session(
        deploy_manager=dm,
        namespace=target.metadata.namespace,
        full_cr=patch,
    )
    comp = TemporaryPatchComponent(
        session=session,
        disabled=True,
        patch_name=patch_name,
        target_api_version=target.apiVersion,
        target_kind=target.kind,
        target_name=target.metadata.name,
    )
    comp.disable(session)
    updated_target = dm.get_obj(
        kind=target.kind,
        api_version=target.apiVersion,
        namespace=target.metadata.namespace,
        name=target.metadata.name,
    )
    assert updated_target is not None
    assert updated_target.metadata.annotations is None


def test_disabled_resource_patch_not_there():
    """Test that if the patch has already been removed, it's a no-op"""
    old_patch_name = "test1"
    new_patch_name = "test2"
    patch_api_version = "foo.bar.com/v2"
    patch_kind = "TestTemporaryPatch"
    existing_anno = {
        old_patch_name: {
            "api_version": patch_api_version,
            "kind": patch_kind,
            "timestamp": datetime.now().isoformat(),
        },
    }
    target = aconfig.Config(
        {
            "kind": "Foo",
            "apiVersion": "v1",
            "metadata": {
                "name": "foo",
                "namespace": "ns",
                "annotations": {
                    TEMPORARY_PATCHES_ANNOTATION_NAME: json.dumps(existing_anno),
                },
            },
        }
    )
    patch = make_patch(
        STRATEGIC_MERGE_PATCH,
        {"foo": {"key": "value"}},
        namespace=target.metadata.namespace,
        target=target,
        name=new_patch_name,
        api_version=patch_api_version,
        kind=patch_kind,
    )
    dm = MockDeployManager(resources=[target])
    session = setup_session(
        deploy_manager=dm,
        namespace=target.metadata.namespace,
        full_cr=patch,
    )
    comp = TemporaryPatchComponent(
        session=session,
        disabled=True,
        patch_name=new_patch_name,
        target_api_version=target.apiVersion,
        target_kind=target.kind,
        target_name=target.metadata.name,
    )
    comp.disable(session)
    updated_target = dm.get_obj(
        kind=target.kind,
        api_version=target.apiVersion,
        namespace=target.metadata.namespace,
        name=target.metadata.name,
    )
    assert updated_target is not None
    patch_anno = updated_target.metadata.annotations[TEMPORARY_PATCHES_ANNOTATION_NAME]
    assert patch_anno is not None
    patch_anno_content = json.loads(patch_anno)
    assert patch_anno_content == existing_anno


## Error Cases #################################################################


def test_enabled_resource_not_found():
    """Test that if the target resource is not found, the patch is rejected"""
    patch_name = "some-patch"
    patch_api_version = "foo.bar.com/v2"
    patch_kind = "TestTemporaryPatch"
    namespace = "ns"
    dm = MockDeployManager()
    session = setup_session(
        deploy_manager=dm,
        full_cr=setup_cr(
            namespace=namespace,
            api_version=patch_api_version,
            kind=patch_kind,
            name=patch_name,
        ),
    )
    with pytest.raises(PreconditionError):
        TemporaryPatchComponent(
            session=session,
            disabled=False,
            patch_name=patch_name,
            target_api_version="foo.bar/v1",
            target_kind="Foo",
            target_name=namespace,
        )


def test_enabled_resource_lookup_error():
    """Test that if the resource errors on lookup, an error is raised"""
    patch_name = "some-patch"
    patch_api_version = "foo.bar.com/v2"
    patch_kind = "TestTemporaryPatch"
    namespace = "ns"
    dm = MockDeployManager(get_state_fail=FailOnce((False, None), fail_number=2))
    session = setup_session(
        deploy_manager=dm,
        full_cr=setup_cr(
            namespace=namespace,
            api_version=patch_api_version,
            kind=patch_kind,
            name=patch_name,
        ),
    )
    with pytest.raises(ClusterError):
        TemporaryPatchComponent(
            session=session,
            disabled=False,
            patch_name=patch_name,
            target_api_version="foo.bar/v1",
            target_kind="Foo",
            target_name=namespace,
        )


def test_enabled_annotation_not_json():
    """Test that if the target resource has a non-json value for the annotation,
    an error is raised
    """
    patch_name = "some-patch"
    patch_api_version = "foo.bar.com/v2"
    patch_kind = "TestTemporaryPatch"
    target = aconfig.Config(
        {
            "kind": "Foo",
            "apiVersion": "v1",
            "metadata": {
                "name": "foo",
                "namespace": "ns",
                "annotations": {
                    TEMPORARY_PATCHES_ANNOTATION_NAME: "{",
                },
            },
        }
    )
    patch = make_patch(
        STRATEGIC_MERGE_PATCH,
        {"foo": {"key": "value"}},
        namespace=target.metadata.namespace,
        target=target,
        name=patch_name,
        api_version=patch_api_version,
        kind=patch_kind,
    )
    dm = MockDeployManager(resources=[target])
    session = setup_session(
        deploy_manager=dm,
        namespace=target.metadata.namespace,
        full_cr=patch,
    )
    comp = TemporaryPatchComponent(
        session=session,
        disabled=False,
        patch_name=patch_name,
        target_api_version=target.apiVersion,
        target_kind=target.kind,
        target_name=target.metadata.name,
    )
    with pytest.raises(ConfigError):
        comp.deploy(session)


def test_enabled_annotation_not_dict():
    """Test that if the target resource has a value that is json, but not a dict
    for the annotation, an error is raised
    """
    patch_name = "some-patch"
    patch_api_version = "foo.bar.com/v2"
    patch_kind = "TestTemporaryPatch"
    target = aconfig.Config(
        {
            "kind": "Foo",
            "apiVersion": "v1",
            "metadata": {
                "name": "foo",
                "namespace": "ns",
                "annotations": {
                    TEMPORARY_PATCHES_ANNOTATION_NAME: "some string",
                },
            },
        }
    )
    patch = make_patch(
        STRATEGIC_MERGE_PATCH,
        {"foo": {"key": "value"}},
        namespace=target.metadata.namespace,
        target=target,
        name=patch_name,
        api_version=patch_api_version,
        kind=patch_kind,
    )
    dm = MockDeployManager(resources=[target])
    session = setup_session(
        deploy_manager=dm,
        namespace=target.metadata.namespace,
        full_cr=patch,
    )
    comp = TemporaryPatchComponent(
        session=session,
        disabled=False,
        patch_name=patch_name,
        target_api_version=target.apiVersion,
        target_kind=target.kind,
        target_name=target.metadata.name,
    )
    with pytest.raises(ConfigError):
        comp.deploy(session)


def test_enabled_deploy_error():
    """Test that if redeploying the resource fails, an error is raised"""
    target = aconfig.Config(
        {
            "kind": "Foo",
            "apiVersion": "v1",
            "metadata": {"name": "foo", "namespace": "ns"},
        }
    )
    patch_name = "some-patch"
    patch = make_patch(
        STRATEGIC_MERGE_PATCH,
        {"foo": {"key": "value"}},
        namespace=target.metadata.namespace,
        target=target,
        name=patch_name,
    )
    dm = MockDeployManager(resources=[target], deploy_fail=True)
    session = setup_session(
        deploy_manager=dm,
        namespace=target.metadata.namespace,
        full_cr=patch,
    )
    comp = TemporaryPatchComponent(
        session=session,
        disabled=False,
        patch_name=patch_name,
        target_api_version=target.apiVersion,
        target_kind=target.kind,
        target_name=target.metadata.name,
    )
    with pytest.raises(ClusterError):
        comp.deploy(session)


def test_enabled_deploy_raise():
    """Test that if redeploying the resource raises, an error is raised"""
    target = aconfig.Config(
        {
            "kind": "Foo",
            "apiVersion": "v1",
            "metadata": {"name": "foo", "namespace": "ns"},
        }
    )
    patch_name = "some-patch"
    patch = make_patch(
        STRATEGIC_MERGE_PATCH,
        {"foo": {"key": "value"}},
        namespace=target.metadata.namespace,
        target=target,
        name=patch_name,
    )
    dm = MockDeployManager(resources=[target], deploy_raise=True)
    session = setup_session(
        deploy_manager=dm,
        namespace=target.metadata.namespace,
        full_cr=patch,
    )
    comp = TemporaryPatchComponent(
        session=session,
        disabled=False,
        patch_name=patch_name,
        target_api_version=target.apiVersion,
        target_kind=target.kind,
        target_name=target.metadata.name,
    )
    with pytest.raises(AssertionError):
        comp.deploy(session)
