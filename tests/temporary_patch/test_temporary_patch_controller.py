"""
Tests for the TemporaryPatchController
"""

# Local
from oper8.patch import STRATEGIC_MERGE_PATCH
from oper8.temporary_patch.temporary_patch_component import TemporaryPatchComponent
from oper8.temporary_patch.temporary_patch_controller import TemporaryPatchController
from oper8.test_helpers.helpers import (
    DummyController,
    MockDeployManager,
    make_patch,
    setup_session,
)

## Tests (non finalier) ########################################################


def test_non_finalizer_patchable_kind_only():
    """Test that when run as a non-finalizer and a patchable kind is given by
    kind only, the component is correctly added to the session
    """
    target = {
        "kind": "Foo",
        "apiVersion": "foo.bar/v2",
        "metadata": {"name": "foo", "namespace": "ns"},
    }
    patch = make_patch(
        patch_type=STRATEGIC_MERGE_PATCH,
        body={"foo": {"key": "value"}},
        target=target,
        namespace="ns",
    )
    dm = MockDeployManager(resources=[target])
    ctrlr = TemporaryPatchController(patchable_kinds=["Foo", "Bar"])
    session = setup_session(full_cr=patch, deploy_manager=dm)
    ctrlr.setup_components(session)
    comps = session.get_components()
    assert len(comps) == 1
    assert isinstance(comps[0], TemporaryPatchComponent)
    assert not session.get_components(disabled=True)


def test_construct_with_controller():
    """Test that constructing with a Controller type as a patchable kind works"""
    ctrlr = TemporaryPatchController(patchable_kinds=[DummyController])
    assert ctrlr._patchable_kinds == [
        "/".join(
            [
                DummyController.group,
                DummyController.version,
                DummyController.kind,
            ]
        )
    ]


def test_non_finalizer_patchable_kind_with_api_version():
    """Test that when run as a non-finalizer and a patchable kind is given by
    kind and api version, the component is correctly added to the session
    """
    target = {
        "kind": "Foo",
        "apiVersion": "foo.bar/v2",
        "metadata": {"name": "foo", "namespace": "ns"},
    }
    patch = make_patch(
        patch_type=STRATEGIC_MERGE_PATCH,
        body={"foo": {"key": "value"}},
        target=target,
        namespace="ns",
    )
    dm = MockDeployManager(resources=[target])
    ctrlr = TemporaryPatchController(patchable_kinds=["foo.bar/v2/Foo"])
    session = setup_session(full_cr=patch, deploy_manager=dm)
    ctrlr.setup_components(session)
    comps = session.get_components()
    assert len(comps) == 1
    assert isinstance(comps[0], TemporaryPatchComponent)
    assert not session.get_components(disabled=True)


def test_non_finalizer_non_patchable_kind_only():
    """Test that when run as a non-finalizer and a patchable kind is given by
    kind only that is not in the list, the component is not added to the session
    """
    target = {
        "kind": "Foo",
        "apiVersion": "foo.bar/v2",
        "metadata": {"name": "foo", "namespace": "ns"},
    }
    patch = make_patch(
        patch_type=STRATEGIC_MERGE_PATCH,
        body={"foo": {"key": "value"}},
        target=target,
        namespace="ns",
    )
    dm = MockDeployManager(resources=[target])
    ctrlr = TemporaryPatchController(patchable_kinds=["Bar"])
    session = setup_session(full_cr=patch, deploy_manager=dm)
    ctrlr.setup_components(session)
    assert not session.get_components()
    assert not session.get_components(disabled=True)


def test_non_finalizer_non_patchable_kind_with_api_version():
    """Test that when run as a non-finalizer and a patchable kind is given by
    kind and api version that is not in the list, the component is not added to
    the session
    """
    target = {
        "kind": "Foo",
        "apiVersion": "foo.bar/v2",
        "metadata": {"name": "foo", "namespace": "ns"},
    }
    patch = make_patch(
        patch_type=STRATEGIC_MERGE_PATCH,
        body={"foo": {"key": "value"}},
        target=target,
        namespace="ns",
    )
    dm = MockDeployManager(resources=[target])
    ctrlr = TemporaryPatchController(patchable_kinds=["foo.bar/v1/Foo"])
    session = setup_session(full_cr=patch, deploy_manager=dm)
    ctrlr.setup_components(session)
    assert not session.get_components()
    assert not session.get_components(disabled=True)


def test_derived_properties():
    """Test that a derived TemporaryPatchController subclass can override the
    desired properties and inherit the non-overridden properties
    """

    class DerivedTempPatchCtrlr(TemporaryPatchController):
        group = "foo.bar.com"

    assert DerivedTempPatchCtrlr.group == "foo.bar.com"
    assert DerivedTempPatchCtrlr.version == "v1"
    assert DerivedTempPatchCtrlr.kind == "TemporaryPatch"
    assert DerivedTempPatchCtrlr.disable_vcs == True


## Tests (finalizer) ###########################################################


def test_finalizer_patchable_kind_only():
    """Test that when run as a finalizer and a patchable kind is given by kind
    only, the component is correctly added to the session
    """
    target = {
        "kind": "Foo",
        "apiVersion": "foo.bar/v2",
        "metadata": {"name": "foo", "namespace": "ns"},
    }
    patch = make_patch(
        patch_type=STRATEGIC_MERGE_PATCH,
        body={"foo": {"key": "value"}},
        target=target,
        namespace="ns",
    )
    dm = MockDeployManager(resources=[target])
    ctrlr = TemporaryPatchController(patchable_kinds=["Foo", "Bar"])
    session = setup_session(full_cr=patch, deploy_manager=dm)
    ctrlr.finalize_components(session)
    comps = session.get_components(disabled=True)
    assert len(comps) == 1
    assert isinstance(comps[0], TemporaryPatchComponent)
    assert not session.get_components(disabled=False)


def test_finalizer_patchable_kind_with_api_version():
    """Test that when run as a finalizer and a patchable kind is given by kind
    and api version, the component is correctly added to the session
    """
    target = {
        "kind": "Foo",
        "apiVersion": "foo.bar/v2",
        "metadata": {"name": "foo", "namespace": "ns"},
    }
    patch = make_patch(
        patch_type=STRATEGIC_MERGE_PATCH,
        body={"foo": {"key": "value"}},
        target=target,
        namespace="ns",
    )
    dm = MockDeployManager(resources=[target])
    ctrlr = TemporaryPatchController(patchable_kinds=["foo.bar/v2/Foo"])
    session = setup_session(full_cr=patch, deploy_manager=dm)
    ctrlr.finalize_components(session)
    comps = session.get_components(disabled=True)
    assert len(comps) == 1
    assert isinstance(comps[0], TemporaryPatchComponent)
    assert not session.get_components(disabled=False)


def test_finalizer_non_patchable_kind_only():
    """Test that when run as a finalizer and a patchable kind is given by kind
    only that is not in the list, the component is not added to the session
    """
    target = {
        "kind": "Foo",
        "apiVersion": "foo.bar/v2",
        "metadata": {"name": "foo", "namespace": "ns"},
    }
    patch = make_patch(
        patch_type=STRATEGIC_MERGE_PATCH,
        body={"foo": {"key": "value"}},
        target=target,
        namespace="ns",
    )
    dm = MockDeployManager(resources=[target])
    ctrlr = TemporaryPatchController(patchable_kinds=["Bar"])
    session = setup_session(full_cr=patch, deploy_manager=dm)
    ctrlr.finalize_components(session)
    assert not session.get_components(disabled=True)
    assert not session.get_components(disabled=False)


def test_finalizer_non_patchable_kind_with_api_version():
    """Test that when run as a finalizer and a patchable kind is given by kind
    and api version that is not in the list, the component is not added to the
    session
    """
    target = {
        "kind": "Foo",
        "apiVersion": "foo.bar/v2",
        "metadata": {"name": "foo", "namespace": "ns"},
    }
    patch = make_patch(
        patch_type=STRATEGIC_MERGE_PATCH,
        body={"foo": {"key": "value"}},
        target=target,
        namespace="ns",
    )
    dm = MockDeployManager(resources=[target])
    ctrlr = TemporaryPatchController(patchable_kinds=["foo.bar/v1/Foo"])
    session = setup_session(full_cr=patch, deploy_manager=dm)
    ctrlr.finalize_components(session)
    assert not session.get_components(disabled=True)
    assert not session.get_components(disabled=False)
