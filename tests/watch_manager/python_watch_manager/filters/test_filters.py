"""
Tests for the Filter classes
"""
# Local
from oper8.deploy_manager.kube_event import KubeEventType
from oper8.status import ReadyReason, make_application_status
from oper8.test_helpers.pwm_helpers import make_managed_object
from oper8.watch_manager.python_watch_manager.filters.filters import (
    AnnotationFilter,
    CreationDeletionFilter,
    DependentWatchFilter,
    GenerationFilter,
    LabelFilter,
    NoGenerationFilter,
    PauseFilter,
    ResourceVersionFilter,
    SubsystemStatusFilter,
    UserAnnotationFilter,
)

## Helpers #####################################################################


def test_filter_creation_deletion():
    resource = make_managed_object()
    filter = CreationDeletionFilter(resource)

    assert filter.update_and_test(resource, KubeEventType.ADDED)
    assert filter.update_and_test(resource, KubeEventType.DELETED)
    assert filter.update_and_test(resource, KubeEventType.MODIFIED) == None


def test_filter_generation():
    resource = make_managed_object(generation=1)
    filter = GenerationFilter(resource)

    # Assert Generation doesn't care the first time it sees a resource
    assert filter.update_and_test(resource, KubeEventType.MODIFIED) == None
    resource.definition["metadata"]["generation"] = 2
    assert filter.update_and_test(resource, KubeEventType.MODIFIED)
    assert filter.update_and_test(resource, KubeEventType.MODIFIED) == False
    assert filter.update_and_test(resource, KubeEventType.DELETED) == None
    assert filter.update_and_test(resource, KubeEventType.ADDED) == None


def test_filter_no_generation():
    resource = make_managed_object(generation=None)
    filter = NoGenerationFilter(resource)

    # Assert Generation doesn't care the first time it sees a resource
    assert filter.update_and_test(resource, KubeEventType.MODIFIED) == None
    resource.definition["spec"]["changed"] = "value"
    assert filter.update_and_test(resource, KubeEventType.MODIFIED)
    assert filter.update_and_test(resource, KubeEventType.MODIFIED) == False
    assert filter.update_and_test(resource, KubeEventType.DELETED) == None
    assert filter.update_and_test(resource, KubeEventType.ADDED) == None


def test_filter_resource_version():
    resource = make_managed_object()
    filter = ResourceVersionFilter(resource)

    assert filter.update_and_test(resource, KubeEventType.MODIFIED)
    resource = make_managed_object(resource_version="arandomvalue")
    assert filter.update_and_test(resource, KubeEventType.MODIFIED)
    assert filter.update_and_test(resource, KubeEventType.MODIFIED) == False
    assert filter.update_and_test(resource, KubeEventType.ADDED) == False
    assert filter.update_and_test(resource, KubeEventType.DELETED) == None


def test_filter_annotation():
    resource = make_managed_object()
    filter = AnnotationFilter(resource)

    assert filter.update_and_test(resource, KubeEventType.MODIFIED)
    resource = make_managed_object(annotations={"updated": "value"})
    assert filter.update_and_test(resource, KubeEventType.MODIFIED)
    assert filter.update_and_test(resource, KubeEventType.MODIFIED) == False
    resource.definition["spec"]["updated"] = "value"
    assert filter.update_and_test(resource, KubeEventType.MODIFIED) == False

    # Check add and deletes are ignored
    assert filter.update_and_test(resource, KubeEventType.ADDED) == None
    assert filter.update_and_test(resource, KubeEventType.DELETED) == None


def test_filter_user_annotation():
    resource = make_managed_object()
    filter = UserAnnotationFilter(resource)

    assert filter.update_and_test(resource, KubeEventType.MODIFIED)
    resource = make_managed_object(annotations={"updated": "value"})
    assert filter.update_and_test(resource, KubeEventType.MODIFIED)
    assert filter.update_and_test(resource, KubeEventType.MODIFIED) == False
    resource = make_managed_object(
        annotations={"updated": "value", "k8s.io/ingress": "nginx"}
    )
    assert filter.update_and_test(resource, KubeEventType.MODIFIED) == False
    resource.definition["spec"]["updated"] = "value"
    assert filter.update_and_test(resource, KubeEventType.MODIFIED) == False

    # Check add and deletes are ignored
    assert filter.update_and_test(resource, KubeEventType.ADDED) == None
    assert filter.update_and_test(resource, KubeEventType.DELETED) == None


def test_filter_pause():
    resource = make_managed_object()
    filter = PauseFilter(resource)

    assert filter.test(resource, KubeEventType.ADDED)
    resource = make_managed_object(annotations={"oper8.org/pause-execution": "true"})
    assert not filter.test(resource, KubeEventType.ADDED)


def test_subsystem_filter():
    resource = make_managed_object()
    filter = SubsystemStatusFilter(resource)

    assert filter.update_and_test(resource, KubeEventType.ADDED) == None
    resource = make_managed_object(
        status=make_application_status(ready_reason=ReadyReason.IN_PROGRESS)
    )
    assert filter.update_and_test(resource, KubeEventType.MODIFIED)
    resource = make_managed_object(
        status=make_application_status(ready_reason=ReadyReason.STABLE)
    )
    assert filter.update_and_test(resource, KubeEventType.MODIFIED)
    assert not filter.update_and_test(resource, KubeEventType.MODIFIED)


def test_filter_dependent():
    resource = make_managed_object()
    filter = DependentWatchFilter(resource)

    assert not filter.test(resource, KubeEventType.ADDED)
    assert filter.test(resource, KubeEventType.MODIFIED)
    assert filter.test(resource, KubeEventType.DELETED)


def test_filter_label():
    class CustomLabelFilter(LabelFilter):
        labels = {"app": "oper8", "subsystem": "test"}

    resource = make_managed_object()
    filter = CustomLabelFilter(resource)

    assert not filter.test(resource, KubeEventType.ADDED)
    resource = make_managed_object(labels={"app": "oper8", "subsystem": "test"})
    assert filter.test(resource, KubeEventType.ADDED)
