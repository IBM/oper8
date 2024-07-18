"""
Tests for the ReconcileProcessEntrypoint
"""
# Standard
from contextlib import contextmanager
from multiprocessing.connection import Connection
from unittest import mock
import logging
import multiprocessing
import time

# Third Party
import pytest

# First Party
import alog

# Local
from oper8 import DeployManagerBase
from oper8.deploy_manager import DeployMethod
from oper8.deploy_manager.dry_run_deploy_manager import DryRunDeployManager
from oper8.deploy_manager.kube_event import KubeEventType
from oper8.reconcile import ReconciliationResult
from oper8.test_helpers.helpers import (
    DummyController,
    configure_logging,
    library_config,
)
from oper8.test_helpers.pwm_helpers import make_managed_object
from oper8.watch_manager.python_watch_manager.filters.filters import DisableFilter
from oper8.watch_manager.python_watch_manager.filters.manager import FilterManager
from oper8.watch_manager.python_watch_manager.reconcile_process_entrypoint import (
    ReconcileProcessDeployManager,
    create_and_start_entrypoint,
)
from oper8.watch_manager.python_watch_manager.utils.types import (
    ReconcileRequest,
    WatchRequest,
)

## Helpers #####################################################################

# NOTE: We need to keep this configure_logging call here since this file will be
#   re-imported inside the subprocess and the parent process log config is not
#   automatically recreated
configure_logging()
log = alog.use_channel("TEST")

context = multiprocessing.get_context("spawn")


class EntrypointDummyController(DummyController):
    pwm_filters = {"Foo.v1": [DisableFilter]}

    def __init__(self, config_defaults=None, deploy_manager=None):
        super().__init__(
            components=[
                {
                    "name": "foo",
                    "api_objects": [("foo", {"kind": "Foo", "apiVersion": "v1"})],
                },
                {
                    "name": "bar",
                    "api_objects": [("bar", {"kind": "Bar", "apiVersion": "v2"})],
                    "upstreams": ["foo"],
                },
                {
                    "name": "baz",
                    "api_objects": [("baz", {"kind": "Baz", "apiVersion": "v3"})],
                    "disabled": True,
                },
            ],
            config_defaults=config_defaults,
            deploy_manager=deploy_manager,
        )


class ParentDummyController(DummyController):
    pwm_subsystems = [EntrypointDummyController]
    version = "v41"

    def __init__(self, config_defaults=None, deploy_manager=None):
        super().__init__(
            components=[
                {
                    "name": "foo",
                    "api_objects": [
                        ("foo", {"kind": "Foo", "apiVersion": "foo.bar.com/v42"})
                    ],
                },
            ],
            config_defaults=config_defaults,
            deploy_manager=deploy_manager,
        )


@contextmanager
def mock_entrypoint_deploy_manager():
    original_deploy = DryRunDeployManager._deploy

    def mocked_deploy(
        self,
        resource_definitions,
        call_watches=True,
        manage_owner_references=True,
        method=DeployMethod.DEFAULT,
    ):
        success, changes = original_deploy(
            self,
            resource_definitions=resource_definitions,
            call_watches=call_watches,
            manage_owner_references=manage_owner_references,
            method=method,
        )
        if not success or not changes:
            log.debug2("Failed to deploy resources")
            return success, changes

        DryRunDeployManager._apply_resource = (
            lambda self, resource_definition: resource_definition
        )
        for resource in resource_definitions:
            resource_metadata = resource.get("metadata")
            _, updated_resource = self.get_object_current_state(
                resource.get("kind"),
                resource_metadata.get("name"),
                resource_metadata.get("namespace"),
                resource.get("apiVersion"),
            )
            self._apply_resource(resource)

        return success, changes

    original_disable = DryRunDeployManager.disable

    def mocked_disable(self, resource_definitions):
        success, changes = original_disable(
            self, resource_definitions=resource_definitions
        )
        if not success or not changes:
            log.debug2("Failed to disable resources")
            return success, changes

        DryRunDeployManager._disable = lambda self, resource_definition: changes
        for resource in resource_definitions:
            self._disable(resource)

        return success, changes

    patched_object = mock.patch.object(
        ReconcileProcessDeployManager, "__bases__", (DryRunDeployManager,)
    )
    with patched_object:
        patched_object.is_local = True
        patched_object_deploy = mock.patch.object(
            ReconcileProcessDeployManager, "_deploy", mocked_deploy
        )
        with patched_object_deploy:
            patched_object_disable = mock.patch.object(
                ReconcileProcessDeployManager, "disable", mocked_disable
            )
            with patched_object_disable:
                yield


def mock_entrypoint_start(
    logging_queue: multiprocessing.Queue,
    request: ReconcileRequest,
    result_pipe: Connection,
    deploy_manager: DeployManagerBase,
    watch_dependent_resources=False,
    subsystem_rollout=False,
):
    with library_config(
        log_level="debug4",
        python_watch_manager={
            "watch_dependent_resources": watch_dependent_resources,
            "subsystem_rollout": subsystem_rollout,
        },
    ), mock_entrypoint_deploy_manager():
        create_and_start_entrypoint(
            logging_queue, request, result_pipe, deploy_manager=deploy_manager
        )


def flatten_multi_level_list(list_obj):
    if list_obj == []:
        return list_obj
    if isinstance(list_obj[0], list):
        return flatten_multi_level_list(list_obj[0]) + flatten_multi_level_list(
            list_obj[1:]
        )
    return list_obj[:1] + flatten_multi_level_list(list_obj[1:])


@pytest.mark.timeout(5)
def test_entrypoint_happy_path():
    recv_pipe, send_pipe = context.Pipe()
    resource = make_managed_object(api_version="foo.bar.com/v42")
    with mock_entrypoint_deploy_manager():
        dm = ReconcileProcessDeployManager(
            EntrypointDummyController, resource.definition, send_pipe
        )
        dm.deploy([resource.definition])

        logging_queue = context.Queue()
        request = ReconcileRequest(
            controller_type=EntrypointDummyController,
            type=KubeEventType.ADDED,
            resource=resource,
        )

        process = context.Process(
            target=mock_entrypoint_start, args=[logging_queue, request, send_pipe, dm]
        )
        process.start()

        time.sleep(2)
        process.join()
        assert process.exitcode == 0
        assert isinstance(logging_queue.get(), logging.LogRecord)
        assert isinstance(recv_pipe.recv(), ReconciliationResult)


@pytest.mark.timeout(5)
def test_entrypoint_subsystem():
    recv_pipe, send_pipe = context.Pipe()
    resource = make_managed_object(api_version="foo.bar.com/v41")
    with mock_entrypoint_deploy_manager():
        dm = ReconcileProcessDeployManager(
            ParentDummyController, resource.definition, send_pipe
        )
        dm.deploy([resource.definition])

        logging_queue = context.Queue()
        request = ReconcileRequest(
            controller_type=ParentDummyController,
            type=KubeEventType.ADDED,
            resource=resource,
        )

        process = context.Process(
            target=mock_entrypoint_start,
            args=[logging_queue, request, send_pipe, dm, False, True],
        )
        process.start()

        time.sleep(2)
        process.join()
        assert process.exitcode == 0
        assert isinstance(logging_queue.get(), logging.LogRecord)
        assert isinstance(recv_pipe.recv(), ReconciliationResult)


def test_entrypoint_dependent_watches():
    recv_pipe, send_pipe = context.Pipe()
    resource = make_managed_object(api_version="foo.bar.com/v42")
    with mock_entrypoint_deploy_manager():
        dm = ReconcileProcessDeployManager(
            EntrypointDummyController, resource.definition, send_pipe
        )
        dm.deploy([resource.definition])

        logging_queue = context.Queue()
        request = ReconcileRequest(
            controller_type=EntrypointDummyController,
            type=KubeEventType.ADDED,
            resource=resource,
        )

        process = context.Process(
            target=mock_entrypoint_start,
            args=[logging_queue, request, send_pipe, dm, True],
        )
        process.start()

        time.sleep(2)
        process.join()
        assert process.exitcode == 0
        assert isinstance(logging_queue.get(), logging.LogRecord)
        result_messages = []
        while recv_pipe.poll():
            result_messages.append(recv_pipe.recv())

        assert ReconciliationResult in [type(result) for result in result_messages]
        assert WatchRequest in [type(result) for result in result_messages]
        for result in result_messages:
            if (
                isinstance(result, WatchRequest)
                and result.watched.kind == "Foo"
                and result.watched.api_version == "v1"
            ):
                parsed_filters = FilterManager.from_info(result.filters_info)
                used_filters = flatten_multi_level_list(parsed_filters)

                assert DisableFilter in used_filters
