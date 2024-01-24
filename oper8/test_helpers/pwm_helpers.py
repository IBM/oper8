"""
Utils and common classes for the python watch manager tests
"""
# Standard
from datetime import datetime
from multiprocessing.connection import Connection
from queue import Queue
from threading import Event
from uuid import uuid4
import multiprocessing
import random
import tempfile
import time

# Third Party
import pytest

# First Party
import alog

# Local
from oper8.managed_object import ManagedObject
from oper8.reconcile import ReconciliationResult
from oper8.watch_manager.python_watch_manager.filters.common import (
    get_configured_filter,
)
from oper8.watch_manager.python_watch_manager.leader_election import (
    LeadershipManagerBase,
)
from oper8.watch_manager.python_watch_manager.leader_election.lease import (
    LeaderWithLeaseManager,
)
from oper8.watch_manager.python_watch_manager.leader_election.life import (
    LeaderForLifeManager,
)
from oper8.watch_manager.python_watch_manager.threads.heartbeat import HeartbeatThread
from oper8.watch_manager.python_watch_manager.threads.reconcile import ReconcileThread
from oper8.watch_manager.python_watch_manager.threads.timer import TimerThread
from oper8.watch_manager.python_watch_manager.utils.types import (
    ReconcileProcess,
    ReconcileRequest,
    WatchRequest,
)

log = alog.use_channel("TEST")


### Mock Classes
class DisabledLeadershipManager(LeadershipManagerBase):
    """Leadership Manager that is always disabled"""

    def __init__(self):
        self.shutdown_event = Event()

    def acquire_resource(self, resource):
        return False

    def acquire(self, force: bool = False) -> bool:
        if force:
            self.shutdown_event.set()
        return self.shutdown_event.wait()

    def release(self):
        raise NotImplementedError()

    def release_resource(self, resource=None):
        raise NotImplementedError()

    def is_leader(self):
        return False


class MockedLeaderForLifeManager(LeaderForLifeManager):
    _disable_singleton = True


class MockedLeaderWithLeaseManager(LeaderWithLeaseManager):
    _disable_singleton = True


class MockedTimerThread(TimerThread):
    _disable_singleton = True


class MockedHeartbeatThread(HeartbeatThread):
    _disable_singleton = True


class MockedReconcileThread(ReconcileThread):
    """Subclass of ReconcileThread that mocks the subprocess. This was more
    reliable than using unittest.mock"""

    _disable_singleton = True

    def __init__(
        self,
        deploy_manager=None,
        leadership_manager=None,
        subprocess_wait_time=0.1,
        returned_messages=None,
    ):
        self.requests = Queue()
        self.timer_events = Queue()
        self.processes_started = 0
        self.processes_finished = 0
        self.watch_threads_created = 0
        self.subprocess_wait_time = subprocess_wait_time
        self.returned_messages = returned_messages or [[]]
        super().__init__(deploy_manager, leadership_manager)

    def push_request(self, request: ReconcileRequest):
        self.requests.put(request)
        super().push_request(request)

    def get_request(self) -> ReconcileRequest:
        return self.requests.get()

    def _handle_watch_request(self, request: WatchRequest):
        self.watch_threads_created += 1
        return super()._handle_watch_request(request)

    def _handle_process_end(self, reconcile_process: ReconcileProcess):
        self.processes_finished += 1
        return super()._handle_process_end(reconcile_process)

    def _start_reconcile_process(
        self, request: ReconcileRequest, pipe: Connection
    ) -> multiprocessing.Process:
        self.processes_started += 1

        returned_messages = []
        if len(self.returned_messages) > 0:
            returned_messages = self.returned_messages.pop(0)

        # Create and start a mocked reconcile process
        process = self.spawn_ctx.Process(
            target=mocked_create_and_start_entrypoint,
            args=[
                self.logging_queue,
                request,
                pipe,
                self.subprocess_wait_time,
                returned_messages,
            ],
        )
        process.start()
        log.debug3(f"Started child process with pid: {process.pid}")

        return process

    def _create_timer_event_for_request(
        self, request: ReconcileRequest, result: ReconciliationResult = None
    ):
        timer_event = super()._create_timer_event_for_request(request, result)
        self.timer_events.put(timer_event)
        return timer_event


### Helper Fixtures
@pytest.fixture(autouse=True)
def clear_caches():
    get_configured_filter.cache_clear()


### Helper functions
def make_ownerref(resource):
    metadata = resource.get("metadata", {})
    return {
        "apiVersion": resource.get("apiVersion"),
        "kind": resource.get("kind"),
        "name": metadata.get("name"),
        "uid": metadata.get("uid"),
    }


def make_resource(
    kind="Foo",
    namespace="test",
    api_version="foo.bar.com/v1",
    name="foo",
    spec=None,
    status=None,
    generation=1,
    resource_version=None,
    annotations=None,
    labels=None,
    owner_refs=None,
):
    return {
        "kind": kind,
        "apiVersion": api_version,
        "metadata": {
            "name": name,
            "namespace": namespace,
            "generation": generation,
            "resourceVersion": resource_version or random.randint(1, 1000),
            "ownerReferences": owner_refs or [],
            "labels": labels or {},
            "uid": str(uuid4()),
            "annotations": annotations or {},
        },
        "spec": spec or {},
        "status": status or {},
    }


def make_managed_object(*args, **kwargs):
    return ManagedObject(make_resource(*args, **kwargs))


def mocked_create_and_start_entrypoint(
    logging_queue: multiprocessing.Queue,
    request: ReconcileRequest,
    result_pipe: Connection,
    wait_time=0.5,
    returned_messages=None,
):
    """"""
    time.sleep(wait_time)
    for message in returned_messages or []:
        result_pipe.send(message)


def read_heartbeat_file(hb_file: str) -> datetime:
    """Parse a heartbeat file into a datetime"""
    with open(hb_file) as handle:
        hb_str = handle.read()

    return datetime.strptime(hb_str, HeartbeatThread._DATE_FORMAT)


@pytest.fixture
def heartbeat_file():
    with tempfile.NamedTemporaryFile() as tmp_file:
        yield tmp_file.name
