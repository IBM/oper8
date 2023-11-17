"""Annotation Based Leadership Manager"""

# Standard
from copy import copy
from datetime import datetime
from typing import Optional

# First Party
import alog

# Local
from .... import config
from ....constants import LEASE_NAME_ANNOTATION_NAME, LEASE_TIME_ANNOTATION_NAME
from ....deploy_manager import DeployManagerBase
from ....exceptions import assert_config
from ....managed_object import ManagedObject
from ..utils import get_pod_name, parse_time_delta
from .base import LeadershipManagerBase

log = alog.use_channel("LDRELC")


class AnnotationLeadershipManager(LeadershipManagerBase):
    """
    Annotation based leadership manager that uses two annotations
    to track leadership on a per-resource basis. This allows for
    horizontally scalable operations.

    EXPERIMENTAL: This has passed basic validation but has not been rigorously tested
     in the field
    """

    def __init__(self, deploy_manager: DeployManagerBase = None):
        """Initialize Leadership and gather current name

        Args:
            deploy_manager: DeployManagerBase = None
                DeployManager for this Manager
        """

        super().__init__(deploy_manager)
        self.duration_delta = parse_time_delta(
            config.python_watch_manager.lock.duration
        )

        # Gather lock_name, namespace and pod manifest
        self.pod_name = get_pod_name()
        assert_config(self.pod_name, "Unable to detect pod name")

    ## Lock Interface ####################################################
    def acquire(self, force: bool = False) -> bool:
        """
        Return true as leadership is managed at resource level
        """
        return True

    def acquire_resource(self, resource: ManagedObject):
        """Check a resource for leadership annotation and add one if it's expired
        or does not exit"""
        success, current_resource = self.deploy_manager.get_object_current_state(
            resource.kind, resource.name, resource.namespace, resource.api_version
        )
        if not success or not current_resource:
            log.warning(
                "Unable to fetch owner resource %s/%s/%s/%s",
                resource.kind,
                resource.api_version,
                resource.namespace,
                resource.name,
            )
            return False

        if "annotations" not in current_resource.get("metadata"):
            current_resource["metadata"]["annotations"] = {}

        # Check the current annotation
        annotations = current_resource["metadata"]["annotations"]
        current_time = datetime.now()

        # If no leader than take ownership
        if not annotations.get(LEASE_NAME_ANNOTATION_NAME):
            annotations[LEASE_NAME_ANNOTATION_NAME] = self.pod_name
            annotations[LEASE_TIME_ANNOTATION_NAME] = current_time.isoformat()

        # If already the current leader then update lease time
        elif self.pod_name == annotations.get(LEASE_NAME_ANNOTATION_NAME):
            annotations[LEASE_TIME_ANNOTATION_NAME] = current_time.isoformat()

        # If the current leader's lease has timed out than take ownership
        elif not self._check_lease_time(
            annotations.get(LEASE_TIME_ANNOTATION_NAME), current_time
        ):
            annotations[LEASE_NAME_ANNOTATION_NAME] = self.pod_name
            annotations[LEASE_TIME_ANNOTATION_NAME] = current_time.isoformat()

        # Otherwise unable to acquire lock
        else:
            return False

        success, _ = self.deploy_manager.deploy([current_resource])
        if not success:
            log.warning(
                "Unable to update resource annotation%s/%s/%s/%s",
                resource.kind,
                resource.api_version,
                resource.namespace,
                resource.name,
            )
            return False

        return True

    def release(self):
        """
        Release lock on global resource
        """
        return True

    def release_resource(self, resource: ManagedObject):
        """
        Release lock on specific resource by removing the annotation
        """
        current_resource = copy(resource.definition)

        # Only clear annotation if we're the current leader
        if self.pod_name == current_resource["metadata"].get("annotations", {}).get(
            LEASE_NAME_ANNOTATION_NAME
        ):
            current_resource["metadata"]["annotations"][
                LEASE_NAME_ANNOTATION_NAME
            ] = None
            current_resource["metadata"]["annotations"][
                LEASE_TIME_ANNOTATION_NAME
            ] = None
            self.deploy_manager.deploy([current_resource])

        return True

    def is_leader(self, resource: Optional[ManagedObject] = None):
        """
        Determines if current instance is leader
        """
        if resource:
            annotations = resource.metadata.get("annotations", {})
            return self.pod_name == annotations.get(
                LEASE_NAME_ANNOTATION_NAME
            ) and self._check_lease_time(annotations.get(LEASE_TIME_ANNOTATION_NAME))

        return True

    def _check_lease_time(
        self, lease_time: str, current_time: Optional[datetime] = None
    ) -> bool:
        """Helper function to check if lease time is still valid

        Args:
            lease_time: str
                A datetime in isoformat
            current_time: Optional[datetime]
                The time to compare the lease_time to. Use datetime.now() if None

        Returns:
            valid_lease: bool
                If the lease should still be an owner
        """
        # Don't default to datetime.now() in function args as that's only evaluated once
        current_time = current_time or datetime.now()
        return current_time < datetime.fromisoformat(lease_time) + self.duration_delta
