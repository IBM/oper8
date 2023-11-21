"""Implementation of the Leader-with-Lease LeaderElection"""
# Standard
from datetime import datetime, timedelta, timezone

# Third Party
from dateutil.parser import parse

# First Party
import alog

# Local
from .... import config
from ....exceptions import assert_config
from ..utils import get_operator_namespace, get_pod_name, parse_time_delta
from .base import ThreadedLeaderManagerBase

log = alog.use_channel("LDRLIFE")


class LeaderWithLeaseManager(ThreadedLeaderManagerBase):
    """
    LeaderWithLeaseManager Class implements the "leader-with-lease" operator-sdk
    lock type. This lock creates a lease object with the operator pod as owner and
    constantly re-acquires the lock.
    """

    def __init__(self, deploy_manager):
        """
        Initialize class with lock_name, current namespace, and pod information
        """
        super().__init__(deploy_manager)

        # Gather lock_name, namespace and pod manifest
        self.lock_name = (
            config.operator_name
            if config.operator_name
            else config.python_watch_manager.lock.name
        )
        self.namespace = get_operator_namespace()
        self.lock_identity = get_pod_name()
        assert_config(self.lock_name, "Unable to detect lock name")
        assert_config(self.namespace, "Unable to detect operator namespace")
        assert_config(self.lock_identity, "Unable to detect lock identity")

    def renew_or_acquire(self):
        """
        Renew or acquire lock by checking the current lease status
        """

        # Template out the expected lease. This is edited based on the current
        # lease status
        current_time = datetime.now(timezone.utc)
        lease_resource_version = None
        expected_lease_data = {
            "holderIdentity": self.lock_identity,
            "acquireTime": current_time.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            "leaseDurationSeconds": round(
                parse_time_delta(
                    config.python_watch_manager.lock.duration
                ).total_seconds()
            ),
            "leaseTransitions": 1,
            "renewTime": current_time.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        }

        # Get current lease
        success, lease_obj = self.deploy_manager.get_object_current_state(
            kind="Lease",
            name=self.lock_name,
            namespace=self.namespace,
            api_version="coordination.k8s.io/v1",
        )
        if not success:
            log.warning("Unable to fetch lease %s/%s", self.namespace, self.lock_name)

        # If lease exists then verify current holder is valid or update the expected
        # lease with the proper values
        if lease_obj and lease_obj.get("spec"):
            log.debug2(
                "Lease object %s already exists, checking holder", self.lock_name
            )

            lease_resource_version = lease_obj.get("metadata", {}).get(
                "resourceVersion"
            )
            lease_spec = lease_obj.get("spec")
            lock_holder = lease_spec.get("holderIdentity")

            if lock_holder != self.lock_identity:
                renew_time = parse(lease_spec.get("renewTime"))
                lease_duration = timedelta(
                    seconds=lease_spec.get("leaseDurationSeconds")
                )

                # If the renew+lease is after the current time than the other
                # lease holder is still valid
                if (renew_time + lease_duration) > current_time:
                    self.release_lock()
                    return

                log.info("Taking leadership from %s", lock_holder)
                # Increment leaseTransitions as we're taking ownership
                expected_lease_data["leaseTransitions"] = (
                    lease_spec.get("leaseTransitions", 1) + 1
                )

            # If we're the current holder than keep the current acquire time
            else:
                log.debug2(
                    "Lease object already owned. Reusing acquireTime and transitions"
                )
                expected_lease_data["acquireTime"] = lease_spec.get("acquireTime")
                expected_lease_data["leaseTransitions"] = lease_spec.get(
                    "leaseTransitions"
                )

        # Create or update the lease obj
        lease_resource = {
            "kind": "Lease",
            "apiVersion": "coordination.k8s.io/v1",
            "metadata": {
                "name": self.lock_name,
                "namespace": self.namespace,
            },
            "spec": expected_lease_data,
        }
        if lease_resource_version:
            lease_resource["metadata"]["resourceVersion"] = lease_resource_version

        success, _ = self.deploy_manager.deploy(
            [lease_resource], manage_owner_references=False
        )
        if not success:
            log.warning("Unable to acquire leadership lock")
            self.release_lock()
        else:
            self.acquire_lock()
