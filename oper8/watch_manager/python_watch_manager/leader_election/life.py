"""Implementation of the Leader-for-Life LeaderElection"""
# First Party
import alog

# Local
from .... import config
from ....deploy_manager.owner_references import update_owner_references
from ....exceptions import ConfigError, assert_config
from ....managed_object import ManagedObject
from ....utils import nested_get
from ..utils import get_operator_namespace, get_pod_name
from .base import ThreadedLeaderManagerBase

log = alog.use_channel("LDRLIFE")


class LeaderForLifeManager(ThreadedLeaderManagerBase):
    """
    LeaderForLifeManager Class implements the old "leader-for-life" operator-sdk
    lock type. This lock creates a configmap with the operator pod as owner in
    the current namespace. This way when the pod is deleted or list so is the
    configmap.
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
        pod_name = get_pod_name()
        assert_config(self.lock_name, "Unable to detect lock name")
        assert_config(self.namespace, "Unable to detect operator namespace")
        assert_config(pod_name, "Unable to detect pod name")

        # Get the current pod context which is used in the lock configmap
        log.debug("Gathering pod context information")
        success, pod_obj = self.deploy_manager.get_object_current_state(
            kind="Pod", name=pod_name, namespace=self.namespace, api_version="v1"
        )
        if not success or not pod_obj:
            log.error(
                "Unable to fetch pod %s/%s Unable to use leader-for-life without ownerReference",
                self.namespace,
                pod_name,
            )
            raise ConfigError(
                f"Unable to fetch pod {self.namespace}/{pod_name}."
                "Unable to use leader-for-life without ownerReference"
            )

        self.pod_manifest = ManagedObject(pod_obj)

    def renew_or_acquire(self):
        """
        Renew or acquire lock by checking the current configmap status
        """
        # Get current config map
        success, cluster_config_map = self.deploy_manager.get_object_current_state(
            kind="ConfigMap",
            name=self.lock_name,
            namespace=self.namespace,
            api_version="v1",
        )
        if not success:
            log.warning(
                "Unable to fetch config map %s/%s", self.namespace, self.lock_name
            )

        # If configmap exists then verify owner ref
        if cluster_config_map:
            log.debug2(
                f"ConfigMap Lock {cluster_config_map} already exists, checking ownership"
            )
            owner_ref_list = nested_get(
                cluster_config_map, "metadata.ownerReferences", []
            )
            if len(owner_ref_list) != 1:
                log.error(
                    "Invalid leadership config map detected. Only one owner allowed"
                )
                self.release_lock()
                return

            if owner_ref_list[0].get("uid") == self.pod_manifest.uid:
                self.acquire_lock()
            else:
                self.release_lock()

        # Create configmap if it doesn't exist
        else:
            log.debug2(f"ConfigMap Lock {cluster_config_map} does not exist, creating")
            config_map = {
                "kind": "ConfigMap",
                "apiVersion": "v1",
                "metadata": {
                    "name": self.lock_name,
                    "namespace": self.namespace,
                },
            }
            update_owner_references(
                self.deploy_manager, self.pod_manifest.definition, config_map
            )
            success, _ = self.deploy_manager.deploy(
                [config_map], manage_owner_references=False
            )
            if not success:
                log.warning("Unable to acquire leadership lock")
                self.release_lock()
            else:
                self.acquire_lock()
