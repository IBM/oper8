"""
The implementation of the cluster resource interactions to manage a
TemporaryPatch resource type
"""

# Standard
from datetime import datetime
import copy
import json

# First Party
import alog

# Local
from ..component import Component
from ..constants import TEMPORARY_PATCHES_ANNOTATION_NAME
from ..decorator import component
from ..exceptions import ConfigError, assert_cluster, assert_config, assert_precondition
from ..session import Session

log = alog.use_channel("PATCH")


@component("temporarypatch")
class TemporaryPatchComponent(Component):
    """
    The TemporaryPatchComponent is responsible for applying the temporary patch
    annotation to the target oper8 resource type.

    See the ADR for full details:
        docs/adr/03-patches.md
    """

    def __init__(
        self,
        session: Session,
        disabled: bool,
        patch_name: str,
        target_api_version: str,
        target_kind: str,
        target_name: str,
    ):
        """At setup, this component will attempt to fetch the state of the
        target resource and fail out if not possible.

        Args:
            session:  Session
                The session that this Component belongs to
            disabled:  bool
                True if this patch should be removed from the target, False if
                it should be added to the target
            patch_name:  str
                The name of the patch itself that will be used to uniquely
                identify this patch within the target
            target_api_version:  str
                The group/version string for the apiVersion of the target type
            target_kind:  str
                The kind string for the target type
            target_name:  str
                The metadata.name string for the target instance
        """
        super().__init__(session=session, disabled=disabled)

        # Hold the target info for later
        self._patch_name = patch_name
        self._target_api_version = target_api_version
        self._target_kind = target_kind
        self._target_name = target_name

        # Look up the target state
        log.debug2(
            "Looking up target [%s/%s/%s/%s]",
            session.namespace,
            target_api_version,
            target_kind,
            target_name,
        )
        success, target_state = session.deploy_manager.get_object_current_state(
            api_version=target_api_version,
            kind=target_kind,
            name=target_name,
            namespace=session.namespace,
        )
        assert_cluster(
            success,
            "Failed to look up target resource: {}/{}/{}/{}".format(
                session.namespace,
                target_api_version,
                target_kind,
                target_name,
            ),
        )
        assert_precondition(
            disabled or target_state is not None,
            "Could not find target resource: {}/{}/{}/{}".format(
                session.namespace,
                target_api_version,
                target_kind,
                target_name,
            ),
        )
        self._target_state = target_state

    def deploy(self, session: Session) -> bool:
        """The TemporaryPatchComponent runs a custom deploy operation to update
        the target resource with the patch annotation.
        """
        return self._manage_patch(session, remove_patch=False)

    def disable(self, session: Session) -> bool:
        """The TemporaryPatchComponent runs a custom disable operation to remove
        the patch from the target resource.
        """
        if self._target_state is not None:
            return self._manage_patch(session, remove_patch=True)
        return True

    ## Implementation Details ##################################################

    def _manage_patch(self, session: Session, remove_patch: bool) -> bool:
        """Manage the presence of the patch on the target"""
        log.debug2("Patch will be [%s]", "removed" if remove_patch else "added")

        # Look for existing annotation entries on the resource
        log.debug4("Attempting to parse target state: %s", self._target_state)
        annotations = self._target_state["metadata"].get("annotations", {})
        anno_content = annotations.get(TEMPORARY_PATCHES_ANNOTATION_NAME, "{}")
        try:
            existing_patches = json.loads(anno_content)
        except (TypeError, json.decoder.JSONDecodeError) as err:
            raise ConfigError(
                f"Patch annotation is not valid json: {anno_content}"
            ) from err
        log.debug3("Existing Patches: %s", existing_patches)
        assert_config(
            isinstance(existing_patches, dict),
            f"Existing patches formatted incorrectly: {anno_content}",
        )

        # Perform the add/delete operation
        updated_patches = copy.deepcopy(existing_patches)
        if remove_patch:
            log.debug2("Removing patch [%s]", self._patch_name)
            updated_patches.pop(self._patch_name, None)
        elif self._patch_name not in updated_patches:
            log.debug("Adding patch [%s]", self._patch_name)
            # Add this patch to the existing patches and timestamp it. The
            # structure of the patch body holds the api_version and kind for the
            # patch itself so that the Controller for the target resource can
            # look it up without hard-coding either value.
            log.debug2("Adding patch [%s]", self._patch_name)
            updated_patches[self._patch_name] = {
                "timestamp": datetime.now().isoformat(),
                "api_version": session.api_version,
                "kind": session.kind,
            }

        if updated_patches != existing_patches:
            log.debug(
                "Re-applying [%s/%s/%s/%s]",
                session.namespace,
                self._target_api_version,
                self._target_kind,
                self._target_name,
            )

            # Re-serialize the annotations
            self._target_state["metadata"].setdefault("annotations", {})[
                TEMPORARY_PATCHES_ANNOTATION_NAME
            ] = json.dumps(updated_patches)

            # Update the resource in the cluster
            success, _ = session.deploy_manager.deploy(
                resource_definitions=[self._target_state],
                manage_owner_references=False,
            )
            assert_cluster(
                success,
                "Failed to re-apply [{}/{}/{}/{}]".format(
                    session.namespace,
                    self._target_api_version,
                    self._target_kind,
                    self._target_name,
                ),
            )

        # To keep with the deploy API, return success at this point
        return True
