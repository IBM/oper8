"""
Controller implementation for the logic to manage a TemporaryPatch resource type
"""

# Standard
from typing import List, Type, Union

# First Party
import alog

# Local
from ..controller import Controller
from ..decorator import controller
from ..session import Session
from .temporary_patch_component import TemporaryPatchComponent

log = alog.use_channel("PATCH")


@controller(
    group="oper8.org",
    version="v1",
    kind="TemporaryPatch",
    extra_properties={"disable_vcs": True},
)
class TemporaryPatchController(Controller):
    """
    The TemporaryPatchController is a custom Controller implementation that
    manages temporary patch resources for `oper8`.

    See docs/temporary_patches.md for full details.

    NOTE: When deploying using OLM, there is a requirement that only a single
        operator in a given namespace control each group/version/kind. In this
        case, it will be necessary for the consuming operator to customize the
        group/version/kind of the TemporaryPatch resource so that it is specific
        to the given operator's product. This can be accomplished by deriving a
        child of TemporaryPatchController as follows:

    ```py
    from oper8 import TemporaryPatchController, controller

    @controller(group="my.group.name", version="v1", kind="MyTemporaryPatch")
    class MyTemporaryPatchController(TemporaryPatchController):
        '''The temporary patch class for my operator!'''
    ```

    NOTE: Custom patch kinds are allowed automatically when the subclass is
        loaded in the same process as the controller for the target resource.
        Otherwise, the kind must be added to `temporary_patch.allowed_kinds`
        in the library config. See docs/temporary_patches.md.
    """

    def __init__(
        self, patchable_kinds: List[Union[str, Type[Controller]]], *args, **kwargs
    ):
        """Construct with the list of kinds that can be patched by this
        controller

        Args:
            patchable_kinds:  List[Union[str, Type[Controller]]]
                This list holds the kinds of resources that can be patched by
                this controller. Entries may be just a `kind` in which case any
                apiVersion will be accepted, or `apiVersion/kind` in which case
                the kind matching will be scoped to the given apiVersion. If a
                Controller type is given, its group/version/kind will be used.
        """
        self._patchable_kinds = [
            self._patchable_kind_label(entry) for entry in patchable_kinds
        ]
        super().__init__(*args, **kwargs)

    def setup_components(self, session: Session):
        """Set up the component that will apply the patch to the patched
        resource if the resource is one that can be patched.
        """
        self._do_setup_components(session, is_finalizer=False)

    def finalize_components(self, session: Session):
        """Set up the component that will remove the patch from the target"""
        self._do_setup_components(session, is_finalizer=True)

    ## Shared Utilities ########################################################

    @classmethod
    def get_temporary_patch_kinds(cls) -> List[str]:
        """Get the `apiVersion/kind` labels for this class and all loaded
        subclasses. This is used to determine which kinds may be used as
        temporary patch sources when they are not explicitly configured.

        NOTE: Only subclasses that have been imported in the current process
            can be discovered.

        Returns:
            patch_kinds:  List[str]
                The unique `group/version/kind` labels for all known
                TemporaryPatchController classes
        """
        patch_kinds = []
        to_visit = [cls]
        while to_visit:
            current = to_visit.pop(0)
            to_visit.extend(current.__subclasses__())
            if not all(
                getattr(current, attr, None) for attr in ["group", "version", "kind"]
            ):
                continue
            kind_label = cls._patchable_kind_label(current)
            if kind_label not in patch_kinds:
                patch_kinds.append(kind_label)
        return patch_kinds

    ## Implementation Details ##################################################

    @staticmethod
    def _patchable_kind_label(entry: Union[str, Type[Controller]]) -> str:
        """This helper implements the logic for registering a controller as a
        patchable kind
        """
        if isinstance(entry, type) and issubclass(entry, Controller):
            return "/".join(
                [
                    entry.group,
                    entry.version,
                    entry.kind,
                ]
            )
        return entry

    def _do_setup_components(self, session: Session, is_finalizer: bool):
        """We set up the same set of components, but configure differently when
        running the finalizer
        """

        # Pull out the information required to look up the resource
        target_api_version = session.spec.apiVersion
        target_kind = session.spec.kind
        target_name = session.spec.name
        namespace = session.namespace
        patch_name = session.name
        assert (
            target_api_version is not None
        ), "Missing required field 'spec.apiVersion'"
        assert target_kind is not None, "Missing required field 'spec.kind'"
        assert target_name is not None, "Missing required field 'spec.name'"
        assert namespace is not None, "No namespace found in metadata"
        assert patch_name is not None, "No name found in metadata"
        log.info(
            "Processing patch for [%s/%s/%s/%s]",
            namespace,
            target_api_version,
            target_kind,
            target_name,
        )

        # Only add the component if this kind (or apiVersion/kind) is patchable
        if (
            target_kind in self._patchable_kinds
            or f"{target_api_version}/{target_kind}" in self._patchable_kinds
        ):
            log.debug(
                "Adding patch component for patchable kind %s/%s",
                target_api_version,
                target_kind,
            )
            TemporaryPatchComponent(
                session=session,
                disabled=is_finalizer,
                patch_name=patch_name,
                target_api_version=target_api_version,
                target_kind=target_kind,
                target_name=target_name,
            )
