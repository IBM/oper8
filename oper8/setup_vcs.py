"""
This module uses VCS to create a trimmed down repo with a selection of local
branches and tags and a fully flattened history.
"""

# Standard
from typing import List, Optional
import os
import re
import shutil

# First Party
import alog

# Local
from .vcs import VCS, VCSConfigError

## Globals #####################################################################

log = alog.use_channel("VCSSU")

# Sentinel so that None can be passed to tag_expr
DEFAULT_TAG_EXPR = r"[0-9]+\.[0-9]+\.[0-9]+"
__UNSET__ = "__UNSET__"

# Default value for the destination
DEFAULT_DEST = "oper8_vcs"

## Public ######################################################################


def setup_vcs(
    source: str,
    destination: Optional[str] = None,
    branch_expr: Optional[List[str]] = None,
    tag_expr: Optional[List[str]] = __UNSET__,
    force: bool = False,
):
    """This utility will initialize an operator's VCS directory for use with
    oper8's VCS versioning.

    Args:
        source (str): The path to the source repository on disk
        destination (Optional[str]): The path where the VCS repo should be
            created
        branch_expr (Optional[List[str]]): Regular expression(s) to use to
            identify branches to retain in the VCS repo
        tag_expr (Optional[List[str]]): Regular expression(s) to use to
            identify tags to retain in the VCS repo
        force (bool): Force overwrite existing destination
    """
    initializer = VCSRepoInitializer(
        source=source, destination=destination or DEFAULT_DEST, force=force
    )
    initializer.initialize_branches(
        branch_expr=branch_expr,
        tag_expr=tag_expr if tag_expr is not __UNSET__ else [DEFAULT_TAG_EXPR],
    )
    initializer.clean_up()


## Impl ########################################################################


class VCSRepoInitializer:
    """This class encapsulates the key attributes of the VCS repo initialization
    process
    """

    # The git repo that is being compressed for VCS versioning
    source_repo: VCS
    # The git repo where the VCS versioning repo is going to be created
    dest_repo: VCS
    # The remote within the destination repo that refers to the source repo
    source_remote: str
    # The reference to the root empty commit in the destination repo
    root_ref: str

    # Default branch name used when creating the repo
    DEFAULT_BRANCH_NAME = "__root__"

    # The name of the source remote
    SOURCE_REMOTE = "__source__"

    def __init__(self, source: str, destination: str, force: bool):
        """Initialize and set up the repos and common attributes"""

        # Make sure the source is a git repo
        try:
            self.source_repo = VCS(source)
        except VCSConfigError as err:
            msg = f"Invalid source git repo: {source}"
            log.error(msg)
            raise ValueError(msg) from err
        log.debug("Source Repo: %s", source)

        # Set up the dest and make sure it's empty
        if os.path.isfile(destination):
            msg = f"Invalid destination: {destination} is a file"
            log.error(msg)
            raise ValueError(msg)
        os.makedirs(destination, exist_ok=True)
        contents = os.listdir(destination)
        if contents:
            if not force:
                msg = f"Invalid destination: {destination} is not empty"
                log.error(msg)
                raise ValueError(msg)
            log.debug("Force cleaning dest %s", destination)
            for entry in contents:
                full_path = os.path.join(destination, entry)
                if os.path.isdir(full_path):
                    log.debug3("Removing dir: %s", full_path)
                    shutil.rmtree(full_path)
                else:
                    log.debug3("Removing file: %s", full_path)
                    os.remove(full_path)

        # Initialize the dest as an empty repo
        log.info("Initializing dest repo: %s", destination)
        self.dest_repo = VCS(
            destination, create_if_needed=True, initial_head=self.DEFAULT_BRANCH_NAME
        )
        self.dest_repo.create_commit("root")
        self.dest_repo.add_remote(self.SOURCE_REMOTE, source)
        self.root_ref = self.dest_repo.head

    def initialize_branches(
        self,
        branch_expr: Optional[List[str]],
        tag_expr: Optional[List[str]],
    ):
        """Perform the initialize of all branches in the destination repo from
        the branches and tags that match the given expressions.
        """
        # Get all tags and branches
        tags = self._list_tags(self.source_repo)
        branches = self._list_branches(self.source_repo)
        log.debug2("All Tags: %s", tags)
        log.debug2("All Branches: %s", branches)

        # Filter the tags and branches by the filter arguments
        keep_tags = self._filter_refs(tags, tag_expr)
        keep_branches = self._filter_refs(branches, branch_expr)
        log.debug2("Keep Tags: %s", keep_tags)
        log.debug2("Keep Branches: %s", keep_branches)

        # For each retained reference, fetch the ref from the source, check out the
        # files to the dest, and make a fresh commit
        for keep_ref in keep_tags:
            log.debug("Making destination branch [%s] from tag", keep_ref)
            self._make_dest_branch(keep_ref, False)
        for keep_ref in keep_branches:
            log.debug("Making destination branch [%s] from branch", keep_ref)
            self._make_dest_branch(keep_ref, True)

    def clean_up(self):
        """Clean out all unnecessary content from the destination repo"""

        # Check the root back out
        log.debug3("Checking out root")
        self.dest_repo.checkout_ref(self.root_ref)

        # Delete the source remote
        self.dest_repo.delete_remote(self.SOURCE_REMOTE)

        # Remove all tags
        for tag_name in self._list_tags(self.dest_repo):
            self.dest_repo.delete_tag(tag_name)

        # Remove the root branch and leave HEAD detached
        self.dest_repo.checkout_detached_head()
        self.dest_repo.delete_branch(self.DEFAULT_BRANCH_NAME)

        # Compress the references to remove orphaned refs and objects
        self.dest_repo.compress_references()

    ## Impl ##

    def _get_all_checkout_files(self, keep_ref: str) -> List[str]:
        """Get all of the file paths in the given ref relative to the dest repo

        # NOTE: This relies on pygit2 syntax!
        """
        commit, _ = self.dest_repo.get_ref(keep_ref)
        diff = commit.tree.diff_to_workdir()
        return [delta.new_file.path for delta in diff.deltas]

    def _make_dest_branch(self, keep_ref: str, is_branch: bool):
        """This is the function that does the main work of copying code from the
        source to the destination and creating a clean commit.
        """
        # Make sure the root is checked out in the destination repo
        log.debug3("Checking out root")
        self.dest_repo.checkout_ref(self.root_ref)

        # Fetch the ref to keep from the source in the dest
        log.debug3("Fetching %s", keep_ref)
        self.dest_repo.fetch_remote(self.SOURCE_REMOTE, {keep_ref})

        # Check out the files
        remote_ref_name = keep_ref
        if is_branch:
            remote_ref_name = f"refs/remotes/{self.SOURCE_REMOTE}/{keep_ref}"
        log.debug3("Checking out files for %s", remote_ref_name)
        self.dest_repo.checkout_ref(
            remote_ref_name, paths=self._get_all_checkout_files(remote_ref_name)
        )

        # Make a new branch named with this ref's shorthand name with any remote
        # information removed
        branch_name = keep_ref
        log.debug2("Dest branch name: %s", branch_name)
        root_commit, _ = self.dest_repo.get_ref(self.root_ref)
        branch = self.dest_repo.create_branch(branch_name, root_commit)
        self.dest_repo.checkout_ref(branch.name)

        # Make a commit with these files
        self.dest_repo.create_commit(keep_ref, parents=[root_commit.oid])

        # Check the root branch back out
        self.dest_repo.checkout_ref(self.DEFAULT_BRANCH_NAME)

    ## Static Helpers ##

    @staticmethod
    def _list_branches(repo: VCS) -> List[str]:
        """List all of the local branches

        Args:
            repo (VCS): The repo to list

        Returns
            refs (List[str]): A set of all branch references
        """
        refs = set()
        for ref in repo.list_refs():
            _, repo_ref = repo.get_ref(ref)
            name_parts = repo_ref.name.split("/")
            if (
                "tags" not in name_parts
                and "HEAD" not in name_parts
                and "remotes" not in name_parts
                and repo_ref.name != "refs/stash"
            ):
                refs.add(ref)
        return sorted(sorted(refs))

    @staticmethod
    def _list_tags(repo: VCS) -> List[str]:
        """List all of the tags and references in the repo

        Args:
            repo (VCS): The repo to list

        Returns
            refs (List[str]): A set of all tag references
        """
        return list(
            sorted(
                {
                    ref
                    for ref in repo.list_refs()
                    if "refs/tags" in repo.get_ref(ref)[1].name
                }
            )
        )

    @staticmethod
    def _filter_refs(refs: List[str], exprs: Optional[List[str]]) -> List[str]:
        """Keep all refs that match at least one of the expressions"""
        return [ref for ref in refs if any(re.match(expr, ref) for expr in exprs or [])]
