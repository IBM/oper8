"""
Version Control System class manages a specific git directory.
"""

# Standard
from enum import Enum
from typing import List, Optional, Set, Tuple
import pathlib
import time

# Third Party
from pygit2 import (  # pylint: disable=no-name-in-module
    GIT_CONFIG_LEVEL_GLOBAL,
    GIT_OPT_GET_SEARCH_PATH,
    AlreadyExistsError,
    Branch,
    Commit,
    Config,
    GitError,
    Reference,
    Repository,
    Signature,
    clone_repository,
    init_repository,
    option,
)

# First Party
import alog

# Local
from .exceptions import ConfigError, Oper8FatalError, PreconditionError

log = alog.use_channel("VCS")


## VCS Constants #############################################################


class VCSCheckoutMethod(Enum):
    """Enum for available VCS checkout methods"""

    WORKTREE = "worktree"
    CLONE = "clone"


## VCS Errors #############################################################


class VCSMultiProcessError(PreconditionError):
    """VCS Error for when multiple git processes attempt to update the git directory
    at the same time"""


class VCSConfigError(ConfigError):
    """Error for VCS Specific config exception"""


class VCSRuntimeError(Oper8FatalError):
    """Error for general git exceptions"""


## VCS  #############################################################


class VCS:
    """Generic class for handling a git repository. This class contains helper functions
    to get, list, and checkout references. Each instance of this class corresponds to a
    different git directory
    """

    def __init__(self, directory: str, create_if_needed: bool = False, **kwargs):
        """Initialize the pygit2 Repository reference

        Args:
            directory: str
                The git directory
            create_if_needed: bool
                If True, the repo will be initialized if it doesn't already
                exist
            **kwargs:
                Passthrough args to the repository setup
        """
        # Get repo reference
        try:
            # Check for global file and create one if needed. This
            # is needed due to this issue: https://github.com/libgit2/pygit2/issues/915
            config_file = (
                pathlib.Path(option(GIT_OPT_GET_SEARCH_PATH, GIT_CONFIG_LEVEL_GLOBAL))
                / ".gitconfig"
            )
            if not config_file.exists():
                config_file.touch(exist_ok=True)

            # Disable safe git directories. This solves a common problem
            # when running in openshift where the running user is different
            # from the owner of the filesystem
            global_config = Config.get_global_config()
            global_config["safe.directory"] = "*"

            self.repo = Repository(directory)
            log.debug2("Found repo: %s", self.repo)
        except GitError as err:
            if create_if_needed:
                self.repo = init_repository(directory, **kwargs)
            else:
                log.error("Invalid Repo: %s", err, exc_info=True)
                raise VCSConfigError(f"Invalid Repo at {directory}") from err

    ### Accessors

    @property
    def head(self) -> str:
        """Get a reference to the current HEAD"""
        return self.repo.head.target.hex

    def get_ref(self, refish: str) -> Tuple[Commit, Reference]:
        """Get a git commit and reference from a shorthand string

        Args:
            refish: str
                The human readable form of a git reference like branch name
                or commit hash

        Returns
            commit_and_reference: Tuple[Commit,Reference]
                Both a commit and reference for a given refish
        """
        try:
            return self.repo.resolve_refish(refish)
        except KeyError as err:
            log.error("Unable to find version %s in repo", refish)
            raise VCSConfigError(  # pylint: disable=raise-missing-from
                f"Version: '{refish}' not found in repo"
            ) from err

    def list_refs(self) -> Set[str]:
        """List all of the tags and references in the repo

        Returns
            ref_list: Set[str]
                A set of all references' shorthand as strings
        """
        # Loop through repo tags to get each tag's short name
        refs_set = set()
        for ref in self.repo.references.objects:
            refs_set.add(ref.shorthand)

        return refs_set

    ### Mutators

    def checkout_ref(
        self,
        refish: str,
        dest_path: Optional[pathlib.Path] = None,
        method: VCSCheckoutMethod = VCSCheckoutMethod.WORKTREE,
        **kwargs,
    ):
        """Checkout a refish to a given destination directory. This function
        first attempts to create a worktree but on failure will do a traditional
        clone

        Args:
            refish: str
                The refish to be checked out in the dest_dir
            dest_path: Optional[pathlib.Path]
                The destination directory if not in-place
            method: VCSCheckoutMethod=VCSCheckoutMethod.WORKTREE
                The checkout method to use, either a git clone or worktree add
            **kwargs
                Kwargs to pass through to checkout
        """

        # Get the commit and ref for a given refish
        commit, ref = self.get_ref(refish)

        # If in-place, check out directly
        if not dest_path:
            log.debug2("Checking out %s in place", refish)
            self.repo.checkout(ref, **kwargs)
            return

        # Check if dest directory already exists and if it has the correct
        # commit
        if dest_path.is_dir():
            dest_vcs = VCS(dest_path)

            # Check if the dest index file has been created. It is the last
            # part of a checkout. If index has not been created than another
            # process must be working on it
            dest_index_file = pathlib.Path(dest_vcs.repo.path) / "index"
            if not dest_index_file.is_file():
                raise VCSMultiProcessError(
                    "Index file not found. Checkout already in progress "
                )

            if dest_vcs.repo.head.peel(Commit) != commit:
                raise VCSConfigError(
                    f"Destination directory {dest_path} already exists with incorrect branch"
                )
            return

        # Create the directory if it doesn't exist
        dest_path.parents[0].mkdir(parents=True, exist_ok=True)

        if method == VCSCheckoutMethod.WORKTREE:
            # Create a unique branch for each worktree
            cleaned_dest_dir = "_".join(dest_path.parts[1:])
            branch_name = f"{refish}_{cleaned_dest_dir}"

            branch = self.create_branch(branch_name, commit)
            self._create_worktree(branch_name, dest_path, branch)
        elif method == VCSCheckoutMethod.CLONE:
            self._clone_ref(dest_path, ref, **kwargs)
        else:
            raise VCSConfigError(f"Invalid checkout method: {method}")

    def create_commit(
        self,
        message: str,
        parents: Optional[List[str]] = None,
        committer_name: str = "Oper8",
        committer_email: str = "noreply@oper8.org",
    ):
        """Create a commit in the repo with the files currently in the index

        Args:
            message: str
                The commit message
            parents: Optional[List[str]]
                Parent commit hashes
            committer_name: str
                The name of the committer
            committer_email: str
                Email address for this committer
        """
        parents = parents or []
        parent_commits = []
        for parent in parents:
            try:
                parent_commits.append(self.repo.get(parent))
            except ValueError as err:
                raise ValueError(f"Invalid parent commit: {parent}") from err
        signature = Signature(committer_name, committer_email)
        self.repo.create_commit(
            "HEAD", signature, signature, message, self.repo.index.write_tree(), parents
        )

    def add_remote(self, remote_name: str, remote_path: str):
        """Add a named remote to the repo

        Args:
            remote_name: str
                The name of the remote
            remote_path: str
                The path on disk to the remote repo
        """
        self.repo.remotes.create(remote_name, remote_path)

    def delete_remote(self, remote_name: str):
        """Remove a remote from the repo

        Args:
            remote_name:  str
                The name of the remote
        """
        self.repo.remotes.delete(remote_name)

    def fetch_remote(
        self,
        remote_name: str,
        refs: Optional[Set[str]] = None,
        wait: bool = True,
    ):
        """Fetch content from the named remote. If no refs given, all refs are
        fetched.

        Args:
            remote_name: str
                The name of the remote to fetch
            refs: Optional[Set[str]]
                The refs to fetch (fetch all if not given)
            wait: bool
                If true, wait for fetch to complete
        """
        remote = self.repo.remotes[remote_name]
        progress = remote.fetch(list(refs or []))
        while wait and progress.received_objects < progress.total_objects:
            time.sleep(0.1)  # pragma: no cover

    def create_branch(self, branch_name: str, commit: Commit) -> Branch:
        """Create branch given a name and commit

        Args:
            branch_name: str
                The name to be created
            commit: Commit
                The commit for the branch to be created from

        Returns:
            branch: Branch
                The created branch"""
        if branch_name in self.repo.branches:
            branch = self.repo.branches.get(branch_name)
            if branch.peel(Commit) != commit:
                raise VCSRuntimeError("Branch already exists with incorrect commit")
            return branch

        try:
            log.debug("Creating branch for %s", branch_name)
            return self.repo.branches.create(branch_name, commit)
        except AlreadyExistsError as err:
            # Branch must have been created by different processes
            log.warning("Branch %s already exists", branch_name)
            raise VCSMultiProcessError(f"Branch {branch_name} already exists") from err

        except OSError as err:
            raise VCSRuntimeError("Unable to create branch") from err

    def delete_branch(self, branch_name: str):
        """Delete a branch from the repo

        Args:
            branch_name:  str
                The name of the branch
        """
        self.repo.branches.delete(branch_name)

    def delete_tag(self, tag_name: str):
        """Delete a tag from the repo

        Args:
            tag_name:  str
                The name of the tag
        """
        self.repo.references.delete(f"refs/tags/{tag_name}")

    def checkout_detached_head(self, refish: Optional[str] = None):
        """Check out the current HEAD commit as a detached head

        Args:
            refish:  Optional[str]
                The ref to check out. If not given, the current HEAD is used
        """
        refish = refish or self.head

        # Create a placeholder reference to a non-existent remote
        dummy_ref = self.repo.references.create(
            "refs/remotes/doesnotexist/foobar", refish
        )
        self.repo.checkout(dummy_ref)
        self.repo.references.delete(dummy_ref.name)

    def compress_references(self):
        """Compress unreachable references in the repo"""
        self.repo.compress_references()

    ### Implementation Details

    def _clone_ref(self, dest_path: pathlib.Path, ref: Reference, **kwargs):
        """Clone a refish to a given destination directory

        Args:
            dest_path: pathlib.Path
                The destination directory
            refish: str
                The branch or ref to be checked out
            **kwargs
                Kwargs to pass through to checkout
        """
        try:
            dest_repo = clone_repository(self.repo.path, dest_path)
            dest_repo.checkout(refname=ref, **kwargs)
        except (OSError, GitError, KeyError) as err:
            log.error("Unable to clone refish: %s", ref.shorthand, exc_info=True)
            raise VCSRuntimeError("Unable to clone ref from repo") from err

    def _create_worktree(
        self, worktree_name: str, dest_path: pathlib.Path, branch: Branch
    ):
        """Create worktree for branch. This is better than a direct checkout
        as it saves space on checkout and is faster. This is especially
        beneficial on repositories with large git directories

        Args:
           worktree_name: str
               The name of the worktree
           dest_path: pathlib.Path
               The destination directory
           branch: Branch
               The branch to be checked out in the worktree
        """
        log.debug("Creating new worktree for %s", worktree_name)
        try:
            self.repo.add_worktree(worktree_name, dest_path, branch)
        except AlreadyExistsError as err:
            # Worktree must have been created by different processes
            log.warning("Worktree %s already exists", worktree_name)
            raise VCSMultiProcessError(
                f"Worktree {worktree_name} already exists"
            ) from err
        except GitError as err:
            # If reference is already checked out it must have been done by a different process
            if str(err) == "reference is already checked out":
                log.warning(
                    "Branch %s already checked out by other process",
                    worktree_name,
                    exc_info=True,
                )
                raise VCSMultiProcessError(
                    f"Branch {worktree_name} already checked out by other process"
                ) from err

            log.error(
                "Unexpected Git Error when adding worktree: %s", err, exc_info=True
            )
            raise VCSRuntimeError(
                "Adding worktree failed with unexpected git error"
            ) from err
