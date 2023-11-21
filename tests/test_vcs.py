"""Tests for the VCS class"""

# Standard
from unittest import mock
import pathlib
import shutil
import subprocess
import tempfile

# Third Party
import pygit2
import pytest

# First Party
import alog

# Local
from oper8.test_helpers.helpers import default_branch_name, vcs_project
from oper8.vcs import (
    VCS,
    VCSCheckoutMethod,
    VCSConfigError,
    VCSMultiProcessError,
    VCSRuntimeError,
)

log = alog.use_channel("TEST")


################################################################################
## Helpers #####################################################################
################################################################################


def validate_vcs_dir(directory, refish):
    """Validate git directory using git fsck"""

    # Validate git directory integrity
    subprocess.run(
        [
            "git",
            "-C",
            directory,
            "fsck",
        ],
        check=True,
    )

    # Validate that the checked out commit matches the expected one
    expected_commit_hash = subprocess.run(
        ["git", "-C", directory, "rev-parse", refish], capture_output=True
    ).stdout
    current_commit_hash = subprocess.run(
        ["git", "-C", directory, "rev-parse", "HEAD"], capture_output=True
    ).stdout
    assert current_commit_hash == expected_commit_hash


################################################################################
## Tests #######################################################################
################################################################################

##################
## Construction ##
##################


def test_construct_defaults(vcs_project):
    """Make sure that a VCS can be constructed with a generic repository"""
    vcs = VCS(vcs_project)
    assert vcs.repo


def test_construct_defaults_invalid_repo():
    """Make sure that a VCS can be constructed with a generic repository"""
    with tempfile.TemporaryDirectory() as temp_dir:
        with pytest.raises(VCSConfigError):
            vcs = VCS(temp_dir)


##################
## get_ref ##
##################


@pytest.mark.parametrize(
    ["refish", "exception"],
    [
        ["1.2.3", None],
        [default_branch_name(), None],
        ["refs/tags/1.2.3", None],
        ["invalid", VCSConfigError],
    ],
)
def test_get_ref(vcs_project, refish, exception):
    vcs = VCS(vcs_project)

    if exception:
        with pytest.raises(exception):
            vcs.get_ref(refish)
    else:
        vcs.get_ref(refish)


##################
## list_refs ##
##################


def test_list_refs(vcs_project):
    vcs = VCS(vcs_project)

    assert "1.2.3" in vcs.list_refs()
    assert default_branch_name() in vcs.list_refs()
    # Ensure only short hand versions are listed
    assert "refs/tags/1.2.3" not in vcs.list_refs()


##################
## checkout_ref ##
##################


@pytest.mark.parametrize(
    ["refish", "method", "exception"],
    [
        ["1.2.3", VCSCheckoutMethod.WORKTREE, None],
        ["1.2.3", VCSCheckoutMethod.CLONE, None],
        ["1.2.3", "invalid", VCSConfigError],
        ["wrong", VCSCheckoutMethod.CLONE, VCSConfigError],
    ],
)
def test_checkout_ref(vcs_project, refish, method, exception):
    with tempfile.TemporaryDirectory() as dest_dir:
        checkout_path = pathlib.Path(dest_dir) / "dest_repo"

        vcs = VCS(vcs_project)

        if exception:
            with pytest.raises(exception):
                vcs.checkout_ref(dest_path=checkout_path, refish=refish, method=method)
        else:
            vcs.checkout_ref(dest_path=checkout_path, refish=refish, method=method)
            validate_vcs_dir(checkout_path, refish)


def test_checkout_ref_existing(vcs_project):
    with tempfile.TemporaryDirectory() as dest_dir:
        dest_path = pathlib.Path(dest_dir)

        vcs = VCS(vcs_project)

        # Copy the vcs project to the destination manually
        shutil.copytree(vcs_project, dest_path, dirs_exist_ok=True)

        # Validate that checking out the same commit to the same destination
        # works as expected
        vcs.checkout_ref(
            dest_path=dest_path,
            refish=default_branch_name(),
        )

        # Check that two refishs that point to the same commit are able to reuse
        # directory
        with pytest.raises(VCSConfigError):
            vcs.checkout_ref(
                dest_path=dest_path,
                refish="1.2.3",
            )

        # Ensure multiprocessing error if destination git directory isn't fully checked out
        (dest_path / ".git" / "index").unlink()
        with pytest.raises(VCSMultiProcessError):
            vcs.checkout_ref(
                dest_path=dest_path,
                refish=default_branch_name(),
            )


def test_clone_ref(vcs_project):
    with tempfile.TemporaryDirectory() as dest_dir:
        dest_path = pathlib.Path(dest_dir)
        vcs = VCS(vcs_project)

        _, ref = vcs.get_ref(default_branch_name())

        vcs._clone_ref(
            dest_path=dest_path,
            ref=ref,
        )

        # Ensure cloned a valid directory
        validate_vcs_dir(dest_path, default_branch_name())
        # Ensure the entire git directory was copied
        assert (dest_path / ".git").is_dir()


def test_clone_ref_exceptions(vcs_project):
    vcs = VCS(vcs_project)
    _, ref = vcs.get_ref("")

    with pytest.raises(VCSRuntimeError):
        vcs._clone_ref(
            dest_path=None,
            ref=ref,
        )


def test_create_branch(vcs_project):
    vcs = VCS(vcs_project)

    commit, _ = vcs.get_ref(default_branch_name())

    # Ensure branch creation works
    vcs.create_branch(
        branch_name="test_branch",
        commit=commit,
    )

    # Ensure branch creation works if an existing branch is already created
    vcs.create_branch(branch_name="test_branch", commit=commit)


def test_create_branch_exceptions(vcs_project):
    vcs = VCS(vcs_project)

    # Test creating a branch with a same name but different commit fails
    commit, _ = vcs.get_ref("1.2.3")
    with pytest.raises(VCSRuntimeError):
        vcs.create_branch(branch_name=default_branch_name(), commit=commit)

    # We can't easily simulate branch multiprocessing errors so mock them
    vcs.repo.branches.create = mock.Mock(side_effect=pygit2.AlreadyExistsError)
    with pytest.raises(VCSMultiProcessError):
        vcs.create_branch(branch_name="dev_branch", commit=commit)

    vcs.repo.branches.create.side_effect = OSError
    with pytest.raises(VCSRuntimeError):
        vcs.create_branch(branch_name="dev_branch", commit=commit)


def test_create_worktree(vcs_project):
    with tempfile.TemporaryDirectory() as dest_dir:
        dest_path = pathlib.Path(dest_dir) / "dest_repo"
        vcs = VCS(vcs_project)

        vcs._create_worktree("test", dest_path, branch=vcs.repo.branches.get("1.2.3"))

        validate_vcs_dir(dest_path, "1.2.3")


def test_create_worktree_exceptions(vcs_project):
    with tempfile.TemporaryDirectory() as dest_dir:
        dest_path = pathlib.Path(dest_dir)
        vcs = VCS(vcs_project)

        # If we try to create a worktree using an existing directory
        with pytest.raises(VCSMultiProcessError):
            vcs._create_worktree(
                "test", dest_path, branch=vcs.repo.branches.get("1.2.3")
            )

        # If we try to create a worktree for a branch that is already checked out
        # then raise a multiprocessing error
        with pytest.raises(VCSMultiProcessError):
            vcs._create_worktree(
                "test",
                dest_path / "dest_dir",
                branch=vcs.repo.branches.get(default_branch_name()),
            )

        # Simulate some other vcs error
        vcs.repo.add_worktree = mock.Mock(side_effect=pygit2.GitError)
        with pytest.raises(VCSRuntimeError):
            vcs._create_worktree(
                "test",
                dest_path / "dest_dir",
                branch=vcs.repo.branches.get(default_branch_name()),
            )
