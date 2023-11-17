"""
Tests for setting up a VCS repo
"""
# Standard
from dataclasses import dataclass, field
from typing import Dict, List, Optional
import os
import tempfile
import time

# Third Party
import pygit2
import pytest

# Local
from oper8.setup_vcs import setup_vcs

## Helpers #####################################################################

TEST = pygit2.Signature("Test", "test@test.it")


@dataclass
class TreeNode:
    files: Dict[str, str]
    message: str = "stub msg"
    children: List["TreeNode"] = field(default_factory=list)
    branch_name: Optional[str] = None
    tag_name: Optional[str] = None


def make_commits(
    repo: pygit2.Repository, node: TreeNode, parent: Optional[pygit2.Reference] = None
):
    # Create the files
    repo_root_path = os.path.realpath(os.path.join(repo.path, ".."))
    for fname, content in node.files.items():
        fdir, file_name = os.path.split(fname)
        base_dir = repo_root_path
        if fdir:
            base_dir = os.path.join(repo_root_path, fdir)
            os.makedirs(base_dir, exist_ok=True)
        with open(os.path.join(base_dir, file_name), "w") as handle:
            handle.write(content)
        repo.index.read()
        repo.index.add(fname)
        repo.index.write()

    # Make the commit
    tree = repo.index.write_tree()
    parents = [parent.target] if parent is not None else []
    commit_oid = repo.create_commit("HEAD", TEST, TEST, node.message, tree, parents)
    commit_ref = repo.head

    # If a branch name is given, make the branch, but don't check it out
    if node.branch_name:
        repo.create_branch(node.branch_name, repo.get(commit_oid))

    # If a tag name is given, make the tag
    if node.tag_name:
        repo.create_tag(
            node.tag_name,
            commit_oid,
            1,
            TEST,
            "some tag",
        )

    # Recurse on all children
    for child in node.children:
        make_commits(repo, child, commit_ref)

    # Reset the main branch to the parent and then check it out
    if parent:
        main_branch = repo.branches["main"]
        main_branch.set_target(parent.target)
        repo.checkout(main_branch)
        try:
            repo.stash(TEST, include_untracked=True)
            repo.stash_drop()
        except KeyError:
            pass


@pytest.fixture
def workdir():
    with tempfile.TemporaryDirectory() as workdir:
        prev_dir = os.getcwd()
        os.chdir(workdir)
        try:
            yield workdir
        finally:
            os.chdir(prev_dir)


def temp_git_repo(parent_dir: str, repo_root: TreeNode):
    repo_path = os.path.join(parent_dir, "repo")
    repo = pygit2.init_repository(repo_path, initial_head="main")
    make_commits(repo, repo_root)
    return repo_path


def validate_vcs_dir(
    source_repo_path: str, dest_repo_path: str, expected_branches: List[str]
):
    dest_repo = pygit2.Repository(dest_repo_path)
    root_commit = dest_repo.get(dest_repo.head.target)
    assert not root_commit.parents

    # Get all branches and make sure the list matches
    all_branches = set(dest_repo.branches)
    assert all_branches == set(expected_branches)

    # Add the source as a remote so we can compare files
    remote = dest_repo.remotes.create("validate", source_repo_path)
    progress = remote.fetch()
    while progress.received_objects < progress.total_objects:
        time.sleep(0.001)

    # For each branch validate that it's exactly one hop from the root and that
    # the content matches the source
    for branch_name in expected_branches:
        branch = dest_repo.branches.get(branch_name)
        branch_commit = dest_repo.get(branch.target)

        # One commit removed from root
        assert branch_commit.parents == [root_commit]

        # Matches remote
        source_ref = dest_repo.references.get(f"refs/tags/{branch_name}")
        if not source_ref:
            source_ref = dest_repo.references.get(
                f"refs/remotes/validate/{branch_name}"
            )
        assert source_ref
        source_commit = dest_repo.get(source_ref.target)
        if not isinstance(source_commit, pygit2.Commit):
            source_commit = dest_repo.get(source_commit.target)
        dest_repo.checkout(source_ref)
        diff = branch_commit.tree.diff_to_workdir()
        assert not list(diff.deltas)


TEST_REPO_TREE = TreeNode(
    {"foo.txt": "Hello!"},
    "Root commit",
    [
        TreeNode({"foo.txt": "Hello World!"}, "some branch", branch_name="branch/one"),
        TreeNode(
            {"bar.txt": "Hey there", "nested/something.md": "# Something"},
            "intermediate",
            [
                TreeNode(
                    {"bar.txt": "Something Else"}, "version it!", tag_name="1.0.0"
                ),
                TreeNode(
                    {"bar.txt": "One More"},
                    "more features",
                    [
                        TreeNode(
                            {"bar.md": "# Mark it down"}, "next rev", tag_name="1.1.0"
                        ),
                    ],
                    branch_name="my-feature",
                ),
            ],
        ),
    ],
)


@pytest.fixture(scope="session")
def repo_path():
    with tempfile.TemporaryDirectory() as workdir:
        repo_path = temp_git_repo(workdir, TEST_REPO_TREE)
        yield repo_path


## Tests #######################################################################


def test_setup_vcs_with_defaults(workdir, repo_path):
    """Make sure setup_vcs works with defaults as expected"""
    setup_vcs(repo_path)
    dest_path = os.path.join(workdir, "oper8_vcs")
    assert os.path.isdir(dest_path)
    validate_vcs_dir(repo_path, dest_path, ["1.0.0", "1.1.0"])


def test_setup_vcs_branches(workdir, repo_path):
    """Make sure that setup_vcs includes branches correctly"""
    setup_vcs(repo_path, branch_expr=[".*"], tag_expr=None)
    dest_path = os.path.join(workdir, "oper8_vcs")
    assert os.path.isdir(dest_path)
    validate_vcs_dir(repo_path, dest_path, ["my-feature", "branch/one", "main"])


def test_setup_vcs_some_of_each(workdir, repo_path):
    """Make sure that setup_vcs includes some branches and tags selectively"""
    setup_vcs(repo_path, branch_expr=["my-feature"], tag_expr=["1.1.0"])
    dest_path = os.path.join(workdir, "oper8_vcs")
    assert os.path.isdir(dest_path)
    validate_vcs_dir(repo_path, dest_path, ["my-feature", "1.1.0"])


def test_setup_vcs_force(workdir, repo_path):
    """Make sure that the dest is only overwritten if "force" is true"""
    dest_path = os.path.join(workdir, "something_else")

    # Set up something that already exists
    os.makedirs(os.path.join(dest_path))
    with open(os.path.join(dest_path, "some_file.txt"), "w") as handle:
        handle.write("Not gonna last long!")
    nested_dir = os.path.join(dest_path, "nested_dir")
    os.makedirs(nested_dir)
    with open(os.path.join(nested_dir, "some_other_file.txt"), "w") as handle:
        handle.write("bye bye")

    # Without force, it's a ValueError
    with pytest.raises(ValueError):
        setup_vcs(repo_path, destination=dest_path)

    # With force, it's fine
    setup_vcs(repo_path, destination=dest_path, force=True)
    validate_vcs_dir(repo_path, dest_path, ["1.0.0", "1.1.0"])


def test_setup_vcs_not_a_repo(workdir):
    """Make sure an error is raised if the source is not a repo"""
    with pytest.raises(ValueError):
        setup_vcs(workdir)


def test_setup_vcs_dest_is_file(workdir, repo_path):
    """Make sure that an error is raised if the destination is a file"""
    dest_path = os.path.join(workdir, "something_else")
    with open(os.path.join(dest_path), "w") as handle:
        handle.write("Not gonna last long!")
    with pytest.raises(ValueError):
        setup_vcs(repo_path, destination=dest_path, force=True)
