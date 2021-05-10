"""Git routines to introspect current state."""

from git import Repo
from gitdb.exc import BadName

import click


def _iter_diff_paths(diffs):
    for diff in diffs:
        if diff.a_path:
            yield diff.a_path
        if diff.b_path and diff.b_path != diff.a_path:
            yield diff.b_path


def get_git_state(repo_dir=".", deployed_hash=None):
    repo = Repo(repo_dir)
    diff_paths = set()
    if deployed_hash:
        deployed_commit = repo.commit(deployed_hash)
        diff_paths = set(_iter_diff_paths(deployed_commit.diff(None)))
    try:
        commit_hash = repo.commit("HEAD").hexsha
    except BadName:
        raise click.ClickException(
            "Git repository error. Check that there is at least one commit."
        )
    return {
        "dirty": repo.is_dirty(),
        "untracked": set(repo.untracked_files),
        "commit_hash": commit_hash,
        "diff": diff_paths,
    }
