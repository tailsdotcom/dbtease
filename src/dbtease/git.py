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


def get_git_state(repo_dir="."):
    repo = Repo(repo_dir)
    try:
        commit_hash = repo.commit("HEAD").hexsha
    except BadName:
        raise click.ClickException(
            "Git repository error. Check that there is at least one commit."
        )
    return {
        "dirty": repo.is_dirty(),
        "commit_hash": commit_hash,
    }
