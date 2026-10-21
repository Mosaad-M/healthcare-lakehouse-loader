"""Refuse stack commands from a folder that does not own the local stack.

The compose project name is fixed (`lakehouse`), so compose from a second
checkout or a git worktree takes over the running stack with a different
.env, whose secrets do not match the volumes. Run by the Makefile before any
target that starts, stops or uses the stack.
"""

import os
import subprocess
import sys

STACK = os.path.realpath(os.path.join(os.path.dirname(__file__), "..", "stack"))


def git(*args):
    out = subprocess.run(["git", *args], capture_output=True, text=True)
    return os.path.realpath(out.stdout.strip()) if out.returncode == 0 else None


def running_from():
    """Folders the running `lakehouse` containers were started from (empty if Docker is down)."""
    try:
        out = subprocess.run(
            ["docker", "ps", "-a", "--filter", "label=com.docker.compose.project=lakehouse",
             "--format", '{{.Label "com.docker.compose.project.working_dir"}}'],
            capture_output=True, text=True,
        )
    except FileNotFoundError:
        return set()
    return {os.path.realpath(d) for d in out.stdout.split() if d}


def main():
    git_dir, common = git("rev-parse", "--git-dir"), git("rev-parse", "--git-common-dir")
    if git_dir and common and git_dir != common:
        sys.exit("refusing: this is a git worktree. Run stack commands from the main "
                 f"checkout ({os.path.dirname(common)}); test a branch there with a detached checkout.")
    others = running_from() - {STACK}
    if others:
        sys.exit(f"refusing: the lakehouse stack belongs to {', '.join(sorted(others))}. "
                 "Run `make down` there first (or `docker compose -p lakehouse down` if that folder is gone).")


if __name__ == "__main__":
    main()
