#!/bin/bash
# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 ReifyDB
#
# Rebase every sibling test repository onto its upstream before the build runs.
#
# Each directory given on the command line is pulled with --rebase. A repository
# holding any local change, staged, unstaged or untracked, is refused before the
# pull starts. A pull that hits a conflict is aborted, so no repository is left
# half-rebased, and the conflicted paths are reported.
#
# Every directory is visited even after a failure, so one run reports every
# problem rather than only the first.
#
# Exit code: 0 if every repository is up to date, 1 if any failed

set -e

status=0
listed=10

echo "Rebasing sibling test repositories..."
echo ""

for dir in "$@"; do
    name=$(basename "$dir")

    if [ ! -d "$dir/.git" ]; then
        echo "Error: $name is not a git repository ($dir)"
        status=1
        continue
    fi

    changes=$(git -C "$dir" status --porcelain)
    if [ -n "$changes" ]; then
        total=$(echo "$changes" | wc -l)
        echo "Error: $name has $total local changes, commit or stash them before running make all"
        echo "$changes" | head -n "$listed" | sed 's/^/    /'
        if [ "$total" -gt "$listed" ]; then
            echo "    ... and $((total - listed)) more"
        fi
        status=1
        continue
    fi

    if git -C "$dir" pull --rebase --quiet; then
        echo "  $name is up to date"
    else
        conflicts=$(git -C "$dir" diff --name-only --diff-filter=U || true)
        git -C "$dir" rebase --abort >/dev/null 2>&1 || true
        echo "Error: $name could not be rebased onto its upstream, the rebase was aborted"
        if [ -n "$conflicts" ]; then
            echo "  conflicted paths:"
            echo "$conflicts" | sed 's/^/    /'
        fi
        status=1
    fi
done

echo ""
if [ $status -eq 0 ]; then
    echo "All sibling test repositories are up to date."
else
    echo "Sibling test repositories are not ready, see the errors above."
fi

exit $status
