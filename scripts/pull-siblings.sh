#!/bin/bash
# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 ReifyDB
#
# Rebase reifydb and every sibling test repository onto its upstream.
#
# Each directory given on the command line is cloned from the same base url as
# reifydb when missing, otherwise pulled with --rebase. A repository
# holding any local change, staged, unstaged or untracked, is refused before the
# pull starts. A pull that hits a conflict is aborted, so no repository is left
# half-rebased, and the conflicted paths are reported.
#
# Every directory is visited even after a failure, so one run reports every
# problem rather than only the first.
#
# Exit code: 0 if every repository is up to date, 1 if any failed

set -e

{
    status=0
    listed=10
    origin=$(git remote get-url origin)
    base=${origin%/*}

    echo "Rebasing repositories..."
    echo ""

    for dir in "$@"; do
        name=$(basename "$(realpath -m "$dir")")

        if [ ! -e "$dir" ]; then
            if git clone --quiet "$base/$name.git" "$dir"; then
                echo "  $name was cloned from $base/$name.git"
            else
                echo "Error: $name could not be cloned from $base/$name.git"
                status=1
            fi
            continue
        fi

        if [ ! -d "$dir/.git" ]; then
            echo "Error: $name is not a git repository ($dir)"
            status=1
            continue
        fi

        changes=$(git -C "$dir" status --porcelain)
        if [ -n "$changes" ]; then
            total=$(echo "$changes" | wc -l)
            echo "Error: $name has $total local changes, commit or stash them first"
            echo "$changes" | head -n "$listed" | sed 's/^/    /'
            if [ "$total" -gt "$listed" ]; then
                echo "    ... and $((total - listed)) more"
            fi
            status=1
            continue
        fi

        if ! git -C "$dir" fetch --quiet; then
            echo "Error: $name could not be fetched from its upstream"
            status=1
            continue
        fi

        if git -C "$dir" merge-base --is-ancestor '@{u}' HEAD; then
            echo "  $name is up to date"
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
        echo "All repositories are up to date."
    else
        echo "Repositories are not ready, see the errors above."
    fi

    exit $status
}
