#!/bin/bash
# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 ReifyDB

set -e

REPO_ROOT=$(git rev-parse --show-toplevel 2>/dev/null || pwd)

echo "Checking for Result type aliases..."

PATTERN='^\s*(pub(\([^)]*\))?\s+)?type\s+\w*Result\s*<|^\s*(pub(\([^)]*\))?\s+)?type\s+\w+\s*<[^=]*>\s*=\s*(::)?((std|core)::result::)?Result\s*<'

violations=$(find "$REPO_ROOT" -name "*.rs" \
    -not -path "*/target/*" \
    -not -path "*/node_modules/*" \
    -not -path "*/generated/*" \
    -not -path "$REPO_ROOT/vendor/*" \
    -not -path "$REPO_ROOT/patch/*" \
    -print0 2>/dev/null | xargs -0 grep -nHE "$PATTERN" 2>/dev/null || true)

if [ -n "$violations" ]; then
    echo "❌ Result type aliases found; write Result<T, E> with the concrete error type instead:"
    echo "$violations" | sed "s|^$REPO_ROOT/|  |"
    exit 1
fi

echo "✅ No Result type aliases found"
