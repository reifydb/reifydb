#!/bin/bash
# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2025 ReifyDB
# Check for em-dashes (U+2014, "—") in Rust comments.
#
# Em-dashes look fine in prose but are typographic noise in source code.
# Use a plain hyphen "-" instead. Em-dashes inside string literals (error
# messages, format strings, CLI help text) are allowed.
#
# Detection heuristic: report any line containing "—" UNLESS every "—"
# on that line sits between a pair of double-quotes on the same line.
# This correctly skips single-line string literals; it does not handle
# em-dashes embedded inside multi-line raw strings (none exist today).
#
# Exit code: 0 if no violations, 1 if violations found

set -e

REPO_ROOT=$(git rev-parse --show-toplevel 2>/dev/null || pwd)

echo "Checking for em-dashes in Rust comments under /crates/, /pkg/, /bin/..."
echo ""

rs_files=$(find "$REPO_ROOT/crates" "$REPO_ROOT/pkg" "$REPO_ROOT/bin" -name "*.rs" \
    -not -path "*/vendor/*" \
    -not -path "*/generated/*" \
    -not -path "*/target/*" 2>/dev/null || true)

if [ -z "$rs_files" ]; then
    echo "No Rust files found"
    exit 0
fi

violations_found=false
violation_count=0

while IFS= read -r file; do
    # Lines containing an em-dash that is NOT between a pair of double-quotes
    # on the same line.
    result=$(grep -nE "—" "$file" 2>/dev/null | grep -vE '"[^"]*—[^"]*"' || true)

    if [ -n "$result" ]; then
        while IFS= read -r violation; do
            line_num=$(echo "$violation" | cut -d: -f1)
            content=$(echo "$violation" | cut -d: -f2-)

            if [ "$violations_found" = false ]; then
                echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
                echo "❌ Em-dash violations detected!"
                echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
                echo ""
                violations_found=true
            fi

            rel_path="${file#$REPO_ROOT/}"
            echo "  📄 $rel_path:$line_num"
            echo "    $content"
            echo ""
            violation_count=$((violation_count + 1))
        done <<< "$result"
    fi
done <<< "$rs_files"

if [ "$violations_found" = true ]; then
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "Found $violation_count violation(s)"
    echo ""
    echo "Em-dashes (—) are not allowed in comments. Use a hyphen (-) instead."
    echo "Em-dashes inside string literals are allowed."
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    exit 1
else
    echo "✅ No em-dash violations found!"
    exit 0
fi
