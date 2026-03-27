#!/bin/bash
# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2025 ReifyDB
# Check for section-divider comments (e.g. "// ---- Section name ----" or
# "// -------...").
#
# These add visual noise without value — use blank lines or doc-comments
# to separate logical sections instead.
#
# Exit code: 0 if no violations, 1 if violations found

set -e

REPO_ROOT=$(git rev-parse --show-toplevel 2>/dev/null || pwd)

echo "Checking for section-divider comments in /crates/..."
echo ""

# Find all .rs files in /crates/ (excluding specific paths)
crates_files=$(find "$REPO_ROOT/crates" -name "*.rs" \
    -not -path "*/tests/*" \
    -not -path "*/test_utils/*" \
    -not -path "*/vendor/*" \
    -not -path "*/generated/*" 2>/dev/null || true)

if [ -z "$crates_files" ]; then
    echo "No Rust files found in /crates/"
    exit 0
fi

violations_found=false
violation_count=0

while IFS= read -r file; do
    # Match lines that are a comment starting with 4+ dashes: // ----
    result=$(grep -nE '^\s*//\s*-{4,}' "$file" || true)

    if [ -n "$result" ]; then
        while IFS= read -r violation; do
            line_num=$(echo "$violation" | cut -d: -f1)
            content=$(echo "$violation" | cut -d: -f2-)

            if [ "$violations_found" = false ]; then
                echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
                echo "❌ Section-divider comment violations detected!"
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
done <<< "$crates_files"

if [ "$violations_found" = true ]; then
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "Found $violation_count violation(s)"
    echo ""
    echo "Section-divider comments like these are not allowed:"
    echo "  ❌ // ---- Section name ----"
    echo "  ❌ // ---------------------------------------------------------------------------"
    echo ""
    echo "Use blank lines or doc-comments to separate sections instead."
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    exit 1
else
    echo "✅ No section-divider comment violations found!"
    echo ""
    echo "No comments starting with // ---- detected."
    exit 0
fi
