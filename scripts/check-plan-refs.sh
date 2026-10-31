#!/bin/bash
# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 ReifyDB

set -e

REPO_ROOT=$(git rev-parse --show-toplevel 2>/dev/null || pwd)
PATTERN='(//|/\*|^\s*\*).*\b([A-Z][0-9]+(-[a-z])?|B-[0-9]+)\s*:'

echo "Checking for plan or decision ids in comments..."
echo ""

files=$(find "$REPO_ROOT/crates" "$REPO_ROOT/pkg" "$REPO_ROOT/bin" \( -name "*.rs" -o -name "*.ts" -o -name "*.tsx" \) \
    -not -path "*/vendor/*" \
    -not -path "*/generated/*" \
    -not -path "*/target/*" \
    -not -path "*/node_modules/*" \
    -not -path "*/dist/*" 2>/dev/null || true)

violation_count=0
while IFS= read -r file; do
    [ -z "$file" ] && continue
    result=$(grep -nE "$PATTERN" "$file" 2>/dev/null || true)
    [ -z "$result" ] && continue
    while IFS= read -r violation; do
        if [ "$violation_count" -eq 0 ]; then
            echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
            echo "❌ Plan or decision ids in comments!"
            echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
            echo ""
        fi
        echo "  📄 ${file#$REPO_ROOT/}:$(echo "$violation" | cut -d: -f1)"
        echo "    $(echo "$violation" | cut -d: -f2-)"
        echo ""
        violation_count=$((violation_count + 1))
    done <<< "$result"
done <<< "$files"

if [ "$violation_count" -gt 0 ]; then
    echo "Found $violation_count violation(s)"
    echo "Comments state the reason itself; ids like D26: or C8-b: belong in plan files only."
    exit 1
fi
echo "✅ No plan or decision ids in comments!"
