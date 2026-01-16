#!/bin/bash
# Check for internal pub use re-exports in the codebase
#
# This script checks ALL files (not just staged) for internal re-exports.
# Useful for CI validation or manual checks.
#
# Exit code: 0 if no violations, 1 if violations found

set -e

REPO_ROOT=$(git rev-parse --show-toplevel 2>/dev/null || pwd)

# Third-party crates that are OK to re-export (whitelist)
ALLOWED_EXTERNAL=(
    "std::"
    "core::"
    "alloc::"
    "tokio::"
    "tracing::"
    "serde::"
    "serde_json::"
    "anyhow::"
    "thiserror::"
    "axum::"
    "futures::"
    "async_trait::"
)

echo "Checking for internal pub use re-exports in /crates/..."
echo ""

# Find all .rs files in /crates/ (excluding specific paths)
crates_files=$(find "$REPO_ROOT/crates" -name "*.rs" \
    -not -path "*/tests/*" \
    -not -path "*/test_utils/*" \
    -not -path "*/vendor/*" \
    -not -name "prelude.rs" 2>/dev/null || true)

if [ -z "$crates_files" ]; then
    echo "No Rust files found in /crates/"
    exit 0
fi

# Check each file for pub use violations
violations_found=false
violation_count=0

for file in $crates_files; do
    # Find all pub use statements
    while IFS= read -r line; do
        line_num=$(echo "$line" | cut -d: -f1)
        content=$(echo "$line" | cut -d: -f2-)

        # Check if it's an allowed external crate
        is_allowed=false
        for allowed in "${ALLOWED_EXTERNAL[@]}"; do
            if echo "$content" | grep -q "$allowed"; then
                is_allowed=true
                break
            fi
        done

        if [ "$is_allowed" = false ]; then
            if [ "$violations_found" = false ]; then
                echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
                echo "❌ Internal re-export violations detected!"
                echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
                echo ""
                violations_found=true
            fi

            # Make path relative to repo root for cleaner output
            rel_path="${file#$REPO_ROOT/}"
            echo "  📄 $rel_path:$line_num"
            echo "     $content"
            echo ""
            ((violation_count++))
        fi
    done < <(grep -n "^[[:space:]]*pub use " "$file" 2>/dev/null || true)
done

if [ "$violations_found" = true ]; then
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "Found $violation_count violation(s)"
    echo ""
    echo "Internal crates (/crates/) should not use 'pub use' for"
    echo "re-exporting internal types. Use full paths instead."
    echo ""
    echo "Example:"
    echo "  ❌ pub use reifydb_core::Row;"
    echo "  ✅ pub mod row;"
    echo ""
    echo "External crate re-exports are allowed:"
    echo "  ✅ pub use std::collections::HashMap;"
    echo "  ✅ pub use tokio::sync::Mutex;"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    exit 1
else
    echo "✅ No internal re-export violations found!"
    echo ""
    echo "All internal crates follow the no-reexport policy."
    exit 0
fi
