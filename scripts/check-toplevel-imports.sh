#!/bin/bash
# Check that all `use` statements are at module level (not inside function bodies,
# match arms, closures, or other code blocks).
#
# This script checks ALL .rs files in /crates/ for inline imports.
# Useful for CI validation or manual checks.
#
# Exit code: 0 if no violations, 1 if violations found

set -e

REPO_ROOT=$(git rev-parse --show-toplevel 2>/dev/null || pwd)

echo "Checking for inline (non-top-level) use statements in /crates/..."
echo ""

# Find all .rs files in /crates/ (excluding specific paths)
crates_files=$(find "$REPO_ROOT/crates" -name "*.rs" \
    -not -path "*/tests/*" \
    -not -path "*/test_utils/*" \
    -not -path "*/vendor/*" 2>/dev/null || true)

if [ -z "$crates_files" ]; then
    echo "No Rust files found in /crates/"
    exit 0
fi

violations_found=false
violation_count=0

while IFS= read -r file; do
    # Use awk to track brace depth and context stack
    # Context: "mod" for module-level braces, "code" for fn/impl/match/etc.
    result=$(awk '
    BEGIN {
        depth = 0
        in_block_comment = 0
        # context_stack stores "mod" or "code" for each brace level
        # At depth 0, we are at file/module level (always OK)
    }

    {
        line = $0
        orig_line = $0
        lineno = NR

        # Handle block comments
        if (in_block_comment) {
            if (match(line, /\*\//)) {
                line = substr(line, RSTART + RLENGTH)
                in_block_comment = 0
            } else {
                next
            }
        }

        # Remove block comments that start and end on same line
        while (match(line, /\/\*[^*]*(\*[^/][^*]*)*\*\//)) {
            line = substr(line, 1, RSTART - 1) substr(line, RSTART + RLENGTH)
        }

        # Check if a block comment starts but does not end on this line
        if (match(line, /\/\*/)) {
            line = substr(line, 1, RSTART - 1)
            in_block_comment = 1
        }

        # Remove line comments
        if (match(line, /\/\//)) {
            line = substr(line, 1, RSTART - 1)
        }

        # Remove string literals (simple handling - does not handle escaped quotes perfectly
        # but sufficient for brace tracking since braces in strings are rare in use statements)
        gsub(/"[^"]*"/, "", line)

        # Check for `use` statement before processing braces on this line
        # A use statement starts with optional whitespace then "use "
        stripped = line
        gsub(/^[[:space:]]+/, "", stripped)
        if (match(stripped, /^use[[:space:]]+/)) {
            # A use statement is a violation if its innermost enclosing
            # brace context is "code" (fn, impl, match, closure, etc.)
            # It is OK if the innermost context is "mod" or depth is 0
            if (depth > 0 && context[depth] == "code") {
                # Trim trailing whitespace from orig_line for cleaner output
                gsub(/[[:space:]]+$/, "", orig_line)
                print lineno ":" orig_line
            }
        }

        # Now process braces character by character to update depth/context
        n = split(line, chars, "")
        for (ci = 1; ci <= n; ci++) {
            ch = chars[ci]
            if (ch == "{") {
                depth++
                # Determine context: check if line before this brace has `mod <ident>`
                prefix = substr(line, 1, ci - 1)
                if (match(prefix, /(^|[^a-zA-Z0-9_])mod[[:space:]]+/)) {
                    context[depth] = "mod"
                } else {
                    context[depth] = "code"
                }
            } else if (ch == "}") {
                if (depth > 0) {
                    delete context[depth]
                    depth--
                }
            }
        }
    }
    ' "$file")

    if [ -n "$result" ]; then
        while IFS= read -r violation; do
            line_num=$(echo "$violation" | cut -d: -f1)
            content=$(echo "$violation" | cut -d: -f2-)

            if [ "$violations_found" = false ]; then
                echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
                echo "❌ Inline import violations detected!"
                echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
                echo ""
                violations_found=true
            fi

            rel_path="${file#$REPO_ROOT/}"
            echo "  📄 $rel_path:$line_num"
            echo "     $content"
            echo ""
            violation_count=$((violation_count + 1))
        done <<< "$result"
    fi
done <<< "$crates_files"

if [ "$violations_found" = true ]; then
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "Found $violation_count violation(s)"
    echo ""
    echo "All 'use' statements must be at module level, not inside"
    echo "function bodies, match arms, closures, or other blocks."
    echo ""
    echo "Example:"
    echo "  ❌ fn foo() { use std::io::Write; ... }"
    echo "  ✅ use std::io::Write;"
    echo "     fn foo() { ... }"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    exit 1
else
    echo "✅ No inline import violations found!"
    echo ""
    echo "All use statements are at module level."
    exit 0
fi
