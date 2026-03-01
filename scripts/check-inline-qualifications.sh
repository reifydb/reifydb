#!/bin/bash
# Check for inline qualified paths (crate::, super::, reifydb_*::, std::, or
# vendored crate paths) that should be top-level `use` imports instead.
#
# This script checks ALL .rs files in /crates/ for inline qualifications.
# Useful for CI validation or manual checks.
#
# Exit code: 0 if no violations, 1 if violations found

set -e

REPO_ROOT=$(git rev-parse --show-toplevel 2>/dev/null || pwd)

echo "Checking for inline qualified paths in /crates/..."
echo ""

# Build pattern from vendor/ directory crate names (strip version suffixes, normalize hyphens)
vendor_pattern=""
if [ -d "$REPO_ROOT/vendor" ]; then
    vendor_pattern=$(ls "$REPO_ROOT/vendor" \
        | sed 's/-[0-9][0-9]*\.[0-9].*//' \
        | sort -u \
        | tr '-' '_' \
        | tr '\n' '|' \
        | sed 's/|$//')
fi

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
    result=$(awk -v vendor_pat="$vendor_pattern" '
    BEGIN {
        in_block_comment = 0
        in_use_block = 0
        use_brace_depth = 0
        in_macro_rules = 0
        macro_brace_depth = 0
        if (vendor_pat != "") {
            external_regex = "(^|[^a-zA-Z0-9_$:])(std|" vendor_pat ")::[^<]"
        } else {
            external_regex = "(^|[^a-zA-Z0-9_$:])std::[^<]"
        }
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

        # Remove string literals
        gsub(/"[^"]*"/, "", line)

        # Skip lines inside macro_rules! blocks (body lines need qualified paths for hygiene)
        if (in_macro_rules) {
            tmp = line
            while (match(tmp, /[{}]/)) {
                ch = substr(tmp, RSTART, 1)
                if (ch == "{") macro_brace_depth++
                else macro_brace_depth--
                tmp = substr(tmp, RSTART + 1)
            }
            if (macro_brace_depth <= 0) { in_macro_rules = 0; macro_brace_depth = 0 }
            next
        }

        # Skip continuation lines of a multi-line use block
        if (in_use_block) {
            tmp = line
            while (match(tmp, /[{}]/)) {
                ch = substr(tmp, RSTART, 1)
                if (ch == "{") use_brace_depth++
                else use_brace_depth--
                tmp = substr(tmp, RSTART + 1)
            }
            if (use_brace_depth <= 0) { in_use_block = 0; use_brace_depth = 0 }
            next
        }

        # Skip lines that are `use` statements (and track multi-line blocks)
        stripped = line
        gsub(/^[[:space:]]+/, "", stripped)
        if (match(stripped, /^(pub([[:space:]]+|\([^)]*\)[[:space:]]*)?)?use[[:space:]]+/)) {
            tmp = line
            while (match(tmp, /[{}]/)) {
                ch = substr(tmp, RSTART, 1)
                if (ch == "{") use_brace_depth++
                else use_brace_depth--
                tmp = substr(tmp, RSTART + 1)
            }
            if (use_brace_depth > 0) in_use_block = 1
            next
        }

        # Skip attribute lines
        if (match(stripped, /^#\[/)) {
            next
        }

        # Detect start of macro_rules! blocks and skip their bodies
        if (match(stripped, /^macro_rules![[:space:]]*[a-zA-Z_][a-zA-Z0-9_]*/)) {
            in_macro_rules = 1
            macro_brace_depth = 0
            tmp = line
            while (match(tmp, /[{}]/)) {
                ch = substr(tmp, RSTART, 1)
                if (ch == "{") macro_brace_depth++
                else macro_brace_depth--
                tmp = substr(tmp, RSTART + 1)
            }
            if (macro_brace_depth <= 0) { in_macro_rules = 0; macro_brace_depth = 0 }
            next
        }

        # Skip type alias definitions (pub type X<T> = qualified::path<T>)
        if (match(stripped, /^(pub[[:space:]]+)?type[[:space:]]+/)) {
            next
        }

        # Remove $crate:: (macro hygiene) before checking
        gsub(/\$crate::/, "", line)

        # Remove pub(crate) before checking
        gsub(/pub\(crate\)/, "", line)

        # Remove pub(in ...) visibility annotations (cannot be replaced with use imports)
        gsub(/pub\(in [^)]*\)/, "", line)

        # Skip lines inside macro_rules! bodies (contain $ pattern variables)
        if (match(line, /\$[a-zA-Z_]/)) {
            next
        }

        # Check for remaining crate::, super::, reifydb_*::, std::, or vendored crate:: occurrences
        # Note: [^a-zA-Z0-9_$:] excludes ':' to avoid flagging nested path components (e.g. ::log:: in local paths)
        if (match(line, /(^|[^a-zA-Z0-9_$:])crate::/) ||
            match(line, /(^|[^a-zA-Z0-9_$:])super::/) ||
            match(line, /(^|[^a-zA-Z0-9_$:])reifydb[a-zA-Z0-9_]*::/) ||
            match(line, external_regex)) {
            gsub(/[[:space:]]+$/, "", orig_line)
            print lineno ":" orig_line
        }
    }
    ' "$file")

    if [ -n "$result" ]; then
        while IFS= read -r violation; do
            line_num=$(echo "$violation" | cut -d: -f1)
            content=$(echo "$violation" | cut -d: -f2-)

            if [ "$violations_found" = false ]; then
                echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
                echo "❌ Inline qualification violations detected!"
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
    echo "Inline qualified paths (crate::, super::, reifydb_*::, std::, vendored crates) should be"
    echo "replaced with top-level 'use' imports."
    echo ""
    echo "Example:"
    echo "  ❌ let x = crate::ast::Foo::Bar;"
    echo "  ❌ let y = reifydb_core::Value::Int(1);"
    echo "  ❌ let m = std::collections::HashMap::new();"
    echo "  ✅ use crate::ast::Foo;"
    echo "  ✅ use reifydb_core::Value;"
    echo "  ✅ use std::collections::HashMap;"
    echo "     let x = Foo::Bar;"
    echo "     let y = Value::Int(1);"
    echo "     let m = HashMap::new();"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    exit 1
else
    echo "✅ No inline qualification violations found!"
    echo ""
    echo "All qualified paths use top-level imports."
    exit 0
fi
