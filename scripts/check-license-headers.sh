#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-or-later
# Copyright (c) 2026 ReifyDB
#
# Check that every tracked source file carries the SPDX license header that
# matches its package's licensing policy.
#
# Policy: every source file is AGPL-3.0-or-later by default. Packages whose
# distributable is permissively licensed (the MIT client-SDK surface) are listed
# in MIT_PATHS below; files under those paths must be MIT instead. A new crate is
# therefore AGPL unless its path prefix is explicitly added to MIT_PATHS.
#
# The header is expected on the first line, or the second line for files that
# start with a shebang.
#
# Exit code: 0 if no violations, 1 if violations found

set -e

# Path prefixes (relative to repo root, trailing slash) whose files must be MIT.
# Everything not listed here defaults to AGPL-3.0-or-later.
MIT_PATHS=(
    "crates/type/"
    "crates/runtime/"
    "crates/wire-format/"
    "crates/macro-impl/"
    "crates/build/"
    "pkg/rust/reifydb-client/"
    "pkg/rust/reifydb-client-derive/"
    "pkg/typescript/auth/"
    "pkg/typescript/auth-solana/"
    "pkg/typescript/client/"
    "pkg/typescript/core/"
    "pkg/typescript/react/"
)

DEFAULT_LICENSE="AGPL-3.0-or-later"

expected_license() {
    local f="$1" p
    for p in "${MIT_PATHS[@]}"; do
        case "$f" in
            "$p"*) echo "MIT"; return;;
        esac
    done
    echo "$DEFAULT_LICENSE"
}

echo "Checking SPDX license headers..."
echo ""

violations=$(git ls-files -- '*.rs' '*.ts' '*.tsx' '*.js' '*.css' '*.sh' '*.mk' 'Makefile' '*/Makefile' \
  | grep -v '^vendor/' \
  | while read f; do
      exp=$(expected_license "$f")
      spdx=$(head -2 "$f" | grep -oE 'SPDX-License-Identifier: [A-Za-z0-9.+-]+' | head -1 | sed 's/SPDX-License-Identifier: //')
      if [ -z "$spdx" ]; then
          echo "MISSING|$f|$exp"
      elif [ "$spdx" != "$exp" ]; then
          echo "WRONG|$f|$spdx|$exp"
      fi
    done)

if [ -n "$violations" ]; then
    count=$(echo "$violations" | wc -l)
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "❌ Incorrect SPDX license headers detected!"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo ""
    echo "$violations" | while IFS='|' read -r kind f a b; do
        if [ "$kind" = "MISSING" ]; then
            echo "  📄 $f"
            echo "       missing header, expected: $a"
        else
            echo "  📄 $f"
            echo "       has: $a, expected: $b"
        fi
    done
    echo ""
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "Found $count file(s) with a missing or incorrect SPDX-License-Identifier."
    echo ""
    echo "Every source file is AGPL-3.0-or-later by default and must start with:"
    echo "  // SPDX-License-Identifier: AGPL-3.0-or-later"
    echo "  // Copyright (c) 2026 ReifyDB"
    echo ""
    echo "Files in the permissive MIT client-SDK surface use:"
    echo "  // SPDX-License-Identifier: MIT"
    echo "  // Copyright (c) 2026 ReifyDB"
    echo ""
    echo "For CSS/Makefile/shell files use # or /* */ comments."
    echo "Shell scripts with a shebang: put the header on line 2."
    echo ""
    echo "To make a new package MIT, add its path prefix to MIT_PATHS in this script."
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    exit 1
else
    echo "✅ All source files carry the expected SPDX license header!"
    exit 0
fi
