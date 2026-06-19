#!/bin/bash
# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 ReifyDB
#
# Check that every tracked source file carries the SPDX license header.
#
# Policy: every source file is Apache-2.0.
#
# The header is expected on the first line, or the second line for files that
# start with a shebang.
#
# Exit code: 0 if no violations, 1 if violations found

set -e

LICENSE="Apache-2.0"

echo "Checking SPDX license headers..."
echo ""

violations=$(git ls-files -- '*.rs' '*.ts' '*.tsx' '*.js' '*.css' '*.sh' '*.mk' '*.py' 'Makefile' '*/Makefile' \
  | grep -v '^vendor/' \
  | while read f; do
      spdx=$(head -2 "$f" | grep -oE 'SPDX-License-Identifier: [A-Za-z0-9.+-]+' | head -1 | sed 's/SPDX-License-Identifier: //')
      if [ -z "$spdx" ]; then
          echo "MISSING|$f|$LICENSE"
      elif [ "$spdx" != "$LICENSE" ]; then
          echo "WRONG|$f|$spdx|$LICENSE"
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
    echo "Every source file is Apache-2.0 and must start with:"
    echo "  // SPDX-License-Identifier: Apache-2.0"
    echo "  // Copyright (c) 2026 ReifyDB"
    echo ""
    echo "For CSS/Makefile/shell files use # or /* */ comments."
    echo "Shell scripts with a shebang: put the header on line 2."
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    exit 1
else
    echo "✅ All source files carry the expected SPDX license header!"
    exit 0
fi
