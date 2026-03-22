#!/bin/bash
# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2025 ReifyDB
#
# Check that every tracked source file has an SPDX license header
#
# This script checks ALL tracked files (not just staged) for the
# SPDX-License-Identifier header on the first line (or second line
# for files that start with a shebang).
#
# Exit code: 0 if no violations, 1 if violations found

set -e

echo "Checking for SPDX license headers..."
echo ""

missing=$(git ls-files -- '*.rs' '*.ts' '*.tsx' '*.js' '*.css' '*.sh' '*.mk' 'Makefile' '*/Makefile' \
  | grep -v '^vendor/' \
  | while read f; do
      head -2 "$f" | grep -q 'SPDX-License-Identifier' || echo "$f"
    done)

if [ -n "$missing" ]; then
    count=$(echo "$missing" | wc -l)
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "❌ Missing SPDX license headers detected!"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo ""
    echo "$missing" | while read f; do
        echo "  📄 $f"
    done
    echo ""
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "Found $count file(s) missing SPDX-License-Identifier header."
    echo ""
    echo "Every source file must start with:"
    echo "  // SPDX-License-Identifier: Apache-2.0"
    echo "  // Copyright (c) 2025 ReifyDB"
    echo ""
    echo "For CSS/Makefile/shell files use # or /* */ comments."
    echo "Shell scripts with a shebang: put the header on line 2."
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    exit 1
else
    echo "✅ All source files have SPDX license headers!"
    exit 0
fi
