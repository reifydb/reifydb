#!/bin/bash
# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 ReifyDB

set -euo pipefail

SUITE=${1:?usage: mutants.sh <suite>}
MUTANT_TEST=${MUTANT_TEST:?MUTANT_TEST must name the make target that runs the tests}
MUTANT_BUILD=${MUTANT_BUILD:-}
REPO=$(cd "$(dirname "$0")/.." && pwd)
NAME=$(basename "$REPO")
REIFYDB=$(cd "$REPO/../reifydb" && pwd)
MANIFEST="$REIFYDB/crates/$SUITE/Cargo.toml"

if [ ! -f "$MANIFEST" ]; then
	echo "Error: suite '$SUITE' mirrors no reifydb crate ($MANIFEST not found)"
	exit 1
fi

if [ ! -f "$REPO/crates/$SUITE/Cargo.toml" ]; then
	echo "Error: $NAME has no suite '$SUITE' ($REPO/crates/$SUITE not found)"
	exit 1
fi

PACKAGE=$(sed -n 's/^name *= *"\(.*\)"/\1/p' "$MANIFEST" | head -1)
if [ -z "$PACKAGE" ]; then
	echo "Error: no package name in $MANIFEST"
	exit 1
fi

REAL_CARGO=$(command -v cargo)
WORK="$REIFYDB/target/mutants/$NAME/$SUITE"
mkdir -p "$WORK"

echo "Syncing reifydb and $NAME into $WORK..."
rsync -a --delete --exclude='/target/' "$REIFYDB/" "$WORK/reifydb/"
rsync -a --delete --exclude='/target/' "$REPO/" "$WORK/$NAME/"

cat > "$WORK/cargo-wrap.sh" <<EOF
#!/bin/bash
set -u
echo "\$(date -Is) \$* RUSTFLAGS=\${RUSTFLAGS:-} CARGO_ENCODED_RUSTFLAGS=\${CARGO_ENCODED_RUSTFLAGS:-}" >> "$WORK/wrap-calls.log"
unset CARGO
case "\${1:-}" in
	build)
		[ -n "$MUTANT_BUILD" ] || exit 0
		cd "$WORK/$NAME" && exec make $MUTANT_BUILD
		;;
	test)
		cd "$WORK/$NAME" || exit 101
		if printf '%s\n' "\$@" | grep -qx -- --no-run; then
			[ -n "$MUTANT_BUILD" ] || exit 0
			exec make $MUTANT_BUILD
		fi
		exec make $MUTANT_TEST SUITE=$SUITE
		;;
	*)
		exec "$REAL_CARGO" "\$@"
		;;
esac
EOF
chmod +x "$WORK/cargo-wrap.sh"

echo "Mutating $PACKAGE against $NAME SUITE=$SUITE..."
rc=0
(cd "$WORK/reifydb" && CARGO="$WORK/cargo-wrap.sh" cargo-mutants mutants --in-place -p "$PACKAGE" -o "$WORK") || rc=$?

echo ""
echo "Missed mutants ($WORK/mutants.out/missed.txt):"
cat "$WORK/mutants.out/missed.txt" 2>/dev/null || echo "  (none written)"
exit $rc
