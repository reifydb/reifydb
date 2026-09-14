#!/bin/bash
# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 ReifyDB

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PROTO="$REPO_ROOT/crates/sub-server-grpc/proto/reifydb.proto"
SERVER_OUT="$REPO_ROOT/crates/sub-server-grpc/src/generated/reifydb.v1.rs"
CLIENT_OUT="$REPO_ROOT/pkg/rust/reifydb-client/src/grpc/generated/reifydb.v1.rs"

command -v protoc >/dev/null || { echo "protoc not found"; exit 1; }

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
mkdir "$TMP/out"

(cd "$TMP" && CARGO_TARGET_DIR="$REPO_ROOT/target/proto-gen" cargo run \
	--manifest-path "$REPO_ROOT/scripts/proto-gen/Cargo.toml" -- "$PROTO" "$TMP/out")

HEADER=$'// SPDX-License-Identifier: Apache-2.0\n// Copyright (c) 2026 ReifyDB\n\n'

{ printf '%s' "$HEADER"; cat "$TMP/out/reifydb.v1.rs"; } > "$TMP/reifydb.v1.rs"
"$REPO_ROOT/scripts/comments.sh" strip "$TMP/reifydb.v1.rs"

cp "$TMP/reifydb.v1.rs" "$SERVER_OUT"
cp "$TMP/reifydb.v1.rs" "$CLIENT_OUT"

echo "Regenerated:"
echo "  $SERVER_OUT"
echo "  $CLIENT_OUT"
