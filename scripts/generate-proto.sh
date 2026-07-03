#!/bin/bash
# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 ReifyDB
#
# Regenerates the two checked-in prost/tonic outputs from
# crates/sub-server-grpc/proto/reifydb.proto:
#   crates/sub-server-grpc/src/generated/reifydb.v1.rs        (messages + tonic server)
#   pkg/rust/reifydb-client/src/grpc/generated/reifydb.v1.rs  (messages + tonic client)
#
# Requires network access: protoc on PATH plus `cargo install protoc-gen-prost
# protoc-gen-tonic`. Both outputs MUST be regenerated together in one commit;
# their message definitions must stay identical.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PROTO="$REPO_ROOT/crates/sub-server-grpc/proto/reifydb.proto"
SERVER_OUT="$REPO_ROOT/crates/sub-server-grpc/src/generated"
CLIENT_OUT="$REPO_ROOT/pkg/rust/reifydb-client/src/grpc/generated"

command -v protoc >/dev/null || { echo "protoc not found"; exit 1; }
command -v protoc-gen-prost >/dev/null || { echo "protoc-gen-prost not found (cargo install protoc-gen-prost)"; exit 1; }
command -v protoc-gen-tonic >/dev/null || { echo "protoc-gen-tonic not found (cargo install protoc-gen-tonic)"; exit 1; }

TMP_SERVER="$(mktemp -d)"
TMP_CLIENT="$(mktemp -d)"
trap 'rm -rf "$TMP_SERVER" "$TMP_CLIENT"' EXIT

protoc --proto_path="$(dirname "$PROTO")" \
	--prost_out="$TMP_SERVER" \
	--tonic_out="$TMP_SERVER" \
	--tonic_opt=no_client \
	"$PROTO"

protoc --proto_path="$(dirname "$PROTO")" \
	--prost_out="$TMP_CLIENT" \
	--tonic_out="$TMP_CLIENT" \
	--tonic_opt=no_server \
	"$PROTO"

HEADER=$'// SPDX-License-Identifier: Apache-2.0\n// Copyright (c) 2026 ReifyDB\n\n'

{ printf '%s' "$HEADER"; cat "$TMP_SERVER"/reifydb.v1.rs "$TMP_SERVER"/reifydb.v1.tonic.rs 2>/dev/null || cat "$TMP_SERVER"/reifydb.v1.rs; } > "$SERVER_OUT/reifydb.v1.rs"
{ printf '%s' "$HEADER"; cat "$TMP_CLIENT"/reifydb.v1.rs "$TMP_CLIENT"/reifydb.v1.tonic.rs 2>/dev/null || cat "$TMP_CLIENT"/reifydb.v1.rs; } > "$CLIENT_OUT/reifydb.v1.rs"

echo "Regenerated:"
echo "  $SERVER_OUT/reifydb.v1.rs"
echo "  $CLIENT_OUT/reifydb.v1.rs"
echo "Verify message sections match: diff the two files' message definitions."
