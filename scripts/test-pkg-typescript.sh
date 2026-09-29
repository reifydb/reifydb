#!/bin/bash
# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 ReifyDB

set -euo pipefail

cd "$(dirname "$0")/.."

log=target/testcontainer-ts.log

MAKEFLAGS= cargo build --offline -p testcontainer

./target/debug/testcontainer > "$log" 2>&1 &
server=$!
trap 'kill "$server" 2>/dev/null || true' EXIT

until (echo > /dev/tcp/127.0.0.1/18090) 2>/dev/null; do
	if ! kill -0 "$server" 2>/dev/null; then
		echo "testcontainer exited before it was ready:"
		cat "$log"
		exit 1
	fi
	sleep 1
done

cd pkg/typescript
pnpm install
pnpm build
pnpm test
pnpm test:integration
