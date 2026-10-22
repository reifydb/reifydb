# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 ReifyDB

PKG := reifydb-place-backend

.PHONY: help service test

help:
	@echo "make service   Start the place backend in memory on :8091"
	@echo "make test      Test the backend, build the bridge, run all web tests"

test:
	cargo test -p $(PKG)
	cargo build --release -p reifydb-place-bridge
	cd web && pnpm install --frozen-lockfile && pnpm test

service:
	cargo run -p $(PKG)
