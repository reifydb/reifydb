# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 ReifyDB

PKG := reifydb-place-backend

.PHONY: help backend backend-memory web test

help:
	@echo "make backend          Start the place backend in memory on :8091"
	@echo "make backend-memory   Start the place backend in memory on :8091"
	@echo "make web              Start the vite dev server"
	@echo "make test             Test the backend, build the bridge, run all web tests"

test:
	cargo test -p $(PKG)
	cargo build --release -p reifydb-place-bridge
	cd web && pnpm install --frozen-lockfile && pnpm test

backend:
	cargo run --release -p $(PKG)

backend-memory:
	cargo run --release -p $(PKG)

web:
	cd web && pnpm install && pnpm dev
