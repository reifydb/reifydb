# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 ReifyDB

PKG := reifydb-canvas-backend

.PHONY: help backend backend-memory web test

help:
	@echo "make backend          Start the canvas backend in memory on :8091"
	@echo "make backend-memory   Start the canvas backend in memory on :8091"
	@echo "make web              Start the vite dev server"
	@echo "make test             Test the backend, build the bridge, run all web tests"

test:
	cargo test --profile gate -p $(PKG)
	cargo build --profile gate -p reifydb-canvas-bridge
	cd web && pnpm install --frozen-lockfile && pnpm test

backend:
	cargo run --release -p $(PKG)

backend-memory:
	cargo run --release -p $(PKG)

web:
	cd web && pnpm install && pnpm dev
