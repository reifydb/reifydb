# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 ReifyDB

PKG := reifydb-uptime-backend

REIFYDB_WS := ws://127.0.0.1:8091

PROBE_A_TOKEN := probe-a-dev-token
PROBE_B_TOKEN := probe-b-dev-token

.PHONY: help service probe-a probe-b test

help:
	@echo "Targets:"
	@echo "  make service   Start the uptime backend WITHOUT embedded probes"
	@echo "                 (still provisions the probe-a/probe-b service identities"
	@echo "                 so standalone probes can authenticate)."
	@echo "  make probe-a   Start standalone probe 'probe-a' against a running service."
	@echo "  make probe-b   Start standalone probe 'probe-b' against a running service."
	@echo "  make test      Test the backend, build the bridge, run all web tests."
	@echo ""
	@echo "Run 'make service' first, then 'make probe-a' and 'make probe-b' in"
	@echo "separate terminals. Tokens are hardcoded to match PROBES in backend/src/main.rs."

test:
	cargo test -p $(PKG)
	REIFYDB_DST=1 cargo build --release -p reifydb-uptime-bridge
	cd web && pnpm test

service:
	cargo run -p $(PKG) -- --no-embedded-probes

probe-a:
	cargo run -p $(PKG) -- --probe-token $(PROBE_A_TOKEN) --probe-reifydb-ws $(REIFYDB_WS)

probe-b:
	cargo run -p $(PKG) -- --probe-token $(PROBE_B_TOKEN) --probe-reifydb-ws $(REIFYDB_WS)
