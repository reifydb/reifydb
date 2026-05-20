# SPDX-License-Identifier: AGPL-3.0-or-later
# Copyright (c) 2026 ReifyDB

.PHONY: all test clean install build

all: install build test

install:
	pnpm install

build: install
	pnpm build

test: build
	pnpm test

clean:
	rm -rf node_modules
	rm -rf dist
	rm -rf coverage