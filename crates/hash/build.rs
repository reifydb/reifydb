// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

#[cfg(feature = "native")]
fn main() {
	let mut cc = cc::Build::new();
	cc.include("src/xxh/c/");
	cc.file("src/xxh/c/xxhash.c");
	cc.warnings(false);
	cc.compile("xxhash");

	let mut cc = cc::Build::new();
	cc.include("src/sha1/c/");
	cc.file("src/sha1/c/sha1.c");
	cc.warnings(false);
	cc.compile("sha1");
}

#[cfg(not(feature = "native"))]
fn main() {
	// No C compilation needed for WASM builds
}
