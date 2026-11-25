// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use super::changed_is_source;

#[test]
fn a_changed_file_is_checked_only_inside_the_three_roots() {
	// Vendored and patched code is not ours to restyle, so it must never be read in either mode.
	assert!(changed_is_source("crates/store/src/tier/range/scan.rs"));
	assert!(changed_is_source("bin/tidy/src/main.rs"));
	assert!(changed_is_source("pkg/rust/reifydb/src/lib.rs"));
	assert!(!changed_is_source("vendor/vortex-array/src/lib.rs"));
	assert!(!changed_is_source("patch/datafusion-common/src/lib.rs"));
	assert!(!changed_is_source("build.rs"));
}

#[test]
fn a_changed_file_in_a_skipped_folder_is_not_checked() {
	// The incremental mode must skip exactly what the full walk skips, otherwise the two modes disagree.
	assert!(!changed_is_source("crates/store/target/debug/build/out.rs"));
	assert!(!changed_is_source("pkg/typescript/node_modules/pkg/src/lib.rs"));
	assert!(!changed_is_source("crates/store/.cache/lib.rs"));
}

#[test]
fn a_changed_file_that_is_not_rust_is_not_checked() {
	// A manifest or a readme handed to the parser would stop the run as a parse error.
	assert!(!changed_is_source("crates/store/Cargo.toml"));
	assert!(!changed_is_source("crates/store/readme.md"));
	assert!(!changed_is_source("crates/store/src/rs"));
}
