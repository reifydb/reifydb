// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::path::Path;

use crate::rule::{check, tests::rules};

#[test]
fn the_two_unused_markers_break_m4() {
	// Only three levels exist, so every other spelling of a narrowed pub must be flagged.
	assert_eq!(rules("pub(super) fn open_tier() {}"), ["M4"]);
	assert_eq!(rules("pub(in crate::tier) fn open_tier() {}"), ["M4"]);
	assert_eq!(rules("pub(in crate) fn open_tier() {}"), ["M4"]);
	assert_eq!(rules("pub(self) fn open_tier() {}"), ["M4"]);
	assert_eq!(rules("struct Tier { pub(super) path: PathBuf }"), ["M4"]);
}

#[test]
fn the_three_levels_pass_m4() {
	// The allowed markers must never be flagged, otherwise the rule leaves no way to write an item.
	assert!(rules("fn merge_runs() {}").is_empty());
	assert!(rules("pub(crate) fn open_tier() {}").is_empty());
	assert!(rules("pub fn open_tier() {}").is_empty());
}

#[test]
fn m4_holds_in_a_test_file_and_names_the_line() {
	// Visibility has no test exemption, and the report must point at the marker itself.
	let found =
		check(Path::new("crates/store/src/scan/tests.rs"), "fn first() {}\npub(super) fn second() {}").unwrap();
	assert_eq!(found.len(), 1);
	assert_eq!((found[0].rule, found[0].line), ("M4", 2));
}
