// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::path::Path;

use crate::rule::{check, tests::rules};

#[test]
fn five_hundred_lines_pass_and_five_hundred_one_break_m1() {
	// The limit must sit exactly at 500 lines with blank lines counted, and the breach must name line 501.
	assert!(rules(&"\n".repeat(500)).is_empty());
	let found = check(Path::new("source.rs"), &"\n".repeat(501)).unwrap();
	assert_eq!(found.len(), 1);
	assert_eq!((found[0].rule, found[0].line, found[0].function.as_deref()), ("M1", 501, None));
}

#[test]
fn only_a_test_file_escapes_the_line_limit() {
	// The exemption must follow the three path shapes exactly, otherwise a file name becomes an opt-out.
	let long = "\n".repeat(600);
	let free = ["crates/store/src/scan/tests.rs", "crates/store/tests/scan.rs", "crates/store/benches/scan.rs"];
	for path in free {
		assert!(check(Path::new(path), &long).unwrap().is_empty(), "{path}");
	}
	for path in ["crates/store/src/scan/test_helpers.rs", "crates/store/src/tests_old.rs"] {
		assert_eq!(check(Path::new(path), &long).unwrap().len(), 1, "{path}");
	}
}
