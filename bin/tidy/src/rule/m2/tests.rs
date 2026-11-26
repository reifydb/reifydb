// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::path::Path;

use crate::rule::{check, tests::rules};

#[test]
fn an_inline_test_module_breaks_m2_once() {
	// One module is one breach, otherwise a file with fifty tests drowns the report in fifty-one lines.
	let text = "fn scan() {}\n#[cfg(test)]\nmod tests {\n#[test]\nfn first() {}\n#[test]\nfn second() {}\n}";
	let found = check(Path::new("source.rs"), text).unwrap();
	assert_eq!(found.len(), 1);
	assert_eq!((found[0].rule, found[0].line), ("M2", 2));
}

#[test]
fn the_tests_declaration_is_clean_only_as_the_last_item() {
	// The one allowed line must stay allowed, and must never drift up into the middle of the code.
	assert!(rules("fn scan() {}\n#[cfg(test)]\nmod tests;").is_empty());
	assert_eq!(rules("#[cfg(test)]\nmod tests;\nfn scan() {}"), ["M2"]);
}

#[test]
fn every_other_test_only_item_breaks_m2() {
	// M2 has no exception beside the tests declaration, so a renamed module or a lone item must never pass.
	assert_eq!(rules("#[cfg(test)]\nmod fixtures;"), ["M2"]);
	assert_eq!(rules("#[cfg(test)]\nmod fixtures { fn store() {} }"), ["M2"]);
	assert_eq!(rules("#[cfg(test)]\nuse crate::store::Store;"), ["M2"]);
	assert_eq!(rules("impl Cache { #[cfg(test)] fn reset(&mut self) {} }"), ["M2"]);
	assert_eq!(rules("#[cfg(test)]\nimpl Cache { fn reset(&mut self) {} }"), ["M2"]);
	assert_eq!(rules("#[test]\nfn scan_runs() {}"), ["M2"]);
	assert_eq!(rules("#[tokio::test]\nasync fn scan_runs() {}"), ["M2"]);
	assert_eq!(rules("struct Cache { #[cfg(test)] hits: usize }"), ["M2"]);
}

#[test]
fn a_cfg_that_is_not_test_passes_m2() {
	// Only test code is banned, so a feature gate or a production-only item must never be flagged.
	assert!(rules("#[cfg(feature = \"wasm\")]\nfn scan() {}").is_empty());
	assert!(rules("#[cfg(not(test))]\nfn scan() {}").is_empty());
}

#[test]
fn a_test_file_may_hold_test_code() {
	// The rule is about source files, so the same text under a test path must be clean.
	let text = "#[test]\nfn scan_runs() {}\n#[cfg(test)]\nmod fixtures {}";
	assert!(check(Path::new("crates/store/src/scan/tests.rs"), text).unwrap().is_empty());
	assert!(check(Path::new("crates/store/tests/scan.rs"), text).unwrap().is_empty());
}
