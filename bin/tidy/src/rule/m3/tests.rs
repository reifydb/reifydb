// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::path::Path;

use crate::rule::check;

#[test]
fn a_file_beside_a_directory_of_its_name_breaks_m3() {
	// The check must look at the real folder, so it is pinned on this crate's own layout.
	let source = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
	let found = check(&source.join("rule.rs"), "").unwrap();
	assert_eq!(found.len(), 1);
	assert_eq!((found[0].rule, found[0].line), ("M3", 1));
	assert!(check(&source.join("main.rs"), "").unwrap().is_empty());
	assert!(check(&source.join("rule").join("mod.rs"), "").unwrap().is_empty());
}
