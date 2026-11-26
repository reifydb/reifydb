// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use crate::rule::tests::rules;

#[test]
fn sixty_body_lines_pass_and_sixty_one_break_f2() {
	// The limit must sit exactly at 60 lines between the braces, blank lines included.
	let body = |lines: usize| format!("fn scan() {{\n{}}}", "\tstep();\n".repeat(lines));
	assert!(rules(&body(60)).is_empty());
	assert_eq!(rules(&body(61)), ["F2"]);
	assert_eq!(rules(&format!("fn scan() {{\n{}}}", "\n".repeat(61))), ["F2"]);
}
