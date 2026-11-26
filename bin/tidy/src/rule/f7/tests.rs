// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use crate::rule::tests::{binding, rules};

#[test]
fn ten_names_pass_and_eleven_break_f7() {
	// The limit must sit exactly at 10 names, otherwise the gate drifts by one in either direction.
	assert!(rules(&format!("fn scan() {{ {} }}", binding(10))).is_empty());
	assert_eq!(rules(&format!("fn scan() {{ {} }}", binding(11))), ["F7"]);
}

#[test]
fn f7_counts_names_and_never_arguments_loop_variables_or_arm_names() {
	// A tuple or an if let must cost what it binds, otherwise locals hide in patterns and F7 rewards
	// nesting.
	let nine = binding(9);
	let tuple = format!("fn scan() {{ {nine} let (start, end) = bounds(); }}");
	let guard = format!(
		"fn scan() {{ {nine} let Some(row) = next() else {{ return; }}; if let Some(timer) = find() {{}} }}"
	);
	let free = format!(
		"fn scan(first: u8, second: u8) {{ {nine} let _ = send(); for row in rows {{}} match event {{ Put(key) => {{}} }} if let None = find() {{}} let last = 0; }}"
	);
	assert_eq!(rules(&tuple), ["F7"]);
	assert_eq!(rules(&guard), ["F7"]);
	assert!(rules(&free).is_empty());
}

#[test]
fn a_closure_counts_toward_its_holder_and_a_nested_function_does_not() {
	// A nested fn is its own function, so its body must be judged alone and never added to the outer one.
	let six = binding(6);
	let closure = format!("fn scan() {{ {six} rows.for_each(|row| {{ {six} }}); }}");
	let nested = format!("fn scan() {{ {six} fn inner() {{ {six} }} }}");
	assert_eq!(rules(&closure), ["F7"]);
	assert!(rules(&nested).is_empty());
	let deep = "fn scan() { for row in rows { if due { fn inner() { for row in rows { if due {} } } } } }";
	assert!(rules(deep).is_empty());
}
