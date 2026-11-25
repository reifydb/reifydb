// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::fs;

use super::{RULES, check};

const GUIDE: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../../style.md");

struct Entry {
	id: String,
	bad: Option<String>,
	good: Option<String>,
}

#[test]
fn the_guide_is_read_entry_by_entry() {
	// An empty or misread guide must fail here, otherwise the two tests below pass while checking nothing.
	let ids: Vec<String> = entries().into_iter().map(|entry| entry.id).collect();
	assert!(ids.len() >= 11, "found only {ids:?}");
	assert_eq!(&ids[..3], ["F1", "F2", "F3"]);
}

#[test]
fn every_bad_block_is_flagged_by_its_own_rule() {
	// The guide is the spec: a rule that does not flag its own Bad example enforces something else.
	let entries = entries();
	for (rule, _) in RULES {
		let entry = entries.iter().find(|entry| entry.id == rule).unwrap_or_else(|| panic!("{rule}: no entry"));
		let Some(bad) = entry.bad.as_ref() else {
			continue;
		};
		let found = check(bad).unwrap_or_else(|error| panic!("{rule}: Bad block does not parse: {error}"));
		assert!(found.iter().any(|breach| breach.rule == rule), "{rule}: its Bad block is not flagged");
	}
}

#[test]
fn every_good_block_is_clean_against_all_rules() {
	// A Good example that any rule rejects teaches the reader code the gate refuses.
	for entry in entries() {
		let Some(good) = entry.good else {
			continue;
		};
		let found =
			check(&good).unwrap_or_else(|error| panic!("{}: Good block does not parse: {error}", entry.id));
		let rules: Vec<&str> = found.iter().map(|breach| breach.rule).collect();
		assert!(rules.is_empty(), "{}: Good block breaks {rules:?}", entry.id);
	}
}

#[test]
fn four_arguments_pass_and_five_break_f4() {
	// The limit must sit exactly at 4, otherwise the gate drifts by one in either direction.
	assert!(rules("fn scan(first: u8, second: u8, third: u8, fourth: u8) {}").is_empty());
	assert_eq!(rules("fn scan(first: u8, second: u8, third: u8, fourth: u8, fifth: u8) {}"), ["F4"]);
}

#[test]
fn the_receiver_counts_as_an_argument() {
	// Without counting self, a method could take one argument more than a free function.
	assert!(rules("impl Store { fn scan(&self, first: u8, second: u8, third: u8) {} }").is_empty());
	assert_eq!(rules("impl Store { fn scan(&self, first: u8, second: u8, third: u8, fourth: u8) {} }"), ["F4"]);
}

#[test]
fn extern_c_functions_are_exempt_from_f4_only() {
	// The exemption must never widen to F5 or to other ABIs, otherwise extern becomes an opt-out marker.
	let crowded = "first: u8, second: u8, third: u8, fourth: u8, fifth: u8";
	assert!(rules(&format!("extern \"C\" fn hook({crowded}) {{}}")).is_empty());
	assert!(rules(&format!("extern \"C\" {{ fn hook({crowded}); }}")).is_empty());
	assert!(rules(&format!("extern fn hook({crowded}) {{}}")).is_empty());
	assert_eq!(rules(&format!("extern \"Rust\" fn hook({crowded}) {{}}")), ["F4"]);
	assert_eq!(rules("extern \"C\" fn hook(armed: bool) {}"), ["F5"]);
}

#[test]
fn a_trait_owns_its_signature_and_its_impls_are_not_counted_again() {
	// Counting the impl copies would report one breach once per implementor, which no implementor can fix.
	let crowded = "&self, first: u8, second: u8, third: u8, fourth: u8";
	assert_eq!(rules(&format!("trait Scan {{ fn scan({crowded}); }}")), ["F4"]);
	assert!(rules(&format!("impl Scan for Store {{ fn scan({crowded}) {{}} }}")).is_empty());
}

#[test]
fn a_function_nested_in_a_trait_impl_method_is_still_checked() {
	// The trait exemption must end at the method signature, otherwise a body becomes a hiding place.
	let text = "impl Scan for Store { fn scan(&self) { fn inner(armed: bool) {} } }";
	assert_eq!(rules(text), ["F5"]);
}

#[test]
fn an_inherent_impl_after_a_trait_impl_is_checked() {
	// The trait exemption must end with the impl block, otherwise it leaks to the rest of the file.
	let text = "impl Scan for Store { fn scan(&self) {} } impl Store { fn arm(&self, armed: bool) {} }";
	assert_eq!(rules(text), ["F5"]);
}

#[test]
fn a_plain_bool_argument_breaks_f5_whatever_the_visibility() {
	// F5 has no exceptions, so a private function or a method must never slip through.
	assert_eq!(rules("fn scan(reverse: bool) {}"), ["F5"]);
	assert_eq!(rules("pub fn scan(reverse: bool) {}"), ["F5"]);
	assert_eq!(rules("impl Store { fn scan(&self, reverse: bool) {} }"), ["F5"]);
	assert!(rules("fn scan(order: Order) -> bool { true }").is_empty());
}

#[test]
fn a_bool_behind_a_reference_or_wrapper_passes_f5() {
	// Pins the known gap: only a plain bool is flagged, and this must fail loudly if that ever changes.
	assert!(rules("fn scan(reverse: &bool) {}").is_empty());
	assert!(rules("fn scan(reverse: Option<bool>) {}").is_empty());
}

#[test]
fn one_function_can_break_both_rules() {
	// Each rule must report on its own, otherwise the first breach hides the second from the baseline.
	let text = "fn scan(first: u8, second: u8, third: u8, fourth: u8, reverse: bool) {}";
	assert_eq!(rules(text), ["F4", "F5"]);
}

#[test]
fn a_breach_names_the_function_and_its_line() {
	// The report is only usable if it points at the exact function, never at the file start.
	let found = check("fn first() {}\n\nfn second(reverse: bool) {}\n").unwrap();
	assert_eq!(found.len(), 1);
	assert_eq!((found[0].line, found[0].function.as_str()), (3, "second"));
}

#[test]
fn sixty_body_lines_pass_and_sixty_one_break_f2() {
	// The limit must sit exactly at 60 lines between the braces, blank lines included.
	let body = |lines: usize| format!("fn scan() {{\n{}}}", "\tstep();\n".repeat(lines));
	assert!(rules(&body(60)).is_empty());
	assert_eq!(rules(&body(61)), ["F2"]);
	assert_eq!(rules(&format!("fn scan() {{\n{}}}", "\n".repeat(61))), ["F2"]);
}

#[test]
fn three_levels_pass_and_four_break_f3() {
	// The limit must sit exactly at 3, and arm braces or plain blocks must never cost a level.
	let three = "fn scan() { for row in rows { if due { match kind { Timer => { unsafe { step(); } } } } } }";
	let four = "fn scan() { for row in rows { if due { match kind { Timer => { loop { step(); } } } } } }";
	assert!(rules(three).is_empty());
	assert_eq!(rules(four), ["F3"]);
}

#[test]
fn an_else_if_chain_does_not_deepen_and_a_block_closure_does() {
	// A chain is one decision, while a closure body must count or it becomes a place to hide depth.
	let chain = "fn scan() { for row in rows { while due { if first {} else if second {} else if third {} else {} } } }";
	let closure = "fn scan() { for row in rows { if due { rows.for_each(|row| { if armed { step(); } }); } } }";
	let inline = "fn scan() { for row in rows { if due { while armed { rows.map(|row| row.key); } } } }";
	assert!(rules(chain).is_empty());
	assert_eq!(rules(closure), ["F3"]);
	assert!(rules(inline).is_empty());
}

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

#[test]
fn a_trait_impl_body_is_checked_and_its_signature_is_not() {
	// The body belongs to the implementor, so only the signature rules may be skipped there.
	let text = format!("impl Scan for Store {{ fn scan(&self, reverse: bool) {{ {} }} }}", binding(11));
	assert_eq!(rules(&text), ["F7"]);
}

#[test]
fn text_that_does_not_parse_is_an_error() {
	// A file that cannot be read must stop the run, never count as zero breaches.
	assert!(check("fn scan(").is_err());
}

fn entries() -> Vec<Entry> {
	let text = fs::read_to_string(GUIDE).unwrap();
	text.split("\n## ").skip(1).map(entries_entry).collect()
}

fn entries_entry(section: &str) -> Entry {
	let id = section.trim_start_matches("## ").split_whitespace().next().unwrap().to_string();
	Entry {
		id,
		bad: entries_entry_block(section, "**Bad.**"),
		good: entries_entry_block(section, "**Good.**"),
	}
}

fn entries_entry_block(section: &str, label: &str) -> Option<String> {
	let code = section.split(label).nth(1)?.split("```rust\n").nth(1)?;
	Some(code.split("```").next()?.to_string())
}

fn rules(text: &str) -> Vec<&'static str> {
	check(text).unwrap().iter().map(|breach| breach.rule).collect()
}

fn binding(count: usize) -> String {
	(0..count).map(|index| format!("let name_{index} = 0; ")).collect()
}
