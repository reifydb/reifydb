// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{fs, path::Path};

use super::{FILE_RULES, RULES, check};

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
	let rules = RULES.iter().map(|(rule, _)| *rule).chain(FILE_RULES.iter().map(|(rule, _)| *rule));
	for rule in rules {
		let entry = entries.iter().find(|entry| entry.id == rule).unwrap_or_else(|| panic!("{rule}: no entry"));
		let Some(bad) = entry.bad.as_ref() else {
			continue;
		};
		let found = check(Path::new("source.rs"), bad)
			.unwrap_or_else(|error| panic!("{rule}: Bad block does not parse: {error}"));
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
		let path = if good.contains("#[test]") {
			"tests.rs"
		} else {
			"source.rs"
		};
		let found = check(Path::new(path), &good)
			.unwrap_or_else(|error| panic!("{}: Good block does not parse: {error}", entry.id));
		let rules: Vec<&str> = found.iter().map(|breach| breach.rule).collect();
		assert!(rules.is_empty(), "{}: Good block breaks {rules:?}", entry.id);
	}
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
fn one_function_can_break_both_rules() {
	// Each rule must report on its own, otherwise the first breach hides the second from the baseline.
	let text = "fn scan(first: u8, second: u8, third: u8, fourth: u8, reverse: bool) {}";
	assert_eq!(rules(text), ["F4", "F5"]);
}

#[test]
fn a_breach_names_the_function_and_its_line() {
	// The report is only usable if it points at the exact function, never at the file start.
	let found = check(Path::new("source.rs"), "fn first() {}\n\nfn second(reverse: bool) {}\n").unwrap();
	assert_eq!(found.len(), 1);
	assert_eq!((found[0].line, found[0].function.as_deref()), (3, Some("second")));
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
	assert!(check(Path::new("source.rs"), "fn scan(").is_err());
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

pub(crate) fn rules(text: &str) -> Vec<&'static str> {
	check(Path::new("source.rs"), text).unwrap().iter().map(|breach| breach.rule).collect()
}

pub(crate) fn binding(count: usize) -> String {
	(0..count).map(|index| format!("let name_{index} = 0; ")).collect()
}
