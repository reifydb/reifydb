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

fn entries() -> Vec<Entry> {
	let text = fs::read_to_string(GUIDE).unwrap();
	text.split("\n## ").map(entry).collect()
}

fn entry(section: &str) -> Entry {
	let id = section.trim_start_matches("## ").split_whitespace().next().unwrap().to_string();
	Entry {
		id,
		bad: block(section, "**Bad.**"),
		good: block(section, "**Good.**"),
	}
}

fn block(section: &str, label: &str) -> Option<String> {
	let code = section.split(label).nth(1)?.split("```rust\n").nth(1)?;
	Some(code.split("```").next()?.to_string())
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
		let bad = entry.bad.as_ref().unwrap_or_else(|| panic!("{rule}: no Bad block"));
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
