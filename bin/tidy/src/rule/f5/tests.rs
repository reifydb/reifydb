// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use crate::rule::tests::rules;

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
