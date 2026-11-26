// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use crate::rule::tests::rules;

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
