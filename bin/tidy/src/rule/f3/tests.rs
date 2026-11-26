// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use crate::rule::tests::rules;

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
