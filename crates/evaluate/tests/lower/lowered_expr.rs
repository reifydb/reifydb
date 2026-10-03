// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::BTreeSet;

use reifydb_core::{expression::PrefixOperator, interface::identifier::ColumnObject, value::column::factory};
use reifydb_evaluate::lower::{CLAIMED, LoweredExpr, kind};

use crate::common::{
	ARITH_OPS, COMPARE_OPS, Env, access, and, arith, batch, between, boolean, column, compare, frag, not, number,
	or, prefix, rows, strings, xor,
};

#[test]
fn the_claimed_list_is_exactly_the_lowered_kinds() {
	// A kind missing from the list falls back silently; an extra kind panics before its step lowers it.
	let claimed: BTreeSet<&str> = CLAIMED.iter().copied().collect();
	let expected: BTreeSet<&str> = [
		"Column",
		"AccessSource",
		"Constant",
		"Prefix(Not)",
		"And",
		"Or",
		"Equal",
		"NotEqual",
		"LessThan",
		"LessThanEqual",
		"GreaterThan",
		"GreaterThanEqual",
		"Between",
		"Add",
		"Sub",
		"Mul",
		"Div",
		"Rem",
	]
	.into_iter()
	.collect();

	assert_eq!(claimed, expected);
	assert_eq!(CLAIMED.len(), expected.len(), "the claimed list must not repeat a kind");
}

#[test]
fn every_claimed_name_is_a_kind_that_kind_can_return() {
	// A typo in the list would never match, so that kind would fall back without the panic.
	let mut samples = vec![
		column("a"),
		access(ColumnObject::Alias(frag("t")), "a"),
		number("1"),
		not(boolean("true")),
		and(boolean("true"), boolean("true")),
		or(boolean("true"), boolean("true")),
		between(number("1"), number("0"), number("2")),
	];
	samples.extend(COMPARE_OPS.map(|op| compare(op, number("1"), number("2"))));
	samples.extend(ARITH_OPS.map(|op| arith(op, number("1"), number("2"))));

	let kinds: BTreeSet<&str> = samples.iter().map(kind).collect();
	let claimed: BTreeSet<&str> = CLAIMED.iter().copied().collect();

	assert_eq!(kinds, claimed);
}

#[test]
fn prefix_kinds_are_split_by_operator() {
	// Only not is claimed in this step, so minus and plus must not share its kind name.
	assert_eq!(kind(&not(number("1"))), "Prefix(Not)");
	assert_eq!(kind(&prefix(PrefixOperator::Minus(frag("-")), number("1"))), "Prefix(Minus)");
	assert_eq!(kind(&prefix(PrefixOperator::Plus(frag("+")), number("1"))), "Prefix(Plus)");
	assert!(!CLAIMED.contains(&"Prefix(Minus)"));
	assert!(!CLAIMED.contains(&"Prefix(Plus)"));
}

#[test]
fn an_unclaimed_kind_falls_back_to_the_old_path_with_the_same_result() {
	// xor is not lowered yet, so it must run on the old path and answer exactly as today.
	let env = Env::new();
	let input = batch(vec![factory::bool("l", [true, true, false]), factory::bool("r", [true, false, false])]);
	let expression = xor(column("l"), column("r"));

	let lowered = env.lowered(&expression, input.clone()).unwrap();
	let old = env.old(&expression, input).unwrap();

	assert_eq!(lowered, old);
	assert_eq!(strings(&lowered), vec!["false", "true", "false"]);
}

#[test]
fn an_unclaimed_kind_under_a_claimed_parent_falls_back_without_a_panic() {
	// The panic must look at the node that failed to lower, not at the claimed and above it.
	let env = Env::new();
	let input = batch(vec![factory::bool("l", [true, false]), factory::bool("r", [false, false])]);
	let expression = and(xor(column("l"), column("r")), column("l"));

	let lowered = env.lowered(&expression, input.clone()).unwrap();
	let old = env.old(&expression, input).unwrap();

	assert_eq!(strings(&lowered), strings(&old));
	assert_eq!(strings(&lowered), vec!["true", "false"]);
}

#[test]
fn a_new_schema_lowers_again_and_reads_the_right_column() {
	// Keeping the first lowering would read column 0 by position after the columns moved.
	let env = Env::new();
	let expression = LoweredExpr::new(column("a"), "test");

	let first = batch(vec![factory::int4("a", [1]), factory::int4("b", [2])]);
	let moved = batch(vec![factory::int4("b", [20]), factory::int4("a", [10])]);

	assert_eq!(strings(&expression.evaluate(&env.ctx(first)).unwrap()), vec!["1"]);
	assert_eq!(strings(&expression.evaluate(&env.ctx(moved)).unwrap()), vec!["10"]);
}

#[test]
fn the_same_schema_reuses_the_lowering_for_later_batches() {
	// A second batch with the same columns must read its own rows, not the rows of the first batch.
	let env = Env::new();
	let expression = LoweredExpr::new(column("a"), "test");

	let first = batch(vec![factory::int4("a", [1, 2])]);
	let second = batch(vec![factory::int4("a", [3, 4, 5])]);

	assert_eq!(strings(&expression.evaluate(&env.ctx(first)).unwrap()), vec!["1", "2"]);
	assert_eq!(strings(&expression.evaluate(&env.ctx(second)).unwrap()), vec!["3", "4", "5"]);
}

#[test]
fn a_lowering_error_is_returned_on_every_evaluate() {
	// Returning Ok after the first error would let a broken filter pass rows on the second batch.
	let env = Env::new();
	let expression = LoweredExpr::new(and(number("1"), boolean("true")), "test");

	let first = expression.evaluate(&env.ctx(rows(2))).unwrap_err();
	let second = expression.evaluate(&env.ctx(rows(2))).unwrap_err();

	assert_eq!(first.code, second.code);
	assert_eq!(first.fragment.text(), second.fragment.text());
	assert_eq!(first.code, env.old(&and(number("1"), boolean("true")), rows(2)).unwrap_err().code);
}
