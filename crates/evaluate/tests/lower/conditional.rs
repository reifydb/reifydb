// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use arrow_array::RecordBatch;
use reifydb_core::value::column::factory;

use crate::common::{
	Env, Frozen, and, arith, batch, between, boolean, code_of, column, compare, conditional, describe, name_of,
	none, number, strings, text,
};

fn input() -> RecordBatch {
	batch(vec![
		factory::bool("c", [true, false, true, false]),
		factory::bool_with_bitvec("o", [true, false, false, true], vec![true, false, true, true]),
		factory::int4("v", [0, 1, 2, 0]),
		factory::utf8("s", ["", "x", "y", ""]),
		factory::int4("a", [10, 20, 30, 40]),
		factory::int8("b", [100, 200, 300, 400]),
		factory::int4_optional("n", [Some(5), None, Some(20), Some(5)]),
	])
}

#[test]
fn every_if_shape_matches_the_old_path() {
	// Every map, extend and match over a conditional moves if the lowered field or values differ from today.
	let mut frozen = Frozen::load("every_if_shape_matches_the_old_path");
	let env = Env::new();
	let shapes = [
		conditional(column("c"), number("1"), vec![], Some(number("2"))),
		conditional(column("o"), column("a"), vec![], Some(column("a"))),
		conditional(column("c"), column("a"), vec![], None),
		conditional(column("c"), column("a"), vec![(column("o"), column("a"))], Some(column("a"))),
		conditional(column("v"), text("yes"), vec![], Some(text("no"))),
		conditional(column("s"), text("yes"), vec![], Some(text("no"))),
		conditional(column("c"), column("a"), vec![], Some(column("b"))),
		conditional(column("c"), number("1"), vec![], Some(number("300"))),
		conditional(column("c"), none(), vec![], Some(column("a"))),
		conditional(column("c"), column("a"), vec![], Some(none())),
	];
	for expression in shapes {
		let old = frozen.recorded();
		match env.lowered(&expression, input()) {
			Ok(lowered) if old.starts_with("Field {") => {
				let renamed =
					(Arc::new(lowered.0.as_ref().clone().with_name(name_of(&old))), lowered.1);
				assert_eq!(describe(&Ok(renamed)), old, "{expression:?}");
			}
			Err(lowered) if !old.starts_with("Field {") => {
				assert_eq!(lowered.code, code_of(&old), "{expression:?}")
			}
			lowered => panic!("{expression:?}: lowered {lowered:?}, old {old}"),
		}
	}
}

#[test]
fn an_if_is_named_after_its_then_branch_whatever_the_rows_pick() {
	// The old path took the name of the first picked branch, which a plan cannot know; the then branch is fixed.
	let env = Env::new();
	let expression = conditional(column("v"), text("yes"), vec![], Some(text("no")));

	let lowered = env.lowered(&expression, input()).unwrap();

	assert_eq!(lowered.0.name(), "\"yes\"");
}

#[test]
fn an_if_lowers_with_the_rest_of_its_expression() {
	// An if must keep the between beside it answering per row over a nullable column, never a type error.
	let env = Env::new();
	let expression = and(
		between(column("n"), number("0"), number("10")),
		conditional(column("c"), boolean("true"), vec![], Some(boolean("false"))),
	);

	let lowered = env.lowered(&expression, input()).unwrap();

	assert_eq!(strings(&lowered), vec!["true", "false", "false", "false"]);
}

#[test]
fn a_branch_runs_only_on_the_rows_that_pick_it() {
	// The old path ran a picked branch on the whole batch, so a guarded division by zero still raised there.
	let mut frozen = Frozen::inline(&["NUMBER_007 \"10/v\" division by zero []"]);
	let env = Env::new();
	let expression = conditional(
		compare("!=", column("v"), number("0")),
		arith("/", number("10"), column("v")),
		vec![],
		Some(arith("-", column("v"), column("v"))),
	);

	let lowered = env.lowered(&expression, input()).unwrap();

	assert_eq!(strings(&lowered), vec!["0", "10", "5", "0"]);
	assert_eq!(code_of(&frozen.recorded()), "NUMBER_007", "today the guard does not protect the division");
}

#[test]
fn a_branch_of_another_type_errors_even_when_no_row_picks_it() {
	// A branch type must never depend on which rows pick it, otherwise the same query fails only on some batches.
	let mut frozen = Frozen::inline(&[
		"RUNTIME_012 \"if\" conditional branches produce different columns [\"first branch yields: [a: Int4]\", \"this branch yields: [\\\"x\\\": Utf8]\"]",
	]);
	let env = Env::new();
	let never = conditional(boolean("true"), column("a"), vec![], Some(text("x")));
	let sometimes = conditional(column("c"), column("a"), vec![], Some(text("x")));

	assert_eq!(env.lowered(&never, input()).unwrap_err().code, "RUNTIME_012");
	assert_eq!(env.lowered(&sometimes, input()).unwrap_err().code, "RUNTIME_012");
	assert_eq!(frozen.old(&env, &sometimes, input()).unwrap_err().code, "RUNTIME_012");
}
