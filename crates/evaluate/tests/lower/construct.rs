// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::RecordBatch;
use reifydb_core::{
	expression::{Expression, ExtendExpression},
	value::column::factory,
};

use crate::common::{Env, and, batch, between, column, compare, frag, list, map, number, strings, text, tuple};

fn input() -> RecordBatch {
	batch(vec![
		factory::int4("a", [1, 2]),
		factory::utf8("s", ["x", "y"]),
		factory::int4_optional("n", [Some(5), None]),
	])
}

#[test]
fn every_constructor_shape_matches_the_old_path() {
	// Tuples, lists and one-column maps reach goldens through in, contains and match; each must equal today.
	let env = Env::new();
	for expression in [
		tuple(vec![column("a")]),
		tuple(vec![column("a"), column("s")]),
		list(vec![column("a"), number("1")]),
		list(vec![text("x")]),
		map(vec![column("s")]),
	] {
		let lowered = env.lowered(&expression, input()).unwrap();
		let old = env.old(&expression, input()).unwrap();
		assert_eq!(lowered.0, old.0, "field of {expression:?}");
		assert_eq!(strings(&lowered), strings(&old), "values of {expression:?}");
	}
}

#[test]
fn an_empty_list_matches_the_old_path_on_one_row() {
	// The old path built one row for an empty list whatever the batch, so only a one-row batch can compare.
	let env = Env::new();
	let one = batch(vec![factory::int4("a", [1])]);
	let expression = list(vec![]);

	let lowered = env.lowered(&expression, one.clone()).unwrap();

	assert_eq!(strings(&lowered), strings(&env.old(&expression, one).unwrap()));
	assert_eq!(strings(&env.lowered(&expression, input()).unwrap()).len(), 2, "one empty list per row");
}

#[test]
fn a_map_of_several_columns_errors_like_the_old_path() {
	// One value is required here, so a multi-column map must keep today's error.
	let env = Env::new();
	let expression = map(vec![column("a"), column("s")]);

	let lowered = env.lowered(&expression, input()).unwrap_err();

	assert_eq!(lowered.code, env.old(&expression, input()).unwrap_err().code);
}

#[test]
fn a_constructor_lowers_with_the_rest_of_its_expression() {
	// Left unclaimed, the whole and falls back and between over a nullable column errors there.
	let env = Env::new();
	let guard = || between(column("n"), number("0"), number("10"));
	let extend = Expression::Extend(ExtendExpression {
		expressions: vec![guard()],
		fragment: frag("extend"),
	});
	for wrapped in [tuple(vec![guard()]), map(vec![guard()]), extend] {
		let lowered = env.lowered(&and(guard(), wrapped.clone()), input()).unwrap();
		assert_eq!(strings(&lowered), vec!["true", "none"], "{wrapped:?}");
	}
	let listed = and(guard(), compare("==", list(vec![guard()]), list(vec![guard()])));
	assert_ne!(env.lowered(&listed, input()).unwrap_err().code, "OPERATOR_028", "a list must not fall back");
}
