// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::value::column::factory;
use reifydb_evaluate::lower::{CLAIMED, kind};
use reifydb_value::value::value_type::{ValueType, field::from_field};

use crate::common::{Env, alias, arith, batch, column, strings, xor};

#[test]
fn an_alias_over_a_column_lowers_to_the_column_renamed() {
	// A map output column is named by its alias, so keeping the inner name would rename the user's column.
	let env = Env::new();
	let input = batch(vec![factory::int4("a", [1, 2])]);
	let expression = alias("x", column("a"));

	let lowered = env.lowered(&expression, input.clone()).unwrap();
	let old = env.old(&expression, input).unwrap();

	assert_eq!(lowered, old);
	assert_eq!(lowered.0.name(), "x");
	assert_eq!(strings(&lowered), vec!["1", "2"]);
}

#[test]
fn an_alias_over_arithmetic_keeps_the_arith_type_and_takes_the_alias_name() {
	// The rename must touch only the name, never the promoted type or nullability of the node below it.
	let env = Env::new();
	let input = batch(vec![factory::int4("l", [1]), factory::int4("r", [2])]);
	let expression = alias("total", arith("+", column("l"), column("r")));

	let lowered = env.lowered(&expression, input.clone()).unwrap();
	let old = env.old(&expression, input).unwrap();

	assert_eq!(lowered, old);
	assert_eq!(lowered.0.name(), "total");
	assert_eq!(from_field(&lowered.0).unwrap().value_type, Some(ValueType::Int8));
	assert_eq!(strings(&lowered), vec!["3"]);
}

#[test]
fn an_alias_over_an_unclaimed_kind_falls_back_without_a_panic() {
	// The panic must name the unclaimed node below the alias, so a claimed alias over xor falls back quietly.
	let env = Env::new();
	let input = batch(vec![factory::bool("l", [true, false]), factory::bool("r", [false, false])]);
	let expression = alias("x", xor(column("l"), column("r")));
	assert!(CLAIMED.contains(&kind(&expression)));

	let lowered = env.lowered(&expression, input.clone()).unwrap();
	let old = env.old(&expression, input).unwrap();

	assert_eq!(lowered, old);
	assert_eq!(lowered.0.name(), "x");
	assert_eq!(strings(&lowered), vec!["true", "false"]);
}
