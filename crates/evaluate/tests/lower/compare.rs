// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::ArrayRef;
use arrow_schema::FieldRef;
use arrow_select::concat::concat;
use reifydb_core::value::column::factory;
use reifydb_value::value::{
	Value,
	blob::Blob,
	date::Date,
	datetime::DateTime,
	decimal::Decimal,
	duration::Duration,
	identity::IdentityId,
	ordered_f32::OrderedF32,
	ordered_f64::OrderedF64,
	time::Time,
	uuid::{Uuid4, Uuid7},
	value_type::ValueType,
};

use crate::common::{COMPARE_OPS, Env, batch, between, column, compare, none, number, strings, text};

pub fn samples() -> Vec<(Value, Value)> {
	vec![
		(Value::Boolean(false), Value::Boolean(true)),
		(Value::Int1(1), Value::Int1(2)),
		(Value::Int2(1), Value::Int2(2)),
		(Value::Int4(1), Value::Int4(2)),
		(Value::Int8(1), Value::Int8(2)),
		(Value::Int16(1), Value::Int16(2)),
		(Value::Uint1(1), Value::Uint1(2)),
		(Value::Uint2(1), Value::Uint2(2)),
		(Value::Uint4(1), Value::Uint4(2)),
		(Value::Uint8(1), Value::Uint8(2)),
		(Value::Uint16(1), Value::Uint16(2)),
		(
			Value::Float4(OrderedF32::try_from(1.0f32).unwrap()),
			Value::Float4(OrderedF32::try_from(2.5f32).unwrap()),
		),
		(
			Value::Float8(OrderedF64::try_from(1.0f64).unwrap()),
			Value::Float8(OrderedF64::try_from(2.5f64).unwrap()),
		),
		(Value::Decimal(Decimal::from_i64(1)), Value::Decimal(Decimal::from_i64(2))),
		(Value::Utf8("a".to_string()), Value::Utf8("b".to_string())),
		(Value::Blob(Blob::new(vec![1])), Value::Blob(Blob::new(vec![2]))),
		(Value::Date(Date::new(2024, 1, 1).unwrap()), Value::Date(Date::new(2024, 1, 2).unwrap())),
		(
			Value::DateTime(DateTime::new(2024, 1, 1, 0, 0, 0, 0).unwrap()),
			Value::DateTime(DateTime::new(2024, 1, 1, 0, 0, 1, 0).unwrap()),
		),
		(Value::Time(Time::new(1, 0, 0, 0).unwrap()), Value::Time(Time::new(2, 0, 0, 0).unwrap())),
		(
			Value::Duration(Duration::from_seconds(1).unwrap()),
			Value::Duration(Duration::from_seconds(2).unwrap()),
		),
		(Value::Uuid4(Uuid4::default()), Value::Uuid4(Uuid4::default())),
		(Value::Uuid7(Uuid7::default()), Value::Uuid7(Uuid7::default())),
		(Value::IdentityId(IdentityId::default()), Value::IdentityId(IdentityId::default())),
	]
}

pub fn nullable_column(name: &str, (first, second): (Value, Value)) -> (FieldRef, ArrayRef) {
	let value_type = first.get_type();
	let parts = [factory::from_many(name, first, 1), factory::from_many(name, second, 1)];
	let (field, missing) = factory::none_typed(name, value_type, 1);
	let array = concat(&[parts[0].1.as_ref(), parts[1].1.as_ref(), missing.as_ref()]).unwrap();
	(field, array)
}

#[test]
fn every_type_pair_compares_like_the_old_path() {
	// A missing or wrong cast on either side changes the answer or the error for that pair.
	let env = Env::new();

	for left in samples() {
		for right in samples() {
			let input =
				batch(vec![nullable_column("l", left.clone()), nullable_column("r", right.clone())]);
			for op in COMPARE_OPS {
				let expression = compare(op, column("l"), column("r"));
				let lowered = env.lowered(&expression, input.clone());
				let old = env.old(&expression, input.clone());
				assert_eq!(lowered, old, "{:?} {op} {:?}", left.0.get_type(), right.0.get_type());
			}
		}
	}
}

#[test]
fn an_untyped_none_side_gives_none_on_every_row() {
	// A bare none compared with anything is unknown, never false, or a negated filter would keep the row.
	let env = Env::new();
	let input = batch(vec![factory::int4("l", [1, 2])]);

	for op in COMPARE_OPS {
		for expression in [compare(op, none(), column("l")), compare(op, column("l"), none())] {
			let lowered = env.lowered(&expression, input.clone()).unwrap();
			assert_eq!(strings(&lowered), vec!["none", "none"], "{expression:?}");
			assert_eq!(strings(&lowered), strings(&env.old(&expression, input.clone()).unwrap()));
		}
	}
}

#[test]
fn a_type_mismatch_errors_even_when_the_batch_is_all_none() {
	// The type check reads the field, so an all none int column against text still reports the mismatch.
	let env = Env::new();
	let input = batch(vec![factory::none_typed("l", ValueType::Int4, 2)]);

	for op in COMPARE_OPS {
		let expression = compare(op, column("l"), text("x"));
		let lowered = env.lowered(&expression, input.clone()).unwrap_err();
		let old = env.old(&expression, input.clone()).unwrap_err();
		assert_eq!(lowered, old, "{op}");
		assert!(lowered.code.starts_with("OPERATOR_02"), "{op}: {}", lowered.code);
	}
}

#[test]
fn between_matches_the_old_path_on_columns_without_none() {
	// Without none rows the Kleene and of the two bounds must give exactly today's answer.
	let env = Env::new();
	let input = batch(vec![factory::int4("v", [1, 5, 10, 11])]);

	for expression in [
		between(column("v"), number("5"), number("10")),
		between(column("v"), number("1.5"), number("10")),
		between(column("v"), number("10"), number("5")),
	] {
		let lowered = env.lowered(&expression, input.clone()).unwrap();
		let old = env.old(&expression, input.clone()).unwrap();
		assert_eq!(strings(&lowered), strings(&old), "{expression:?}");
	}
}

#[test]
fn between_with_a_none_bound_follows_kleene_where_today_it_errors() {
	// A none bound leaves its side unknown, so the other bound alone can still make the row false.
	let env = Env::new();
	let input = batch(vec![factory::int4("v", [1, 5])]);
	let expression = between(column("v"), none(), number("3"));

	let lowered = env.lowered(&expression, input.clone()).unwrap();
	let old = env.old(&expression, input).unwrap_err();

	assert_eq!(strings(&lowered), vec!["none", "false"]);
	assert_eq!(old.code, "OPERATOR_028");
}

#[test]
fn between_over_a_nullable_column_keeps_its_defined_rows() {
	// Today a nullable value errors outright; the lowered between answers per row and leaves none rows unknown.
	let env = Env::new();
	let input = batch(vec![factory::int4_optional("v", [Some(1), Some(5), None])]);
	let expression = between(column("v"), number("2"), number("10"));

	let lowered = env.lowered(&expression, input.clone()).unwrap();

	assert_eq!(strings(&lowered), vec!["false", "true", "none"]);
	assert_eq!(env.old(&expression, input).unwrap_err().code, "OPERATOR_028");
}

#[test]
fn between_with_a_bound_of_another_type_errors_like_the_old_path() {
	// The mismatch must name between and its own fragment, the same as today.
	let env = Env::new();
	let input = batch(vec![factory::int4("v", [1, 5])]);

	for expression in [between(column("v"), text("a"), number("3")), between(column("v"), number("1"), text("z"))] {
		let lowered = env.lowered(&expression, input.clone()).unwrap_err();
		let old = env.old(&expression, input.clone()).unwrap_err();
		assert_eq!(lowered, old, "{expression:?}");
		assert_eq!(lowered.code, "OPERATOR_028");
	}
}
