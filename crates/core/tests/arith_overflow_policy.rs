// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::ArrayRef;
use arrow_schema::FieldRef;
use reifydb_core::value::column::{
	arith::{
		add::add_columns,
		context::{ArithContext, Overflow},
		div::div_columns,
	},
	factory,
};
use reifydb_value::{fragment::Fragment, value::column_view::ColumnView};

fn values(column: &(FieldRef, ArrayRef)) -> Vec<String> {
	let view = ColumnView::try_from(column).unwrap();
	(0..view.len()).map(|row| view.as_string(row)).collect()
}

fn policy(overflow: Overflow) -> ArithContext {
	ArithContext {
		target: None,
		overflow,
	}
}

#[test]
fn the_none_policy_turns_an_overflowing_row_into_none_and_keeps_the_rest() {
	// Integers add in the widest type, so only int16 can overflow; the none strategies rely on a none row there.
	let left = factory::int16("l", [i128::MAX, 1]);
	let right = factory::int16("r", [1, 1]);

	let result = add_columns(&policy(Overflow::None), &left, &right, || Fragment::internal("add")).unwrap();

	assert_eq!(values(&result), vec!["none", "2"]);
}

#[test]
fn the_none_policy_turns_a_division_by_zero_into_none() {
	// A zero divisor must not raise under the none policy, otherwise math::div_none errors where it should not.
	let left = factory::int4("l", [10, 10]);
	let right = factory::int4("r", [0, 5]);

	let result = div_columns(&policy(Overflow::None), &left, &right, || Fragment::internal("div")).unwrap();

	assert_eq!(values(&result), vec!["none", "2"]);
}

#[test]
fn the_error_policy_raises_out_of_range_on_overflow() {
	// The default strategy must fail loudly on overflow instead of wrapping or dropping the row.
	let left = factory::int16("l", [i128::MAX]);
	let right = factory::int16("r", [1]);

	let err = add_columns(&policy(Overflow::Error), &left, &right, || Fragment::internal("add")).unwrap_err();

	assert_eq!(err.code, "NUMBER_002");
}

#[test]
fn the_saturate_and_wrap_policies_clamp_and_wrap_an_overflowing_row() {
	// math::add_saturate and math::add_wrap answer through these two policies on the shared kernel.
	let left = factory::int16("l", [i128::MAX]);
	let right = factory::int16("r", [1]);

	let saturated = add_columns(&policy(Overflow::Saturate), &left, &right, || Fragment::internal("add")).unwrap();
	let wrapped = add_columns(&policy(Overflow::Wrap), &left, &right, || Fragment::internal("add")).unwrap();

	assert_eq!(values(&saturated), vec![i128::MAX.to_string()]);
	assert_eq!(values(&wrapped), vec![i128::MIN.to_string()]);
}
