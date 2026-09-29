// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::LazyLock;

use arrow_array::{Array, ArrayRef};
use arrow_schema::FieldRef;
use reifydb_core::value::column::{
	factory::{int16, uint16},
	view::group_by::GroupId,
};
use reifydb_routine::function::math::{
	abs::Abs,
	add::{basic::Add, saturate::AddSaturate},
	avg::Avg,
	clamp::Clamp,
	max::Max,
	min::Min,
	power::Power,
	sqrt::Sqrt,
	sum::Sum,
};
use reifydb_routine_abi::{Function, context::FunctionContext, error::RoutineError};
use reifydb_runtime::context::RuntimeContext;
use reifydb_value::{
	fragment::Fragment,
	value::{
		Value,
		column_view::{ColumnView, ViewData},
		container::wide_int_array::wides,
		decimal::Decimal,
		identity::IdentityId,
	},
};

const TWO_POW_64: u128 = 1 << 64;

fn ctx(row_count: usize) -> FunctionContext<'static> {
	static RUNTIME: LazyLock<RuntimeContext> = LazyLock::new(|| RuntimeContext::testing(0, 0));
	FunctionContext {
		fragment: Fragment::internal("math"),
		identity: IdentityId::root(),
		row_count,
		runtime_context: &RUNTIME,
	}
}

fn call(function: impl Function, args: Vec<(FieldRef, ArrayRef)>) -> Result<(FieldRef, ArrayRef), RoutineError> {
	let row_count = args.first().map_or(0, |(_, array)| array.len());
	function.call(&mut ctx(row_count), &args)
}

fn aggregate(function: impl Function, data: (FieldRef, ArrayRef)) -> (FieldRef, ArrayRef) {
	let rows = (0..data.1.len()).collect();
	let mut accumulator =
		function.accumulator(&mut ctx(0), &[]).unwrap().expect("the function must be an aggregate");
	accumulator.update(&[data], &vec![(GroupId(0), rows)]).unwrap();
	let (groups, result) = accumulator.finalize().unwrap();
	assert_eq!(groups, vec![GroupId(0)]);
	result
}

fn uint16_rows(column: &(FieldRef, ArrayRef)) -> Vec<u128> {
	let view = ColumnView::try_from(column).unwrap();
	let ViewData::Uint16(array) = &view.data else {
		panic!("expected a Uint16 column, got {:?}", view.get_type());
	};
	wides::<u128>(array)
}

fn int16_rows(column: &(FieldRef, ArrayRef)) -> Vec<i128> {
	let view = ColumnView::try_from(column).unwrap();
	let ViewData::Int16(array) = &view.data else {
		panic!("expected an Int16 column, got {:?}", view.get_type());
	};
	wides::<i128>(array)
}

fn out_of_range_code(err: RoutineError) -> String {
	match err {
		RoutineError::Wrapped(e) => e.diagnostic().code,
		other => panic!("expected a wrapped type error, got {other:?}"),
	}
}

#[test]
fn abs_keeps_uint16_rows_above_64_bits() {
	// A read through a 64 bit cast turns every row above u64::MAX into none or zero.
	let out = call(Abs::new(), vec![uint16("arg0", vec![u128::MAX, TWO_POW_64, 0, 1 << 127])]).unwrap();
	assert_eq!(uint16_rows(&out), vec![u128::MAX, TWO_POW_64, 0, 1 << 127]);
}

#[test]
fn abs_of_int16_extremes_keeps_every_bit() {
	// Narrowing the i128 read clips values near i128::MAX before abs sees them.
	let out = call(Abs::new(), vec![int16("arg0", vec![i128::MIN + 1, i128::MAX, -(1 << 64), 0])]).unwrap();
	assert_eq!(int16_rows(&out), vec![i128::MAX, i128::MAX, 1 << 64, 0]);
}

#[test]
fn sqrt_reads_uint16_and_int16_rows_above_64_bits_as_numbers() {
	// A lossy cast gives none above 64 bits, which would turn these rows into none instead of a root.
	let out = call(Sqrt::new(), vec![uint16("arg0", vec![u128::MAX, TWO_POW_64])]).unwrap();
	let view = ColumnView::try_from(&out).unwrap();
	assert_eq!(view.get_value(0), Value::float8((u128::MAX as f64).sqrt()));
	assert_eq!(view.get_value(1), Value::float8(4_294_967_296.0));

	let out = call(Sqrt::new(), vec![int16("arg0", vec![i128::MAX])]).unwrap();
	let view = ColumnView::try_from(&out).unwrap();
	assert_eq!(view.get_value(0), Value::float8((i128::MAX as f64).sqrt()));
}

#[test]
fn clamp_compares_full_u128_values() {
	// Comparing truncated low halves would pick the wrong bound for rows that differ only above bit 64.
	let out = call(
		Clamp::new(),
		vec![
			uint16("arg0", vec![u128::MAX, 0, TWO_POW_64 + 5]),
			uint16("arg1", vec![TWO_POW_64, TWO_POW_64, TWO_POW_64]),
			uint16("arg2", vec![u128::MAX - 1, u128::MAX, u128::MAX]),
		],
	)
	.unwrap();
	assert_eq!(uint16_rows(&out), vec![u128::MAX - 1, TWO_POW_64, TWO_POW_64 + 5]);
}

#[test]
fn clamp_keeps_the_int16_extremes() {
	// A sign or width slip at i128::MIN / MAX would clamp to the wrong bound.
	let out = call(
		Clamp::new(),
		vec![
			int16("arg0", vec![i128::MIN, i128::MAX]),
			int16("arg1", vec![i128::MIN + 1, i128::MIN]),
			int16("arg2", vec![i128::MAX, i128::MAX - 1]),
		],
	)
	.unwrap();
	assert_eq!(int16_rows(&out), vec![i128::MIN + 1, i128::MAX - 1]);
}

#[test]
fn power_reaches_2_pow_127_and_overflows_past_u128_max() {
	// The result must be computed in u128: an i256 native would not overflow at 2^128 and a 64 bit read gives none.
	let out = call(
		Power::new(),
		vec![uint16("arg0", vec![2, u128::MAX, TWO_POW_64]), uint16("arg1", vec![127, 1, 1])],
	)
	.unwrap();
	assert_eq!(uint16_rows(&out), vec![1 << 127, u128::MAX, TWO_POW_64]);

	let err = call(Power::new(), vec![uint16("arg0", vec![TWO_POW_64]), uint16("arg1", vec![2])]).unwrap_err();
	assert_eq!(out_of_range_code(err), "NUMBER_002");
}

#[test]
fn add_saturates_and_overflows_exactly_at_u128_max() {
	// Adding in a wider native would push past u128::MAX instead of saturating or raising the overflow error.
	let out = call(
		AddSaturate::new(),
		vec![uint16("arg0", vec![u128::MAX - 1, TWO_POW_64]), uint16("arg1", vec![5, TWO_POW_64])],
	)
	.unwrap();
	assert_eq!(uint16_rows(&out), vec![u128::MAX, 1 << 65]);

	let err = call(Add::new(), vec![uint16("arg0", vec![u128::MAX]), uint16("arg1", vec![1])]).unwrap_err();
	assert_eq!(out_of_range_code(err), "NUMBER_002");
}

#[test]
fn sum_min_max_aggregate_uint16_rows_above_64_bits() {
	// Dropping the high half of each row would sum and compare only the low 64 bits.
	let rows = vec![u128::MAX, TWO_POW_64, TWO_POW_64 + 1];

	let min = aggregate(Min::new(), uint16("arg0", rows.clone()));
	assert_eq!(uint16_rows(&min), vec![TWO_POW_64]);

	let max = aggregate(Max::new(), uint16("arg0", rows));
	assert_eq!(uint16_rows(&max), vec![u128::MAX]);

	let sum = aggregate(Sum::new(), uint16("arg0", vec![TWO_POW_64, TWO_POW_64, 1 << 126]));
	assert_eq!(uint16_rows(&sum), vec![(1 << 65) + (1 << 126)]);
}

#[test]
fn sum_retract_on_uint16_subtracts_rows_above_64_bits() {
	// Retracting a truncated row would leave the high half of the removed value in the sum.
	let mut accumulator = Sum::new().accumulator(&mut ctx(0), &[]).unwrap().expect("sum is an aggregate");
	let group = vec![(GroupId(0), vec![0])];
	accumulator.update(&[uint16("arg0", vec![u128::MAX])], &group).unwrap();
	accumulator.retract(&[uint16("arg0", vec![TWO_POW_64])], &group).unwrap();
	let (_, out) = accumulator.finalize().unwrap();
	assert_eq!(uint16_rows(&out), vec![u128::MAX - TWO_POW_64]);
}

#[test]
fn sum_min_max_aggregate_the_int16_extremes() {
	// A narrowed or wrapped i128 read would lose i128::MIN / MAX in the aggregate.
	let rows = vec![i128::MIN, 0, i128::MAX];

	let min = aggregate(Min::new(), int16("arg0", rows.clone()));
	assert_eq!(int16_rows(&min), vec![i128::MIN]);

	let max = aggregate(Max::new(), int16("arg0", rows.clone()));
	assert_eq!(int16_rows(&max), vec![i128::MAX]);

	let sum = aggregate(Sum::new(), int16("arg0", rows));
	assert_eq!(int16_rows(&sum), vec![-1]);
}

#[test]
fn avg_of_uint16_rows_above_64_bits_is_exact() {
	// A 64 bit read would average the low halves, or skip the rows as none and give no average at all.
	let out = aggregate(Avg::new(), uint16("arg0", vec![u128::MAX, u128::MAX]));
	assert_eq!(ColumnView::try_from(&out).unwrap().get_value(0), Value::Decimal(Decimal::from(u128::MAX)));

	let out = call(
		Avg::new(),
		vec![uint16("arg0", vec![TWO_POW_64, u128::MAX]), uint16("arg1", vec![TWO_POW_64 + 2, u128::MAX])],
	)
	.unwrap();
	let view = ColumnView::try_from(&out).unwrap();
	assert_eq!(view.get_value(0), Value::Decimal(Decimal::from(TWO_POW_64 + 1)));
	assert_eq!(view.get_value(1), Value::Decimal(Decimal::from(u128::MAX)));
}
