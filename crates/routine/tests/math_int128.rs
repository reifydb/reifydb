// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::LazyLock;

use arrow_array::Array;
use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns, view::group_by::GroupId};
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
		container::decimal_array::{INT16_DATA_TYPE, UINT16_DATA_TYPE, u128s},
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

fn columns(args: Vec<ColumnBuffer>) -> Columns {
	Columns::new(
		args.into_iter()
			.enumerate()
			.map(|(i, data)| ColumnWithName::new(Fragment::internal(format!("arg{i}")), data))
			.collect(),
	)
}

fn call(function: impl Function, args: Vec<ColumnBuffer>) -> Result<ColumnBuffer, RoutineError> {
	let row_count = args.first().map_or(0, ColumnBuffer::len);
	let result = function.call(&mut ctx(row_count), &columns(args))?;
	assert_eq!(result.len(), 1, "a scalar function must return exactly one column");
	Ok(result.data_at(0).clone())
}

fn aggregate(function: impl Function, data: ColumnBuffer) -> ColumnBuffer {
	let rows = (0..data.len()).collect();
	let mut accumulator =
		function.accumulator(&mut ctx(0), &[]).unwrap().expect("the function must be an aggregate");
	accumulator.update(&columns(vec![data]), &vec![(GroupId(0), rows)]).unwrap();
	let (groups, result) = accumulator.finalize().unwrap();
	assert_eq!(groups, vec![GroupId(0)]);
	result
}

fn uint16_rows(column: &ColumnBuffer) -> Vec<u128> {
	let ColumnBuffer::Uint16(array) = column else {
		panic!("expected a Uint16 column, got {:?}", column.get_type());
	};
	assert_eq!(array.data_type(), &UINT16_DATA_TYPE, "a Uint16 result must keep Decimal256(39, 0)");
	u128s(array)
}

fn int16_rows(column: &ColumnBuffer) -> Vec<i128> {
	let ColumnBuffer::Int16(array) = column else {
		panic!("expected an Int16 column, got {:?}", column.get_type());
	};
	assert_eq!(array.data_type(), &INT16_DATA_TYPE, "an Int16 result must keep Decimal128(38, 0)");
	array.values().to_vec()
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
	let out = call(Abs::new(), vec![ColumnBuffer::uint16(vec![u128::MAX, TWO_POW_64, 0, 1 << 127])]).unwrap();
	assert_eq!(uint16_rows(&out), vec![u128::MAX, TWO_POW_64, 0, 1 << 127]);
}

#[test]
fn abs_of_int16_extremes_keeps_every_bit() {
	// Narrowing the i128 read clips values near i128::MAX before abs sees them.
	let out = call(Abs::new(), vec![ColumnBuffer::int16(vec![i128::MIN + 1, i128::MAX, -(1 << 64), 0])]).unwrap();
	assert_eq!(int16_rows(&out), vec![i128::MAX, i128::MAX, 1 << 64, 0]);
}

#[test]
fn sqrt_reads_uint16_and_int16_rows_above_64_bits_as_numbers() {
	// A lossy cast gives none above 64 bits, which would turn these rows into none instead of a root.
	let out = call(Sqrt::new(), vec![ColumnBuffer::uint16(vec![u128::MAX, TWO_POW_64])]).unwrap();
	assert_eq!(out.get_value(0), Value::float8((u128::MAX as f64).sqrt()));
	assert_eq!(out.get_value(1), Value::float8(4_294_967_296.0));

	let out = call(Sqrt::new(), vec![ColumnBuffer::int16(vec![i128::MAX])]).unwrap();
	assert_eq!(out.get_value(0), Value::float8((i128::MAX as f64).sqrt()));
}

#[test]
fn clamp_compares_full_u128_values() {
	// Comparing truncated low halves would pick the wrong bound for rows that differ only above bit 64.
	let out = call(
		Clamp::new(),
		vec![
			ColumnBuffer::uint16(vec![u128::MAX, 0, TWO_POW_64 + 5]),
			ColumnBuffer::uint16(vec![TWO_POW_64, TWO_POW_64, TWO_POW_64]),
			ColumnBuffer::uint16(vec![u128::MAX - 1, u128::MAX, u128::MAX]),
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
			ColumnBuffer::int16(vec![i128::MIN, i128::MAX]),
			ColumnBuffer::int16(vec![i128::MIN + 1, i128::MIN]),
			ColumnBuffer::int16(vec![i128::MAX, i128::MAX - 1]),
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
		vec![ColumnBuffer::uint16(vec![2, u128::MAX, TWO_POW_64]), ColumnBuffer::uint16(vec![127, 1, 1])],
	)
	.unwrap();
	assert_eq!(uint16_rows(&out), vec![1 << 127, u128::MAX, TWO_POW_64]);

	let err = call(Power::new(), vec![ColumnBuffer::uint16(vec![TWO_POW_64]), ColumnBuffer::uint16(vec![2])])
		.unwrap_err();
	assert_eq!(out_of_range_code(err), "NUMBER_002");
}

#[test]
fn add_saturates_and_overflows_exactly_at_u128_max() {
	// Adding in a wider native would push past u128::MAX instead of saturating or raising the overflow error.
	let out = call(
		AddSaturate::new(),
		vec![ColumnBuffer::uint16(vec![u128::MAX - 1, TWO_POW_64]), ColumnBuffer::uint16(vec![5, TWO_POW_64])],
	)
	.unwrap();
	assert_eq!(uint16_rows(&out), vec![u128::MAX, 1 << 65]);

	let err = call(Add::new(), vec![ColumnBuffer::uint16(vec![u128::MAX]), ColumnBuffer::uint16(vec![1])])
		.unwrap_err();
	assert_eq!(out_of_range_code(err), "NUMBER_002");
}

#[test]
fn sum_min_max_aggregate_uint16_rows_above_64_bits() {
	// Dropping the high half of each row would sum and compare only the low 64 bits.
	let rows = vec![u128::MAX, TWO_POW_64, TWO_POW_64 + 1];

	let min = aggregate(Min::new(), ColumnBuffer::uint16(rows.clone()));
	assert_eq!(uint16_rows(&min), vec![TWO_POW_64]);

	let max = aggregate(Max::new(), ColumnBuffer::uint16(rows));
	assert_eq!(uint16_rows(&max), vec![u128::MAX]);

	let sum = aggregate(Sum::new(), ColumnBuffer::uint16(vec![TWO_POW_64, TWO_POW_64, 1 << 126]));
	assert_eq!(uint16_rows(&sum), vec![(1 << 65) + (1 << 126)]);
}

#[test]
fn sum_retract_on_uint16_subtracts_rows_above_64_bits() {
	// Retracting a truncated row would leave the high half of the removed value in the sum.
	let mut accumulator = Sum::new().accumulator(&mut ctx(0), &[]).unwrap().expect("sum is an aggregate");
	let group = vec![(GroupId(0), vec![0])];
	accumulator.update(&columns(vec![ColumnBuffer::uint16(vec![u128::MAX])]), &group).unwrap();
	accumulator.retract(&columns(vec![ColumnBuffer::uint16(vec![TWO_POW_64])]), &group).unwrap();
	let (_, out) = accumulator.finalize().unwrap();
	assert_eq!(uint16_rows(&out), vec![u128::MAX - TWO_POW_64]);
}

#[test]
fn sum_min_max_aggregate_the_int16_extremes() {
	// A narrowed or wrapped i128 read would lose i128::MIN / MAX in the aggregate.
	let rows = vec![i128::MIN, 0, i128::MAX];

	let min = aggregate(Min::new(), ColumnBuffer::int16(rows.clone()));
	assert_eq!(int16_rows(&min), vec![i128::MIN]);

	let max = aggregate(Max::new(), ColumnBuffer::int16(rows.clone()));
	assert_eq!(int16_rows(&max), vec![i128::MAX]);

	let sum = aggregate(Sum::new(), ColumnBuffer::int16(rows));
	assert_eq!(int16_rows(&sum), vec![-1]);
}

#[test]
fn avg_of_uint16_rows_above_64_bits_is_exact() {
	// A 64 bit read would average the low halves, or skip the rows as none and give no average at all.
	let out = aggregate(Avg::new(), ColumnBuffer::uint16(vec![u128::MAX, u128::MAX]));
	assert_eq!(out.get_value(0), Value::Decimal(Decimal::from(u128::MAX)));

	let out = call(
		Avg::new(),
		vec![
			ColumnBuffer::uint16(vec![TWO_POW_64, u128::MAX]),
			ColumnBuffer::uint16(vec![TWO_POW_64 + 2, u128::MAX]),
		],
	)
	.unwrap();
	assert_eq!(out.get_value(0), Value::Decimal(Decimal::from(TWO_POW_64 + 1)));
	assert_eq!(out.get_value(1), Value::Decimal(Decimal::from(u128::MAX)));
}
