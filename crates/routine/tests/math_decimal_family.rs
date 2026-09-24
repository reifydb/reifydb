// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::LazyLock;

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns, view::group_by::GroupId};
use reifydb_routine::function::math::{
	abs::Abs,
	add::basic::Add,
	avg::Avg,
	ceil::Ceil,
	clamp::Clamp,
	div::{basic::Div, saturate::DivSaturate},
	floor::Floor,
	mul::basic::Mul,
	power::Power,
	round::Round,
	sub::none::SubNone,
	sum::Sum,
	truncate::Truncate,
};
use reifydb_routine_abi::{Function, context::FunctionContext, error::RoutineError};
use reifydb_runtime::context::RuntimeContext;
use reifydb_value::{
	fragment::Fragment,
	value::{
		constraint::{precision::Precision, scale::Scale},
		decimal::Decimal,
		identity::IdentityId,
		int::Int,
		uint::Uint,
		value_type::ValueType,
	},
};

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

fn aggregate(function: impl Function, data: ColumnBuffer) -> Result<ColumnBuffer, RoutineError> {
	let rows = (0..data.len()).collect();
	let mut accumulator =
		function.accumulator(&mut ctx(0), &[]).unwrap().expect("the function must be an aggregate");
	accumulator.update(&columns(vec![data]), &vec![(GroupId(0), rows)])?;
	let (groups, result) = accumulator.finalize()?;
	assert_eq!(groups, vec![GroupId(0)]);
	Ok(result)
}

fn decimal(precision: u8, scale: u8, values: &[&str]) -> ColumnBuffer {
	ColumnBuffer::decimal(
		Precision::new(precision),
		Scale::new(scale),
		values.iter().map(|value| Decimal::parse(value).unwrap()),
	)
}

fn decimal_type(precision: u8, scale: u8) -> ValueType {
	ValueType::decimal(Precision::new(precision), Scale::new(scale))
}

fn int(precision: u8, values: &[i64]) -> ColumnBuffer {
	ColumnBuffer::int(Precision::new(precision), values.iter().map(|&value| Int::from_i64(value)))
}

fn error_code(err: RoutineError) -> String {
	match err {
		RoutineError::Wrapped(e) => e.diagnostic().code,
		other => panic!("expected a wrapped type error, got {other:?}"),
	}
}

#[test]
fn add_takes_the_wider_scale_and_one_more_integer_digit() {
	// decimal(10,2) + decimal(5,1): 8 + 1 integer digits at scale 2, not the promoted decimal(76, 2).
	let out = call(Add::new(), vec![decimal(10, 2, &["1.25"]), decimal(5, 1, &["2.5"])]).unwrap();
	assert_eq!(out.get_type(), decimal_type(11, 2));
	assert_eq!(out.as_string(0), "3.75");
}

#[test]
fn mul_adds_the_scales() {
	// A product rounded back to the common scale would print 3.13 instead of the exact 3.125.
	let out = call(Mul::new(), vec![decimal(4, 2, &["1.25"]), decimal(3, 1, &["2.5"])]).unwrap();
	assert_eq!(out.get_type(), decimal_type(7, 3));
	assert_eq!(out.as_string(0), "3.125");
}

#[test]
fn div_keeps_at_least_six_fraction_digits() {
	// An integer divisor must not drop the quotient to scale 1 (0.3) or truncate the sixth digit.
	let out = call(Div::new(), vec![decimal(4, 1, &["1.0", "2.0"]), ColumnBuffer::int4([3, 3])]).unwrap();
	assert_eq!(out.get_type(), decimal_type(9, 6));
	assert_eq!(out.as_string(0), "0.333333");
	assert_eq!(out.as_string(1), "0.666667");
}

#[test]
fn saturating_div_by_zero_stays_within_the_result_precision() {
	// A saturated row wider than int(3) would panic in the column builder instead of being clamped.
	let out = call(DivSaturate::new(), vec![int(3, &[5, -5, 9]), int(3, &[0, 0, 3])]).unwrap();
	assert_eq!(out.get_type(), ValueType::int(Precision::new(3)));
	assert!(out.is_defined(0) && out.is_defined(1));
	assert!(out.as_string(0).len() <= 3 && out.as_string(1).trim_start_matches('-').len() <= 3);
	assert_eq!(out.as_string(2), "3");
}

#[test]
fn uint_below_zero_is_none_under_the_none_policy() {
	// N5: uint 1 - 2 must become a none row, never a wrapped or negative value.
	let one = ColumnBuffer::uint(Precision::new(5), [Uint::from(1u64), Uint::from(5u64)]);
	let two = ColumnBuffer::uint(Precision::new(5), [Uint::from(2u64), Uint::from(2u64)]);
	let out = call(SubNone::new(), vec![one, two]).unwrap();
	assert!(!out.is_defined(0));
	assert_eq!(out.as_string(1), "3");
}

#[test]
fn sum_past_seventy_six_digits_is_an_error() {
	// The old bignum sum had no bound; two 76-digit maxima must raise NUMBER_002, not wrap or clamp.
	let err = aggregate(Sum::new(), ColumnBuffer::int(Precision::MAX, [Int::MAX, Int::MAX])).unwrap_err();
	assert_eq!(error_code(err), "NUMBER_002");
}

#[test]
fn sum_of_decimals_keeps_the_scale_at_full_precision() {
	// A sum typed as the input decimal(4,2) could not hold 99.99 + 99.99.
	let out = aggregate(Sum::new(), decimal(4, 2, &["99.99", "99.99"])).unwrap();
	assert_eq!(out.get_type(), decimal_type(76, 2));
	assert_eq!(out.as_string(0), "199.98");
}

#[test]
fn avg_of_integers_has_six_fraction_digits() {
	// The average follows the division rule, so 3 / 2 keeps its half.
	let out = aggregate(Avg::new(), ColumnBuffer::int4([1, 2])).unwrap();
	assert_eq!(out.get_type(), decimal_type(76, 6));
	assert_eq!(out.as_string(0), "1.500000");
}

#[test]
fn ceil_keeps_precision_and_scale() {
	// Rounding must still hand back the input decimal(5,2), not a scale 0 decimal.
	let out = call(Ceil::new(), vec![decimal(5, 2, &["1.21", "-1.21"])]).unwrap();
	assert_eq!(out.get_type(), decimal_type(5, 2));
	assert_eq!(out.as_string(0), "2.00");
	assert_eq!(out.as_string(1), "-1.00");
}

#[test]
fn ceil_that_outgrows_the_precision_is_an_error() {
	// ceil(9.99) is 10.00, four digits; decimal(3,2) cannot hold it and must not silently widen.
	assert!(call(Ceil::new(), vec![decimal(3, 2, &["9.99"])]).is_err());
}

#[test]
fn ceil_floor_and_truncate_keep_every_digit_of_a_wide_decimal() {
	// Through f64 the 29 integer digits would come back as 12345678901234568227576610816.0.
	let wide = || decimal(38, 1, &["12345678901234567890123456789.5", "-12345678901234567890123456789.5"]);
	let ceil = call(Ceil::new(), vec![wide()]).unwrap();
	assert_eq!(ceil.as_string(0), "12345678901234567890123456790.0");
	assert_eq!(ceil.as_string(1), "-12345678901234567890123456789.0");
	let floor = call(Floor::new(), vec![wide()]).unwrap();
	assert_eq!(floor.as_string(0), "12345678901234567890123456789.0");
	assert_eq!(floor.as_string(1), "-12345678901234567890123456790.0");
	let truncate = call(Truncate::new(), vec![wide()]).unwrap();
	assert_eq!(truncate.as_string(0), "12345678901234567890123456789.0");
	assert_eq!(truncate.as_string(1), "-12345678901234567890123456789.0");
}

#[test]
fn round_to_negative_digits_rounds_to_tens_at_the_column_scale() {
	// A negative digit count must round left of the point, and the row must stay at scale 2.
	let out = call(Round::new(), vec![decimal(6, 2, &["123.45", "125.00"]), ColumnBuffer::int4([-1, -1])]).unwrap();
	assert_eq!(out.get_type(), decimal_type(6, 2));
	assert_eq!(out.as_string(0), "120.00");
	assert_eq!(out.as_string(1), "130.00");
}

#[test]
fn round_reads_a_precision_from_an_int_family_column() {
	// An int(p) digit count read as none would round to 0 digits and give 1.00.
	let out = call(Round::new(), vec![decimal(4, 2, &["1.25"]), int(5, &[1])]).unwrap();
	assert_eq!(out.as_string(0), "1.30");
}

#[test]
fn abs_keeps_precision_and_scale() {
	// abs must not reset the column to the default decimal(76, 10).
	let out = call(Abs::new(), vec![decimal(5, 2, &["-1.50", "2.25"])]).unwrap();
	assert_eq!(out.get_type(), decimal_type(5, 2));
	assert_eq!(out.as_string(0), "1.50");
	assert_eq!(out.as_string(1), "2.25");
}

#[test]
fn clamp_accepts_decimals_of_different_scales() {
	// Bounds at scale 1 and 3 must compare by value against the scale 2 input.
	let out =
		call(Clamp::new(), vec![decimal(5, 2, &["1.25"]), decimal(3, 1, &["1.5"]), decimal(5, 3, &["2.000"])])
			.unwrap();
	assert_eq!(out.as_string(0), "1.500");
}

#[test]
fn power_past_seventy_six_digits_is_an_error() {
	// 10^76 has 77 digits; the i256 result must be range checked, not stored.
	let err = call(Power::new(), vec![int(2, &[10]), int(2, &[76])]).unwrap_err();
	assert_eq!(error_code(err), "NUMBER_002");
}

#[test]
fn power_with_an_integral_decimal_exponent_is_exact() {
	// f64 gives 1.5241578753238836e34, losing the last 19 digits of the square.
	let out =
		call(Power::new(), vec![decimal(76, 1, &["123456789012345678.5"]), decimal(76, 1, &["2.0"])]).unwrap();
	assert_eq!(out.as_string(0), "15241578753238836651425088777625362.3");
}

#[test]
fn power_whose_exact_scale_overflows_falls_back_to_f64() {
	// 0.5^200 needs scale 200; refusing it would turn a value that rounds to zero into an error.
	let out = call(Power::new(), vec![decimal(76, 1, &["0.5"]), decimal(76, 1, &["200.0"])]).unwrap();
	assert_eq!(out.as_string(0), "0.0");
}
