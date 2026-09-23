// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::str::FromStr;

use arrow_array::Array;
use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer};
use reifydb_evaluate::expression::compare::{CompareOp, Equal, GreaterThan, LessThan, compare_columns};
use reifydb_value::{
	Result,
	error::Diagnostic,
	fragment::Fragment,
	value::{
		constraint::{precision::Precision, scale::Scale},
		date::Date,
		datetime::DateTime,
		decimal::Decimal,
		duration::Duration,
		uuid::parse::parse_uuid7,
		value_type::ValueType,
	},
};

fn compare<Op: CompareOp>(left: ColumnBuffer, right: ColumnBuffer) -> ColumnBuffer {
	// A pair that should compare must panic here, never come back as an ordinary type error.
	compare_columns::<Op>(
		&ColumnWithName::new("left", left),
		&ColumnWithName::new("right", right),
		Fragment::testing_empty(),
		|_, l: ValueType, r: ValueType| -> Diagnostic { panic!("no comparison between {l:?} and {r:?}") },
	)
	.unwrap()
	.data
}

fn try_compare<Op: CompareOp>(left: ColumnBuffer, right: ColumnBuffer) -> Result<ColumnBuffer> {
	compare_columns::<Op>(
		&ColumnWithName::new("left", left),
		&ColumnWithName::new("right", right),
		Fragment::testing_empty(),
		|_, l: ValueType, r: ValueType| -> Diagnostic {
			Diagnostic {
				code: "TEST".to_string(),
				message: format!("no comparison between {l:?} and {r:?}"),
				..Default::default()
			}
		},
	)
	.map(|column| column.data)
}

fn bools(column: ColumnBuffer) -> Vec<Option<bool>> {
	let ColumnBuffer::Bool(a) = column else {
		panic!("comparison must return a bool column, got {:?}", column.get_type())
	};
	(0..a.len())
		.map(|i| {
			if a.is_null(i) {
				None
			} else {
				Some(a.value(i))
			}
		})
		.collect()
}

#[test]
fn nan_equals_nan() {
	// Without float normalization plus total order, NaN = NaN would be false.
	let result = compare::<Equal>(ColumnBuffer::float8([f64::NAN]), ColumnBuffer::float8([f64::NAN]));
	assert_eq!(bools(result), vec![Some(true)]);
}

#[test]
fn nan_above_every_number() {
	// Total order must place NaN above every number, otherwise sort and filter disagree.
	let result = compare::<GreaterThan>(ColumnBuffer::float8([f64::NAN]), ColumnBuffer::float8([1.0]));
	assert_eq!(bools(result), vec![Some(true)]);
}

#[test]
fn negative_zero_equals_zero() {
	// Total order alone ranks -0.0 below 0.0; the key normalization must fold them.
	let result = compare::<Equal>(ColumnBuffer::float8([-0.0]), ColumnBuffer::float8([0.0]));
	assert_eq!(bools(result), vec![Some(true)]);
}

#[test]
fn float4_nan_equals_float8_nan() {
	// A float4 NaN must stay the canonical NaN after the cast to float8, otherwise mixed-width NaNs differ.
	let result = compare::<Equal>(ColumnBuffer::float4([f32::NAN]), ColumnBuffer::float8([f64::NAN]));
	assert_eq!(bools(result), vec![Some(true)]);
}

#[test]
fn false_below_true() {
	// Ordering on bool must be allowed and must put false before true.
	let result = compare::<LessThan>(ColumnBuffer::bool([false, true]), ColumnBuffer::bool([true, true]));
	assert_eq!(bools(result), vec![Some(true), Some(false)]);
}

#[test]
fn int4_column_vs_one_row_int1() {
	// A one-row right side must broadcast to every row after widening int1 to int4.
	let result = compare::<Equal>(ColumnBuffer::int4([4, 5, 6]), ColumnBuffer::int1([5]));
	assert_eq!(bools(result), vec![Some(false), Some(true), Some(false)]);
}

#[test]
fn one_row_left_side() {
	// A one-row left side must broadcast too, and keep the operand order for a non-symmetric op.
	let result = compare::<LessThan>(ColumnBuffer::int1([5]), ColumnBuffer::int4([4, 5, 6]));
	assert_eq!(bools(result), vec![Some(false), Some(false), Some(true)]);
}

#[test]
fn signed_vs_unsigned_never_wraps() {
	// Casting -1 into u64 or u64::MAX into i64 would flip this; the common type must hold both.
	let result = compare::<LessThan>(ColumnBuffer::int8([-1]), ColumnBuffer::uint8([u64::MAX]));
	assert_eq!(bools(result), vec![Some(true)]);
}

#[test]
fn i128_vs_u128_extremes() {
	// Neither i128 nor u128 holds both sides; only the 256-bit storage target orders them right.
	let result = compare::<LessThan>(
		ColumnBuffer::int16([i128::MIN, i128::MAX]),
		ColumnBuffer::uint16([u128::MAX, u128::MAX]),
	);
	assert_eq!(bools(result), vec![Some(true), Some(true)]);
}

#[test]
fn uuid7_order() {
	// Byte order of uuid7 must follow creation time, otherwise time-ordered ids compare wrong.
	let earlier = parse_uuid7(Fragment::internal("01890a5d-ac96-774b-bcce-b302099a8057")).unwrap();
	let later = parse_uuid7(Fragment::internal("01890a5d-ac97-774b-bcce-b302099a8057")).unwrap();
	let result = compare::<LessThan>(ColumnBuffer::uuid7([earlier]), ColumnBuffer::uuid7([later]));
	assert_eq!(bools(result), vec![Some(true)]);
}

#[test]
fn duration_months_before_days() {
	// Months must outrank days in the field order, so 1 month is above 31 days.
	let result = compare::<GreaterThan>(
		ColumnBuffer::duration([Duration::from_months(1).unwrap()]),
		ColumnBuffer::duration([Duration::from_days(31).unwrap()]),
	);
	assert_eq!(bools(result), vec![Some(true)]);
}

#[test]
fn none_on_either_side() {
	// A none row must give none, never a stale value from the storage behind it.
	let result =
		compare::<Equal>(ColumnBuffer::int4_with_bitvec([1, 2], vec![true, false]), ColumnBuffer::int4([1, 2]));
	assert_eq!(bools(result), vec![Some(true), None]);
}

#[test]
fn untyped_none() {
	// An untyped none against a typed column must give none, never a type error.
	let result = compare::<Equal>(ColumnBuffer::none_typed(ValueType::Any, 2), ColumnBuffer::int4([1, 2]));
	assert_eq!(bools(result), vec![None, None]);
}

#[test]
fn decimal_vs_int4() {
	// Decimal must still compare against ints through the kernel, not fail as a type error.
	let result = compare::<GreaterThan>(
		ColumnBuffer::decimal(Precision::new(2), Scale::new(1), [Decimal::from_str("1.5").unwrap()]),
		ColumnBuffer::int4([1]),
	);
	assert_eq!(bools(result), vec![Some(true)]);
}

#[test]
fn date_vs_datetime() {
	// Date and datetime share no common type, so the comparison must be a type error.
	let result = try_compare::<LessThan>(
		ColumnBuffer::date(vec![Date::new(2024, 1, 1).unwrap()]),
		ColumnBuffer::datetime(vec![DateTime::from_nanos(0)]),
	);
	assert!(result.is_err(), "date vs datetime must be a type error, got {result:?}");
}

#[test]
fn length_mismatch() {
	// Two multi-row columns of different length must fail, never silently truncate.
	let result = try_compare::<Equal>(ColumnBuffer::int4([1, 2]), ColumnBuffer::int4([1, 2, 3]));
	assert!(result.is_err(), "length mismatch must be an error, got {result:?}");
}
