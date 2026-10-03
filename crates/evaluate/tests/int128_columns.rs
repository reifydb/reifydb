// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::ArrayRef;
use arrow_schema::FieldRef;
use reifydb_core::{expression::PrefixOperator, value::column::factory};
use reifydb_evaluate::expression::{
	arith::add::add_columns,
	compare::{CompareOp, Equal, GreaterThan, LessThan, NotEqual, compare_columns},
	context::EvalContext,
	prefix::prefix_apply,
};
use reifydb_value::{
	Result,
	error::Diagnostic,
	fragment::Fragment,
	value::{
		column_view::{ColumnView, ViewData},
		container::wide_int_array::wides,
		value_type::ValueType,
	},
};

const TWO_POW_64: u128 = 1 << 64;

fn compare<Op: CompareOp>(left: (FieldRef, ArrayRef), right: (FieldRef, ArrayRef)) -> ArrayRef {
	// A missing dispatch arm must panic here, never come back as an ordinary type error.
	compare_columns::<Op>(
		&factory::rename(left, "left"),
		&factory::rename(right, "right"),
		Fragment::testing_empty(),
		|_, l: ValueType, r: ValueType| -> Diagnostic { panic!("no comparison between {l:?} and {r:?}") },
	)
	.unwrap()
	.1
}

fn add(left: (FieldRef, ArrayRef), right: (FieldRef, ArrayRef)) -> Result<(FieldRef, ArrayRef)> {
	let ctx = EvalContext::testing();
	let fragment = Fragment::testing_empty();
	add_columns(&ctx.arith(), &factory::rename(left, "left"), &factory::rename(right, "right"), &fragment)
}

fn prefix(column: (FieldRef, ArrayRef), operator: PrefixOperator) -> (FieldRef, ArrayRef) {
	prefix_apply(&factory::rename(column, "column"), &operator, &Fragment::testing_empty()).unwrap()
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

#[test]
fn uint16_compare_orders_rows_at_2_pow_64_and_u128_max() {
	// A lossy i256 read drops the bits above 64, so 2^64 and 2^64 + 1 would compare equal.
	let left = || factory::uint16("", [u128::MAX, TWO_POW_64, TWO_POW_64 + 1, u128::MAX - 1]);
	let right = || factory::uint16("", [u128::MAX, TWO_POW_64 + 1, TWO_POW_64, u128::MAX]);

	assert_eq!(
		compare::<Equal>(left(), right()).as_ref(),
		factory::bool("", [true, false, false, false]).1.as_ref()
	);
	assert_eq!(
		compare::<NotEqual>(left(), right()).as_ref(),
		factory::bool("", [false, true, true, true]).1.as_ref()
	);
	assert_eq!(
		compare::<LessThan>(left(), right()).as_ref(),
		factory::bool("", [false, true, false, true]).1.as_ref()
	);
	assert_eq!(
		compare::<GreaterThan>(left(), right()).as_ref(),
		factory::bool("", [false, false, true, false]).1.as_ref()
	);
}

#[test]
fn uint16_compare_against_uint8_on_either_side() {
	// The left and right Uint16 reads are separate dispatch paths; either one losing the high bits breaks here.
	let wide = || factory::uint16("", [TWO_POW_64, u64::MAX as u128, u128::MAX]);
	let narrow = || factory::uint8("", [u64::MAX, u64::MAX, 0]);

	assert_eq!(
		compare::<GreaterThan>(wide(), narrow()).as_ref(),
		factory::bool("", [true, false, true]).1.as_ref()
	);
	assert_eq!(compare::<Equal>(wide(), narrow()).as_ref(), factory::bool("", [false, true, false]).1.as_ref());
	assert_eq!(compare::<LessThan>(narrow(), wide()).as_ref(), factory::bool("", [true, false, true]).1.as_ref());
	assert_eq!(compare::<Equal>(narrow(), wide()).as_ref(), factory::bool("", [false, true, false]).1.as_ref());
}

#[test]
fn uint16_add_keeps_u128_max_and_2_pow_64_exact() {
	// A lossy i256 read or a result built with a default decimal type would change these sums.
	let sum = add(
		factory::uint16("", [u128::MAX - 1, TWO_POW_64, TWO_POW_64 - 1]),
		factory::uint16("", [1, TWO_POW_64, 1]),
	)
	.unwrap();

	assert_eq!(uint16_rows(&sum), [u128::MAX, 1 << 65, TWO_POW_64]);
}

#[test]
fn uint16_add_with_uint8_on_either_side() {
	// Each side of the dispatch reads Uint16 rows separately; the result must still be a (39, 0) Uint16 column.
	let wide = || factory::uint16("", [u128::MAX - u64::MAX as u128, TWO_POW_64]);
	let narrow = || factory::uint8("", [u64::MAX, 1]);

	assert_eq!(uint16_rows(&add(wide(), narrow()).unwrap()), [u128::MAX, TWO_POW_64 + 1]);
	assert_eq!(uint16_rows(&add(narrow(), wide()).unwrap()), [u128::MAX, TWO_POW_64 + 1]);
}

#[test]
fn uint16_add_past_u128_max_is_an_error() {
	// Wrapping or saturating on overflow would hand back a wrong sum instead of the range error.
	let err = add(factory::uint16("", [u128::MAX]), factory::uint16("", [1])).unwrap_err();

	assert_eq!(err.0.code, "NUMBER_002");
}

#[test]
fn int16_compare_orders_i128_min_and_max() {
	// Any narrowing of the signed 128 bit rows would flip or equalise these extremes.
	let left = || factory::int16("", [i128::MIN, i128::MAX, i128::MIN, i128::MAX]);
	let right = || factory::int16("", [i128::MAX, i128::MIN, i128::MIN, i128::MAX]);

	assert_eq!(
		compare::<Equal>(left(), right()).as_ref(),
		factory::bool("", [false, false, true, true]).1.as_ref()
	);
	assert_eq!(
		compare::<LessThan>(left(), right()).as_ref(),
		factory::bool("", [true, false, false, false]).1.as_ref()
	);
	assert_eq!(
		compare::<GreaterThan>(left(), right()).as_ref(),
		factory::bool("", [false, true, false, false]).1.as_ref()
	);
}

#[test]
fn int16_compare_against_int8_on_either_side() {
	// i128 extremes lie outside i64, so a narrowing read on either side would make them equal to the i64 bounds.
	let wide = || factory::int16("", [i128::MIN, i128::MAX, i64::MAX as i128]);
	let narrow = || factory::int8("", [i64::MIN, i64::MAX, i64::MAX]);

	assert_eq!(compare::<LessThan>(wide(), narrow()).as_ref(), factory::bool("", [true, false, false]).1.as_ref());
	assert_eq!(
		compare::<GreaterThan>(wide(), narrow()).as_ref(),
		factory::bool("", [false, true, false]).1.as_ref()
	);
	assert_eq!(compare::<Equal>(narrow(), wide()).as_ref(), factory::bool("", [false, false, true]).1.as_ref());
	assert_eq!(
		compare::<GreaterThan>(narrow(), wide()).as_ref(),
		factory::bool("", [true, false, false]).1.as_ref()
	);
}

#[test]
fn int16_add_keeps_i128_min_and_max_exact() {
	// A narrowed read or a result built with a default decimal type would change these sums.
	let sum =
		add(factory::int16("", [i128::MIN, i128::MAX, i128::MIN + 1]), factory::int16("", [0, 0, -1])).unwrap();
	assert_eq!(int16_rows(&sum), [i128::MIN, i128::MAX, i128::MIN]);

	let mixed = add(factory::int8("", [i64::MAX]), factory::int16("", [i128::MAX - i64::MAX as i128])).unwrap();
	assert_eq!(int16_rows(&mixed), [i128::MAX]);
}

#[test]
fn int16_add_past_i128_bounds_is_an_error() {
	// Wrapping or saturating at either i128 bound would hand back a wrong sum instead of the range error.
	let above = add(factory::int16("", [i128::MAX]), factory::int16("", [1])).unwrap_err();
	assert_eq!(above.0.code, "NUMBER_002");

	let below = add(factory::int16("", [i128::MIN]), factory::int16("", [-1])).unwrap_err();
	assert_eq!(below.0.code, "NUMBER_002");
}

#[test]
fn int16_prefix_plus_keeps_i128_min_and_max() {
	// The prefix result is rebuilt from values, so it must come back with the exact (38, 0) type.
	let result =
		prefix(factory::int16("", [i128::MIN, i128::MAX]), PrefixOperator::Plus(Fragment::testing_empty()));

	assert_eq!(int16_rows(&result), [i128::MIN, i128::MAX]);
}

#[test]
fn uint16_prefix_keeps_bits_above_64() {
	// A lossy read of the Uint16 rows would drop every bit above 64; unary plus keeps the unsigned type.
	let plus = prefix(
		factory::uint16("", [TWO_POW_64, (1 << 127) - 1]),
		PrefixOperator::Plus(Fragment::testing_empty()),
	);
	assert_eq!(uint16_rows(&plus), [TWO_POW_64, (1 << 127) - 1]);

	let minus = prefix(factory::uint16("", [TWO_POW_64]), PrefixOperator::Minus(Fragment::testing_empty()));
	assert_eq!(int16_rows(&minus), [-(1 << 64)]);
}
