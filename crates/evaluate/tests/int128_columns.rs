// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer};
use reifydb_evaluate::expression::{
	arith::add::add_columns,
	compare::{CompareOp, Equal, GreaterThan, LessThan, NotEqual, compare_columns},
	context::EvalContext,
	prefix::prefix_apply,
};
use reifydb_rql::expression::PrefixOperator;
use reifydb_value::{
	Result,
	error::Diagnostic,
	fragment::Fragment,
	value::{constraint::precision::Precision, container::decimal_array::u128s, uint::Uint, value_type::ValueType},
};

const TWO_POW_64: u128 = 1 << 64;

fn compare<Op: CompareOp>(left: ColumnBuffer, right: ColumnBuffer) -> ColumnBuffer {
	// A missing dispatch arm must panic here, never come back as an ordinary type error.
	compare_columns::<Op>(
		&ColumnWithName::new("left", left),
		&ColumnWithName::new("right", right),
		Fragment::testing_empty(),
		|_, l: ValueType, r: ValueType| -> Diagnostic { panic!("no comparison between {l:?} and {r:?}") },
	)
	.unwrap()
	.data
}

fn add(left: ColumnBuffer, right: ColumnBuffer) -> Result<ColumnBuffer> {
	let ctx = EvalContext::testing();
	let fragment = Fragment::testing_empty();
	add_columns(&ctx, &ColumnWithName::new("left", left), &ColumnWithName::new("right", right), &fragment)
		.map(|column| column.data)
}

fn prefix(column: ColumnBuffer, operator: PrefixOperator) -> ColumnBuffer {
	prefix_apply(&ColumnWithName::new("column", column), &operator, &Fragment::testing_empty()).unwrap().data
}

fn uint16_rows(column: &ColumnBuffer) -> Vec<u128> {
	// Arrow's default decimal type (76, 10) would read every row 10^10 times too small on export.
	let ColumnBuffer::Uint16(array) = column else {
		panic!("expected a Uint16 column, got {:?}", column.get_type());
	};
	assert_eq!((array.precision(), array.scale()), (39, 0));
	u128s(array)
}

fn int16_rows(column: &ColumnBuffer) -> Vec<i128> {
	// Arrow's default decimal type (38, 10) would read every row 10^10 times too small on export.
	let ColumnBuffer::Int16(array) = column else {
		panic!("expected an Int16 column, got {:?}", column.get_type());
	};
	assert_eq!((array.precision(), array.scale()), (38, 0));
	array.values().to_vec()
}

#[test]
fn uint16_compare_orders_rows_at_2_pow_64_and_u128_max() {
	// A lossy i256 read drops the bits above 64, so 2^64 and 2^64 + 1 would compare equal.
	let left = || ColumnBuffer::uint16([u128::MAX, TWO_POW_64, TWO_POW_64 + 1, u128::MAX - 1]);
	let right = || ColumnBuffer::uint16([u128::MAX, TWO_POW_64 + 1, TWO_POW_64, u128::MAX]);

	assert_eq!(compare::<Equal>(left(), right()), ColumnBuffer::bool([true, false, false, false]));
	assert_eq!(compare::<NotEqual>(left(), right()), ColumnBuffer::bool([false, true, true, true]));
	assert_eq!(compare::<LessThan>(left(), right()), ColumnBuffer::bool([false, true, false, true]));
	assert_eq!(compare::<GreaterThan>(left(), right()), ColumnBuffer::bool([false, false, true, false]));
}

#[test]
fn uint16_compare_against_uint8_on_either_side() {
	// The left and right Uint16 reads are separate dispatch paths; either one losing the high bits breaks here.
	let wide = || ColumnBuffer::uint16([TWO_POW_64, u64::MAX as u128, u128::MAX]);
	let narrow = || ColumnBuffer::uint8([u64::MAX, u64::MAX, 0]);

	assert_eq!(compare::<GreaterThan>(wide(), narrow()), ColumnBuffer::bool([true, false, true]));
	assert_eq!(compare::<Equal>(wide(), narrow()), ColumnBuffer::bool([false, true, false]));
	assert_eq!(compare::<LessThan>(narrow(), wide()), ColumnBuffer::bool([true, false, true]));
	assert_eq!(compare::<Equal>(narrow(), wide()), ColumnBuffer::bool([false, true, false]));
}

#[test]
fn uint16_compare_against_arbitrary_uint_on_either_side() {
	// The arbitrary precision arms read Uint16 rows on their own; a lossy read there shrinks u128::MAX.
	let wide = || ColumnBuffer::uint16([u128::MAX, TWO_POW_64, TWO_POW_64]);
	let big = || {
		ColumnBuffer::uint(
			Precision::MAX,
			[Uint::from(u128::MAX), Uint::from(TWO_POW_64 - 1), Uint::from(TWO_POW_64)],
		)
	};

	assert_eq!(compare::<Equal>(wide(), big()), ColumnBuffer::bool([true, false, true]));
	assert_eq!(compare::<GreaterThan>(wide(), big()), ColumnBuffer::bool([false, true, false]));
	assert_eq!(compare::<Equal>(big(), wide()), ColumnBuffer::bool([true, false, true]));
	assert_eq!(compare::<LessThan>(big(), wide()), ColumnBuffer::bool([false, true, false]));
}

#[test]
fn uint16_add_keeps_u128_max_and_2_pow_64_exact() {
	// A lossy i256 read or a result built with a default decimal type would change these sums.
	let sum = add(
		ColumnBuffer::uint16([u128::MAX - 1, TWO_POW_64, TWO_POW_64 - 1]),
		ColumnBuffer::uint16([1, TWO_POW_64, 1]),
	)
	.unwrap();

	assert_eq!(uint16_rows(&sum), [u128::MAX, 1 << 65, TWO_POW_64]);
}

#[test]
fn uint16_add_with_uint8_on_either_side() {
	// Each side of the dispatch reads Uint16 rows separately; the result must still be a (39, 0) Uint16 column.
	let wide = || ColumnBuffer::uint16([u128::MAX - u64::MAX as u128, TWO_POW_64]);
	let narrow = || ColumnBuffer::uint8([u64::MAX, 1]);

	assert_eq!(uint16_rows(&add(wide(), narrow()).unwrap()), [u128::MAX, TWO_POW_64 + 1]);
	assert_eq!(uint16_rows(&add(narrow(), wide()).unwrap()), [u128::MAX, TWO_POW_64 + 1]);
}

#[test]
fn uint16_add_past_u128_max_is_an_error() {
	// Wrapping or saturating on overflow would hand back a wrong sum instead of the range error.
	let err = add(ColumnBuffer::uint16([u128::MAX]), ColumnBuffer::uint16([1])).unwrap_err();

	assert_eq!(err.0.code, "NUMBER_002");
}

#[test]
fn int16_compare_orders_i128_min_and_max() {
	// Any narrowing of the signed 128 bit rows would flip or equalise these extremes.
	let left = || ColumnBuffer::int16([i128::MIN, i128::MAX, i128::MIN, i128::MAX]);
	let right = || ColumnBuffer::int16([i128::MAX, i128::MIN, i128::MIN, i128::MAX]);

	assert_eq!(compare::<Equal>(left(), right()), ColumnBuffer::bool([false, false, true, true]));
	assert_eq!(compare::<LessThan>(left(), right()), ColumnBuffer::bool([true, false, false, false]));
	assert_eq!(compare::<GreaterThan>(left(), right()), ColumnBuffer::bool([false, true, false, false]));
}

#[test]
fn int16_compare_against_int8_on_either_side() {
	// i128 extremes lie outside i64, so a narrowing read on either side would make them equal to the i64 bounds.
	let wide = || ColumnBuffer::int16([i128::MIN, i128::MAX, i64::MAX as i128]);
	let narrow = || ColumnBuffer::int8([i64::MIN, i64::MAX, i64::MAX]);

	assert_eq!(compare::<LessThan>(wide(), narrow()), ColumnBuffer::bool([true, false, false]));
	assert_eq!(compare::<GreaterThan>(wide(), narrow()), ColumnBuffer::bool([false, true, false]));
	assert_eq!(compare::<Equal>(narrow(), wide()), ColumnBuffer::bool([false, false, true]));
	assert_eq!(compare::<GreaterThan>(narrow(), wide()), ColumnBuffer::bool([true, false, false]));
}

#[test]
fn int16_add_keeps_i128_min_and_max_exact() {
	// A narrowed read or a result built with a default decimal type would change these sums.
	let sum = add(ColumnBuffer::int16([i128::MIN, i128::MAX, i128::MIN + 1]), ColumnBuffer::int16([0, 0, -1]))
		.unwrap();
	assert_eq!(int16_rows(&sum), [i128::MIN, i128::MAX, i128::MIN]);

	let mixed = add(ColumnBuffer::int8([i64::MAX]), ColumnBuffer::int16([i128::MAX - i64::MAX as i128])).unwrap();
	assert_eq!(int16_rows(&mixed), [i128::MAX]);
}

#[test]
fn int16_add_past_i128_bounds_is_an_error() {
	// Wrapping or saturating at either i128 bound would hand back a wrong sum instead of the range error.
	let above = add(ColumnBuffer::int16([i128::MAX]), ColumnBuffer::int16([1])).unwrap_err();
	assert_eq!(above.0.code, "NUMBER_002");

	let below = add(ColumnBuffer::int16([i128::MIN]), ColumnBuffer::int16([-1])).unwrap_err();
	assert_eq!(below.0.code, "NUMBER_002");
}

#[test]
fn int16_prefix_plus_keeps_i128_min_and_max() {
	// The prefix result is rebuilt from values, so it must come back with the exact (38, 0) type.
	let result =
		prefix(ColumnBuffer::int16([i128::MIN, i128::MAX]), PrefixOperator::Plus(Fragment::testing_empty()));

	assert_eq!(int16_rows(&result), [i128::MIN, i128::MAX]);
}

#[test]
fn uint16_prefix_keeps_bits_above_64() {
	// A lossy read of the Uint16 rows would drop every bit above 64; unary plus keeps the unsigned type.
	let plus = prefix(
		ColumnBuffer::uint16([TWO_POW_64, (1 << 127) - 1]),
		PrefixOperator::Plus(Fragment::testing_empty()),
	);
	assert_eq!(uint16_rows(&plus), [TWO_POW_64, (1 << 127) - 1]);

	let minus = prefix(ColumnBuffer::uint16([TWO_POW_64]), PrefixOperator::Minus(Fragment::testing_empty()));
	assert_eq!(int16_rows(&minus), [-(1 << 64)]);
}
