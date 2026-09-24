// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::str::FromStr;

use arrow_array::Array;
use reifydb_core::{
	interface::evaluate::TargetColumn,
	value::column::{ColumnWithName, buffer::ColumnBuffer},
};
use reifydb_evaluate::expression::{
	arith::{add::add_columns, div::div_columns, mul::mul_columns, rem::rem_columns, sub::sub_columns},
	compare::{CompareOp, Equal, GreaterThan, LessThan, compare_columns},
	context::EvalContext,
	eval::evaluate,
	prefix::prefix_apply,
};
use reifydb_rql::expression::{CastExpression, ConstantExpression, Expression, PrefixOperator, TypeExpression};
use reifydb_value::{
	Result,
	error::Diagnostic,
	fragment::Fragment,
	value::{
		constraint::{precision::Precision, scale::Scale},
		container::decimal_array::{decimals, ints},
		decimal::Decimal,
		int::Int,
		uint::Uint,
		value_type::ValueType,
	},
};

fn compare<Op: CompareOp>(left: ColumnBuffer, right: ColumnBuffer) -> Vec<Option<bool>> {
	// A pair that should compare must panic here, never come back as an ordinary type error.
	let column = compare_columns::<Op>(
		&ColumnWithName::new("left", left),
		&ColumnWithName::new("right", right),
		Fragment::testing_empty(),
		|_, l: ValueType, r: ValueType| -> Diagnostic { panic!("no comparison between {l:?} and {r:?}") },
	)
	.unwrap()
	.data;
	let ColumnBuffer::Bool(a) = column else {
		panic!("comparison must return a bool column, got {:?}", column.get_type())
	};
	(0..a.len()).map(|i| (!a.is_null(i)).then(|| a.value(i))).collect()
}

fn dec(text: &str) -> Decimal {
	Decimal::from_str(text).unwrap()
}

fn decimal(precision: u8, scale: u8, values: &[&str]) -> ColumnBuffer {
	ColumnBuffer::decimal(Precision::new(precision), Scale::new(scale), values.iter().map(|text| dec(text)))
}

macro_rules! arith {
	($op:ident, $left:expr, $right:expr) => {{
		let ctx = EvalContext::testing();
		let fragment = Fragment::testing_empty();
		$op(&ctx, &ColumnWithName::new("left", $left), &ColumnWithName::new("right", $right), &fragment)
			.map(|column| column.data)
	}};
}

fn literal(text: &str) -> Result<ColumnBuffer> {
	let ctx = EvalContext::testing();
	let expr = Expression::Constant(ConstantExpression::Number {
		fragment: Fragment::internal(text),
	});
	evaluate(&ctx, &expr).map(|column| column.data)
}

fn decimal_strings(column: &ColumnBuffer) -> Vec<String> {
	let ColumnBuffer::Decimal(array) = column else {
		panic!("expected a decimal column, got {:?}", column.get_type())
	};
	decimals(array).iter().map(ToString::to_string).collect()
}

#[test]
fn decimal_compares_against_int4_through_the_kernel() {
	// The rescale to the common scale must keep 1.50 above 1 and 3.00 equal to 3, on both sides.
	let left = || decimal(10, 2, &["1.50", "-2.25", "3.00", "12345678.99"]);
	let right = || ColumnBuffer::int4([1, -2, 3, 12345679]);

	assert_eq!(compare::<GreaterThan>(left(), right()), [Some(true), Some(false), Some(false), Some(false)]);
	assert_eq!(compare::<Equal>(left(), right()), [Some(false), Some(false), Some(true), Some(false)]);
	assert_eq!(compare::<LessThan>(right(), left()), [Some(true), Some(false), Some(false), Some(false)]);
}

#[test]
fn decimal_compares_against_uint8_including_u64_max() {
	// u64::MAX has 20 digits, so a target narrower than decimal(22, 2) would wrap it negative.
	let left = || decimal(10, 2, &["1.50", "0.00", "3.00", "99999999.99"]);
	let right = || ColumnBuffer::uint8([1, 0, 3, u64::MAX]);

	assert_eq!(compare::<GreaterThan>(left(), right()), [Some(true), Some(false), Some(false), Some(false)]);
	assert_eq!(compare::<Equal>(left(), right()), [Some(false), Some(true), Some(true), Some(false)]);
	assert_eq!(compare::<LessThan>(left(), right()), [Some(false), Some(false), Some(false), Some(true)]);
}

#[test]
fn decimal_compares_against_float8() {
	// The unscaled value must be divided by 10^scale, otherwise 1.50 would read as 150.0.
	let left = || decimal(10, 2, &["1.50", "-2.25", "3.00", "0.10"]);
	let right = || ColumnBuffer::float8([1.5, -2.25, 2.99, 0.1]);

	assert_eq!(compare::<Equal>(left(), right()), [Some(true), Some(true), Some(false), Some(true)]);
	assert_eq!(compare::<GreaterThan>(left(), right()), [Some(false), Some(false), Some(true), Some(false)]);
}

#[test]
fn decimal_against_one_row_int_goes_through_the_scalar_path() {
	// A one-row side must broadcast, not fail on a length mismatch or compare only the first row.
	let result = compare::<LessThan>(decimal(10, 2, &["0.99", "1.00", "1.01"]), ColumnBuffer::int1([1]));
	assert_eq!(result, [Some(true), Some(false), Some(false)]);
}

#[test]
fn equal_values_of_different_scale_compare_equal() {
	// N2: 1.5 and 1.50 are one value; comparing raw unscaled 15 against 150 would say they differ.
	let left = || decimal(2, 1, &["1.5", "1.5", "-0.5"]);
	let right = || decimal(5, 2, &["1.50", "1.51", "-0.50"]);

	assert_eq!(compare::<Equal>(left(), right()), [Some(true), Some(false), Some(true)]);
	assert_eq!(compare::<LessThan>(left(), right()), [Some(false), Some(true), Some(false)]);
}

#[test]
fn decimal128_against_decimal256_columns() {
	// The two widths must meet in one storage type; mismatched arrow types would make the kernel fail.
	let narrow = decimal(10, 2, &["1.25", "-7.00"]);
	let wide = decimal(60, 4, &["1.2500", "123456789012345678901234567890123456789012345678901.0000"]);

	assert_eq!(compare::<Equal>(narrow, wide), [Some(true), Some(false)]);
}

#[test]
fn a_rescale_past_76_digits_still_orders_correctly() {
	// int(76) values near 10^75 cannot rescale to scale 5; they must stay above or below every decimal(10, 5).
	let big = Int::MAX;
	let ints = || ColumnBuffer::int(Precision::MAX, [big.clone(), big.negate(), Int::from(3i64)]);
	let decimals = || decimal(10, 5, &["99999.99999", "-99999.99999", "3.00000"]);

	assert_eq!(compare::<GreaterThan>(ints(), decimals()), [Some(true), Some(false), Some(false)]);
	assert_eq!(compare::<LessThan>(ints(), decimals()), [Some(false), Some(true), Some(false)]);
	assert_eq!(compare::<Equal>(ints(), decimals()), [Some(false), Some(false), Some(true)]);
}

#[test]
fn uint_family_against_int_family() {
	// Signed and unsigned family columns share one signed storage; a negative int must stay below any uint.
	let uints = ColumnBuffer::uint(Precision::new(5), [Uint::from(0u64), Uint::from(7u64)]);
	let ints = ColumnBuffer::int(Precision::new(3), [Int::from(-1i64), Int::from(7i64)]);

	assert_eq!(compare::<GreaterThan>(uints, ints), [Some(true), Some(false)]);
}

#[test]
fn decimal_against_text_is_a_type_error() {
	// Only numbers join the family target; text must not be cast into a decimal array.
	let result = compare_columns::<Equal>(
		&ColumnWithName::new("left", decimal(10, 2, &["1.00"])),
		&ColumnWithName::new("right", ColumnBuffer::utf8(["1.00"])),
		Fragment::testing_empty(),
		|_, _, _| Diagnostic {
			code: "TEST".to_string(),
			..Default::default()
		},
	);
	assert!(result.is_err(), "decimal vs utf8 must be a type error, got {result:?}");
}

#[test]
fn arithmetic_result_types_follow_the_fixed_rules() {
	// add/sub: max scale and one more digit; mul: both add; div: scale at least 6; rem: the narrower integer part.
	let left = || decimal(10, 2, &["12.34"]);
	let right = || decimal(5, 3, &["1.500"]);

	let cases = [
		(arith!(add_columns, left(), right()), ValueType::decimal(Precision::new(12), Scale::new(3)), "13.840"),
		(arith!(sub_columns, left(), right()), ValueType::decimal(Precision::new(12), Scale::new(3)), "10.840"),
		(
			arith!(mul_columns, left(), right()),
			ValueType::decimal(Precision::new(15), Scale::new(5)),
			"18.51000",
		),
		(
			arith!(div_columns, left(), right()),
			ValueType::decimal(Precision::new(17), Scale::new(6)),
			"8.226667",
		),
		(arith!(rem_columns, left(), right()), ValueType::decimal(Precision::new(5), Scale::new(3)), "0.340"),
	];
	for (result, ty, text) in cases {
		let result: ColumnBuffer = result.unwrap();
		assert_eq!(result.get_type(), ty);
		assert_eq!(decimal_strings(&result), [text]);
	}
}

#[test]
fn integer_family_arithmetic_keeps_its_kind_and_precision() {
	// int(p) with int4 counts int4 as int(10); the sum needs 11 digits and stays an int, not a decimal.
	let left = || ColumnBuffer::int(Precision::new(8), [Int::from(99_999_999i64)]);
	let sum = arith!(add_columns, left(), ColumnBuffer::int4([i32::MAX])).unwrap();
	assert_eq!(sum.get_type(), ValueType::int(Precision::new(11)));

	let product = arith!(mul_columns, left(), ColumnBuffer::int4([2])).unwrap();
	assert_eq!(product.get_type(), ValueType::int(Precision::new(18)));
	let ColumnBuffer::Int(array) = &product else {
		panic!("expected an int column, got {:?}", product.get_type())
	};
	assert_eq!(ints(array), [Int::from(199_999_998i64)]);
}

#[test]
fn uint_one_minus_two_is_a_range_error_by_default() {
	// N5: a uint below 0 follows the saturation policy, which defaults to an error, never a wrap.
	let one = ColumnBuffer::uint(Precision::new(3), [Uint::from(1u64)]);
	let two = ColumnBuffer::uint(Precision::new(3), [Uint::from(2u64)]);
	let err = arith!(sub_columns, one, two).unwrap_err();
	assert_eq!(err.0.code, "NUMBER_002");
}

#[test]
fn a_product_whose_scales_add_past_76_digits_keeps_six_fraction_digits() {
	// Scale 40 + 40 leaves no whole digit, so 1.5 * 1.5 overflowed although 2.25 needs only one.
	let wide = || decimal(76, 40, &["1.5"]);
	let product: ColumnBuffer = arith!(mul_columns, wide(), wide()).unwrap();
	assert_eq!(product.get_type(), ValueType::decimal(Precision::new(76), Scale::new(6)));
	assert_eq!(decimal_strings(&product), ["2.250000"]);
}

#[test]
fn a_product_trims_its_scale_only_down_to_what_the_whole_digits_leave() {
	// Whole digits 30 + 10 leave 36 fraction digits, so trimming to 6 would round away digits that still fit.
	let product: ColumnBuffer =
		arith!(mul_columns, decimal(60, 30, &["1.5"]), decimal(30, 20, &["1.5"])).unwrap();
	assert_eq!(product.get_type(), ValueType::decimal(Precision::new(76), Scale::new(36)));
	assert_eq!(decimal_strings(&product), [format!("2.25{}", "0".repeat(34))]);
}

#[test]
fn a_product_past_76_digits_is_a_range_error() {
	// P2-Q3: two 40 digit ints multiply past 76 digits; the result must fail, not saturate or wrap.
	let big = || ColumnBuffer::int(Precision::new(40), [Int::from_str(&"9".repeat(40)).unwrap()]);
	let err = arith!(mul_columns, big(), big()).unwrap_err();
	assert_eq!(err.0.code, "NUMBER_002");
}

#[test]
fn prefix_minus_keeps_precision_and_scale() {
	// Negation must not fall back to a default decimal(76, 10) or drop the column scale.
	let result = prefix_apply(
		&ColumnWithName::new("column", decimal(10, 2, &["1.50", "-0.25"])),
		&PrefixOperator::Minus(Fragment::testing_empty()),
		&Fragment::testing_empty(),
	)
	.unwrap()
	.data;
	assert_eq!(result.get_type(), ValueType::decimal(Precision::new(10), Scale::new(2)));
	assert_eq!(decimal_strings(&result), ["-1.50", "0.25"]);
}

#[test]
fn prefix_minus_on_uint_gives_an_int_of_the_same_precision() {
	// A uint negated must become a signed int of the same width, never wrap to a huge uint.
	let column = ColumnBuffer::uint(Precision::new(20), [Uint::from(u64::MAX)]);
	let result = prefix_apply(
		&ColumnWithName::new("column", column),
		&PrefixOperator::Minus(Fragment::testing_empty()),
		&Fragment::testing_empty(),
	)
	.unwrap()
	.data;
	assert_eq!(result.get_type(), ValueType::int(Precision::new(20)));
	let ColumnBuffer::Int(array) = &result else {
		panic!("expected an int column, got {:?}", result.get_type())
	};
	assert_eq!(ints(array), [Int::from(u64::MAX).negate()]);
}

#[test]
fn a_decimal_literal_takes_precision_and_scale_from_its_text() {
	// N20: the text decides the type; 1.50 keeps scale 2 and 0.05 needs precision 2 to hold two fraction digits.
	let cases = [
		("1.50", ValueType::decimal(Precision::new(3), Scale::new(2))),
		("0.05", ValueType::decimal(Precision::new(2), Scale::new(2))),
		("123.4", ValueType::decimal(Precision::new(4), Scale::new(1))),
	];
	for (text, ty) in cases {
		assert_eq!(literal(text).unwrap().get_type(), ty, "literal {text}");
	}
	assert!(matches!(literal("1e3").unwrap(), ColumnBuffer::Decimal(_)));
	assert!(matches!(literal("1E3").unwrap(), ColumnBuffer::Decimal(_)));
}

#[test]
fn an_integer_literal_takes_the_narrowest_type_then_u128_then_int() {
	// N20: i8 .. i128, then u128, then int; a negative past i128 must become an int, not an error.
	assert_eq!(literal("127").unwrap().get_type(), ValueType::Int1);
	assert_eq!(literal("170141183460469231731687303715884105727").unwrap().get_type(), ValueType::Int16);
	assert_eq!(literal("170141183460469231731687303715884105728").unwrap().get_type(), ValueType::Uint16);
	assert_eq!(literal("340282366920938463463374607431768211456").unwrap().get_type(), ValueType::INT);
	assert_eq!(literal("-170141183460469231731687303715884105729").unwrap().get_type(), ValueType::INT);
}

#[test]
fn a_77_digit_literal_is_a_range_error() {
	// P2-Q3: past 76 digits there is no family type; the literal must fail instead of saturating.
	let err = literal(&"1".repeat(77)).unwrap_err();
	assert_eq!(err.0.code, "NUMBER_002");
	let err = literal(&format!("{}.5", "1".repeat(76))).unwrap_err();
	assert_eq!(err.0.code, "NUMBER_002");
}

fn written(expr: Expression, column_type: ValueType) -> Result<ColumnBuffer> {
	let mut ctx = EvalContext::testing();
	ctx.target = Some(TargetColumn::Partial {
		source_name: None,
		column_name: None,
		column_type,
		properties: vec![],
	});
	evaluate(&ctx, &expr).map(|column| column.data)
}

fn number(text: &str) -> Expression {
	Expression::Constant(ConstantExpression::Number {
		fragment: Fragment::internal(text),
	})
}

#[test]
fn a_write_refuses_fraction_digits_the_column_scale_would_drop() {
	// O1b: every write through evaluate must refuse 1.235 into decimal(10, 2), never round it to 1.24.
	let target = ValueType::decimal(Precision::new(10), Scale::new(2));

	let err = written(number("1.235"), target.clone()).unwrap_err();
	assert_eq!(err.0.code, "CONSTRAINT_006", "got: {err:?}");

	let exact = written(number("1.230"), target.clone()).unwrap();
	assert_eq!(exact.get_type(), target);
	assert_eq!(decimal_strings(&exact), vec!["1.23"]);
}

#[test]
fn an_explicit_cast_still_rounds_before_the_write() {
	// O1c: a cast rounds half up, so the write sees scale 2 and has nothing to refuse.
	let target = ValueType::decimal(Precision::new(10), Scale::new(2));
	let cast = Expression::Cast(CastExpression {
		fragment: Fragment::testing_empty(),
		expression: Box::new(number("1.235")),
		to: TypeExpression {
			fragment: Fragment::testing_empty(),
			ty: target.clone(),
		},
	});

	let column = written(cast, target).unwrap();
	assert_eq!(decimal_strings(&column), vec!["1.24"]);
}
