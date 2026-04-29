// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::buffer::ColumnBuffer;
use reifydb_type::value::int::Int;

use super::common::{assert_column_eq, round_trip_column};

#[test]
fn bigint_zero() {
	let input = ColumnBuffer::int([Int::zero()]);
	let output = round_trip_column("i", input.clone());
	assert_column_eq("bigint_zero", &input, &output);
}

#[test]
fn bigint_small_positive() {
	let input = ColumnBuffer::int([Int::from_i64(1), Int::from_i64(42), Int::from_i64(-1)]);
	let output = round_trip_column("i", input.clone());
	assert_column_eq("bigint_small", &input, &output);
}

#[test]
fn bigint_i128_max_min() {
	let input = ColumnBuffer::int([Int::from_i128(i128::MIN), Int::from_i128(i128::MAX)]);
	let output = round_trip_column("i", input.clone());
	assert_column_eq("bigint_i128_extremes", &input, &output);
}

#[test]
fn bigint_outside_i128_range() {
	// 2 * i128::MAX exceeds any fixed-width integer; the BigInt
	// representation must round-trip through the postcard-fallback path.
	let mut big: Int = Int::from_i128(i128::MAX);
	big.0 += Int::from_i128(i128::MAX).0;
	let mut neg_big: Int = Int::from_i128(i128::MIN);
	neg_big.0 += Int::from_i128(i128::MIN).0;
	let input = ColumnBuffer::int([big, neg_big]);
	let output = round_trip_column("i", input.clone());
	assert_column_eq("bigint_outside_i128", &input, &output);
}

#[test]
fn bigint_thirty_two_rows() {
	let values: Vec<Int> = (0..32i64).map(Int::from_i64).collect();
	let input = ColumnBuffer::int(values);
	let output = round_trip_column("i", input.clone());
	assert_column_eq("bigint_thirty_two", &input, &output);
}

#[test]
fn bigint_with_undefined() {
	let input =
		ColumnBuffer::int_optional([Some(Int::from_i64(7)), None, Some(Int::zero()), None, Some(Int::one())]);
	let output = round_trip_column("i", input.clone());
	assert_column_eq("bigint_with_undefined", &input, &output);
}
