// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::buffer::ColumnBuffer;
use reifydb_type::value::uint::Uint;

use super::common::{assert_column_eq, round_trip_column};

#[test]
fn biguint_zero() {
	let input = ColumnBuffer::uint([Uint::zero()]);
	let output = round_trip_column("u", input.clone());
	assert_column_eq("biguint_zero", &input, &output);
}

#[test]
fn biguint_small() {
	let input = ColumnBuffer::uint([Uint::from_u64(1), Uint::from_u64(42), Uint::from_u64(u64::MAX)]);
	let output = round_trip_column("u", input.clone());
	assert_column_eq("biguint_small", &input, &output);
}

#[test]
fn biguint_u128_max() {
	let input = ColumnBuffer::uint([Uint::from_u128(u128::MAX)]);
	let output = round_trip_column("u", input.clone());
	assert_column_eq("biguint_u128_max", &input, &output);
}

#[test]
fn biguint_outside_u128_range() {
	let mut big: Uint = Uint::from_u128(u128::MAX);
	big.0 += Uint::from_u128(u128::MAX).0;
	let input = ColumnBuffer::uint([big]);
	let output = round_trip_column("u", input.clone());
	assert_column_eq("biguint_outside_u128", &input, &output);
}

#[test]
fn biguint_thirty_two_rows() {
	let values: Vec<Uint> = (0..32u64).map(Uint::from_u64).collect();
	let input = ColumnBuffer::uint(values);
	let output = round_trip_column("u", input.clone());
	assert_column_eq("biguint_thirty_two", &input, &output);
}

#[test]
fn biguint_with_undefined() {
	let input = ColumnBuffer::uint_optional([
		Some(Uint::from_u64(7)),
		None,
		Some(Uint::zero()),
		None,
		Some(Uint::one()),
	]);
	let output = round_trip_column("u", input.clone());
	assert_column_eq("biguint_with_undefined", &input, &output);
}
