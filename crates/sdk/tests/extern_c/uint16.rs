// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::Array;
use reifydb_core::value::column::buffer::ColumnBuffer;
use reifydb_value::value::container::decimal_array::{UINT16_DATA_TYPE, u128s};

use super::common::{assert_column_eq, round_trip_column};

#[test]
fn uint16_zero() {
	let input = ColumnBuffer::uint16([0u128]);
	let output = round_trip_column("u", input.clone());
	assert_column_eq("uint16_zero", &input, &output);
}

#[test]
fn uint16_max() {
	let input = ColumnBuffer::uint16([u128::MAX]);
	let output = round_trip_column("u", input.clone());
	assert_column_eq("uint16_max", &input, &output);
}

#[test]
fn uint16_high_low_word_witness() {
	let v: u128 = 0x0102_0304_0506_0708_090A_0B0C_0D0E_0F10u128;
	let input = ColumnBuffer::uint16([v, !v]);
	let output = round_trip_column("u", input.clone());
	assert_column_eq("uint16_high_low", &input, &output);
}

#[test]
fn uint16_thirty_two_rows() {
	let values: Vec<u128> = (0..32u128).map(|i| i << 80).collect();
	let input = ColumnBuffer::uint16(values);
	let output = round_trip_column("u", input.clone());
	assert_column_eq("uint16_thirty_two_rows", &input, &output);
}

#[test]
fn uint16_with_undefined() {
	let input = ColumnBuffer::uint16_with_bitvec(
		[0u128, 0, 1u128 << 100, 0, u128::MAX],
		vec![true, false, true, false, true],
	);
	let output = round_trip_column("u", input.clone());
	assert_column_eq("uint16_with_undefined", &input, &output);
}

#[test]
fn uint16_past_64_bits_come_back_with_the_uint16_data_type() {
	// Any hop that narrows the 256 bit native through a 64 bit conversion loses 2^64 and u128::MAX.
	let values = [1u128 << 64, u128::MAX, (1u128 << 64) - 1, 1u128 << 127];
	let input = ColumnBuffer::uint16(values);
	let output = round_trip_column("u", input.clone());
	assert_column_eq("uint16_past_64_bits", &input, &output);
	let ColumnBuffer::Uint16(array) = &output else {
		panic!("expected a plain Uint16 column, got {:?}", output.get_type())
	};
	assert_eq!(array.data_type(), &UINT16_DATA_TYPE);
	assert_eq!(u128s(array), values);
}

#[test]
fn sliced_uint16_column_reaches_the_guest_from_the_slice_start() {
	// Copying from the parent buffer start instead of the slice offset hands the guest the wrong rows.
	let parent = ColumnBuffer::uint16([7, 1u128 << 64, u128::MAX, 9]);
	let output = round_trip_column("u", parent.slice(1, 3));
	assert_column_eq("sliced_uint16", &ColumnBuffer::uint16([1u128 << 64, u128::MAX]), &output);
}
