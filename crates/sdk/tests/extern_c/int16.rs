// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::value::column::factory;
use reifydb_value::value::{
	column_view::{ColumnView, ViewData},
	container::wide_int_array::wides,
};

use super::common::{assert_column_eq, round_trip_column, slice};

#[test]
fn int16_zero() {
	let input = factory::int16("c", [0i128]);
	let output = round_trip_column("i", input.clone());
	assert_column_eq("int16_zero", &input, &output);
}

#[test]
fn int16_min_max() {
	let input = factory::int16("c", [i128::MIN, i128::MAX]);
	let output = round_trip_column("i", input.clone());
	assert_column_eq("int16_min_max", &input, &output);
}

#[test]
fn int16_high_low_word_witness() {
	// Distinct bytes in every 8-byte half catch any 8-byte stride bug.
	let v: i128 = (0x0102_0304_0506_0708i128) | ((0x090A_0B0C_0D0E_0F10i128) << 64);
	let input = factory::int16("c", [v, -v]);
	let output = round_trip_column("i", input.clone());
	assert_column_eq("int16_high_low", &input, &output);
}

#[test]
fn int16_thirty_two_rows() {
	let values: Vec<i128> = (0..32).map(|i| (i as i128) << 80).collect();
	let input = factory::int16("c", values);
	let output = round_trip_column("i", input.clone());
	assert_column_eq("int16_thirty_two_rows", &input, &output);
}

#[test]
fn int16_with_undefined() {
	let input = factory::int16_with_bitvec(
		"c",
		[i128::MIN, 0, 0i128, 0, i128::MAX],
		vec![true, false, true, false, true],
	);
	let output = round_trip_column("i", input.clone());
	assert_column_eq("int16_with_undefined", &input, &output);
}

#[test]
fn int16_min_max_come_back_with_the_int16_data_type() {
	// A guest-built column that skips the ordered row encoding reads every value wrong at export.
	let input = factory::int16("c", [i128::MIN, i128::MAX]);
	let output = round_trip_column("i", input.clone());
	assert_column_eq("int16_min_max_data_type", &input, &output);
	let view = ColumnView::try_from(&output).unwrap();
	let ViewData::Int16(array) = view.data else {
		panic!("expected a plain Int16 column, got {:?}", view.get_type())
	};
	assert_eq!(wides::<i128>(array), [i128::MIN, i128::MAX]);
}

#[test]
fn sliced_int16_column_reaches_the_guest_from_the_slice_start() {
	// Borrowing from the parent buffer start instead of the slice offset hands the guest the wrong rows.
	let parent = factory::int16("c", [7, i128::MIN, i128::MAX, -7]);
	let output = round_trip_column("i", slice(parent, 1, 3));
	assert_column_eq("sliced_int16", &factory::int16("c", [i128::MIN, i128::MAX]), &output);
}
