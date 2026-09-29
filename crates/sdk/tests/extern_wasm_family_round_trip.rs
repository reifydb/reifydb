// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::slice::from_ref;

use arrow_array::ArrayRef;
use arrow_schema::FieldRef;
use reifydb_core::value::column::factory;
use reifydb_sdk::common::extern_wasm::{
	layout::EXTERN_WASM_COLUMNS_HEADER_SIZE,
	marshal::{marshal_columns_to_bytes, unmarshal_columns_from_bytes},
};
use reifydb_value::value::{
	column_view::ColumnView,
	constraint::{precision::Precision, scale::Scale},
	decimal::Decimal,
	system_columns::user_columns,
};

fn assert_wasm_round_trip(label: &str, input: (FieldRef, ArrayRef), precision: u8, scale: u8, cell_width: usize) {
	// The descriptor must carry precision and scale and the data must be one fixed cell per row, or the guest
	// misreads it.
	let rows = input.1.len();
	let bytes = marshal_columns_to_bytes(from_ref(&input), rows, &[]).unwrap();
	let descriptor = &bytes[EXTERN_WASM_COLUMNS_HEADER_SIZE..];
	assert_eq!((descriptor[9], descriptor[10]), (precision, scale), "{label}: descriptor precision and scale");
	let data_len = u32::from_le_bytes(descriptor[19..23].try_into().unwrap()) as usize;
	assert_eq!(data_len, rows * cell_width, "{label}: data must be {cell_width} bytes per row");
	let offsets_len = u32::from_le_bytes(descriptor[35..39].try_into().unwrap());
	assert_eq!(offsets_len, 0, "{label}: fixed cells carry no offsets");
	let output = unmarshal_columns_from_bytes(&bytes).unwrap();
	let (field, array) = user_columns(&output).next().expect("one column was unmarshalled");
	let output = (field.clone(), array.clone());
	let (output_data, input_data) = (output.1.to_data(), input.1.to_data());
	assert_eq!(output_data.nulls(), input_data.nulls(), "{label}: nones");
	assert_eq!(output_data.buffers(), input_data.buffers(), "{label}: values");
	assert_eq!(
		ColumnView::try_from(&output).unwrap().get_type(),
		ColumnView::try_from(&input).unwrap().get_type(),
		"{label}: type"
	);
}

fn decimals(texts: &[&str]) -> Vec<Decimal> {
	texts.iter().map(|t| Decimal::parse(t).unwrap()).collect()
}

#[test]
fn decimal_round_trips_through_wasm_at_both_widths() {
	// Dropping the scale from the descriptor reads every unscaled value back as a whole number.
	let narrow = factory::decimal(
		"c",
		Precision::new(38),
		Scale::new(10),
		decimals(&["9999999999999999999999999999.9999999999", "-0.0000000001", "0"]),
	);
	assert_wasm_round_trip("decimal at precision 38", narrow, 38, 10, 16);
	let optional = factory::decimal_with_bitvec(
		"c",
		Precision::new(38),
		Scale::new(10),
		decimals(&["0", "12.5"]),
		vec![false, true],
	);
	assert_wasm_round_trip("optional decimal at precision 38", optional, 38, 10, 16);
	let wide = factory::decimal(
		"c",
		Precision::MAX,
		Scale::new(40),
		decimals(&[
			"999999999999999999999999999999999999.9999999999999999999999999999999999999999",
			"-0.0000000000000000000000000000000000000001",
		]),
	);
	assert_wasm_round_trip("decimal at precision 76", wide, 76, 40, 32);
}
