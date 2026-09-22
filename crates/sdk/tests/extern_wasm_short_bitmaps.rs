// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::fmt::Display;

use reifydb_codec::tag::ValueKind;
use reifydb_core::value::column::columns::Columns;
use reifydb_sdk::common::extern_wasm::{
	layout::{EXTERN_WASM_COLUMN_SIZE, EXTERN_WASM_COLUMNS_HEADER_SIZE, ExternWasmColumn, ExternWasmColumns},
	marshal::unmarshal_columns_from_bytes,
};

const ROWS: u32 = 16;

trait Unmarshalled {
	// Must accept a bare Columns and a Result alike, otherwise a fallible unmarshal stops these tests compiling.
	fn outcome(self) -> Result<Columns, String>;
}

impl Unmarshalled for Columns {
	fn outcome(self) -> Result<Columns, String> {
		Ok(self)
	}
}

impl<E: Display> Unmarshalled for Result<Columns, E> {
	fn outcome(self) -> Result<Columns, String> {
		self.map_err(|error| error.to_string())
	}
}

fn guest_bytes(kind: ValueKind, bitvec: &[u8], data: &[u8]) -> Vec<u8> {
	let name_offset = (EXTERN_WASM_COLUMNS_HEADER_SIZE + EXTERN_WASM_COLUMN_SIZE) as u32;
	let bitvec_offset = name_offset + 1;
	let data_offset = bitvec_offset + bitvec.len() as u32;
	let mut bytes = Vec::new();
	ExternWasmColumns {
		row_count: ROWS,
		column_count: 1,
		row_numbers_offset: 0,
		row_numbers_len: 0,
	}
	.write_to_bytes(&mut bytes);
	ExternWasmColumn {
		name_offset,
		name_len: 1,
		type_code: kind.byte(),
		data_row_count: ROWS,
		data_offset,
		data_len: data.len() as u32,
		bitvec_offset: if bitvec.is_empty() {
			0
		} else {
			bitvec_offset
		},
		bitvec_len: bitvec.len() as u32,
		offsets_offset: 0,
		offsets_len: 0,
	}
	.write_to_bytes(&mut bytes);
	bytes.push(b'c');
	bytes.extend_from_slice(bitvec);
	bytes.extend_from_slice(data);
	bytes
}

#[track_caller]
fn assert_rejected(bytes: &[u8]) {
	// A value the guest never sent must not stand in for the one it did send.
	if let Ok(columns) = unmarshal_columns_from_bytes(bytes).outcome() {
		let data = columns.iter().next().expect("one column was unmarshalled").data().clone();
		let values: Vec<_> = (0..data.len()).map(|row| data.get_value(row)).collect();
		panic!("short guest bitmap bytes were accepted as {values:?}");
	}
}

#[test]
fn a_guest_bitvec_shorter_than_its_rows_is_an_error_not_defined_rows() {
	// Rows past the bitvec bytes the guest sent must fail the call, never read back as defined values.
	let data: Vec<u8> = (0..ROWS as i32).flat_map(|value| value.to_le_bytes()).collect();
	assert_rejected(&guest_bytes(ValueKind::Int4, &[0x00], &data));
}

#[test]
fn a_guest_bool_column_shorter_than_its_rows_is_an_error_not_false() {
	// Rows past the bool bytes the guest sent must fail the call, never read back as false.
	assert_rejected(&guest_bytes(ValueKind::Boolean, &[], &[0xff]));
}
