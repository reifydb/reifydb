// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{fmt::Display, panic::catch_unwind};

use reifydb_codec::{extern_c::cells::decode_any_cell, tag::ValueKind};
use reifydb_core::value::column::columns::Columns;
use reifydb_sdk::common::extern_wasm::{
	layout::{EXTERN_WASM_COLUMN_SIZE, EXTERN_WASM_COLUMNS_HEADER_SIZE, ExternWasmColumn, ExternWasmColumns},
	marshal::unmarshal_columns_from_bytes,
};

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

fn guest_bytes(type_code: u8, name: &[u8], rows: u32, data: &[u8], offsets: &[u64]) -> Vec<u8> {
	let name_offset = (EXTERN_WASM_COLUMNS_HEADER_SIZE + EXTERN_WASM_COLUMN_SIZE) as u32;
	let data_offset = name_offset + name.len() as u32;
	let offsets_offset = data_offset + data.len() as u32;
	let mut bytes = Vec::new();
	ExternWasmColumns {
		row_count: rows,
		column_count: 1,
		row_numbers_offset: 0,
		row_numbers_len: 0,
	}
	.write_to_bytes(&mut bytes);
	ExternWasmColumn {
		name_offset,
		name_len: name.len() as u32,
		type_code,
		data_row_count: rows,
		data_offset,
		data_len: data.len() as u32,
		bitvec_offset: 0,
		bitvec_len: 0,
		offsets_offset,
		offsets_len: (offsets.len() * 8) as u32,
	}
	.write_to_bytes(&mut bytes);
	bytes.extend_from_slice(name);
	bytes.extend_from_slice(data);
	for offset in offsets {
		bytes.extend_from_slice(&offset.to_le_bytes());
	}
	bytes
}

fn uuid_bytes(version: u8) -> [u8; 16] {
	let mut bytes = [0x11u8; 16];
	bytes[6] = (version << 4) | 0x01;
	bytes[8] = 0x81;
	bytes
}

fn rendered(columns: &Columns) -> String {
	let rendered: Vec<String> = columns
		.iter()
		.map(|column| {
			let data = column.data();
			let values: Vec<_> = (0..data.len()).map(|row| data.get_value(row)).collect();
			format!("{:?}: {values:?}", column.name().text())
		})
		.collect();
	format!("[{}]", rendered.join(", "))
}

#[track_caller]
fn assert_rejected(bytes: &[u8]) {
	// A value the guest never sent must not stand in for the one it did send.
	if let Ok(columns) = unmarshal_columns_from_bytes(bytes).outcome() {
		panic!("malformed guest bytes were accepted as {}", rendered(&columns));
	}
}

fn not_rejected(bytes: &[u8]) -> Option<String> {
	match catch_unwind(|| unmarshal_columns_from_bytes(bytes).outcome()) {
		Ok(Err(_)) => None,
		Ok(Ok(columns)) => Some(format!("accepted as {}", rendered(&columns))),
		Err(_) => Some("panicked".to_string()),
	}
}

#[test]
fn guest_decimal_that_does_not_parse_is_an_error_not_zero() {
	// A decimal cell that does not parse must fail the call, never read back as zero.
	let cell = b"not a decimal";
	assert_rejected(&guest_bytes(ValueKind::Decimal.byte(), b"c", 1, cell, &[0, cell.len() as u64]));
}

#[test]
fn guest_any_cell_that_does_not_decode_is_an_error_not_none() {
	// An any cell that does not decode must fail the call, never read back as none.
	let cell = [0xffu8];
	assert!(decode_any_cell(&cell).is_err(), "the cell must be undecodable for this test to mean anything");
	assert_rejected(&guest_bytes(ValueKind::Any.byte(), b"c", 1, &cell, &[0, cell.len() as u64]));
}

#[test]
fn guest_column_name_that_is_not_utf8_is_an_error_not_an_empty_name() {
	// A column name that is not UTF-8 must fail the call, never become "" and bind to the wrong column.
	assert_rejected(&guest_bytes(ValueKind::Int4.byte(), &[0xff, 0xfe], 1, &7i32.to_le_bytes(), &[]));
}

#[test]
fn guest_unknown_type_code_is_an_error_not_a_none_column() {
	// A type code the host does not know must fail the call, never become a column of nones.
	let type_code = 0xff;
	assert!(
		ValueKind::from_byte(type_code).is_none(),
		"the type code must be unknown for this test to mean anything"
	);
	assert_rejected(&guest_bytes(type_code, b"c", 1, &7i32.to_le_bytes(), &[]));
}

#[test]
fn guest_bytes_shorter_than_the_header_are_an_error_not_empty_columns() {
	// A truncated header must fail the call, never read back as a result with no columns.
	assert_rejected(&vec![0x01u8; EXTERN_WASM_COLUMNS_HEADER_SIZE - 1]);
}

#[test]
fn guest_sixteen_byte_columns_with_a_partial_value_are_an_error_not_a_panic() {
	// A trailing partial 16 byte value from the guest must fail the call, never panic the host.
	let failures: Vec<String> = [(ValueKind::Uuid4, 4), (ValueKind::Uuid7, 7), (ValueKind::IdentityId, 7)]
		.into_iter()
		.filter_map(|(kind, version)| {
			let mut data = uuid_bytes(version).to_vec();
			data.push(0x00);
			not_rejected(&guest_bytes(kind.byte(), b"c", 2, &data, &[]))
				.map(|failure| format!("{kind:?} {failure}"))
		})
		.collect();
	assert!(failures.is_empty(), "17 data bytes for 2 rows were not rejected: {failures:?}");
}

#[test]
fn guest_offsets_shorter_than_the_row_count_are_an_error_not_a_panic() {
	// Two rows need three offsets; a guest that sends fewer must fail the call, never panic the host.
	let failures: Vec<String> = [
		ValueKind::Utf8,
		ValueKind::Blob,
		ValueKind::Duration,
		ValueKind::Int,
		ValueKind::Uint,
		ValueKind::Decimal,
		ValueKind::Any,
	]
	.into_iter()
	.filter_map(|kind| {
		not_rejected(&guest_bytes(kind.byte(), b"c", 2, b"ab", &[0]))
			.map(|failure| format!("{kind:?} {failure}"))
	})
	.collect();
	assert!(failures.is_empty(), "one offset for 2 rows was not rejected: {failures:?}");
}

#[test]
fn guest_identity_id_that_is_not_a_uuid_v7_is_an_error() {
	// An identity id is always a UUID v7; accepting another version admits an id no identity can hold.
	assert_rejected(&guest_bytes(ValueKind::IdentityId.byte(), b"c", 1, &uuid_bytes(4), &[]));
}
