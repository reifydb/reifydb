// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::fmt::Display;

use reifydb_codec::tag::ValueKind;
use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_sdk::common::extern_wasm::{
	layout::{EXTERN_WASM_COLUMN_SIZE, EXTERN_WASM_COLUMNS_HEADER_SIZE, ExternWasmColumn, ExternWasmColumns},
	marshal::{marshal_columns_to_bytes, unmarshal_columns_from_bytes},
};
use reifydb_value::{fragment::Fragment, value::dictionary::DictionaryEntryId};

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

fn guest_bytes(kind: ValueKind, rows: u32, data: &[u8], offsets: &[u64]) -> Vec<u8> {
	let name = b"c";
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
		type_code: kind.byte(),
		precision: 0,
		scale: 0,
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

fn only_column(columns: &Columns) -> ColumnBuffer {
	columns.iter().next().expect("one column was unmarshalled").data().clone()
}

#[track_caller]
fn assert_rejected(bytes: &[u8]) {
	// A value the guest never sent must not stand in for the one it did send.
	if let Ok(columns) = unmarshal_columns_from_bytes(bytes).outcome() {
		let data = only_column(&columns);
		let values: Vec<_> = (0..data.len()).map(|row| data.get_value(row)).collect();
		panic!("malformed guest bytes were accepted as {values:?}");
	}
}

#[test]
fn guest_utf8_that_is_not_utf8_is_an_error_not_an_empty_string() {
	// Replacing bytes that are not UTF-8 with "" hides a broken guest behind a plausible value.
	assert_rejected(&guest_bytes(ValueKind::Utf8, 1, &[0xff, 0xfe], &[0, 2]));
}

#[test]
fn guest_duration_with_mixed_signs_is_an_error_not_zero() {
	// A duration that fails normalisation must fail the call, never read back as a zero duration.
	let mut cell = Vec::new();
	cell.extend_from_slice(&0i32.to_le_bytes());
	cell.extend_from_slice(&1i32.to_le_bytes());
	cell.extend_from_slice(&(-1i64).to_le_bytes());
	assert_rejected(&guest_bytes(ValueKind::Duration, 1, &cell, &[0, cell.len() as u64]));
}

#[test]
fn guest_date_out_of_range_is_an_error_not_the_default() {
	// A day count outside the Date range must fail the call, never read back as the epoch.
	assert_rejected(&guest_bytes(ValueKind::Date, 1, &400_000_000i32.to_le_bytes(), &[]));
}

#[test]
fn guest_time_past_the_end_of_the_day_is_an_error_not_the_default() {
	// A time of day of 24h or more must fail the call, never read back as midnight.
	assert_rejected(&guest_bytes(ValueKind::Time, 1, &86_400_000_000_000u64.to_le_bytes(), &[]));
}

#[test]
fn guest_datetime_before_the_epoch_is_an_error_not_the_default() {
	// A timestamp DateTime cannot represent must fail the call, never read back as the epoch.
	assert_rejected(&guest_bytes(ValueKind::DateTime, 1, &(-1i64).to_le_bytes(), &[]));
}

#[test]
fn guest_int4_with_fewer_bytes_than_rows_never_shrinks_the_column() {
	// A column shorter than the declared row count misaligns it against its bitvec and its sibling columns.
	let data: Vec<u8> = [1i32, 2].iter().flat_map(|v| v.to_le_bytes()).collect();
	if let Ok(columns) = unmarshal_columns_from_bytes(&guest_bytes(ValueKind::Int4, 3, &data, &[])).outcome() {
		assert_eq!(only_column(&columns).len(), 3, "the guest declared 3 rows");
	}
}

#[test]
fn dictionary_ids_keep_their_width_through_the_wasm_marshal() {
	// Entry ids compare and hash by width, so a round trip that widens them all breaks equality with the input.
	let entries = vec![
		DictionaryEntryId::U4(1),
		DictionaryEntryId::U1(2),
		DictionaryEntryId::U2(u16::MAX),
		DictionaryEntryId::U8(u64::MAX),
		DictionaryEntryId::U16(u128::MAX),
	];
	let input = ColumnBuffer::dictionary_id(entries.clone());
	let columns = Columns::new(vec![ColumnWithName::new(Fragment::internal("c"), input)]);
	let bytes = marshal_columns_to_bytes(&columns).expect("a dictionary id column marshals");
	let output = unmarshal_columns_from_bytes(&bytes).outcome().expect("well formed bytes unmarshal");
	assert_eq!(only_column(&output), ColumnBuffer::dictionary_id(entries));
}
