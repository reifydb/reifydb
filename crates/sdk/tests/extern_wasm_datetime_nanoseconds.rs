// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::fmt::Display;

use reifydb_codec::tag::ValueKind;
use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_sdk::common::extern_wasm::{
	layout::{EXTERN_WASM_COLUMN_SIZE, EXTERN_WASM_COLUMNS_HEADER_SIZE, ExternWasmColumn, ExternWasmColumns},
	marshal::{marshal_columns_to_bytes, unmarshal_columns_from_bytes},
};
use reifydb_value::{
	fragment::Fragment,
	value::{Value, datetime::DateTime},
};

const SUB_SECOND_NANOS: u64 = 1_700_000_000_123_456_789;

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

fn datetime_columns(values: &[DateTime]) -> Columns {
	Columns::new(vec![ColumnWithName::new(Fragment::internal("c"), ColumnBuffer::datetime(values.to_vec()))])
}

fn guest_datetime_bytes(nanos: &[i64]) -> Vec<u8> {
	let name_offset = (EXTERN_WASM_COLUMNS_HEADER_SIZE + EXTERN_WASM_COLUMN_SIZE) as u32;
	let data_offset = name_offset + 1;
	let data: Vec<u8> = nanos.iter().flat_map(|value| value.to_le_bytes()).collect();
	let mut bytes = Vec::new();
	ExternWasmColumns {
		row_count: nanos.len() as u32,
		column_count: 1,
		row_numbers_offset: 0,
		row_numbers_len: 0,
	}
	.write_to_bytes(&mut bytes);
	ExternWasmColumn {
		name_offset,
		name_len: 1,
		type_code: ValueKind::DateTime.byte(),
		data_row_count: nanos.len() as u32,
		data_offset,
		data_len: data.len() as u32,
		bitvec_offset: 0,
		bitvec_len: 0,
		offsets_offset: 0,
		offsets_len: 0,
	}
	.write_to_bytes(&mut bytes);
	bytes.push(b'c');
	bytes.extend_from_slice(&data);
	bytes
}

fn row_values(columns: &Columns) -> Vec<Value> {
	let data = columns.iter().next().expect("one column was unmarshalled").data().clone();
	(0..data.len()).map(|row| data.get_value(row)).collect()
}

#[test]
fn a_sub_second_datetime_survives_the_wasm_marshal_round_trip_exactly() {
	// A marshal that carries whole seconds truncates every function, transform and procedure timestamp.
	let values = [DateTime::from_nanos(SUB_SECOND_NANOS), DateTime::from_nanos(1)];
	let bytes = marshal_columns_to_bytes(&datetime_columns(&values)).expect("a datetime column marshals");
	let output = unmarshal_columns_from_bytes(&bytes).outcome().expect("well formed bytes unmarshal");
	let expected: Vec<Value> = values.iter().map(|value| Value::DateTime(*value)).collect();
	assert_eq!(row_values(&output), expected);
}

#[test]
fn the_guest_receives_datetime_as_i64_nanoseconds_like_the_extern_c_abi() {
	// The guest must see the same unit as an extern-c operator, otherwise one guest reads the other's time wrong.
	let bytes = marshal_columns_to_bytes(&datetime_columns(&[DateTime::from_nanos(SUB_SECOND_NANOS)]))
		.expect("a datetime column marshals");
	let column = ExternWasmColumn::read_from_bytes(&bytes[EXTERN_WASM_COLUMNS_HEADER_SIZE..]);
	let start = column.data_offset as usize;
	let data = &bytes[start..start + column.data_len as usize];
	let sent: Vec<i64> = data.chunks_exact(8).map(|chunk| i64::from_le_bytes(chunk.try_into().unwrap())).collect();
	assert_eq!(sent, vec![SUB_SECOND_NANOS as i64]);
}

#[test]
fn a_guest_datetime_is_read_as_i64_nanoseconds_like_the_extern_c_abi() {
	// Reading the guest's nanoseconds as seconds overflows DateTime and returns a value the guest never sent.
	let output = unmarshal_columns_from_bytes(&guest_datetime_bytes(&[SUB_SECOND_NANOS as i64]))
		.outcome()
		.expect("well formed guest bytes unmarshal");
	assert_eq!(row_values(&output), vec![Value::DateTime(DateTime::from_nanos(SUB_SECOND_NANOS))]);
}
