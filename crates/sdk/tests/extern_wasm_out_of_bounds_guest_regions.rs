// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{fmt::Display, panic::catch_unwind};

use reifydb_codec::tag::ValueKind;
use reifydb_core::value::column::columns::Columns;
use reifydb_sdk::common::extern_wasm::{
	layout::{EXTERN_WASM_COLUMN_SIZE, EXTERN_WASM_COLUMNS_HEADER_SIZE, ExternWasmColumn, ExternWasmColumns},
	marshal::unmarshal_columns_from_bytes,
};
use reifydb_value::value::{Value, row_number::RowNumber};

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

fn guest_bytes(patch: impl Fn(&mut ExternWasmColumns, &mut ExternWasmColumn)) -> Vec<u8> {
	// Every region is present and in bounds, so a patch that moves one region is the only defect.
	let row_numbers_offset = (EXTERN_WASM_COLUMNS_HEADER_SIZE + EXTERN_WASM_COLUMN_SIZE) as u32;
	let name_offset = row_numbers_offset + 8;
	let bitvec_offset = name_offset + 1;
	let data_offset = bitvec_offset + 1;
	let offsets_offset = data_offset + 2;
	let mut header = ExternWasmColumns {
		row_count: 1,
		column_count: 1,
		row_numbers_offset,
		row_numbers_len: 8,
	};
	let mut column = ExternWasmColumn {
		name_offset,
		name_len: 1,
		type_code: ValueKind::Utf8.byte(),
		data_row_count: 1,
		data_offset,
		data_len: 2,
		bitvec_offset,
		bitvec_len: 1,
		offsets_offset,
		offsets_len: 16,
	};
	patch(&mut header, &mut column);
	let mut bytes = Vec::new();
	header.write_to_bytes(&mut bytes);
	column.write_to_bytes(&mut bytes);
	bytes.extend_from_slice(&1u64.to_le_bytes());
	bytes.push(b'c');
	bytes.push(0x01);
	bytes.extend_from_slice(b"ab");
	bytes.extend_from_slice(&0u64.to_le_bytes());
	bytes.extend_from_slice(&2u64.to_le_bytes());
	bytes
}

fn cell_bytes(kind: ValueKind, data: &[u8], offsets: &[u64]) -> Vec<u8> {
	// One column with no row numbers and no bitvec, so only the offsets decide whether a cell is in bounds.
	let name_offset = (EXTERN_WASM_COLUMNS_HEADER_SIZE + EXTERN_WASM_COLUMN_SIZE) as u32;
	let data_offset = name_offset + 1;
	let offsets_offset = data_offset + data.len() as u32;
	let mut bytes = Vec::new();
	ExternWasmColumns {
		row_count: 1,
		column_count: 1,
		row_numbers_offset: 0,
		row_numbers_len: 0,
	}
	.write_to_bytes(&mut bytes);
	ExternWasmColumn {
		name_offset,
		name_len: 1,
		type_code: kind.byte(),
		data_row_count: 1,
		data_offset,
		data_len: data.len() as u32,
		bitvec_offset: 0,
		bitvec_len: 0,
		offsets_offset,
		offsets_len: (offsets.len() * 8) as u32,
	}
	.write_to_bytes(&mut bytes);
	bytes.push(b'c');
	bytes.extend_from_slice(data);
	for offset in offsets {
		bytes.extend_from_slice(&offset.to_le_bytes());
	}
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

fn not_rejected(bytes: &[u8]) -> Option<String> {
	match catch_unwind(|| unmarshal_columns_from_bytes(bytes).outcome()) {
		Ok(Err(_)) => None,
		Ok(Ok(columns)) => Some(format!("accepted as {}", rendered(&columns))),
		Err(_) => Some("panicked".to_string()),
	}
}

#[test]
fn guest_column_count_past_the_descriptors_sent_is_an_error_not_a_panic() {
	// A column count that promises descriptors the guest never sent must fail the call, never panic the host.
	let mut header_only = Vec::new();
	ExternWasmColumns {
		row_count: 1,
		column_count: 1,
		row_numbers_offset: 0,
		row_numbers_len: 0,
	}
	.write_to_bytes(&mut header_only);
	let one_descriptor_for_two_columns = guest_bytes(|header, _| header.column_count = 2);
	let failures: Vec<String> = [
		("one column, no descriptor", header_only),
		("two columns, one descriptor", one_descriptor_for_two_columns),
	]
	.into_iter()
	.filter_map(|(case, bytes)| not_rejected(&bytes).map(|failure| format!("{case}: {failure}")))
	.collect();
	assert!(failures.is_empty(), "a column count past the descriptors was not rejected: {failures:?}");
}

#[test]
fn guest_regions_past_the_end_of_the_bytes_are_an_error_not_a_panic() {
	// A region offset the guest bytes do not reach must fail the call, never panic the host.
	let well_formed = guest_bytes(|_, _| {});
	let columns =
		unmarshal_columns_from_bytes(&well_formed).outcome().expect("the unpatched guest bytes unmarshal");
	assert_eq!(columns.row_numbers(), &[RowNumber(1)], "the unpatched row numbers must read back");
	let column = columns.iter().next().expect("the unpatched column must read back");
	assert_eq!(column.data().get_value(0), Value::Utf8("ab".to_string()), "the unpatched cell must read back");
	let end = well_formed.len() as u32;
	let patches: [(&str, fn(&mut ExternWasmColumns, &mut ExternWasmColumn, u32)); 5] = [
		("row numbers", |header, _, end| header.row_numbers_offset = end),
		("name", |_, column, end| column.name_offset = end),
		("bitvec", |_, column, end| column.bitvec_offset = end),
		("data", |_, column, end| column.data_offset = end),
		("offsets", |_, column, end| column.offsets_offset = end),
	];
	let failures: Vec<String> = patches
		.into_iter()
		.filter_map(|(region, patch)| {
			not_rejected(&guest_bytes(|header, column| patch(header, column, end)))
				.map(|failure| format!("{region}: {failure}"))
		})
		.collect();
	assert!(failures.is_empty(), "a region past the end of the guest bytes was not rejected: {failures:?}");
}

#[test]
fn guest_offsets_outside_the_data_are_an_error_not_a_panic() {
	// A cell whose offsets do not lie inside the data region must fail the call, never panic the host.
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
	.flat_map(|kind| [(kind, "past the end", [0u64, 3]), (kind, "reversed", [2, 1])])
	.filter_map(|(kind, case, offsets)| {
		not_rejected(&cell_bytes(kind, b"ab", &offsets)).map(|failure| format!("{kind:?} {case}: {failure}"))
	})
	.collect();
	assert!(failures.is_empty(), "offsets outside 2 data bytes were not rejected: {failures:?}");
}
