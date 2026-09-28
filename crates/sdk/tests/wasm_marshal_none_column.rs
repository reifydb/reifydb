// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_sdk::common::extern_wasm::marshal::{marshal_columns_to_bytes, unmarshal_columns_from_bytes};
use reifydb_value::fragment::Fragment;

fn round_trip(buffer: ColumnBuffer) -> ColumnBuffer {
	let columns = Columns::new(vec![ColumnWithName::new(Fragment::internal("c"), buffer)]);
	let bytes = marshal_columns_to_bytes(&columns).unwrap();
	unmarshal_columns_from_bytes(&bytes).unwrap().columns[0].clone()
}

#[test]
fn a_none_column_crosses_the_wasm_marshal_as_a_none_column() {
	// Otherwise a guest none column comes back as an any column that a typed column beside it can not join.
	let back = round_trip(ColumnBuffer::none(3));

	assert!(back.is_none(), "got {:?}", back.get_type());
	assert_eq!(back.len(), 3);
}

#[test]
fn a_column_of_no_rows_crosses_the_wasm_marshal_as_a_none_column() {
	// A zero row guest column carries no type the host can trust, so it must be the untyped none column.
	let back = round_trip(ColumnBuffer::none(0));

	assert!(back.is_none(), "got {:?}", back.get_type());
	assert_eq!(back.len(), 0);
}
