// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::ArrayRef;
use arrow_schema::FieldRef;
use reifydb_core::value::column::factory;
use reifydb_sdk::common::extern_wasm::marshal::{marshal_columns_to_bytes, unmarshal_columns_from_bytes};
use reifydb_value::value::{column_view::ColumnView, system_columns::user_columns};

fn round_trip(buffer: (FieldRef, ArrayRef)) -> (FieldRef, ArrayRef) {
	let rows = buffer.1.len();
	let bytes = marshal_columns_to_bytes(&[buffer], rows, &[]).unwrap();
	let batch = unmarshal_columns_from_bytes(&bytes).unwrap();
	let (field, array) = user_columns(&batch).next().unwrap();
	(field.clone(), array.clone())
}

#[test]
fn a_none_column_crosses_the_wasm_marshal_as_a_none_column() {
	// Otherwise a guest none column comes back as an any column that a typed column beside it can not join.
	let back = round_trip(factory::none("c", 3));
	let back = ColumnView::try_from(&back).unwrap();

	assert!(back.is_none(), "got {:?}", back.get_type());
	assert_eq!(back.len(), 3);
}

#[test]
fn a_column_of_no_rows_crosses_the_wasm_marshal_as_a_none_column() {
	// A zero row guest column carries no type the host can trust, so it must be the untyped none column.
	let back = round_trip(factory::none("c", 0));
	let back = ColumnView::try_from(&back).unwrap();

	assert!(back.is_none(), "got {:?}", back.get_type());
	assert_eq!(back.len(), 0);
}
