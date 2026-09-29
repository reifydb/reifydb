// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::RecordBatch;
use reifydb_codec::row::{
	bytes::EncodedBytes,
	shape::{RowFamily, RowShape, RowShapeField},
};
use reifydb_core::value::{
	batch::{append_rows, batch},
	column::builder::ColumnBuilder,
};
use reifydb_value::value::{Value, row_number::RowNumber, value_type::ValueType};

fn shape() -> RowShape {
	RowShape::new(RowFamily::Table, vec![RowShapeField::unconstrained("k", ValueType::Int4)])
}

fn rows(shape: &RowShape, keys: &[i32]) -> Vec<EncodedBytes> {
	keys.iter()
		.map(|key| {
			let mut row = shape.allocate_table();
			shape.set_values(&mut row, &[Value::Int4(*key)]);
			EncodedBytes::from(row.freeze())
		})
		.collect()
}

fn empty(shape: &RowShape) -> RecordBatch {
	let field = &shape.fields()[0];
	batch(vec![ColumnBuilder::with_capacity(field.constraint.get_type(), 0).finish(field.name.as_str())]).unwrap()
}

#[test]
fn appending_without_row_numbers_onto_a_stamped_batch_is_not_an_internal_error() {
	// The caller picks whether row numbers are passed, so this must be a user-facing error, never INTERNAL_ERROR.
	let shape = shape();
	let stamped =
		append_rows(empty(&shape), &shape, rows(&shape, &[1, 2]), vec![RowNumber(1), RowNumber(2)]).unwrap();

	let err = append_rows(stamped, &shape, rows(&shape, &[3]), Vec::new()).unwrap_err().diagnostic();

	assert_eq!(err.code, "ENG_008", "got: {}", err.message);
	assert_eq!(err.message, "cannot append rows: '#rownum' is present on one side but not the other");
}

#[test]
fn appending_with_row_numbers_onto_an_unstamped_batch_is_not_an_internal_error() {
	// The mirror case must also be user-facing, never INTERNAL_ERROR.
	let shape = shape();
	let unstamped = append_rows(empty(&shape), &shape, rows(&shape, &[1, 2]), Vec::new()).unwrap();

	let err = append_rows(unstamped, &shape, rows(&shape, &[3]), vec![RowNumber(3)]).unwrap_err().diagnostic();

	assert_eq!(err.code, "ENG_008", "got: {}", err.message);
	assert_eq!(err.message, "cannot append rows: '#rownum' is present on one side but not the other");
}
