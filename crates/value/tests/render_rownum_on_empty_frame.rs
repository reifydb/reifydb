// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use arrow_array::{ArrayRef, Int32Array, RecordBatch, UInt64Array};
use arrow_schema::Schema;
use reifydb_value::value::{
	frame::frame::Frame,
	system_columns::{SystemColumn, system_column, with_system_column},
	value_type::{
		ValueType,
		field::{FieldType, to_field},
	},
};

fn rows(values: Vec<i32>) -> RecordBatch {
	let field = to_field(
		"x",
		&FieldType {
			value_type: Some(ValueType::Int4),
			..FieldType::default()
		},
	);
	let array: ArrayRef = Arc::new(Int32Array::from(values));
	RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![array]).unwrap()
}

#[test]
fn a_frame_emptied_by_take_still_renders_the_rownum_column() {
	// Without the column, dropping the last row drops #rownum and the empty answer disagrees with the full one.
	let numbered = with_system_column(
		rows(vec![1, 2]),
		SystemColumn::RowNumbers,
		Arc::new(UInt64Array::from(vec![1u64, 2])),
	)
	.unwrap();
	let frame = Frame::from(numbered.slice(0, 0));

	assert!(system_column(&frame.batch, SystemColumn::RowNumbers).is_some());
	let rendered = frame.to_string();
	assert!(rendered.contains("#rownum"), "an emptied frame must keep its #rownum header, got:\n{rendered}");
}

#[test]
fn a_frame_without_row_numbers_renders_no_rownum_column() {
	// Row numbers must not appear by default, or aggregates and dictionaries would grow a #rownum they never carry.
	let frame = Frame::from(rows(vec![]));

	assert!(system_column(&frame.batch, SystemColumn::RowNumbers).is_none());
	let rendered = frame.to_string();
	assert!(!rendered.contains("#rownum"), "a frame without row numbers must not render #rownum, got:\n{rendered}");
}
