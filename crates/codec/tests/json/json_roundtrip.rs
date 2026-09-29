// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{slice::from_ref, sync::Arc};

use arrow_array::{ArrayRef, BooleanArray, Int32Array, LargeStringArray, RecordBatch, UInt64Array};
use arrow_schema::{FieldRef, Schema};
use reifydb_codec::json::{from::frames_from_json, to::frames_to_json};
use reifydb_value::value::{
	blob::Blob,
	column_view::ColumnView,
	container::varlen_array::blob_array,
	frame::frame::Frame,
	value_type::{ValueType, field::named},
};

fn frame(columns: Vec<(&str, ValueType, ArrayRef)>) -> Frame {
	let (fields, arrays): (Vec<FieldRef>, Vec<ArrayRef>) =
		columns.into_iter().map(|(name, value_type, array)| named(name, value_type.into(), array)).unzip();
	Frame::from(RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap())
}

fn view(frame: &Frame, index: usize) -> ColumnView<'_> {
	ColumnView::try_from((frame.batch.column(index), frame.batch.schema_ref().field(index))).unwrap()
}

fn round_trip(frame: Frame) {
	let json = frames_to_json(from_ref(&frame)).expect("to_json failed");
	let decoded = frames_from_json(&json).expect("from_json failed");
	assert_eq!(decoded.len(), 1);
	let got = &decoded[0];
	assert_eq!(frame.batch.num_columns(), got.batch.num_columns());
	for index in 0..frame.batch.num_columns() {
		let (a, b) = (view(&frame, index), view(got, index));
		assert_eq!(a.field.name(), b.field.name(), "column name mismatch");
		assert_eq!(a.len(), b.len(), "column len mismatch");
		for i in 0..a.len() {
			assert_eq!(a.get_value(i), b.get_value(i), "cell {} of {} differs", i, a.field.name());
		}
	}
}

#[test]
fn empty_frame() {
	round_trip(Frame::from(RecordBatch::new_empty(Arc::new(Schema::empty()))));
}

#[test]
fn primitives() {
	round_trip(frame(vec![
		("b", ValueType::Boolean, Arc::new(BooleanArray::from(vec![true, false, true]))),
		("i4", ValueType::Int4, Arc::new(Int32Array::from(vec![-1, 0, i32::MAX]))),
		("u8", ValueType::Uint8, Arc::new(UInt64Array::from(vec![0, 1, u64::MAX]))),
		(
			"s",
			ValueType::Utf8,
			Arc::new(LargeStringArray::from(vec![
				"".to_string(),
				"hello".to_string(),
				"日本語".to_string(),
			])),
		),
		(
			"blob",
			ValueType::Blob,
			Arc::new(blob_array(&[
				Blob::new(vec![]),
				Blob::new(vec![0xde, 0xad, 0xbe, 0xef]),
				Blob::new(vec![0x00, 0xff]),
			])),
		),
	]));
}

#[test]
fn option_with_nones() {
	let inner = Int32Array::from(vec![Some(10), None, Some(30)]);
	round_trip(frame(vec![("maybe", ValueType::Option(Box::new(ValueType::Int4)), Arc::new(inner))]));
}

#[test]
fn multi_frame_serialization() {
	let frames = vec![
		frame(vec![("a", ValueType::Int4, Arc::new(Int32Array::from(vec![1, 2])))]),
		frame(vec![("b", ValueType::Utf8, Arc::new(LargeStringArray::from(vec!["x".to_string()])))]),
	];
	let json = frames_to_json(&frames).expect("to_json failed");
	let decoded = frames_from_json(&json).expect("from_json failed");
	assert_eq!(decoded.len(), 2);
}
