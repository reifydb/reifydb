// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::Int32Array;
use arrow_buffer::BooleanBuffer;
use reifydb_codec::frame::{decode::decode_frames, encode::encode_frames, options::EncodeOptions};
use reifydb_core::value::column::{
	buffer::ColumnBuffer,
	data::{Column, canonical::Canonical},
};
use reifydb_value::value::{
	Value,
	frame::{column::FrameColumn, data::FrameColumnData, frame::Frame},
};

fn wire_round_trip(data: FrameColumnData) -> FrameColumnData {
	let frame = Frame::new(vec![FrameColumn {
		name: "c".to_string(),
		data,
	}]);
	let bytes = encode_frames(&[frame], &EncodeOptions::default()).expect("encode failed");
	decode_frames(&bytes).expect("decode failed").remove(0).columns.remove(0).data
}

#[test]
fn a_canonical_column_prints_a_none_row_as_none() {
	// A none row must print as none, never as the placeholder value stored under it.
	let buffer = ColumnBuffer::int4_optional([Some(1), None]);
	let column = Column::from_canonical(Canonical::from_buffer(buffer.clone()));
	assert_eq!(column.data().as_string(1), "none");
	for row in 0..2 {
		assert_eq!(column.data().as_string(row), buffer.as_string(row), "row {row}");
	}
}

#[test]
fn a_canonical_column_over_a_nested_option_keeps_the_inner_nones() {
	// A row that is none in the inner layer must stay none after the outer layer is lifted.
	let nested = FrameColumnData::Option {
		inner: Box::new(FrameColumnData::Option {
			inner: Box::new(FrameColumnData::Int4(Int32Array::from(vec![7, 0, 9]))),
			bitvec: BooleanBuffer::from(vec![true, false, true]),
		}),
		bitvec: BooleanBuffer::from(vec![true, true, false]),
	};
	let buffer = ColumnBuffer::from(wire_round_trip(nested));
	assert!(matches!(buffer.get_value(1), Value::None { .. }), "the decoded buffer must read row 1 as none");
	let column = Column::from_canonical(Canonical::from_buffer(buffer));
	assert_eq!(column.data().get_value(0), Value::Int4(7));
	let nones: Vec<bool> = (0..3).map(|row| matches!(column.data().get_value(row), Value::None { .. })).collect();
	assert_eq!(nones, vec![false, true, true], "rows that read as none, by value");
	let defined: Vec<bool> = (0..3).map(|row| column.nones().is_none_or(|nones| nones.is_valid(row))).collect();
	assert_eq!(defined, vec![true, false, false], "rows that read as defined");
}
