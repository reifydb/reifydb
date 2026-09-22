// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::Int32Array;
use arrow_buffer::BooleanBuffer;
use reifydb_codec::frame::{decode::decode_frames, encode::encode_frames, options::EncodeOptions};
use reifydb_core::value::column::buffer::ColumnBuffer;
use reifydb_value::value::{
	Value,
	frame::{column::FrameColumn, data::FrameColumnData, frame::Frame},
};

#[test]
fn a_row_that_is_none_in_the_inner_option_layer_is_not_defined() {
	// A row that reads back as none must never report as defined, whichever option layer holds the none.
	let nested = FrameColumnData::Option {
		inner: Box::new(FrameColumnData::Option {
			inner: Box::new(FrameColumnData::Int4(Int32Array::from(vec![7, 0, 9]))),
			bitvec: BooleanBuffer::from(vec![true, false, true]),
		}),
		bitvec: BooleanBuffer::from(vec![true, true, false]),
	};
	let frame = Frame::new(vec![FrameColumn {
		name: "c".to_string(),
		data: nested,
	}]);
	let bytes = encode_frames(&[frame], &EncodeOptions::default()).expect("a depth 2 frame encodes");
	let decoded = decode_frames(&bytes).expect("a depth 2 frame decodes").remove(0).columns.remove(0).data;
	let buffer = ColumnBuffer::from(decoded);
	assert!(matches!(buffer.get_value(1), Value::None { .. }), "row 1 must read back as none");
	let defined: Vec<bool> = (0..3).map(|row| buffer.is_defined(row)).collect();
	assert_eq!(defined, vec![true, false, false], "rows that report as defined");
}
