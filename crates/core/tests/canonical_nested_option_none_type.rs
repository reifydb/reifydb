// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::Int32Array;
use arrow_buffer::BooleanBuffer;
use reifydb_codec::frame::{decode::decode_frames, encode::encode_frames, options::EncodeOptions};
use reifydb_core::value::column::{
	buffer::ColumnBuffer,
	data::{ColumnData, canonical::Canonical},
};
use reifydb_value::value::{
	Value,
	frame::{column::FrameColumn, data::FrameColumnData, frame::Frame},
	value_type::ValueType,
};

#[test]
fn an_outer_none_row_of_a_nested_option_reads_the_same_none_from_canonical_as_from_the_buffer() {
	// The canonical form must answer exactly like the buffer, so an outer none must keep its Option(Int4) type.
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
	let outer_none = Value::none_of(ValueType::Option(Box::new(ValueType::Int4)));
	assert_eq!(buffer.get_value(2), outer_none, "the buffer reads the outer none row");
	let canonical = Canonical::from_buffer(buffer);
	assert_eq!(canonical.get_value(2), outer_none, "the canonical form must read the same none");
}
