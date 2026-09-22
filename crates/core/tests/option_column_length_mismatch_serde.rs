// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_buffer::BooleanBuffer;
use postcard::{from_bytes, to_stdvec};
use reifydb_core::value::column::buffer::ColumnBuffer;
use reifydb_value::value::frame::data::FrameColumnData;
use serde_json::{from_str, to_string};

fn mismatched(inner_rows: usize, bitvec_rows: usize) -> ColumnBuffer {
	ColumnBuffer::Option {
		inner: Box::new(ColumnBuffer::int4(vec![7; inner_rows])),
		bitvec: BooleanBuffer::from(vec![true; bitvec_rows]),
	}
}

fn shapes() -> [(usize, usize); 2] {
	[(1, 3), (3, 1)]
}

#[test]
fn an_option_column_whose_bitvec_length_differs_from_its_inner_column_does_not_deserialize() {
	// A bitvec that disagrees with its inner column must be a serde error, not a panic on a later read.
	for (inner_rows, bitvec_rows) in shapes() {
		let column = mismatched(inner_rows, bitvec_rows);
		let json = from_str::<ColumnBuffer>(&to_string(&column).unwrap());
		assert!(json.is_err(), "json with inner {inner_rows} and bitvec {bitvec_rows} rows decoded: {json:?}");
		let bytes = from_bytes::<ColumnBuffer>(&to_stdvec(&column).unwrap());
		assert!(
			bytes.is_err(),
			"postcard with inner {inner_rows} and bitvec {bitvec_rows} rows decoded: {bytes:?}"
		);
	}
}

#[test]
fn an_option_frame_column_whose_bitvec_length_differs_from_its_inner_column_does_not_deserialize() {
	// Wire frames take the same shape, so a mismatched bitvec must be refused there too.
	for (inner_rows, bitvec_rows) in shapes() {
		let column = FrameColumnData::from(mismatched(inner_rows, bitvec_rows));
		let json = from_str::<FrameColumnData>(&to_string(&column).unwrap());
		assert!(json.is_err(), "json with inner {inner_rows} and bitvec {bitvec_rows} rows decoded: {json:?}");
		let bytes = from_bytes::<FrameColumnData>(&to_stdvec(&column).unwrap());
		assert!(
			bytes.is_err(),
			"postcard with inner {inner_rows} and bitvec {bitvec_rows} rows decoded: {bytes:?}"
		);
	}
}
