// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::BooleanArray;
use arrow_buffer::{BooleanBuffer, Buffer};
use postcard::to_stdvec;
use reifydb_core::value::column::buffer::ColumnBuffer;
use serde_json::to_string;

fn bool_column_over(bytes: Vec<u8>, len: usize) -> ColumnBuffer {
	ColumnBuffer::Bool(BooleanArray::from(BooleanBuffer::new(Buffer::from_vec(bytes), 0, len)))
}

#[test]
fn a_bool_column_over_a_buffer_with_spare_bytes_serializes_like_a_compact_one() {
	// Bytes past ceil(len / 8) must never reach the wire, or equal Bool columns serialize differently.
	let spare = bool_column_over(vec![0b0000_0101, 0, 0], 3);
	let compact = ColumnBuffer::bool([true, false, true]);
	assert_eq!(to_string(&spare).unwrap(), to_string(&compact).unwrap());
	assert_eq!(to_stdvec(&spare).unwrap(), to_stdvec(&compact).unwrap());
}
