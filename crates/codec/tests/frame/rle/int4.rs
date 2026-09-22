// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::Int32Array;
use reifydb_value::value::frame::data::FrameColumnData;

fn make(v: Vec<i32>) -> FrameColumnData {
	FrameColumnData::Int4(Int32Array::from(v))
}

crate::rle_tests! {
	repeated: {
		let mut v = Vec::new();
		for val in [10i32, 20, 30, 40, 50] {
			v.extend(std::iter::repeat(val).take(100));
		}
		v
	},
	unique: (0..100).map(|i| (i * 7 + 13) % 97).collect::<Vec<_>>(),
}
