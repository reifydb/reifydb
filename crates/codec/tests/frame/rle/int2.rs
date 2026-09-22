// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::Int16Array;
use reifydb_value::value::frame::data::FrameColumnData;

fn make(v: Vec<i16>) -> FrameColumnData {
	FrameColumnData::Int2(Int16Array::from(v))
}

crate::rle_tests! {
	repeated: {
		let mut v = Vec::new();
		for val in [100i16, 200, 300, 400, 500] {
			v.extend(std::iter::repeat(val).take(100));
		}
		v
	},
	unique: (0..100).map(|i| i as i16 * 7 + 13).collect::<Vec<_>>(),
}
