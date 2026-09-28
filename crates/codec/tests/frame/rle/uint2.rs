// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::iter::repeat_n;

use arrow_array::UInt16Array;
use reifydb_value::value::frame::data::FrameColumnData;

fn make(v: Vec<u16>) -> FrameColumnData {
	FrameColumnData::Uint2(UInt16Array::from(v))
}

crate::rle_tests! {
	repeated: {
		let mut v = Vec::new();
		for val in [100u16, 200, 300, 400, 500] {
			v.extend(repeat_n(val, 100));
		}
		v
	},
	unique: (0..100).map(|i| i as u16 * 7).collect::<Vec<_>>(),
}
