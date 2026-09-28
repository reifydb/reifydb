// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::iter::repeat_n;

use arrow_array::UInt64Array;
use reifydb_value::value::frame::data::FrameColumnData;

fn make(v: Vec<u64>) -> FrameColumnData {
	FrameColumnData::Uint8(UInt64Array::from(v))
}

crate::rle_tests! {
	repeated: {
		let mut v = Vec::new();
		for val in [1000u64, 2000, 3000, 4000, 5000] {
			v.extend(repeat_n(val, 100));
		}
		v
	},
	unique: (0..100).map(|i| i as u64 * 7 + 13).collect::<Vec<_>>(),
}
