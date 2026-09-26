// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::iter::repeat_n;

use arrow_array::UInt8Array;
use reifydb_value::value::frame::data::FrameColumnData;

fn make(v: Vec<u8>) -> FrameColumnData {
	FrameColumnData::Uint1(UInt8Array::from(v))
}

crate::rle_tests! {
	repeated: {
		let mut v = Vec::new();
		for val in [10u8, 20, 30, 40, 50] {
			v.extend(repeat_n(val, 100));
		}
		v
	},
	unique: (0..100).map(|i| i as u8).collect::<Vec<_>>(),
}
