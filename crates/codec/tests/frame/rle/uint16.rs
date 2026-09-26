// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::iter::repeat_n;

use reifydb_value::value::{container::decimal_array::uint16_array, frame::data::FrameColumnData};

fn make(v: Vec<u128>) -> FrameColumnData {
	FrameColumnData::Uint16(uint16_array(v))
}

crate::rle_tests! {
	repeated: {
		let mut v = Vec::new();
		for val in [1_000_000_000_000u128, 2_000_000_000_000, 3_000_000_000_000, 4_000_000_000_000, 5_000_000_000_000] {
			v.extend(repeat_n(val, 100));
		}
		v
	},
	unique: (0..100).map(|i| i as u128 * 7 + 13).collect::<Vec<_>>(),
}
